import asyncio
import os
import json
import time
import logging
import httpx
from dotenv import load_dotenv

from livekit.agents import (
    AutoSubscribe,
    JobContext,
    JobProcess,
    WorkerOptions,
    cli,
    llm,
)
from livekit.agents.pipeline import VoicePipelineAgent
from livekit.plugins import deepgram, openai, silero, cartesia

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("voice_worker")

API_BACKEND_URL = os.getenv("API_BACKEND_URL", "http://saas-api:8000")


async def generate_call_summary(transcript_text: str, groq_api_key: str) -> dict:
    """
    Calls Groq Llama to generate a concise summary and lead sentiment from transcript.
    """
    if not transcript_text or len(transcript_text.strip()) < 10:
        return {"summary": "Call ended without conversation", "sentiment": "NOT_INTERESTED"}

    url = "https://api.groq.com/openai/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {groq_api_key}",
        "Content-Type": "application/json"
    }

    prompt = (
        "You are an AI sales supervisor for Call Genie (by MyBot Genie).\n"
        "Analyze the following call transcript and output valid JSON only:\n"
        "{\n"
        '  "summary": "2-3 sentence clear summary of what customer wanted and outcome",\n'
        '  "sentiment": "INTERESTED" | "NOT_INTERESTED" | "CALLBACK_REQUESTED" | "NEUTRAL"\n'
        "}\n\n"
        f"Transcript:\n{transcript_text}"
    )

    payload = {
        "model": "llama-3.1-8b-instant",
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.2,
        "response_format": {"type": "json_object"}
    }

    try:
        async with httpx.AsyncClient(timeout=8.0) as client:
            resp = await client.post(url, headers=headers, json=payload)
            if resp.status_code == 200:
                data = resp.json()
                content = data["choices"][0]["message"]["content"]
                return json.loads(content)
    except Exception as e:
        logger.error(f"Error generating call summary: {str(e)}")

    return {"summary": "Conversation completed.", "sentiment": "NEUTRAL"}


async def notify_backend_call_ended(
    room_name: str,
    duration_seconds: int,
    transcript: str,
    summary: str,
    sentiment: str
):
    """
    Sends call completion event to Call Genie FastAPI backend.
    """
    webhook_url = f"{API_BACKEND_URL}/api/webhooks/call-ended"
    payload = {
        "room_name": room_name,
        "duration_seconds": duration_seconds,
        "transcript": transcript,
        "summary": summary,
        "sentiment": sentiment
    }
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(webhook_url, json=payload)
            logger.info(f"Webhook delivered to backend: {resp.status_code}")
    except Exception as e:
        logger.error(f"Failed to post call-ended webhook: {str(e)}")


async def entrypoint(ctx: JobContext):
    logger.info(f"Connecting to room: {ctx.room.name}")
    await ctx.connect(auto_subscribe=AutoSubscribe.AUDIO_ONLY)

    # 1. Parse Room Metadata to get Agent configuration
    room_meta = {}
    try:
        if ctx.room.metadata:
            room_meta = json.loads(ctx.room.metadata)
    except Exception:
        room_meta = {}

    system_prompt = room_meta.get(
        "system_prompt",
        "You are an AI assistant for Call Genie (by MyBot Genie). Speak warmly, concisely, and naturally in Hindi or Hinglish."
    )
    first_message = room_meta.get(
        "first_message",
        "Namaste! Main Call Genie AI se baat kar raha hoon. Kya main aapki koi madad kar sakta hoon?"
    )
    language = room_meta.get("language", "hi-IN")

    # 2. Setup Ultra-Low-Cost Pipeline:
    # Speech-to-Text: Deepgram Nova-2 (~₹0.36/min)
    stt_provider = deepgram.STT(
        api_key=os.getenv("DEEPGRAM_API_KEY"),
        model="nova-2-general",
        language=language if language.startswith("en") else "hi"
    )

    # Brain / LLM: Groq Llama 3.3 70B (~₹0.10/min, <150ms latency)
    llm_provider = openai.LLM(
        base_url="https://api.groq.com/openai/v1",
        api_key=os.getenv("GROQ_API_KEY"),
        model="llama-3.3-70b-versatile",
        temperature=0.7
    )

    # Voice / TTS: Cartesia Sonic (<150ms) or Sarvam AI
    cartesia_key = os.getenv("CARTESIA_API_KEY")
    tts_provider = cartesia.TTS(
        api_key=cartesia_key,
        model="sonic-multilingual"
    ) if cartesia_key else deepgram.TTS(api_key=os.getenv("DEEPGRAM_API_KEY"))

    # Voice Activity Detection (Silero VAD for instant barge-in interruption)
    vad_provider = silero.VAD.load()

    # 3. Assemble Voice Agent Pipeline
    agent = VoicePipelineAgent(
        vad=vad_provider,
        stt=stt_provider,
        llm=llm_provider,
        tts=tts_provider,
        chat_ctx=llm.ChatContext().append(
            role="system",
            text=system_prompt
        ),
        interrupt_speech_duration=0.4,
        min_endpointing_delay=0.5
    )

    agent.start(ctx.room)
    start_time = time.time()

    # Greet user with first message
    await agent.say(first_message, allow_interruptions=True)

    # Wait until participant leaves or call ends
    await ctx.job.wait_for_completion()

    # Call concluded
    duration_seconds = int(time.time() - start_time)
    logger.info(f"Call finished. Total duration: {duration_seconds}s")

    # Collect transcript
    transcript_lines = []
    for msg in agent.chat_ctx.messages:
        if msg.role in ["user", "assistant"]:
            transcript_lines.append(f"{msg.role.upper()}: {msg.content}")
    full_transcript = "\n".join(transcript_lines)

    # Generate summary & sentiment via Groq
    groq_key = os.getenv("GROQ_API_KEY", "")
    analysis = await generate_call_summary(full_transcript, groq_key)

    # Notify FastAPI backend for billing & WhatsApp automation
    await notify_backend_call_ended(
        room_name=ctx.room.name,
        duration_seconds=duration_seconds,
        transcript=full_transcript,
        summary=analysis.get("summary", ""),
        sentiment=analysis.get("sentiment", "NEUTRAL")
    )


if __name__ == "__main__":
    cli.run_app(
        WorkerOptions(
            entrypoint_fnc=entrypoint,
            ws_url=os.getenv("LIVEKIT_URL", "ws://localhost:7880"),
            api_key=os.getenv("LIVEKIT_API_KEY", "devkey"),
            api_secret=os.getenv("LIVEKIT_API_SECRET", "secret")
        )
    )
