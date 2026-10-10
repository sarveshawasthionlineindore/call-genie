import logging
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.config import settings
from app.database import engine, Base
from app.routers import (
    auth, agents, campaigns, calls, wallet,
    webhooks, demo, knowledge, onboarding, integrations
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("call_genie")

app = FastAPI(
    title="Call Genie API (by MyBot Genie)",
    description="Multi-tenant AI Voice Calling SaaS with LiveKit, Deepgram, Groq, Sarvam AI, Knowledge Base (RAG), Public Developer APIs & Webhooks.",
    version="1.2.0"
)

# Enable CORS for all external origins (websites, embeds, dashboard)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Register All API Routers
app.include_router(auth.router)
app.include_router(onboarding.router)
app.include_router(agents.router)
app.include_router(knowledge.router)
app.include_router(campaigns.router)
app.include_router(calls.router)
app.include_router(wallet.router)
app.include_router(webhooks.router)
app.include_router(demo.router)
app.include_router(integrations.router)


@app.on_event("startup")
async def on_startup():
    logger.info("Initializing Call Genie Database...")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    logger.info("Database tables initialized successfully with Integrations & API Keys.")


@app.get("/health")
async def health_check():
    return {
        "status": "healthy",
        "service": "Call Genie Backend",
        "app_name": settings.APP_NAME,
        "environment": settings.ENVIRONMENT
    }
