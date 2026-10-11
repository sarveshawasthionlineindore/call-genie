import os
import sys
import uuid
import math
import secrets
import logging
import asyncio
import httpx
import json
import re
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any

from fastapi import (
    FastAPI, APIRouter, Depends, HTTPException, status,
    UploadFile, File, Form, BackgroundTasks, Request, Security, Response
)
from fastapi.responses import PlainTextResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import OAuth2PasswordBearer
from fastapi.security.api_key import APIKeyHeader
from pydantic import BaseModel, EmailStr
from pydantic_settings import BaseSettings
from jose import JWTError, jwt
from passlib.context import CryptContext
import pandas as pd

from sqlalchemy import (
    Column, String, Boolean, DateTime, ForeignKey, Text, Float, Integer, JSON, select, desc
)
from sqlalchemy.orm import declarative_base, relationship
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from livekit import api

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("call_genie")

# ==============================================================================
# 1. CONFIGURATION (MYBOT GENIE CALL GENIE CLOUD)
# ==============================================================================
class Settings(BaseSettings):
    APP_NAME: str = "Call Genie"
    ENVIRONMENT: str = "production"
    SECRET_KEY: str = os.getenv("SECRET_KEY", "call_genie_super_secret_jwt_key_2026")
    ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 1440
    DATABASE_URL: str = os.getenv("DATABASE_URL", "sqlite+aiosqlite:////tmp/call_genie_auth.db")

    # LiveKit Cloud
    LIVEKIT_URL: str = os.getenv("LIVEKIT_URL", "wss://call-genie-bjkq1kkd.livekit.cloud")
    LIVEKIT_API_KEY: str = os.getenv("LIVEKIT_API_KEY", "")
    LIVEKIT_API_SECRET: str = os.getenv("LIVEKIT_API_SECRET", "")

    # Exotel Indian Telephony
    EXOTEL_SIP_TRUNK_ID: str = os.getenv("EXOTEL_SIP_TRUNK_ID", "")
    EXOTEL_CALLER_NUMBER: str = os.getenv("EXOTEL_CALLER_NUMBER", "")

    # Indian AI Speech & LLM Pipeline
    DEEPGRAM_API_KEY: str = os.getenv("DEEPGRAM_API_KEY", "")
    GROQ_API_KEY: str = os.getenv("GROQ_API_KEY", "")
    SARVAM_API_KEY: str = os.getenv("SARVAM_API_KEY", "")

    # Meta WhatsApp Cloud API via WhatAPI Gateway
    WHATSAPP_API_URL: str = os.getenv("WHATSAPP_API_URL", "https://crmapi1.whatapi.in/api/meta")
    WHATSAPP_TOKEN: Optional[str] = os.getenv("WHATSAPP_TOKEN", None)
    WHATSAPP_PHONE_NUMBER_ID: Optional[str] = os.getenv("WHATSAPP_PHONE_NUMBER_ID", None)
    WHATSAPP_BUSINESS_ACCOUNT_ID: Optional[str] = os.getenv("WHATSAPP_BUSINESS_ACCOUNT_ID", None)
    WHATSAPP_TEMPLATE_NAME: str = os.getenv("WHATSAPP_TEMPLATE_NAME", "newmsg_4578000")
    WHATSAPP_TEMPLATE_LANG: str = os.getenv("WHATSAPP_TEMPLATE_LANG", "nl")
    WHATSAPP_BANNER_IMAGE_URL: str = os.getenv("WHATSAPP_BANNER_IMAGE_URL", "https://call.mybotgenie.in/assets/banner.png")

    DEFAULT_RETAIL_RATE_PER_MIN_INR: float = 5.00
    BILLING_PULSE_SECONDS: int = 60

    class Config:
        env_file = ".env"
        extra = "ignore"

settings = Settings()

# ==============================================================================
# 2. DATABASE & ASYNC ENGINE (WITH ZERO-FAILURE SQLITE RESILIENCE)
# ==============================================================================
db_url = settings.DATABASE_URL
if db_url.startswith("postgres://"):
    db_url = db_url.replace("postgres://", "postgresql+asyncpg://", 1)
elif db_url.startswith("postgresql://") and not db_url.startswith("postgresql+asyncpg://"):
    db_url = db_url.replace("postgresql://", "postgresql+asyncpg://", 1)

def create_engine_safe(url: str):
    if "sqlite" in url:
        return create_async_engine(url, echo=False)
    return create_async_engine(
        url,
        echo=False,
        pool_pre_ping=True,
        pool_size=10,
        max_overflow=20
    )

engine = create_engine_safe(db_url)
AsyncSessionLocal = async_sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autocommit=False,
    autoflush=False
)

Base = declarative_base()

async def get_db():
    async with AsyncSessionLocal() as session:
        try:
            yield session
        finally:
            await session.close()

def generate_uuid():
    return str(uuid.uuid4())

# ==============================================================================
# 3. MODELS
# ==============================================================================
class Tenant(Base):
    __tablename__ = "tenants"
    id = Column(String(36), primary_key=True, default=generate_uuid)
    name = Column(String(255), nullable=False)
    industry = Column(String(100), nullable=True)
    rate_per_min_inr = Column(Float, default=5.00)
    is_active = Column(Boolean, default=True)
    onboarding_completed = Column(Boolean, default=False)
    created_at = Column(DateTime, default=datetime.utcnow)

    users = relationship("User", back_populates="tenant", cascade="all, delete-orphan")
    wallet = relationship("Wallet", back_populates="tenant", uselist=False, cascade="all, delete-orphan")
    agents = relationship("Agent", back_populates="tenant", cascade="all, delete-orphan")
    campaigns = relationship("Campaign", back_populates="tenant", cascade="all, delete-orphan")
    call_logs = relationship("CallLog", back_populates="tenant", cascade="all, delete-orphan")

class User(Base):
    __tablename__ = "users"
    id = Column(String(36), primary_key=True, default=generate_uuid)
    tenant_id = Column(String(36), ForeignKey("tenants.id"), nullable=False)
    email = Column(String(255), unique=True, index=True, nullable=False)
    hashed_password = Column(String(255), nullable=False)
    full_name = Column(String(255), nullable=True)
    role = Column(String(50), default="tenant_admin")
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)

    tenant = relationship("Tenant", back_populates="users")

class Wallet(Base):
    __tablename__ = "wallets"
    id = Column(String(36), primary_key=True, default=generate_uuid)
    tenant_id = Column(String(36), ForeignKey("tenants.id"), unique=True, nullable=False)
    balance_inr = Column(Float, default=100.0)
    total_recharged_inr = Column(Float, default=100.0)
    total_spent_inr = Column(Float, default=0.0)
    currency = Column(String(10), default="INR")
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    tenant = relationship("Tenant", back_populates="wallet")
    transactions = relationship("WalletTransaction", back_populates="wallet", cascade="all, delete-orphan")

class WalletTransaction(Base):
    __tablename__ = "wallet_transactions"
    id = Column(String(36), primary_key=True, default=generate_uuid)
    wallet_id = Column(String(36), ForeignKey("wallets.id"), nullable=False)
    transaction_type = Column(String(50), nullable=False)
    amount_inr = Column(Float, nullable=False)
    balance_after_inr = Column(Float, nullable=False)
    reference = Column(String(255), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)

    wallet = relationship("Wallet", back_populates="transactions")

class Agent(Base):
    __tablename__ = "agents"
    id = Column(String(36), primary_key=True, default=generate_uuid)
    tenant_id = Column(String(36), ForeignKey("tenants.id"), nullable=False)
    name = Column(String(255), nullable=False)
    description = Column(Text, nullable=True)
    language = Column(String(20), default="hi-IN")
    stt_provider = Column(String(50), default="deepgram")
    stt_model = Column(String(50), default="nova-2-hi")
    llm_provider = Column(String(50), default="groq")
    llm_model = Column(String(50), default="llama-3.3-70b-versatile")
    tts_provider = Column(String(50), default="sarvam")
    tts_voice = Column(String(50), default="suman_hi_companion")
    system_prompt = Column(Text, nullable=False)
    greeting_message = Column(Text, nullable=False)
    call_direction = Column(String(20), default="both")
    max_duration_seconds = Column(Integer, default=600)
    interruption_threshold = Column(Float, default=0.5)
    post_call_whatsapp_enabled = Column(Boolean, default=True)
    whatsapp_template_name = Column(String(100), nullable=True)
    whatsapp_followup_message = Column(Text, nullable=True)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)

    tenant = relationship("Tenant", back_populates="agents")
    call_logs = relationship("CallLog", back_populates="agent")

class KnowledgeDocument(Base):
    __tablename__ = "knowledge_documents"
    id = Column(String(36), primary_key=True, default=generate_uuid)
    tenant_id = Column(String(36), ForeignKey("tenants.id"), nullable=False)
    title = Column(String(255), nullable=False)
    doc_type = Column(String(50), default="text")
    raw_content = Column(Text, nullable=False)
    cleaned_chunks = Column(JSON, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)

class Campaign(Base):
    __tablename__ = "campaigns"
    id = Column(String(36), primary_key=True, default=generate_uuid)
    tenant_id = Column(String(36), ForeignKey("tenants.id"), nullable=False)
    agent_id = Column(String(36), ForeignKey("agents.id"), nullable=False)
    name = Column(String(255), nullable=False)
    status = Column(String(50), default="draft")
    total_leads = Column(Integer, default=0)
    completed_calls = Column(Integer, default=0)
    failed_calls = Column(Integer, default=0)
    created_at = Column(DateTime, default=datetime.utcnow)

    tenant = relationship("Tenant", back_populates="campaigns")
    leads = relationship("Lead", back_populates="campaign", cascade="all, delete-orphan")

class Lead(Base):
    __tablename__ = "leads"
    id = Column(String(36), primary_key=True, default=generate_uuid)
    campaign_id = Column(String(36), ForeignKey("campaigns.id"), nullable=False)
    phone_number = Column(String(30), nullable=False)
    first_name = Column(String(100), nullable=True)
    custom_variables = Column(JSON, nullable=True)
    status = Column(String(50), default="pending")
    call_attempts = Column(Integer, default=0)
    created_at = Column(DateTime, default=datetime.utcnow)

    campaign = relationship("Campaign", back_populates="leads")

class CallLog(Base):
    __tablename__ = "call_logs"
    id = Column(String(36), primary_key=True, default=generate_uuid)
    tenant_id = Column(String(36), ForeignKey("tenants.id"), nullable=False)
    agent_id = Column(String(36), ForeignKey("agents.id"), nullable=True)
    lead_phone = Column(String(30), nullable=False)
    customer_name = Column(String(100), nullable=True)
    direction = Column(String(20), default="outbound")
    duration_seconds = Column(Integer, default=0)
    billed_minutes = Column(Integer, default=0)
    cost_inr = Column(Float, default=0.0)
    call_status = Column(String(50), default="completed")
    end_reason = Column(String(100), nullable=True)
    transcript = Column(JSON, nullable=True)
    summary = Column(Text, nullable=True)
    sentiment = Column(String(50), nullable=True)
    whatsapp_status = Column(String(50), default="NONE")
    created_at = Column(DateTime, default=datetime.utcnow)

    tenant = relationship("Tenant", back_populates="call_logs")
    agent = relationship("Agent", back_populates="call_logs")

class ApiKey(Base):
    __tablename__ = "api_keys"
    id = Column(String(36), primary_key=True, default=generate_uuid)
    tenant_id = Column(String(36), ForeignKey("tenants.id"), nullable=False)
    key = Column(String(128), unique=True, index=True, nullable=False)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)

class ClientWebhookEndpoint(Base):
    __tablename__ = "client_webhook_endpoints"
    id = Column(String(36), primary_key=True, default=generate_uuid)
    tenant_id = Column(String(36), ForeignKey("tenants.id"), nullable=False)
    target_url = Column(String(512), nullable=False)
    event_type = Column(String(100), default="call.completed")
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)

# ==============================================================================
# 4. SCHEMAS
# ==============================================================================
class UserRegister(BaseModel):
    email: EmailStr
    password: str
    company_name: str
    full_name: Optional[str] = None

class UserLogin(BaseModel):
    email: EmailStr
    password: str

class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    tenant_id: str
    email: str
    company_name: str

class AgentCreate(BaseModel):
    name: str
    description: Optional[str] = None
    language: Optional[str] = "hi-IN"
    tts_voice: Optional[str] = "suman_hi_companion"
    system_prompt: str
    greeting_message: str
    post_call_whatsapp_enabled: Optional[bool] = True
    whatsapp_template_name: Optional[str] = None
    whatsapp_followup_message: Optional[str] = None

class AgentOut(BaseModel):
    id: str
    name: str
    description: Optional[str]
    language: str
    tts_voice: str
    system_prompt: str
    greeting_message: str
    post_call_whatsapp_enabled: bool
    whatsapp_followup_message: Optional[str]
    is_active: bool
    created_at: datetime
    class Config: from_attributes = True

class WalletOut(BaseModel):
    balance_inr: float
    currency: str
    rate_per_min_inr: float

class WalletRechargeRequest(BaseModel):
    amount_inr: float
    payment_reference: Optional[str] = None

class CallLogOut(BaseModel):
    id: str
    lead_phone: str
    direction: str
    duration_seconds: int
    billed_minutes: int
    cost_inr: float
    call_status: str
    summary: Optional[str]
    sentiment: Optional[str]
    whatsapp_status: str
    created_at: datetime
    class Config: from_attributes = True

class SingleCallRequest(BaseModel):
    agent_id: str
    phone_number: str
    custom_variables: Optional[Dict[str, Any]] = None

# ==============================================================================
# 5. CORE SERVICES (WHATSAPP, LIVEKIT, BILLING)
# ==============================================================================
class WhatsAppService:
    @staticmethod
    def _clean_phone_number(phone: str) -> str:
        digits = "".join(filter(str.isdigit, phone or ""))
        if len(digits) == 10:
            return f"91{digits}"
        if digits.startswith("0") and len(digits) == 11:
            return f"91{digits[1:]}"
        return digits

    @staticmethod
    async def send_post_call_message(
        phone_number: str,
        customer_name: Optional[str] = None,
        custom_message: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Sends post-call WhatsApp follow-up via WhatAPI Meta Gateway with approved 9-variable template.
        """
        if not settings.WHATSAPP_TOKEN or not settings.WHATSAPP_PHONE_NUMBER_ID:
            logger.warning("WhatsApp credentials not set in environment.")
            return {"success": False, "error": "Missing WHATSAPP_TOKEN or WHATSAPP_PHONE_NUMBER_ID"}

        clean_number = WhatsAppService._clean_phone_number(phone_number)
        template_name = settings.WHATSAPP_TEMPLATE_NAME.strip().replace(" ", "_")
        template_lang = settings.WHATSAPP_TEMPLATE_LANG or "nl"
        banner_url = settings.WHATSAPP_BANNER_IMAGE_URL or "https://call.mybotgenie.in/assets/banner.png"
        
        # Display name logic: Discovered name or fallback to 'Sir'
        display_name = customer_name.strip() if customer_name and customer_name.strip().lower() not in ["ji", "sir", "customer", "user", "none", "null", ""] else "Sir"

        # Clean endpoint URL
        base_url = settings.WHATSAPP_API_URL.rstrip("/")
        if base_url.endswith("/messages"):
            endpoint_url = base_url
        else:
            endpoint_url = f"{base_url}/{settings.WHATSAPP_PHONE_NUMBER_ID}/messages"

        headers = {
            "Authorization": f"Bearer {settings.WHATSAPP_TOKEN}",
            "Content-Type": "application/json"
        }

        # Approved Template Components with Image Header + 9 Body Parameters
        payload_template = {
            "messaging_product": "whatsapp",
            "recipient_type": "individual",
            "to": clean_number,
            "type": "template",
            "template": {
                "name": template_name,
                "language": {"code": template_lang},
                "components": [
                    {
                        "type": "header",
                        "parameters": [
                            {
                                "type": "image",
                                "image": {"link": banner_url}
                            }
                        ]
                    },
                    {
                        "type": "body",
                        "parameters": [
                            {"type": "text", "text": display_name},
                            {"type": "text", "text": "Automated AI Calls — Handle customer calls 24/7."},
                            {"type": "text", "text": "Instant Lead Follow-ups — Connect with leads faster."},
                            {"type": "text", "text": "Human-Like Conversations — Engage customers naturally with AI."},
                            {"type": "text", "text": "More Conversions — Never miss an opportunity to follow up."},
                            {"type": "text", "text": "Save Time & Costs — Reduce manual calling efforts."},
                            {"type": "text", "text": "Smart Lead Management — Streamline your calling workflow."},
                            {"type": "text", "text": "Scale Your Business with intelligent AI-powered calling!"},
                            {"type": "text", "text": "Team MyBot Genie, Indore 💜"}
                        ]
                    }
                ]
            }
        }

        async with httpx.AsyncClient(timeout=15.0) as client:
            try:
                # 1. Try sending full approved template
                res = await client.post(endpoint_url, headers=headers, json=payload_template)
                if res.status_code in [200, 201]:
                    logger.info(f"WhatsApp template delivered successfully to {clean_number}")
                    return {"success": True, "mode": "template_9_params", "status_code": res.status_code, "response": res.json()}
                
                logger.warning(f"Template attempt failed ({res.status_code}): {res.text}. Trying single-variable fallback...")
                
                # 2. Fallback: Template with single variable (name only) in case template is 1-param version
                payload_single_var = {
                    "messaging_product": "whatsapp",
                    "recipient_type": "individual",
                    "to": clean_number,
                    "type": "template",
                    "template": {
                        "name": template_name,
                        "language": {"code": template_lang},
                        "components": [
                            {
                                "type": "header",
                                "parameters": [{"type": "image", "image": {"link": banner_url}}]
                            },
                            {
                                "type": "body",
                                "parameters": [{"type": "text", "text": display_name}]
                            }
                        ]
                    }
                }
                res_single = await client.post(endpoint_url, headers=headers, json=payload_single_var)
                if res_single.status_code in [200, 201]:
                    return {"success": True, "mode": "template_1_param", "status_code": res_single.status_code, "response": res_single.json()}
                
                # 3. Fallback: Plain text follow-up (works inside 24h window)
                text_msg = custom_message or (
                    f"👋 Hi {display_name}, thank you for your interest in MyBot Genie AI Calling Solution! 🤖\n\n"
                    f"📞 Automated AI Calls — Handle customer calls 24/7.\n"
                    f"⚡ Instant Lead Follow-ups — Connect with leads faster.\n"
                    f"🎙️ Human-Like Conversations — Engage customers naturally with AI.\n"
                    f"📈 More Conversions — Never miss an opportunity to follow up.\n"
                    f"⏱️ Save Time & Costs — Reduce manual calling efforts.\n"
                    f"🔄 Smart Lead Management — Streamline your calling workflow.\n\n"
                    f"🚀 Scale Your Business with intelligent AI-powered calling!\n"
                    f"Team MyBot Genie, Indore 💜"
                )
                payload_text = {
                    "messaging_product": "whatsapp",
                    "recipient_type": "individual",
                    "to": clean_number,
                    "type": "text",
                    "text": {"preview_url": True, "body": text_msg}
                }
                res_text = await client.post(endpoint_url, headers=headers, json=payload_text)
                return {
                    "success": res_text.status_code in [200, 201],
                    "mode": "text_fallback",
                    "status_code": res_text.status_code,
                    "response": res_text.json() if res_text.headers.get("content-type", "").startswith("application/json") else res_text.text,
                    "primary_template_error": res.text
                }
            except Exception as e:
                logger.error(f"WhatsApp dispatch exception: {str(e)}")
                return {"success": False, "error": str(e)}

class LiveKitService:
    @staticmethod
    def get_api_client():
        return api.LiveKitAPI(url=settings.LIVEKIT_URL, api_key=settings.LIVEKIT_API_KEY, api_secret=settings.LIVEKIT_API_SECRET)

    @staticmethod
    async def create_outbound_call(phone_number: str, agent_id: str, tenant_id: str, custom_metadata: Optional[Dict[str, Any]] = None) -> str:
        room_name = f"call_{tenant_id[:8]}_{uuid.uuid4().hex[:8]}"
        try:
            livekit_client = LiveKitService.get_api_client()
            await livekit_client.room.create_room(api.CreateRoomRequest(name=room_name, empty_timeout=300, max_participants=2))
            await livekit_client.aclose()
        except Exception as e:
            logger.warning(f"LiveKit room creation note: {str(e)}")
        return room_name

class BillingService:
    @staticmethod
    async def credit_wallet(db: AsyncSession, tenant_id: str, amount_inr: float, reference: Optional[str] = None) -> float:
        wallet = (await db.execute(select(Wallet).where(Wallet.tenant_id == tenant_id))).scalar_one_or_none()
        if not wallet:
            wallet = Wallet(tenant_id=tenant_id, balance_inr=0.0)
            db.add(wallet)
        wallet.balance_inr += amount_inr
        wallet.total_recharged_inr += amount_inr
        tx = WalletTransaction(wallet_id=wallet.id, transaction_type="CREDIT", amount_inr=amount_inr, balance_after_inr=wallet.balance_inr, reference=reference)
        db.add(tx)
        await db.commit()
        return wallet.balance_inr

# ==============================================================================
# 6. FASTAPI SETUP & AUTH
# ==============================================================================
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/auth/login")
api_key_header_scheme = APIKeyHeader(name="X-CallGenie-Key", auto_error=False)

def verify_password(plain, hashed): return pwd_context.verify(plain, hashed)
def get_password_hash(password): return pwd_context.hash(password)
def create_access_token(data: dict):
    to_encode = data.copy()
    to_encode.update({"exp": datetime.utcnow() + timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES)})
    return jwt.encode(to_encode, settings.SECRET_KEY, algorithm=settings.ALGORITHM)

async def get_current_user(token: str = Depends(oauth2_scheme), db: AsyncSession = Depends(get_db)) -> User:
    try:
        payload = jwt.decode(token, settings.SECRET_KEY, algorithms=[settings.ALGORITHM])
        user_id = payload.get("sub")
        if not user_id: raise HTTPException(status_code=401, detail="Invalid token")
    except JWTError: raise HTTPException(status_code=401, detail="Invalid credentials")
    user = (await db.execute(select(User).where(User.id == user_id))).scalar_one_or_none()
    if not user: raise HTTPException(status_code=401, detail="User not found")
    return user

app = FastAPI(
    title="Call Genie API (by MyBot Genie)",
    description="AI Voice Calling & WhatsApp Automation Platform with Groq, Sarvam AI Bulbul, LiveKit and Meta Cloud API.",
    version="1.3.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.on_event("startup")
async def on_startup():
    global engine, AsyncSessionLocal
    logger.info("Checking Call Genie Database...")
    try:
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        logger.info("Primary database connected successfully.")
    except Exception as e:
        logger.warning(f"Primary database connection failed: {e}. Activating zero-failure SQLite fallback in /tmp/call_genie_auth.db")
        sqlite_url = "sqlite+aiosqlite:////tmp/call_genie_auth.db"
        engine = create_async_engine(sqlite_url, echo=False)
        AsyncSessionLocal = async_sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        logger.info("SQLite fallback database initialized successfully.")

@app.get("/")
async def root(): return {"status": "online", "app": "Call Genie API", "powered_by": "MyBot Genie", "city": "Indore"}

@app.get("/health")
async def health(): return {"status": "healthy", "app": settings.APP_NAME}

# ==============================================================================
# 7. VOICE CONVERSATIONAL ENGINE (RIYA - ENERGETIC, CHEERFUL, 1-TURN DISCOVERY)
# ==============================================================================
class VoiceChatRequest(BaseModel):
    message: str
    client_name: Optional[str] = "ji"
    business_name: Optional[str] = "MyBot Genie"
    business_type: Optional[str] = "AI Calling Solution"
    conversation_history: Optional[List[Dict[str, str]]] = None

def discover_caller_name(user_text: str) -> Optional[str]:
    """
    Detects if the user introduced themselves, e.g. 'मेरा नाम सर्वेश है', 'I am Sarvesh', 'सर्वेश बात कर रहा हूँ'
    """
    text = (user_text or "").strip()
    match = re.search(r'(?:मेरा नाम|mera naam|naam hai|my name is|i am|this is|मैं)\s+([A-Za-z\u0900-\u097F]+)', text, re.IGNORECASE)
    if match:
        name = match.group(1).strip()
        if name.lower() not in ["call", "genie", "bot", "ai", "ji", "sir", "kya", "hai", "haan"]:
            return name
    return None

def generate_dynamic_hindi_reply(user_msg: str, client_name: str, biz_name: str, biz_type: str) -> str:
    msg = (user_msg or "").lower().strip()
    c_name = client_name.strip() if client_name and client_name.strip().lower() not in ["ji", ""] else "सर"
    b_name = biz_name.strip() if biz_name and biz_name.strip().lower() not in ["business", "aapke business", ""] else "मायबॉट जीनी"
    
    if any(w in msg for w in ["नमस्ते", "हैलो", "हेलो", "हाय", "namaste", "hello", "hi", "hey", "kaise ho", "kya haal"]):
        return f"अरे नमस्ते {c_name} जी! मैं {b_name} से रिया बोल रही हूँ। बताइये, आज मैं आपकी क्या सहायता कर सकती हूँ?"
    if any(w in msg for w in ["कौन हो", "kaun ho", "kya naam", "naam kya", "kya ho", "who are you", "tum kaun"]):
        return f"जी! मैं रिया हूँ, {b_name} की एआई कॉलिंग कंसलटेंट! मैं चौबीस घंटे आपके ग्राहकों से बिल्कुल इंसानों की तरह बात कर सकती हूँ।"
    if any(w in msg for w in ["kya kar sakte", "kya kaam", "kaise kaam", "kya service", "features", "kya karte", "madad"]):
        return f"अरे वाह! मैं आपके लीड्स को तुरंत कॉल करके फॉलो-अप कर सकती हूँ, अपॉइंटमेंट बुक कर सकती हूँ और सीधे व्हाट्सएप पर मैसेज भी भेज सकती हूँ।"
    if any(w in msg for w in ["kitna paisa", "kitna kharcha", "rate", "cost", "price", "kitna lagega", "charge", "fees"]):
        return "कॉल जीनी का रेट बहुत ही किफायती है, सिर्फ पाँच रुपये प्रति मिनट! कोई सेटअप चार्ज नहीं है, आप तुरंत शुरू कर सकते हैं।"
    if any(w in msg for w in ["demo", "test", "aawaz", "bol ke", "live", "suno", "check"]):
        return "हाँ जी बिल्कुल! आप इस समय मेरा ही लाइव डेमो सुन रहे हैं। क्या आप अपने बिज़नेस में एआई कॉलिंग शुरू करना चाहेंगे?"
    if any(w in msg for w in ["dhanyawad", "shukriya", "bye", "alvida", "theek hai", "ok", "thank"]):
        return f"ओह, थैंक यू सो मच {c_name} जी! आपसे बात करके बहुत अच्छा लगा। मैं व्हाट्सएप पर सारी डिटेल्स भेज रही हूँ!"
    return f"जी {c_name} जी, मैंने समझ लिया! आपके बिज़नेस के लिए यह कॉलिंग ऑटोमेशन बहुत जबरदस्त काम करेगा। क्या मैं इस पर व्हाट्सएप डिटेल्स भेज दूँ?"

@app.post("/api/voice/chat")
async def voice_chat_endpoint(chat_in: VoiceChatRequest):
    """
    Energetic, cheerful telecaller 'Riya' with 1-turn discovery and Sarvam Bulbul native voice.
    """
    discovered = discover_caller_name(chat_in.message)
    active_name = discovered or chat_in.client_name or "सर"

    system_prompt = (
        f"You are Riya, a cheerful, smiling, energetic female telecaller from Indore representing '{chat_in.business_name}' by MyBot Genie. "
        f"You are having a live 1-on-1 phone conversation with {active_name}. "
        "CRITICAL RULES: "
        "1. Always use strictly feminine Hindi grammar (बोल रही हूँ, करती हूँ, बता सकती हूँ, मदद करूँगी). Never use masculine forms. "
        "2. Speak in short, snappy, natural 1-turn telecaller responses (strictly 12 to 20 words). Never give long lectures. "
        "3. Include natural conversational exclamations like 'अरे वाह!', 'हाँ जी बिल्कुल!', 'ओह थैंक यू!', 'अरे ग्रेट!'. "
        "4. Write ONLY in Devanagari Hindi script (हिंदी). Never write English alphabets or romanized words. "
        "5. Ask 1 engaging discovery question at a time to keep the customer engaged."
    )

    messages = [{"role": "system", "content": system_prompt}]
    if chat_in.conversation_history:
        for msg in chat_in.conversation_history[-6:]:
            messages.append({"role": msg.get("role", "user"), "content": msg.get("content", "")})
    messages.append({"role": "user", "content": chat_in.message})

    ai_reply = None
    if settings.GROQ_API_KEY:
        try:
            async with httpx.AsyncClient(timeout=6.0) as client:
                res = await client.post(
                    "https://api.groq.com/openai/v1/chat/completions",
                    headers={"Authorization": f"Bearer {settings.GROQ_API_KEY}", "Content-Type": "application/json"},
                    json={
                        "model": "llama-3.3-70b-versatile",
                        "messages": messages,
                        "temperature": 0.6,
                        "max_tokens": 80
                    }
                )
                if res.status_code == 200:
                    ai_reply = res.json()["choices"][0]["message"]["content"].strip()
        except Exception as e:
            logger.warning(f"Groq error: {str(e)}")

    if not ai_reply:
        ai_reply = generate_dynamic_hindi_reply(
            chat_in.message,
            active_name,
            chat_in.business_name or "MyBot Genie",
            chat_in.business_type or "AI Calling Solution"
        )

    clean_speech_text = ai_reply.replace("*", "").replace("#", "").replace("_", "").strip()

    audio_base64 = None
    if settings.SARVAM_API_KEY:
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                # Primary: Sarvam Bulbul v4-flash / v2 / v1
                payload_sarvam = {
                    "inputs": [clean_speech_text],
                    "target_language_code": "hi-IN",
                    "speaker": "suman_hi_companion",
                    "model": "bulbul:v4-flash",
                    "pace": 1.15,
                    "speech_sample_rate": 16000,
                    "enable_preprocessing": True
                }
                sarvam_res = await client.post(
                    "https://api.sarvam.ai/text-to-speech",
                    headers={"api-subscription-key": settings.SARVAM_API_KEY, "Content-Type": "application/json"},
                    json=payload_sarvam
                )
                if sarvam_res.status_code == 200:
                    audio_base64 = sarvam_res.json().get("audios", [None])[0]
                else:
                    # Fallback to Bulbul v1 Meera
                    payload_sarvam["model"] = "bulbul:v1"
                    payload_sarvam["speaker"] = "meera"
                    fb_res = await client.post(
                        "https://api.sarvam.ai/text-to-speech",
                        headers={"api-subscription-key": settings.SARVAM_API_KEY, "Content-Type": "application/json"},
                        json=payload_sarvam
                    )
                    if fb_res.status_code == 200:
                        audio_base64 = fb_res.json().get("audios", [None])[0]
        except Exception as e:
            logger.warning(f"Sarvam TTS error: {str(e)}")

    return {
        "reply": ai_reply,
        "discovered_name": discovered,
        "active_name": active_name,
        "audio_base64": audio_base64
    }

# ==============================================================================
# 8. AUDIO SAMPLES & DIAGNOSTIC APIS
# ==============================================================================
SAMPLE_SCRIPTS = {
    "automotive": {
        "text": "नमस्ते! ऑटोकेयर वर्कशॉप इंदौर से रोहन बात कर रहा हूँ। आपकी हुंडई क्रेटा की सर्विस और वाशिंग पूरी हो चुकी है। जॉब कार्ड नंबर 4092 का कुल बिल 4,850 रुपये है। क्या मैं आपके इसी नंबर पर पिकअप गेटपास और ऑनलाइन बिल व्हाट्सएप कर दूँ?",
        "speaker": "shubh_enhi_ads",
        "fallback_speaker": "arvind"
    },
    "clinic": {
        "text": "हेलो! केयर डेंटल क्लिनिक से रिया बोल रही हूँ। डॉक्टर अमित के पास आज शाम 6:15 का स्लॉट खाली है। क्या मैं आपका अपॉइंटमेंट टोकन कन्फर्म कर दूँ?",
        "speaker": "suman_hi_companion",
        "fallback_speaker": "meera"
    },
    "realestate": {
        "text": "नमस्ते! मैं एपेक्स रियल्टी इंदौर से रिया बोल रही हूँ। आपने सुपर कॉरिडोर पर प्लॉट्स के लिए इन्क्वायरी की थी। हमारे पास 3,200 रुपये प्रति वर्ग फीट पर प्राइम प्लॉट्स उपलब्ध हैं। क्या मैं इस रविवार आपके लिए एक साइट विजिट शेड्यूल कर दूँ?",
        "speaker": "suman_hi_companion",
        "fallback_speaker": "meera"
    },
    "finance": {
        "text": "हेलो! मैं फिनकॉर्प लोन सर्विसेज से आर्यन बात कर रहा हूँ। आपके 4,500 रुपये की ईएमआई की ड्यू डेट कल है। क्या मैं आपको तुरंत सिक्योर यूपीआई पेमेंट लिंक व्हाट्सएप पर भेज दूँ?",
        "speaker": "shubh_enhi_ads",
        "fallback_speaker": "arvind"
    }
}

@app.get("/api/voice/sample-audio/{sample_id}")
async def get_sample_audio(sample_id: str):
    """Generates and serves real Sarvam Bulbul audio with distinct Indian Male and Female personas."""
    sample_key = sample_id.lower().strip()
    profile = SAMPLE_SCRIPTS.get(sample_key, SAMPLE_SCRIPTS["automotive" if "auto" in sample_key else "realestate"])
    
    audio_base64 = None
    if settings.SARVAM_API_KEY:
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                res = await client.post(
                    "https://api.sarvam.ai/text-to-speech",
                    headers={"api-subscription-key": settings.SARVAM_API_KEY, "Content-Type": "application/json"},
                    json={
                        "inputs": [profile["text"]],
                        "target_language_code": "hi-IN",
                        "speaker": profile["speaker"],
                        "model": "bulbul:v4-flash",
                        "pace": 1.15,
                        "speech_sample_rate": 16000,
                        "enable_preprocessing": True
                    }
                )
                if res.status_code == 200:
                    audio_base64 = res.json().get("audios", [None])[0]
                else:
                    res_fb = await client.post(
                        "https://api.sarvam.ai/text-to-speech",
                        headers={"api-subscription-key": settings.SARVAM_API_KEY, "Content-Type": "application/json"},
                        json={
                            "inputs": [profile["text"]],
                            "target_language_code": "hi-IN",
                            "speaker": profile["fallback_speaker"],
                            "model": "bulbul:v1",
                            "pace": 1.15,
                            "speech_sample_rate": 16000,
                            "enable_preprocessing": True
                        }
                    )
                    if res_fb.status_code == 200:
                        audio_base64 = res_fb.json().get("audios", [None])[0]
        except Exception as e:
            logger.warning(f"Sarvam sample audio error: {str(e)}")
            
    return {
        "sample_id": sample_id,
        "audio_base64": audio_base64,
        "text": profile["text"],
        "speaker": profile["speaker"],
        "status": "ready"
    }

@app.get("/api/voice/status")
async def voice_status():
    return {
        "groq": bool(settings.GROQ_API_KEY),
        "sarvam": bool(settings.SARVAM_API_KEY),
        "livekit": bool(settings.LIVEKIT_API_KEY),
        "whatsapp": bool(settings.WHATSAPP_TOKEN and settings.WHATSAPP_PHONE_NUMBER_ID),
        "template": settings.WHATSAPP_TEMPLATE_NAME,
        "lang": settings.WHATSAPP_TEMPLATE_LANG
    }

# ==============================================================================
# 9. 1-CLICK WHATSAPP LIVE TESTING ENDPOINT
# ==============================================================================
@app.get("/api/whatsapp/test/{phone_number}")
@app.post("/api/whatsapp/test/{phone_number}")
async def test_whatsapp_delivery(phone_number: str, name: Optional[str] = "Sarvesh"):
    """1-Click browser test endpoint for verifying Meta WhatsApp Cloud API / WhatAPI proxy."""
    result = await WhatsAppService.send_post_call_message(
        phone_number=phone_number,
        customer_name=name
    )
    return {
        "requested_phone": phone_number,
        "cleaned_phone": WhatsAppService._clean_phone_number(phone_number),
        "test_name": name,
        "result": result,
        "config": {
            "gateway": settings.WHATSAPP_API_URL,
            "phone_number_id": settings.WHATSAPP_PHONE_NUMBER_ID,
            "template": settings.WHATSAPP_TEMPLATE_NAME,
            "lang": settings.WHATSAPP_TEMPLATE_LANG,
            "banner_url": settings.WHATSAPP_BANNER_IMAGE_URL
        }
    }

# ==============================================================================
# 10. AUTH, AGENTS, WALLET & DEMO APIS
# ==============================================================================
@app.post("/api/auth/register", response_model=TokenResponse)
async def register(user_in: UserRegister, db: AsyncSession = Depends(get_db)):
    if (await db.execute(select(User).where(User.email == user_in.email))).scalar_one_or_none():
        raise HTTPException(status_code=400, detail="Email already registered")
    tenant = Tenant(name=user_in.company_name)
    db.add(tenant); await db.flush()
    new_user = User(tenant_id=tenant.id, email=user_in.email, hashed_password=get_password_hash(user_in.password), full_name=user_in.full_name)
    db.add(new_user); db.add(Wallet(tenant_id=tenant.id, balance_inr=100.0))
    await db.commit(); await db.refresh(new_user)
    return {"access_token": create_access_token({"sub": new_user.id, "tenant_id": tenant.id}), "token_type": "bearer", "tenant_id": tenant.id, "email": new_user.email, "company_name": tenant.name}

@app.post("/api/auth/login", response_model=TokenResponse)
async def login(user_in: UserLogin, db: AsyncSession = Depends(get_db)):
    user = (await db.execute(select(User).where(User.email == user_in.email))).scalar_one_or_none()
    if not user or not verify_password(user_in.password, user.hashed_password):
        raise HTTPException(status_code=400, detail="Invalid credentials")
    tenant = (await db.execute(select(Tenant).where(Tenant.id == user.tenant_id))).scalar_one_or_none()
    return {"access_token": create_access_token({"sub": user.id, "tenant_id": user.tenant_id}), "token_type": "bearer", "tenant_id": user.tenant_id, "email": user.email, "company_name": tenant.name if tenant else "Client"}

@app.post("/api/agents", response_model=AgentOut)
async def create_agent(agent_in: AgentCreate, user: User = Depends(get_current_user), db: AsyncSession = Depends(get_db)):
    agent = Agent(tenant_id=user.tenant_id, **agent_in.model_dump())
    db.add(agent); await db.commit(); await db.refresh(agent); return agent

@app.get("/api/agents", response_model=List[AgentOut])
async def list_agents(user: User = Depends(get_current_user), db: AsyncSession = Depends(get_db)):
    return (await db.execute(select(Agent).where(Agent.tenant_id == user.tenant_id).order_by(desc(Agent.created_at)))).scalars().all()

@app.get("/api/wallet", response_model=WalletOut)
async def get_wallet(user: User = Depends(get_current_user), db: AsyncSession = Depends(get_db)):
    wallet = (await db.execute(select(Wallet).where(Wallet.tenant_id == user.tenant_id))).scalar_one_or_none()
    tenant = (await db.execute(select(Tenant).where(Tenant.id == user.tenant_id))).scalar_one_or_none()
    return {"balance_inr": wallet.balance_inr if wallet else 0.0, "currency": "INR", "rate_per_min_inr": tenant.rate_per_min_inr if tenant else 5.0}

@app.post("/api/wallet/recharge")
async def recharge_wallet(req: WalletRechargeRequest, user: User = Depends(get_current_user), db: AsyncSession = Depends(get_db)):
    bal = await BillingService.credit_wallet(db, user.tenant_id, req.amount_inr, req.payment_reference)
    return {"new_balance_inr": bal}

@app.get("/api/calls", response_model=List[CallLogOut])
async def list_calls(user: User = Depends(get_current_user), db: AsyncSession = Depends(get_db)):
    return (await db.execute(select(CallLog).where(CallLog.tenant_id == user.tenant_id).order_by(desc(CallLog.created_at)).limit(50))).scalars().all()

class PersonalizedDemoRequest(BaseModel):
    client_name: str
    business_name: str
    business_type: str
    phone_number: str
    language: Optional[str] = "hi-IN"

@app.post("/api/demo/personalized-call")
async def personalized_demo(demo_in: PersonalizedDemoRequest, bg: BackgroundTasks):
    clean_phone = demo_in.phone_number.strip().replace(" ", "").replace("-", "")
    bg.add_task(LiveKitService.create_outbound_call, clean_phone, "demo", "demo_tenant", {"system_prompt": "Speak warm Hindi."})
    return {"status": "initiated", "message": f"Demo call queued for {clean_phone}"}

@app.post("/api/campaigns/single-call")
async def place_single_call(call_req: SingleCallRequest, user: User = Depends(get_current_user), bg: BackgroundTasks = None, db: AsyncSession = Depends(get_db)):
    clean_phone = call_req.phone_number.strip().replace(" ", "").replace("-", "")
    room = await LiveKitService.create_outbound_call(clean_phone, call_req.agent_id, user.tenant_id, call_req.custom_variables)
    return {"room_name": room, "status": "initiated"}

@app.post("/api/integrations/api-keys")
async def make_key(user: User = Depends(get_current_user), db: AsyncSession = Depends(get_db)):
    raw = f"cg_live_{secrets.token_urlsafe(32)}"
    key_obj = ApiKey(tenant_id=user.tenant_id, key=raw)
    db.add(key_obj); await db.commit(); await db.refresh(key_obj)
    return {"key": raw}
