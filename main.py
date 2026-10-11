import os
import sys
import uuid
import math
import secrets
import logging
import asyncio
import httpx
import json
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any

from fastapi import (
    FastAPI, APIRouter, Depends, HTTPException, status,
    UploadFile, File, Form, BackgroundTasks, Request, Security, Response
)
from fastapi.responses import PlainTextResponse
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

# 1. CONFIGURATION
class Settings(BaseSettings):
    APP_NAME: str = "Call Genie"
    ENVIRONMENT: str = "production"
    SECRET_KEY: str = "call_genie_super_secret_jwt_key_2026"
    ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 1440
    DATABASE_URL: str = "postgresql+asyncpg://postgres:postgres@localhost:5432/call_genie"

    LIVEKIT_URL: str = "wss://livekit.example.com"
    LIVEKIT_API_KEY: str = "devkey"
    LIVEKIT_API_SECRET: str = "secret"
    SIP_TRUNK_ID: str = ""
    SIP_CALLER_NUMBER: str = "+17372508034"

    DEEPGRAM_API_KEY: str = ""
    GROQ_API_KEY: str = ""
    SARVAM_API_KEY: str = ""
    CARTESIA_API_KEY: str = ""

    WHATSAPP_TOKEN: Optional[str] = None
    WHATSAPP_PHONE_NUMBER_ID: Optional[str] = None
    WHATSAPP_BUSINESS_ACCOUNT_ID: Optional[str] = None

    DEFAULT_RETAIL_RATE_PER_MIN_INR: float = 5.00
    BILLING_PULSE_SECONDS: int = 60

    class Config:
        env_file = ".env"
        extra = "ignore"

settings = Settings()

# 2. DATABASE & ASYNC ENGINE
db_url = settings.DATABASE_URL
if db_url.startswith("postgres://"):
    db_url = db_url.replace("postgres://", "postgresql+asyncpg://", 1)
elif db_url.startswith("postgresql://") and not db_url.startswith("postgresql+asyncpg://"):
    db_url = db_url.replace("postgresql://", "postgresql+asyncpg://", 1)

engine = create_async_engine(
    db_url,
    echo=False,
    pool_pre_ping=True,
    pool_size=10,
    max_overflow=20
)

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

# 3. MODELS
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
    api_keys = relationship("ApiKey", back_populates="tenant", cascade="all, delete-orphan")
    webhook_endpoints = relationship("ClientWebhookEndpoint", back_populates="tenant", cascade="all, delete-orphan")

class User(Base):
    __tablename__ = "users"
    id = Column(String(36), primary_key=True, default=generate_uuid)
    tenant_id = Column(String(36), ForeignKey("tenants.id", ondelete="CASCADE"), nullable=False)
    email = Column(String(255), unique=True, index=True, nullable=False)
    hashed_password = Column(String(255), nullable=False)
    full_name = Column(String(255), nullable=True)
    phone_number = Column(String(50), nullable=True)
    role = Column(String(50), default="admin")
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    tenant = relationship("Tenant", back_populates="users")

class ApiKey(Base):
    __tablename__ = "api_keys"
    id = Column(String(36), primary_key=True, default=generate_uuid)
    tenant_id = Column(String(36), ForeignKey("tenants.id", ondelete="CASCADE"), nullable=False, index=True)
    name = Column(String(100), nullable=False, default="Default API Key")
    key = Column(String(128), unique=True, index=True, nullable=False)
    is_active = Column(Boolean, default=True)
    last_used_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    tenant = relationship("Tenant", back_populates="api_keys")

class ClientWebhookEndpoint(Base):
    __tablename__ = "client_webhook_endpoints"
    id = Column(String(36), primary_key=True, default=generate_uuid)
    tenant_id = Column(String(36), ForeignKey("tenants.id", ondelete="CASCADE"), nullable=False, index=True)
    target_url = Column(String(512), nullable=False)
    description = Column(String(255), nullable=True)
    secret = Column(String(64), nullable=True)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    tenant = relationship("Tenant", back_populates="webhook_endpoints")

class Wallet(Base):
    __tablename__ = "wallets"
    id = Column(String(36), primary_key=True, default=generate_uuid)
    tenant_id = Column(String(36), ForeignKey("tenants.id", ondelete="CASCADE"), unique=True, nullable=False)
    balance_inr = Column(Float, default=100.00)
    currency = Column(String(10), default="INR")
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    tenant = relationship("Tenant", back_populates="wallet")
    transactions = relationship("WalletTransaction", back_populates="wallet", cascade="all, delete-orphan")

class WalletTransaction(Base):
    __tablename__ = "wallet_transactions"
    id = Column(String(36), primary_key=True, default=generate_uuid)
    wallet_id = Column(String(36), ForeignKey("wallets.id", ondelete="CASCADE"), nullable=False)
    tenant_id = Column(String(36), nullable=False, index=True)
    amount_inr = Column(Float, nullable=False)
    type = Column(String(20), nullable=False)
    description = Column(String(255), nullable=True)
    reference_id = Column(String(100), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    wallet = relationship("Wallet", back_populates="transactions")

class Agent(Base):
    __tablename__ = "agents"
    id = Column(String(36), primary_key=True, default=generate_uuid)
    tenant_id = Column(String(36), ForeignKey("tenants.id", ondelete="CASCADE"), nullable=False, index=True)
    name = Column(String(255), nullable=False)
    system_prompt = Column(Text, nullable=False)
    first_message = Column(Text, nullable=False)
    voice_provider = Column(String(50), default="sarvam")
    voice_id = Column(String(100), default="bulbul:v1")
    language = Column(String(50), default="hi-IN")
    temperature = Column(Float, default=0.7)
    inbound_phone_number = Column(String(50), nullable=True)
    post_call_whatsapp_enabled = Column(Boolean, default=True)
    whatsapp_template_name = Column(String(100), nullable=True)
    whatsapp_followup_message = Column(Text, nullable=True)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    tenant = relationship("Tenant", back_populates="agents")
    campaigns = relationship("Campaign", back_populates="agent")
    call_logs = relationship("CallLog", back_populates="agent")
    knowledge_docs = relationship("KnowledgeDocument", back_populates="agent", cascade="all, delete-orphan")

class KnowledgeDocument(Base):
    __tablename__ = "knowledge_documents"
    id = Column(String(36), primary_key=True, default=generate_uuid)
    tenant_id = Column(String(36), ForeignKey("tenants.id", ondelete="CASCADE"), nullable=False, index=True)
    agent_id = Column(String(36), ForeignKey("agents.id", ondelete="CASCADE"), nullable=False, index=True)
    title = Column(String(255), nullable=False)
    doc_type = Column(String(50), default="TEXT")
    raw_content = Column(Text, nullable=False)
    structured_qa = Column(JSON, default=list)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    agent = relationship("Agent", back_populates="knowledge_docs")

class Campaign(Base):
    __tablename__ = "campaigns"
    id = Column(String(36), primary_key=True, default=generate_uuid)
    tenant_id = Column(String(36), ForeignKey("tenants.id", ondelete="CASCADE"), nullable=False, index=True)
    agent_id = Column(String(36), ForeignKey("agents.id", ondelete="CASCADE"), nullable=False)
    name = Column(String(255), nullable=False)
    status = Column(String(50), default="DRAFT")
    total_contacts = Column(Integer, default=0)
    completed_contacts = Column(Integer, default=0)
    successful_calls = Column(Integer, default=0)
    concurrency_limit = Column(Integer, default=1)
    created_at = Column(DateTime, default=datetime.utcnow)
    tenant = relationship("Tenant", back_populates="campaigns")
    agent = relationship("Agent", back_populates="campaigns")
    contacts = relationship("Contact", back_populates="campaign", cascade="all, delete-orphan")

class Contact(Base):
    __tablename__ = "contacts"
    id = Column(String(36), primary_key=True, default=generate_uuid)
    campaign_id = Column(String(36), ForeignKey("campaigns.id", ondelete="CASCADE"), nullable=False, index=True)
    phone_number = Column(String(50), nullable=False)
    name = Column(String(255), nullable=True)
    custom_data = Column(JSON, default=dict)
    status = Column(String(50), default="PENDING")
    call_id = Column(String(100), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    campaign = relationship("Campaign", back_populates="contacts")

class CallLog(Base):
    __tablename__ = "call_logs"
    id = Column(String(36), primary_key=True, default=generate_uuid)
    tenant_id = Column(String(36), ForeignKey("tenants.id", ondelete="CASCADE"), nullable=False, index=True)
    agent_id = Column(String(36), ForeignKey("agents.id", ondelete="SET NULL"), nullable=True)
    campaign_id = Column(String(36), ForeignKey("campaigns.id", ondelete="SET NULL"), nullable=True)
    contact_id = Column(String(36), ForeignKey("contacts.id", ondelete="SET NULL"), nullable=True)
    room_name = Column(String(255), unique=True, index=True, nullable=False)
    caller_number = Column(String(50), nullable=True)
    receiver_number = Column(String(50), nullable=True)
    direction = Column(String(20), default="OUTBOUND")
    duration_seconds = Column(Integer, default=0)
    billable_minutes = Column(Float, default=0.0)
    cost_incurred_inr = Column(Float, default=0.0)
    revenue_charged_inr = Column(Float, default=0.0)
    status = Column(String(50), default="INITIATED")
    recording_url = Column(Text, nullable=True)
    transcript = Column(Text, nullable=True)
    summary = Column(Text, nullable=True)
    sentiment = Column(String(50), nullable=True)
    whatsapp_status = Column(String(50), default="NONE")
    created_at = Column(DateTime, default=datetime.utcnow)
    tenant = relationship("Tenant", back_populates="call_logs")
    agent = relationship("Agent", back_populates="call_logs")

# 4. SCHEMAS
class UserRegister(BaseModel):
    email: str
    password: str
    full_name: str
    company_name: str

class UserLogin(BaseModel):
    email: str
    password: str

class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    tenant_id: str
    email: str
    company_name: str

class AgentCreate(BaseModel):
    name: str
    system_prompt: str
    first_message: str
    voice_provider: Optional[str] = "sarvam"
    voice_id: Optional[str] = "bulbul:v1"
    language: Optional[str] = "hi-IN"
    temperature: Optional[float] = 0.7
    inbound_phone_number: Optional[str] = None
    post_call_whatsapp_enabled: Optional[bool] = True
    whatsapp_template_name: Optional[str] = None
    whatsapp_followup_message: Optional[str] = None

class AgentOut(BaseModel):
    id: str
    name: str
    system_prompt: str
    first_message: str
    voice_provider: str
    voice_id: str
    language: str
    temperature: float
    inbound_phone_number: Optional[str]
    post_call_whatsapp_enabled: bool
    whatsapp_followup_message: Optional[str]
    is_active: bool
    created_at: datetime
    class Config:
        from_attributes = True

class SingleCallRequest(BaseModel):
    agent_id: str
    phone_number: str
    customer_name: Optional[str] = None
    custom_variables: Optional[Dict[str, Any]] = None

class WalletOut(BaseModel):
    balance_inr: float
    currency: str
    rate_per_min_inr: float

class WalletRechargeRequest(BaseModel):
    amount_inr: float
    payment_reference: str

class CallLogOut(BaseModel):
    id: str
    room_name: str
    caller_number: Optional[str]
    receiver_number: Optional[str]
    direction: str
    duration_seconds: int
    billable_minutes: float
    revenue_charged_inr: float
    status: str
    recording_url: Optional[str]
    transcript: Optional[str]
    summary: Optional[str]
    sentiment: Optional[str]
    whatsapp_status: str
    created_at: datetime
    class Config:
        from_attributes = True

# 5. CORE SERVICES
class BillingService:
    @staticmethod
    def calculate_billable_units(duration_seconds: int, pulse_seconds: int = 60) -> float:
        if duration_seconds <= 0: return 0.0
        pulses = math.ceil(duration_seconds / pulse_seconds)
        return (pulses * pulse_seconds) / 60.0

    @staticmethod
    async def check_wallet_balance(db: AsyncSession, tenant_id: str, min_required_inr: float = 5.0) -> bool:
        stmt = select(Wallet).where(Wallet.tenant_id == tenant_id)
        wallet = (await db.execute(stmt)).scalar_one_or_none()
        return bool(wallet and wallet.balance_inr >= min_required_inr)

    @staticmethod
    async def process_call_billing(db: AsyncSession, tenant_id: str, duration_seconds: int, call_log_id: str) -> dict:
        stmt = select(Tenant).where(Tenant.id == tenant_id)
        tenant = (await db.execute(stmt)).scalar_one_or_none()
        rate_per_min = tenant.rate_per_min_inr if tenant else settings.DEFAULT_RETAIL_RATE_PER_MIN_INR
        billable_minutes = BillingService.calculate_billable_units(duration_seconds, settings.BILLING_PULSE_SECONDS)
        revenue_charged = round(billable_minutes * rate_per_min, 2)
        internal_cogs = round(billable_minutes * 2.10, 2)

        if revenue_charged > 0:
            wallet = (await db.execute(select(Wallet).where(Wallet.tenant_id == tenant_id))).scalar_one_or_none()
            if wallet:
                wallet.balance_inr = round(wallet.balance_inr - revenue_charged, 2)
                db.add(WalletTransaction(wallet_id=wallet.id, tenant_id=tenant_id, amount_inr=revenue_charged, type="DEBIT", description=f"Call {duration_seconds}s", reference_id=call_log_id))
                await db.commit()

        return {"billable_minutes": billable_minutes, "revenue_charged_inr": revenue_charged, "cost_incurred_inr": internal_cogs}

    @staticmethod
    async def credit_wallet(db: AsyncSession, tenant_id: str, amount_inr: float, reference_id: str, description: str = "Wallet Recharge") -> float:
        wallet = (await db.execute(select(Wallet).where(Wallet.tenant_id == tenant_id))).scalar_one_or_none()
        if not wallet:
            wallet = Wallet(tenant_id=tenant_id, balance_inr=amount_inr)
            db.add(wallet); await db.flush()
        else:
            wallet.balance_inr = round(wallet.balance_inr + amount_inr, 2)

        db.add(WalletTransaction(wallet_id=wallet.id, tenant_id=tenant_id, amount_inr=amount_inr, type="CREDIT", description=description, reference_id=reference_id))
        await db.commit()
        return wallet.balance_inr

class WhatsAppService:
    @staticmethod
    def _clean_phone_number(phone: str) -> str:
        digits = "".join(filter(str.isdigit, phone))
        return f"91{digits}" if len(digits) == 10 else digits

    @staticmethod
    async def send_post_call_message(phone_number: str, message_text: Optional[str] = None) -> bool:
        if not settings.WHATSAPP_TOKEN or not settings.WHATSAPP_PHONE_NUMBER_ID: return False
        clean_number = WhatsAppService._clean_phone_number(phone_number)
        url = f"https://graph.facebook.com/v20.0/{settings.WHATSAPP_PHONE_NUMBER_ID}/messages"
        headers = {"Authorization": f"Bearer {settings.WHATSAPP_TOKEN}", "Content-Type": "application/json"}
        payload = {"messaging_product": "whatsapp", "recipient_type": "individual", "to": clean_number, "type": "text", "text": {"preview_url": True, "body": message_text or "Thank you for speaking with Call Genie AI!"}}
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                res = await client.post(url, headers=headers, json=payload)
                return res.status_code in [200, 201]
        except Exception:
            return False

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
        except Exception:
            pass
        return room_name

class WebhookDispatcherService:
    @staticmethod
    async def dispatch_call_event(tenant_id: str, event_type: str, payload: Dict[str, Any]):
        async with AsyncSessionLocal() as db:
            endpoints = (await db.execute(select(ClientWebhookEndpoint).where(ClientWebhookEndpoint.tenant_id == tenant_id, ClientWebhookEndpoint.is_active == True))).scalars().all()
            if not endpoints: return
            async with httpx.AsyncClient(timeout=10.0) as client:
                for ep in endpoints:
                    try: await client.post(ep.target_url, json={"event": event_type, "data": payload}, headers={"Content-Type": "application/json"})
                    except Exception: pass

# 6. FASTAPI SETUP & AUTH
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

async def get_tenant_from_api_key(api_key: Optional[str] = Security(api_key_header_scheme), db: AsyncSession = Depends(get_db)) -> Tenant:
    if not api_key: raise HTTPException(status_code=401, detail="Missing X-CallGenie-Key header")
    key_obj = (await db.execute(select(ApiKey).where(ApiKey.key == api_key, ApiKey.is_active == True))).scalar_one_or_none()
    if not key_obj: raise HTTPException(status_code=401, detail="Invalid API key")
    tenant = (await db.execute(select(Tenant).where(Tenant.id == key_obj.tenant_id))).scalar_one_or_none()
    if not tenant: raise HTTPException(status_code=403, detail="Tenant inactive")
    return tenant

app = FastAPI(title="Call Genie API (by MyBot Genie)", version="1.3.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.on_event("startup")
async def on_startup():
    try:
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
    except Exception as e:
        logger.warning(f"Database table sync notice: {str(e)}")

@app.get("/")
async def root(): return {"status": "online", "app": "Call Genie API", "powered_by": "MyBot Genie"}

@app.get("/health")
async def health(): return {"status": "healthy", "app": settings.APP_NAME}

# 7. LIVE BROWSER VOICE CHAT ENDPOINT (ZERO TELECOM, 100% FREE & BULLETPROOF)
class VoiceChatRequest(BaseModel):
    message: str
    client_name: Optional[str] = "ji"
    business_name: Optional[str] = "aapke business"
    business_type: Optional[str] = "Business"
    conversation_history: Optional[List[Dict[str, str]]] = None

@app.post("/api/voice/chat")
async def voice_chat_endpoint(chat_in: VoiceChatRequest):
    system_prompt = (
        f"You are Call Genie, an intelligent conversational AI voice assistant for '{chat_in.business_name}' ({chat_in.business_type}) by MyBot Genie. "
        f"You are speaking live with {chat_in.client_name}. "
        "Speak natural, polite Hindi and Hinglish with an authentic Indian conversational cadence. "
        "Keep your response concise (1 to 2 sentences, maximum 25 words) so it sounds like a fast telephone response. "
        "Highlight that you can answer customer queries 24/7, book appointments, and send WhatsApp brochures instantly."
    )
    
    messages = [{"role": "system", "content": system_prompt}]
    if chat_in.conversation_history:
        for msg in chat_in.conversation_history[-6:]:
            messages.append({"role": msg.get("role", "user"), "content": msg.get("content", "")})
    messages.append({"role": "user", "content": chat_in.message})

    ai_reply = "Namaste! Main Call Genie AI bol raha hoon. Main aapke business ke liye 24 ghante customer calls handle kar sakta hoon."
    if settings.GROQ_API_KEY:
        try:
            async with httpx.AsyncClient(timeout=8.0) as client:
                res = await client.post(
                    "https://api.groq.com/openai/v1/chat/completions",
                    headers={"Authorization": f"Bearer {settings.GROQ_API_KEY}", "Content-Type": "application/json"},
                    json={
                        "model": "llama-3.1-8b-instant",
                        "messages": messages,
                        "temperature": 0.5
                    }
                )
                if res.status_code == 200:
                    ai_reply = res.json()["choices"][0]["message"]["content"]
        except Exception as e:
            logger.warning(f"Groq error: {str(e)}")

    audio_base64 = None
    if settings.SARVAM_API_KEY:
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                sarvam_res = await client.post(
                    "https://api.sarvam.ai/text-to-speech",
                    headers={"api-subscription-key": settings.SARVAM_API_KEY, "Content-Type": "application/json"},
                    json={
                        "inputs": [ai_reply],
                        "target_language_code": "hi-IN",
                        "speaker": "bulbul:v1",
                        "model": "bulbul:v1",
                        "pace": 1.05
                    }
                )
                if sarvam_res.status_code == 200:
                    audio_base64 = sarvam_res.json().get("audios", [None])[0]
        except Exception as e:
            logger.warning(f"Sarvam TTS error: {str(e)}")

    return {
        "reply": ai_reply,
        "audio_base64": audio_base64
    }

# 8. GENERAL APIS
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
    bg.add_task(LiveKitService.create_outbound_call, clean_phone, "demo", "demo_tenant", {"system_prompt": "Speak warm Hindi/Hinglish."})
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
