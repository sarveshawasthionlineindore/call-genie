from dotenv import load_dotenv
load_dotenv()

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

# ==============================================================================
# 1. CONFIGURATION (CLEAN ARCHITECTURE - ZERO TWILIO)
# ==============================================================================
class Settings(BaseSettings):
    APP_NAME: str = "Call Genie"
    ENVIRONMENT: str = "production"
    SECRET_KEY: str = "call_genie_super_secret_jwt_key_2026"
    ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 1440
    DATABASE_URL: str = "postgresql+asyncpg://postgres:postgres@localhost:5432/call_genie"

    # LiveKit Cloud (Audio Streaming & Future Exotel SIP Trunking)
    LIVEKIT_URL: str = "wss://call-genie-bjkq1kkd.livekit.cloud"
    LIVEKIT_API_KEY: str = ""
    LIVEKIT_API_SECRET: str = ""

    # Future Indian Telephony (Exotel SIP Trunk ID & Caller ID)
    EXOTEL_SIP_TRUNK_ID: str = ""
    EXOTEL_CALLER_NUMBER: str = ""

    # Direct Indian AI Pipeline
    DEEPGRAM_API_KEY: str = ""
    GROQ_API_KEY: str = ""
    SARVAM_API_KEY: str = ""

    # WhatsApp Cloud API (MyBot Genie Automations)
    WHATSAPP_TOKEN: Optional[str] = None
    WHATSAPP_PHONE_NUMBER_ID: Optional[str] = None
    WHATSAPP_BUSINESS_ACCOUNT_ID: Optional[str] = None

    DEFAULT_RETAIL_RATE_PER_MIN_INR: float = 5.00
    BILLING_PULSE_SECONDS: int = 60

    class Config:
        env_file = ".env"
        extra = "ignore"

settings = Settings()

# ==============================================================================
# 2. DATABASE & ASYNC ENGINE
# ==============================================================================
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

# ==============================================================================
# 4. SCHEMAS
# ==============================================================================
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

# ==============================================================================
# 5. CORE SERVICES (BILLING, WHATSAPP & LIVEKIT ENGINE)
# ==============================================================================
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


import sqlite3

LOCAL_DB_PATH = "/tmp/call_genie_auth.db"

def init_local_sqlite():
    try:
        conn = sqlite3.connect(LOCAL_DB_PATH)
        c = conn.cursor()
        c.execute("""CREATE TABLE IF NOT EXISTS tenants (
            id TEXT PRIMARY KEY,
            name TEXT,
            rate_per_min_inr REAL DEFAULT 5.0,
            is_active INTEGER DEFAULT 1,
            created_at TEXT
        )""")
        c.execute("""CREATE TABLE IF NOT EXISTS users (
            id TEXT PRIMARY KEY,
            tenant_id TEXT,
            email TEXT UNIQUE,
            hashed_password TEXT,
            full_name TEXT,
            role TEXT DEFAULT 'admin',
            is_active INTEGER DEFAULT 1,
            created_at TEXT
        )""")
        c.execute("""CREATE TABLE IF NOT EXISTS wallets (
            id TEXT PRIMARY KEY,
            tenant_id TEXT UNIQUE,
            balance_inr REAL DEFAULT 100.0,
            currency TEXT DEFAULT 'INR',
            updated_at TEXT
        )""")
        conn.commit()
        conn.close()
    except Exception as e:
        logger.warning(f"Local sqlite init notice: {str(e)}")

init_local_sqlite()

# ==============================================================================
# 6. FASTAPI SETUP & AUTH
# ==============================================================================
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/auth/login")
api_key_header_scheme = APIKeyHeader(name="X-CallGenie-Key", auto_error=False)

import hashlib, hmac

def get_password_hash(password: str) -> str:
    try:
        return pwd_context.hash(password)
    except Exception as err:
        logger.warning(f"passlib fallback to hmac-sha256: {str(err)}")
        salt = settings.SECRET_KEY[:16].encode()
        return "sha256$" + hmac.new(salt, password.encode(), hashlib.sha256).hexdigest()

def verify_password(plain: str, hashed: str) -> bool:
    if str(hashed).startswith("sha256$"):
        salt = settings.SECRET_KEY[:16].encode()
        expected = "sha256$" + hmac.new(salt, plain.encode(), hashlib.sha256).hexdigest()
        return hmac.compare_digest(expected, str(hashed))
    try:
        return pwd_context.verify(plain, hashed)
    except Exception:
        salt = settings.SECRET_KEY[:16].encode()
        expected = "sha256$" + hmac.new(salt, plain.encode(), hashlib.sha256).hexdigest()
        return hmac.compare_digest(expected, str(hashed))
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

app = FastAPI(title="Call Genie API (by MyBot Genie)", version="2.0.0")
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

# ==============================================================================
# 7. LIVE CONVERSATIONAL VOICE ENGINE (GROQ LLM + SARVAM AI NATIVE HINDI VOICE)
# ==============================================================================
def generate_dynamic_hindi_reply(user_msg: str, client_name: str, biz_name: str, biz_type: str) -> str:
    msg = (user_msg or "").lower().strip()
    
    caller = ""
    if client_name and client_name.strip().lower() not in ["ji", "sir", "sar", "", "business", "client"]:
        caller = f"{client_name.strip()} जी, "

    b_name = biz_name.strip() if biz_name and biz_name.strip().lower() not in ["business", "aapke business", "", "general"] else "आपके बिज़नेस"

    # 0. LIVE OUTBOUND DEMO CALL & ROLEPLAY (Immediate Simulation)
    is_outbound_demo = any(w in msg for w in [
        "outbound call", "outbound", "आउटबाउंड कॉल", "आउटबाउंड", "आउटबॉउंड",
        "कॉल करके बताओ", "कॉल करके दिखाओ", "डेमो कॉल", "demo call",
        "कॉल लगाओ", "roleplay", "रोलप्ले", "कॉल करो", "pitch karke",
        "डेमो बताओ", "डेमो दो"
    ])

    if is_outbound_demo:
        if any(w in msg for w in ["real estate", "property", "plot", "flat", "रियल एस्टेट", "प्रॉपर्टी", "फ्लैट", "प्लॉट", "मकान"]):
            return "नमस्ते! मैं एपेक्स रियल्टी से रिया बात कर रही हूँ। क्या मेरी बात सर से हो रही है? दरअसल आपने हाल ही में हमारे नए 2BHK और 3BHK प्रोजेक्ट के लिए इन्क्वायरी की थी। क्या आप अपने रहने के लिए प्रॉपर्टी देख रहे हैं या इन्वेस्टमेंट के लिए?"
        if any(w in msg for w in ["ecommerce", "e-commerce", "order", "cod", "delivery", "ई-कॉमर्स", "ऑर्डर"]):
            return "नमस्ते! मैं माय स्टोर से बात कर रही हूँ। आपका 1,499 रुपये का कैश ऑन डिलीवरी ऑर्डर हमारे पास रिसीव हुआ है। क्या मैं आपका यह ऑर्डर शिपिंग के लिए कन्फर्म कर दूँ?"
        if any(w in msg for w in ["school", "college", "admission", "coaching", "स्कूल", "एडमिशन"]):
            return "नमस्ते! मैं ग्रीनवुड स्कूल के एडमिशन ऑफिस से बात कर रही हूँ। आपने नए सेशन में एडमिशन के लिए इन्क्वायरी फॉर्म भरा था। क्या मैं आपका प्रोस्पेक्टस व्हाट्सएप पर भेज दूँ?"
        return f"नमस्ते! मैं {b_name} से आपकी पर्सनल एग्जीक्यूटिव बात कर रही हूँ। क्या मेरी बात सर से हो रही है? आपने हमारी सेवाओं के बारे में जानकारी माँगी थी। क्या मैं आपको 2 मिनट में इसकी पूरी डिटेल्स समझा सकती हूँ?"

    # 1. E-Commerce & D2C
    if any(w in msg for w in ["e-commerce", "ecommerce", "online", "order", "delivery", "cod", "d2c", "store", "shopping", "dispatch", "rto", "ई-कॉमर्स", "ऑर्डर"]):
        return f"जी {caller}ई-कॉमर्स के लिए मैं नए ऑर्डर पर तुरंत COD कन्फर्मेशन कॉल करूँगी ताकि RTO कम हो, डिस्पैच व डिलीवरी का अपडेट दूँगी, रिटर्न सपोर्ट संभालूँगी और ऑफर्स की कॉल्स भी करूँगी।"

    # 2. Real Estate, Builders & Properties
    if any(w in msg for w in ["plot", "flat", "property", "zameen", "bhk", "location", "realty", "builder", "site visit", "real estate", "प्रॉपर्टी", "प्लॉट", "फ्लैट", "रियल एस्टेट"]):
        return f"जी {caller}रियल एस्टेट में जैसे ही कोई ग्राहक विज्ञापन देखकर कॉल करेगा, मैं उनका बजट और 2BHK या 3BHK की पसंद पूछ लूँगी, साइट विजिट शेड्यूल कर दूँगी और व्हाट्सएप पर ब्रोशर भेज दूँगी।"

    # 3. Gym, Fitness, Yoga & Crossfit
    if any(w in msg for w in ["gym", "fitness", "yoga", "workout", "trainer", "जिम", "फिटनेस", "योगा", "कसरत"]):
        return f"जी {caller}जिम के लिए जैसे ही कोई इंक्वायरी आएगी, मैं उनका फिटनेस गोल पूछकर फ्री ट्रायल स्लॉट बुक कर लूँगी, मेंबरशिप प्लान्स समझाऊँगी और तुरंत व्हाट्सएप पर टाइमिंग्स भेज दूँगी।"

    # 4. Interior Design, Architecture & Furniture
    if any(w in msg for w in ["interior", "designer", "decor", "architect", "renovation", "furniture", "इंटीरियर", "डिजाइनर", "फर्नीचर"]):
        return f"जी {caller}इंटीरियर डिजाइनिंग के लिए जैसे ही कोई कॉल करेगा, मैं उनसे कारपेट एरिया और बजट पूछकर आपकी कंसल्टेशन मीटिंग फिक्स कर लूँगी और तुरंत व्हाट्सएप पर आपका पोर्टफोलियो भेज दूँगी।"

    # 5. Hotels, Resorts, Homestays & Banquets
    if any(w in msg for w in ["hotel", "resort", "room", "booking", "stay", "banquet", "होटल", "रिसॉर्ट", "कमरा"]):
        return f"जी {caller}होटल के लिए मैं 24 घंटे रूम बुकिंग की इन्क्वायरी लूँगी, चेक-इन टाइम व टैरिफ बताऊँगी, बुकिंग कन्फर्म करूँगी और तुरंत व्हाट्सएप पर लोकेशन व तस्वीरें भेज दूँगी।"

    # 6. CA, Legal, Accounting & Tax Consultants
    if any(w in msg for w in ["ca", "chartered", "tax", "gst", "itr", "lawyer", "advocate", "legal", "अकाउंटेंट", "वकील", "टैक्स"]):
        return f"जी {caller}आपकी फर्म के लिए मैं जीएसटी और आईटीआर की डेडलाइन पर ग्राहकों को रिमाइंडर कॉल्स करूँगी, कंसल्टेशन स्लॉट्स बुक करूँगी और जरूरी डॉक्युमेंट्स की लिस्ट व्हाट्सएप पर भेज दूँगी।"

    # 7. Travel, Tours, Visa & Holiday Packages
    if any(w in msg for w in ["travel", "tour", "package", "holiday", "flight", "visa", "ट्रैवल", "टूर", "यात्रा", "घूमना"]):
        return f"जी {caller}आपकी ट्रैवल एजेंसी के लिए जब भी कोई कॉल करेगा, मैं डेस्टिनेशन और बजट पूछकर कस्टमाइज्ड पैकेज डिटेल्स समझाऊँगी और तुरंत व्हाट्सएप पर पूरी इटिनरेरी भेज दूँगी।"

    # 8. Restaurants, Cafes & Food Delivery
    if any(w in msg for w in ["restaurant", "cafe", "food", "table", "dining", "रेस्तरां", "कैफे", "खाना"]):
        return f"जी {caller}रेस्तरां के लिए मैं ग्राहकों के लिए टेबल रिज़र्वेशन करूँगी, पार्टी व कैटरिंग की बुकिंग लूँगी, मेन्यू व स्पेशल डिशेस बताऊँगी और व्हाट्सएप पर लोकेशन भेज दूँगी।"

    # 9. Salons, Spas & Beauty Parlors
    if any(w in msg for w in ["salon", "spa", "parlour", "beauty", "hair", "makeup", "सैलून", "पार्लर", "स्पा"]):
        return f"जी {caller}सैलून के लिए मैं ग्राहकों के लिए अपॉइंटमेंट स्लॉट बुक करूँगी, ब्राइडल व हेयर पैकेजेस के रेट्स बताऊँगी और कन्फर्मेशन मैसेज तुरंत उनके व्हाट्सएप पर भेज दूँगी।"

    # 10. Schools, Colleges, Coaching & Classes
    if any(w in msg for w in ["school", "college", "education", "coaching", "admission", "fees", "student", "parent", "class", "स्कूल", "कॉलेज", "एडमिशन"]):
        return f"जी {caller}स्कूल और संस्थान के लिए जब भी पेरेंट्स कॉल करेंगे, मैं नए एडमिशन की जानकारी, फीस स्ट्रक्चर और टाइमिंग बता दूँगी, और तुरंत उनके फोन पर प्रोस्पेक्टस भेज दूँगी।"

    # 11. Hospital, Clinic, Doctors & Healthcare
    if any(w in msg for w in ["doctor", "clinic", "appointment", "dawai", "ilaj", "timing", "hospital", "patient", "डॉक्टर", "अस्पताल", "इलाज"]):
        return f"जी {caller}क्लिनिक और हॉस्पिटल के लिए मैं मरीज़ों के अपॉइंटमेंट बुक कर लूँगी, डॉक्टर का उपलब्ध समय बता दूँगी और कन्फर्मेशन मैसेज तुरंत उनके व्हाट्सएप पर भेज दूँगी।"

    # 12. Automobile, Car/Bike Showrooms & Service Centers
    if any(w in msg for w in ["car", "bike", "vehicle", "automobile", "dealership", "showroom", "service", "test drive", "गाड़ी", "कार"]):
        return f"जी {caller}ऑटोमोबाइल शोरूम के लिए मैं ग्राहकों के लिए टेस्ट ड्राइव बुक कर लूँगी, गाड़ियों की कीमतें और फीचर्स बता दूँगी और हॉट लीड्स सीधे आपकी सेल्स टीम को दे दूँगी।"

    # 13. Events, Weddings & Photography
    if any(w in msg for w in ["event", "wedding", "photographer", "photography", "dj", "शादी", "इवेंट", "फोटोग्राफी"]):
        return f"जी {caller}इवेंट्स और वेडिंग के लिए मैं क्लाइंट्स से डेट और बजट पूछकर आपकी प्लानिंग मीटिंग शेड्यूल कर लूँगी और तुरंत व्हाट्सएप पर आपके पिछले इवेंट्स का पोर्टफोलियो भेज दूँगी।"

    # 14. Astrology, Vastu & Horoscopes
    if any(w in msg for w in ["astrology", "jyotish", "kundali", "vastu", "pandit", "ज्योतिष", "कुंडली", "वास्तु"]):
        return f"जी {caller}कंसल्टेशन के लिए मैं क्लाइंट्स से जन्म विवरण लेकर आपका अपॉइंटमेंट स्लॉट बुक कर लूँगी, फीस बता दूँगी और व्हाट्सएप पर स्लॉट कन्फर्मेशन भेज दूँगी।"

    # 15. Logistics, Transport & Packers Movers
    if any(w in msg for w in ["transport", "packers", "movers", "courier", "logistics", "ट्रांसपोर्ट", "पैकर्स"]):
        return f"जी {caller}ट्रांसपोर्ट और शिफ्टिंग के लिए मैं पिकअप लोकेशन और सामान का विवरण लेकर तुरंत कोटेशन जनरेट करूँगी और बुकिंग डिटेल्स व्हाट्सएप पर भेज दूँगी।"

    # 16. Solar, Green Energy & Rooftop
    if any(w in msg for w in ["solar", "rooftop", "panel", "bijli", "सोलर", "बिजली"]):
        return f"जी {caller}सोलर के लिए मैं ग्राहक का मासिक बिजली बिल पूछकर साइट सर्वे बुक कर लूँगी, सब्सिडी की जानकारी दूँगी और व्हाट्सएप पर अनुमानित बचत का कोटेशन भेज दूँगी।"

    # 17. Security, Pest Control & Cleaning Services
    if any(w in msg for w in ["pest", "cleaning", "security", "guard", "pest control", "सफाई"]):
        return f"जी {caller}होम सर्विसेज के लिए मैं ग्राहक की लोकेशन और समस्या समझकर टेक्नीशियन का विजिट शेड्यूल कर लूँगी और तुरंत व्हाट्सएप पर सर्विस चार्ज भेज दूँगी।"

    # 18. Pricing / Rate / Cost
    if any(w in msg for w in ["kitna paisa", "kitna kharcha", "rate", "cost", "price", "kitna lagega", "charge", "fees", "किराया", "दाम", "खर्चा", "रेट"]):
        return "कॉल जीनी का रेट बहुत ही पारदर्शी और किफायती है, लगभग सिर्फ पाँच रुपये प्रति मिनट। आप जब चाहें एक छोटे से बैलेंस से अपनी कॉलिंग शुरू कर सकती हैं।"

    # 19. Capabilities / What can you do / Help
    if any(w in msg for w in ["kya kar sakte", "kya kar sakti", "kya kaam", "kaise kaam", "kya service", "features", "kya karte", "kya karti", "madad", "help", "क्या कर सकते", "क्या कर सकती"]):
        return f"मैं {b_name} की हर कॉल चौबीस घंटे अटेंड करूँगी, ग्राहकों के सवालों के सटीक जवाब दूँगी और जो भी ग्राहक रुचि दिखाएँ, उनकी पूरी डिटेल और नंबर आपके व्हाट्सएप पर तुरंत भेज दूँगी।"

    # 20. Greetings
    if any(w in msg for w in ["नमस्ते", "हैलो", "हेलो", "हाय", "namaste", "hello", "hi", "hey", "kaise ho", "kya haal"]):
        return f"नमस्ते {caller}मायबॉट जीनी के कॉल जीनी में आपका स्वागत है! बताइये, आज {b_name} के लिए मैं आपकी किस तरह मदद करूँ?"

    # 21. Identity / Who are you
    if any(w in msg for w in ["कौन हो", "kaun ho", "kya naam", "naam kya", "kya ho", "who are you", "tum kaun"]):
        return "मैं कॉल जीनी एआई बोल रही हूँ मायबॉट जीनी से। मैं एक वॉइस असिस्टेंट हूँ जो आपके ग्राहकों से बिल्कुल एक समझदार रिलेशनशिप मैनेजर की तरह बात करती हूँ।"

    # 22. Thanks / Bye
    if any(w in msg for w in ["dhanyawad", "shukriya", "bye", "alvida", "theek hai", "ok", "thank"]):
        return f"बहुत-बहुत धन्यवाद {caller}आपकी सेवा के लिए मैं हमेशा उपलब्ध हूँ। आपका दिन बहुत शुभ हो!"

    # Universal Bespoke Fallback for ANY OTHER Unlisted Business
    return f"जी {caller}आपके बिज़नेस में जब भी कोई ग्राहक कॉल करेगा, मैं उनकी ज़रूरत व बजट समझकर आपकी अपॉइंटमेंट या बुकिंग फिक्स करूँगी, उनके सवालों के सटीक जवाब दूँगी और तुरंत उनके व्हाट्सएप पर आपकी पूरी प्रोफाइल व रेट्स भेज दूँगी।"

class VoiceChatRequest(BaseModel):
    message: str
    client_name: Optional[str] = "ji"
    business_name: Optional[str] = "aapke business"
    business_type: Optional[str] = "Business"
    conversation_history: Optional[List[Dict[str, str]]] = None

@app.get("/api/voice/status")
async def voice_status_check():
    """
    Diagnostic endpoint to verify API key presence and test live connectivity
    to Groq and Sarvam AI directly from Render.
    """
    groq_api_key = (os.getenv("GROQ_API_KEY") or os.getenv("GROK_API_KEY") or getattr(settings, "GROQ_API_KEY", "") or "").strip()
    sarvam_api_key = (os.getenv("SARVAM_API_KEY") or os.getenv("SARVAM_AI_API_KEY") or getattr(settings, "SARVAM_API_KEY", "") or "").strip()

    groq_test_result = "not_configured"
    if groq_api_key:
        try:
            async with httpx.AsyncClient(timeout=6.0) as client:
                res = await client.post(
                    "https://api.groq.com/openai/v1/chat/completions",
                    headers={"Authorization": f"Bearer {groq_api_key}", "Content-Type": "application/json"},
                    json={
                        "model": "llama-3.1-8b-instant",
                        "messages": [{"role": "user", "content": "hi"}],
                        "max_tokens": 5
                    }
                )
                groq_test_result = f"status_{res.status_code}"
                if res.status_code != 200:
                    groq_test_result += f" ({res.text[:120]})"
        except Exception as e:
            groq_test_result = f"exception: {str(e)}"

    sarvam_test_result = "not_configured"
    if sarvam_api_key:
        try:
            async with httpx.AsyncClient(timeout=6.0) as client:
                res = await client.post(
                    "https://api.sarvam.ai/text-to-speech",
                    headers={"api-subscription-key": sarvam_api_key, "Content-Type": "application/json"},
                    json={
                        "inputs": ["नमस्ते"],
                        "target_language_code": "hi-IN",
                        "model": "bulbul:v4-flash"
                    }
                )
                sarvam_test_result = f"status_{res.status_code}"
                if res.status_code != 200:
                    sarvam_test_result += f" ({res.text[:120]})"
        except Exception as e:
            sarvam_test_result = f"exception: {str(e)}"

    return {
        "status": "online",
        "groq_configured": bool(groq_api_key),
        "groq_key_prefix": groq_api_key[:6] + "..." if groq_api_key else None,
        "groq_test": groq_test_result,
        "sarvam_configured": bool(sarvam_api_key),
        "sarvam_key_prefix": sarvam_api_key[:6] + "..." if sarvam_api_key else None,
        "sarvam_test": sarvam_test_result
    }

@app.post("/api/voice/chat")
async def voice_chat_endpoint(chat_in: VoiceChatRequest):
    """
    Main conversational voice endpoint powering live browser calls and upcoming Exotel lines.
    Thinks via Groq Llama 3.3 and speaks via Sarvam AI with 100% human Indian emotions.
    """
    system_prompt = (
        f"You are Call Genie (कॉल जीनी), an intelligent, sharp, polite female AI relationship executive representing '{chat_in.business_name}' ({chat_in.business_type}) by MyBot Genie. "
        f"You are speaking live on a phone call with {chat_in.client_name}. "
        "CRITICAL RULES FOR SPEECH & PERSONA: "
        "1. STRICT FEMALE GRAMMAR (स्त्रीलिंग क्रिया रूप अनिवार्य): You are a female executive speaking with Sarvam's Bulbul female voice. You MUST ALWAYS speak using female Hindi verb endings: say 'कर सकती हूँ' (NEVER 'कर सकता हूँ'), 'बोल रही हूँ' (NEVER 'बोल रहा हूँ'), 'बताऊँगी', 'पूछ लूँगी', 'करूँगी', 'भेज दूँगी'. "
        "2. NO ROBOTIC CLICHES (घिसी-पिटी बातें मना हैं): NEVER say 'मैं आपके प्रोसेस को ऑटोमेट कर सकती हूँ' or robotic automation lines. Give real, practical business solutions. "
        "   - If they ask about E-Commerce / D2C: Explain handling COD order confirmation calls to reduce RTO, sending dispatch/delivery updates, managing 24/7 customer support for returns/tracking, and making promotional/festive offer calls. "
        "   - If they ask about Real Estate: Explain how you answer property calls, ask buyer budget/BHK, book site visits, and send details on WhatsApp. "
        "   - If they ask about Schools: Explain managing parent admission queries, fee structures, school timings, and sending prospectus on WhatsApp. "
        "   - If they ask about Clinics: Explain booking appointments and doctor availability. "
        "3. LIVE OUTBOUND DEMO ROLEPLAY (लाइव आउटबाउंड डेमो कॉल): "
        "If the caller asks for an OUTBOUND DEMO call or asks how you pitch (e.g. 'आउटबाउंड कॉल करके बताओ', 'डेमो कॉल करो', 'कॉल करके दिखाओ', 'roleplay', 'pitch करके बताओ'): "
        "DO NOT explain what you do in third person! "
        "IMMEDIATELY BECOME THE CALLER AND START THE OUTBOUND SALES CALL DIRECTLY TO THE CALLER! "
        "Example Real Estate: 'नमस्ते! मैं एपेक्स रियल्टी से रिया बात कर रही हूँ। क्या मेरी बात सर से हो रही है? दरअसल आपने हाल ही में हमारे नए 2BHK प्रोजेक्ट के लिए इन्क्वायरी की थी। क्या आप रहने के लिए प्रॉपर्टी देख रहे हैं या इन्वेस्टमेंट के लिए?' "
        "Pitch and ask an engaging qualifying question so the caller can reply and experience the live call! "
        "4. PURE DEVANAGARI HINDI: Respond strictly in natural, fluent spoken Hindi in Devanagari script (हिंदी). Never write English/Roman letters. "
        "5. FAST PHONE RHYTHM: Keep responses concise (strictly between 25 and 35 words), warm, polite ('आप'), and speak like a sharp, human business executive."
    )

    messages = [{"role": "system", "content": system_prompt}]
    if chat_in.conversation_history:
        for msg in chat_in.conversation_history[-6:]:
            messages.append({"role": msg.get("role", "user"), "content": msg.get("content", "")})
    messages.append({"role": "user", "content": chat_in.message})

    ai_reply = None
    groq_api_key = os.getenv("GROQ_API_KEY") or os.getenv("GROK_API_KEY") or os.getenv("GROQ_KEY") or getattr(settings, "GROQ_API_KEY", "")
    
    if groq_api_key:
        try:
            async with httpx.AsyncClient(timeout=7.0) as client:
                res = await client.post(
                    "https://api.groq.com/openai/v1/chat/completions",
                    headers={"Authorization": f"Bearer {groq_api_key}", "Content-Type": "application/json"},
                    json={
                        "model": "llama-3.3-70b-versatile",
                        "messages": messages,
                        "temperature": 0.5,
                        "max_tokens": 120
                    }
                )
                if res.status_code == 200:
                    ai_reply = res.json()["choices"][0]["message"]["content"].strip()
                else:
                    logger.warning(f"Groq API returned status {res.status_code}: {res.text}. Trying fallback model...")
                    res2 = await client.post(
                        "https://api.groq.com/openai/v1/chat/completions",
                        headers={"Authorization": f"Bearer {groq_api_key}", "Content-Type": "application/json"},
                        json={
                            "model": "llama-3.1-8b-instant",
                            "messages": messages,
                            "temperature": 0.5,
                            "max_tokens": 120
                        }
                    )
                    if res2.status_code == 200:
                        ai_reply = res2.json()["choices"][0]["message"]["content"].strip()
                    else:
                        logger.warning(f"Groq fallback model also returned status {res2.status_code}")
        except Exception as e:
            logger.warning(f"Groq conversational error: {str(e)}")

    if not ai_reply:
        ai_reply = generate_dynamic_hindi_reply(
            chat_in.message,
            chat_in.client_name or "",
            chat_in.business_name or "aapke business",
            chat_in.business_type or "Business"
        )

    # Clean text: remove markdown artifacts
    clean_speech_text = ai_reply.replace("*", "").replace("#", "").replace("_", "").strip()

    audio_base64 = None
    sarvam_debug_info = None
    sarvam_api_key = (os.getenv("SARVAM_API_KEY") or os.getenv("SARVAM_AI_API_KEY") or os.getenv("SARVAM_KEY") or getattr(settings, "SARVAM_API_KEY", "") or "").strip()
    
    voice_engine_used = "browser_tts_fallback"
    if sarvam_api_key:
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                # 1. Primary: Sarvam Bulbul v4-flash (Latest Model with conversational female persona)
                sarvam_res = await client.post(
                    "https://api.sarvam.ai/text-to-speech",
                    headers={"api-subscription-key": sarvam_api_key, "Content-Type": "application/json"},
                    json={
                        "inputs": [clean_speech_text],
                        "target_language_code": "hi-IN",
                        "speaker": "suman_hi_companion",
                        "model": "bulbul:v4-flash",
                        "pace": 1.05,
                        "speech_sample_rate": 16000
                    }
                )
                if sarvam_res.status_code == 200:
                    audios = sarvam_res.json().get("audios", [])
                    if audios:
                        audio_base64 = audios[0]
                        voice_engine_used = "sarvam_bulbul_v4_flash"
                        logger.info("Sarvam Bulbul v4-flash (suman_hi_companion) generated successfully")
                else:
                    logger.warning(f"Sarvam v4 persona failed {sarvam_res.status_code}: {sarvam_res.text}. Trying v4 default persona...")
                    sarvam_debug_info = f"v4_status_{sarvam_res.status_code}: {sarvam_res.text[:120]}"
                    # 2. Secondary: Sarvam Bulbul v4-flash with default persona (omitting speaker parameter)
                    sarvam_res2 = await client.post(
                        "https://api.sarvam.ai/text-to-speech",
                        headers={"api-subscription-key": sarvam_api_key, "Content-Type": "application/json"},
                        json={
                            "inputs": [clean_speech_text],
                            "target_language_code": "hi-IN",
                            "model": "bulbul:v4-flash",
                            "pace": 1.05,
                            "speech_sample_rate": 16000
                        }
                    )
                    if sarvam_res2.status_code == 200:
                        audios2 = sarvam_res2.json().get("audios", [])
                        if audios2:
                            audio_base64 = audios2[0]
                            voice_engine_used = "sarvam_bulbul_v4_flash"
                            logger.info("Sarvam Bulbul v4-flash (default persona) generated successfully")
                    else:
                        # 3. Tertiary: Fallback to Bulbul v3 with ritu
                        logger.warning(f"Sarvam v4 default failed {sarvam_res2.status_code}. Fallback to Bulbul v3...")
                        sarvam_res3 = await client.post(
                            "https://api.sarvam.ai/text-to-speech",
                            headers={"api-subscription-key": sarvam_api_key, "Content-Type": "application/json"},
                            json={
                                "inputs": [clean_speech_text],
                                "target_language_code": "hi-IN",
                                "speaker": "ritu",
                                "model": "bulbul:v3",
                                "pace": 1.05,
                                "speech_sample_rate": 16000
                            }
                        )
                        if sarvam_res3.status_code == 200:
                            audios3 = sarvam_res3.json().get("audios", [])
                            if audios3:
                                audio_base64 = audios3[0]
                                voice_engine_used = "sarvam_bulbul_v3"
                                logger.info("Sarvam Bulbul v3 (ritu) generated successfully")
        except Exception as e:
            logger.warning(f"Sarvam TTS exception: {str(e)}")
            sarvam_debug_info = str(e)

    return {
        "reply": ai_reply,
        "audio_base64": audio_base64,
        "voice_engine": voice_engine_used,
        "sarvam_debug": sarvam_debug_info
    }

# ==============================================================================
# 8. GENERAL APIS (AUTH, AGENTS, WALLET, LOGS, API KEYS)
# ==============================================================================
@app.post("/api/auth/register", response_model=TokenResponse)
async def register(user_in: UserRegister, db: AsyncSession = Depends(get_db)):
    # Try PostgreSQL first
    pg_success = False
    try:
        existing = await db.execute(select(User).where(User.email == user_in.email))
        if existing.scalar_one_or_none():
            raise HTTPException(status_code=400, detail="Email already registered. Kripya login karein.")
        
        tenant = Tenant(name=user_in.company_name)
        db.add(tenant)
        await db.flush()
        
        hashed_pwd = get_password_hash(user_in.password)
        new_user = User(
            tenant_id=tenant.id,
            email=user_in.email,
            hashed_password=hashed_pwd,
            full_name=user_in.full_name
        )
        db.add(new_user)
        db.add(Wallet(tenant_id=tenant.id, balance_inr=100.0))
        await db.commit()
        await db.refresh(new_user)

        token = create_access_token({"sub": new_user.id, "tenant_id": tenant.id})
        return {
            "access_token": token,
            "token_type": "bearer",
            "tenant_id": tenant.id,
            "email": new_user.email,
            "company_name": tenant.name
        }
    except HTTPException:
        raise
    except Exception as pg_err:
        logger.warning(f"PostgreSQL unavailable ({str(pg_err)[:60]}), seamless fallback to local SQLite auth...")
    
    # Seamless Fallback: Local SQLite Auth (100% Zero-Failure Guarantee)
    try:
        conn = sqlite3.connect(LOCAL_DB_PATH)
        c = conn.cursor()
        c.execute("SELECT id FROM users WHERE email = ?", (user_in.email,))
        if c.fetchone():
            conn.close()
            raise HTTPException(status_code=400, detail="Email already registered. Kripya login karein.")
        
        t_id = str(uuid.uuid4())
        u_id = str(uuid.uuid4())
        w_id = str(uuid.uuid4())
        now_str = datetime.utcnow().isoformat()
        hashed_pwd = get_password_hash(user_in.password)

        c.execute("INSERT INTO tenants (id, name, created_at) VALUES (?, ?, ?)", (t_id, user_in.company_name, now_str))
        c.execute("INSERT INTO users (id, tenant_id, email, hashed_password, full_name, created_at) VALUES (?, ?, ?, ?, ?, ?)",
                  (u_id, t_id, user_in.email, hashed_pwd, user_in.full_name, now_str))
        c.execute("INSERT INTO wallets (id, tenant_id, balance_inr, updated_at) VALUES (?, ?, ?, ?)", (w_id, t_id, 100.0, now_str))
        conn.commit()
        conn.close()

        token = create_access_token({"sub": u_id, "tenant_id": t_id})
        return {
            "access_token": token,
            "token_type": "bearer",
            "tenant_id": t_id,
            "email": user_in.email,
            "company_name": user_in.company_name
        }
    except HTTPException:
        raise
    except Exception as sql_err:
        logger.error(f"Fallback SQLite registration error: {str(sql_err)}")
        raise HTTPException(status_code=500, detail="Registration service error. Please try again.")

@app.post("/api/auth/login", response_model=TokenResponse)
async def login(user_in: UserLogin, db: AsyncSession = Depends(get_db)):
    # Try PostgreSQL first
    try:
        user = (await db.execute(select(User).where(User.email == user_in.email))).scalar_one_or_none()
        if user and verify_password(user_in.password, user.hashed_password):
            tenant = (await db.execute(select(Tenant).where(Tenant.id == user.tenant_id))).scalar_one_or_none()
            return {"access_token": create_access_token({"sub": user.id, "tenant_id": user.tenant_id}), "token_type": "bearer", "tenant_id": user.tenant_id, "email": user.email, "company_name": tenant.name if tenant else "Client"}
        elif user:
            raise HTTPException(status_code=400, detail="Invalid password")
    except HTTPException:
        raise
    except Exception as pg_err:
        logger.warning(f"PostgreSQL login check failed ({str(pg_err)[:60]}), fallback to SQLite...")
    
    # Fallback to local SQLite
    try:
        conn = sqlite3.connect(LOCAL_DB_PATH)
        c = conn.cursor()
        c.execute("SELECT id, tenant_id, hashed_password, email FROM users WHERE email = ?", (user_in.email,))
        row = c.fetchone()
        if not row:
            conn.close()
            raise HTTPException(status_code=400, detail="Invalid credentials")
        u_id, t_id, hashed_pwd, email = row
        if not verify_password(user_in.password, hashed_pwd):
            conn.close()
            raise HTTPException(status_code=400, detail="Invalid credentials")
        
        c.execute("SELECT name FROM tenants WHERE id = ?", (t_id,))
        t_row = c.fetchone()
        company_name = t_row[0] if t_row else "Client"
        conn.close()

        return {"access_token": create_access_token({"sub": u_id, "tenant_id": t_id}), "token_type": "bearer", "tenant_id": t_id, "email": email, "company_name": company_name}
    except HTTPException:
        raise
    except Exception as sql_err:
        logger.error(f"Fallback SQLite login error: {str(sql_err)}")
        raise HTTPException(status_code=400, detail="Invalid credentials")

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
