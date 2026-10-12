import os
import sys
import uuid
import math
import secrets
import logging
import asyncio
import httpx
import json
import smtplib
import ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import re
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any

from fastapi import (
    Header,
    Query,
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
    func,
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
    DEFAULT_SUPABASE_URL: str = "postgresql+asyncpg://postgres.xtrmygedjgvcvtduvbx:%23Grow%40202627@aws-0-ap-northeast-2.pooler.supabase.com:6543/postgres"
    DATABASE_URL: str = os.getenv("DATABASE_URL", "postgresql+asyncpg://postgres.xtrmygedjgvcvtduvbx:%23Grow%40202627@aws-0-ap-northeast-2.pooler.supabase.com:6543/postgres")

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

    # Hostinger Business Email SMTP (bot@mybotgenie.in)
    SMTP_HOST: str = os.getenv("SMTP_HOST", "smtp.hostinger.com")
    SMTP_PORT: int = int(os.getenv("SMTP_PORT", "465"))
    SMTP_USER: str = os.getenv("SMTP_USER", "bot@mybotgenie.in")
    SMTP_PASSWORD: str = os.getenv("SMTP_PASSWORD", "#Grow@2025")
    SMTP_FROM_NAME: str = "Call Genie by MyBot Genie"

    DEFAULT_RETAIL_RATE_PER_MIN_INR: float = 5.00
    BILLING_PULSE_SECONDS: int = 60

    class Config:
        env_file = ".env"
        extra = "ignore"

settings = Settings()

# ==============================================================================
# 2. DATABASE & ASYNC ENGINE (WITH ZERO-FAILURE SQLITE RESILIENCE)
# ==============================================================================
DEFAULT_POSTGRES_FALLBACK = "postgresql+asyncpg://postgres.xtrmygedjgvcvtduvbx:%23Grow%40202627@aws-0-ap-northeast-2.pooler.supabase.com:6543/postgres"

def clean_db_url(raw_url: str) -> str:
    url = (raw_url or "").strip()
    if not url:
        return DEFAULT_POSTGRES_FALLBACK
    if url.startswith("postgres://"):
        return url.replace("postgres://", "postgresql+asyncpg://", 1)
    if url.startswith("postgresql://") and not url.startswith("postgresql+asyncpg://"):
        return url.replace("postgresql://", "postgresql+asyncpg://", 1)
    return url

db_url = clean_db_url(settings.DATABASE_URL)

def create_engine_safe(url: str):
    if "sqlite" in url:
        try:
            import aiosqlite
            return create_async_engine(url, echo=False)
        except ImportError:
            logger.warning("aiosqlite driver not installed on host. Connecting to PostgreSQL asyncpg driver...")
            return create_async_engine(
                DEFAULT_POSTGRES_FALLBACK,
                echo=False,
                pool_pre_ping=True,
                pool_size=5,
                max_overflow=10
            )
    return create_async_engine(
        url,
        echo=False,
        pool_pre_ping=True,
        pool_size=5,
        max_overflow=10
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
    is_verified = Column(Boolean, default=False)
    reset_token = Column(String(100), nullable=True)
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

class CRMIntegration(Base):
    __tablename__ = "crm_integrations"
    id = Column(String(36), primary_key=True, default=generate_uuid)
    tenant_id = Column(String(36), ForeignKey("tenants.id"), nullable=False)
    name = Column(String(100), default="Primary CRM")
    provider = Column(String(50), default="webhook") # webhook, google_sheets, zoho, custom_api
    lookup_url = Column(String(512), nullable=True) # e.g. https://api.hospital.com/v1/patient/lookup
    lookup_method = Column(String(10), default="GET")
    auth_header_name = Column(String(100), default="Authorization")
    auth_token = Column(String(255), nullable=True)
    sync_url = Column(String(512), nullable=True) # Webhook to update Google Sheets / CRM
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


class RechargeRequest(Base):
    __tablename__ = "recharge_requests"
    id = Column(String(36), primary_key=True, default=generate_uuid)
    tenant_id = Column(String(36), ForeignKey("tenants.id"), index=True, nullable=False)
    user_id = Column(String(36), ForeignKey("users.id"), index=True, nullable=False)
    amount_inr = Column(Float, nullable=False)
    utr_number = Column(String(64), nullable=False, index=True)
    payment_method = Column(String(32), default="UPI_QR")
    status = Column(String(32), default="PENDING") # PENDING, APPROVED, REJECTED
    admin_notes = Column(String(255), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

class Invoice(Base):
    __tablename__ = "invoices"
    id = Column(String(36), primary_key=True, default=generate_uuid)
    invoice_number = Column(String(64), unique=True, index=True, nullable=False) # e.g. INV-2026-1001
    tenant_id = Column(String(36), ForeignKey("tenants.id"), index=True, nullable=False)
    user_id = Column(String(36), ForeignKey("users.id"), index=True, nullable=False)
    recharge_request_id = Column(String(36), nullable=True)
    amount_inr = Column(Float, nullable=False)
    gst_rate = Column(Float, default=0.0) # 0.0% GST Exempt
    gst_amount_inr = Column(Float, default=0.0)
    total_amount_inr = Column(Float, nullable=False)
    payment_method = Column(String(32), default="UPI")
    utr_number = Column(String(64), nullable=True)
    status = Column(String(32), default="PAID")
    created_at = Column(DateTime, default=datetime.utcnow)

# ==============================================================================
# 4. SCHEMAS
# ==============================================================================
class CRMIntegrationCreate(BaseModel):
    name: Optional[str] = "Primary CRM"
    provider: Optional[str] = "webhook"
    lookup_url: Optional[str] = None
    lookup_method: Optional[str] = "GET"
    auth_header_name: Optional[str] = "Authorization"
    auth_token: Optional[str] = None
    sync_url: Optional[str] = None
    is_active: Optional[bool] = True

class CRMLookupTestRequest(BaseModel):
    query_type: str = "phone" # phone, bill_id, patient_id
    query_value: str

class UpiRechargeSubmitRequest(BaseModel):
    amount_inr: float
    utr_number: str
    notes: Optional[str] = None

class RechargeApprovalRequest(BaseModel):
    notes: Optional[str] = None

class RechargeRejectRequest(BaseModel):
    reason: Optional[str] = "Invalid or unverified UTR"


class VerifyOTPOpRequest(BaseModel):
    email: str
    otp: str

class ResendOTPRequest(BaseModel):
    email: str
    purpose: Optional[str] = "signup"

class ForgotPasswordRequest(BaseModel):
    email: str

class ResetPasswordRequest(BaseModel):
    email: str
    otp: str
    new_password: str

class UserRegister(BaseModel):
    email: str
    password: str
    company_name: str
    full_name: Optional[str] = None

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
# ==============================================================================
# 5. EMAIL SERVICE (HOSTINGER SMTP - bot@mybotgenie.in)
# ==============================================================================
OTP_STORE: Dict[str, Dict[str, Any]] = {}

def is_valid_email(email: str) -> bool:
    if not email or "@" not in email:
        return False
    clean = email.strip().lower()
    pattern = r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$"
    if not re.match(pattern, clean):
        return False
    domain = clean.split("@")[-1]
    parts = domain.split(".")
    return len(parts) >= 2 and len(parts[-1]) >= 2

def generate_6digit_otp() -> str:
    import random
    return f"{random.randint(100000, 999999)}"

class EmailService:
    @staticmethod
    async def send_via_hostinger_relay(to_email: str, subject: str, html_content: str) -> bool:
        """
        Dispatches email via Hostinger HTTPS Mail Relay (Port 443).
        Completely bypasses cloud outbound raw SMTP socket blocks (Errno 101).
        """
        relay_url = os.getenv("MAIL_RELAY_URL", "https://call.mybotgenie.in/send_mail.php")
        relay_token = os.getenv("MAIL_RELAY_TOKEN", "cg_mail_secret_mybotgenie_2026")
        
        try:
            async with httpx.AsyncClient(timeout=15.0) as client:
                res = await client.post(
                    relay_url,
                    headers={
                        "Content-Type": "application/json",
                        "X-CallGenie-Mail-Token": relay_token
                    },
                    json={
                        "to": to_email,
                        "subject": subject,
                        "html": html_content,
                        "from_name": settings.SMTP_FROM_NAME
                    }
                )
                if res.status_code == 200:
                    logger.info(f"Email delivered successfully to {to_email} via Hostinger HTTPS Relay!")
                    return True
                else:
                    logger.warning(f"Hostinger HTTPS Relay returned {res.status_code}: {res.text}")
                    return False
        except Exception as e:
            logger.warning(f"Hostinger HTTPS Relay request failed: {e}")
            return False

    @staticmethod
    async def send_via_resend(to_email: str, subject: str, html_content: str) -> bool:
        """Sends email via Resend REST API (Port 443) if RESEND_API_KEY is configured."""
        resend_key = os.getenv("RESEND_API_KEY")
        if not resend_key:
            return False

        try:
            async with httpx.AsyncClient(timeout=12.0) as client:
                res = await client.post(
                    "https://api.resend.com/emails",
                    headers={
                        "Authorization": f"Bearer {resend_key}",
                        "Content-Type": "application/json"
                    },
                    json={
                        "from": f"{settings.SMTP_FROM_NAME} <bot@mybotgenie.in>",
                        "to": [to_email],
                        "subject": subject,
                        "html": html_content
                    }
                )
                if res.status_code in [200, 201]:
                    logger.info(f"Email delivered successfully via Resend API to {to_email}")
                    return True
                else:
                    logger.warning(f"Resend API returned {res.status_code}: {res.text}")
                    return False
        except Exception as e:
            logger.warning(f"Resend API request failed: {e}")
            return False

    @staticmethod
    def _send_smtp_sync(to_email: str, subject: str, html_content: str, text_content: str) -> bool:
        if not settings.SMTP_USER or not settings.SMTP_PASSWORD:
            logger.warning("SMTP credentials not configured. Skipping email send.")
            return False

        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = f"{settings.SMTP_FROM_NAME} <{settings.SMTP_USER}>"
        msg["To"] = to_email

        msg.attach(MIMEText(text_content, "plain", "utf-8"))
        msg.attach(MIMEText(html_content, "html", "utf-8"))

        try:
            context = ssl.create_default_context()
            with smtplib.SMTP_SSL(settings.SMTP_HOST, settings.SMTP_PORT, context=context, timeout=12.0) as server:
                server.login(settings.SMTP_USER, settings.SMTP_PASSWORD)
                server.sendmail(settings.SMTP_USER, [to_email], msg.as_string())
            logger.info(f"Verification email delivered successfully to {to_email} via Port {settings.SMTP_PORT}")
            return True
        except Exception as e:
            logger.error(f"Primary SMTP (Port {settings.SMTP_PORT}) error to {to_email}: {e}. Retrying Port 587 TLS...")
            try:
                with smtplib.SMTP(settings.SMTP_HOST, 587, timeout=12.0) as server:
                    server.ehlo()
                    server.starttls()
                    server.ehlo()
                    server.login(settings.SMTP_USER, settings.SMTP_PASSWORD)
                    server.sendmail(settings.SMTP_USER, [to_email], msg.as_string())
                logger.info(f"Fallback SMTP (Port 587) delivered successfully to {to_email}")
                return True
            except Exception as e2:
                logger.error(f"Fallback SMTP also failed to {to_email}: {e2}")
                return False

    @staticmethod
    async def send_otp_email(to_email: str, otp: str, purpose: str = "signup") -> bool:
        if purpose == "signup":
            subject = f"Verify your Call Genie Account: {otp}"
            headline = "Welcome to Call Genie by MyBot Genie!"
            subheadline = "Please verify your email address to activate your account and claim your ₹100 Free Test Credits."
        else:
            subject = f"Your Call Genie Password Reset Code: {otp}"
            headline = "Password Reset Request"
            subheadline = "We received a request to reset your password. Use the 6-digit verification code below to proceed."

        html_content = f"""<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <style>
    body {{ font-family: 'Helvetica Neue', Helvetica, Arial, sans-serif; background-color: #0b0f19; margin: 0; padding: 20px; }}
    .container {{ max-width: 520px; margin: 0 auto; background: #111827; border-radius: 16px; border: 1px solid #1f2937; overflow: hidden; color: #f9fafb; }}
    .header {{ padding: 32px 24px 20px; text-align: center; border-bottom: 1px solid #1f2937; background: #0f172a; }}
    .logo-text {{ font-size: 24px; font-weight: 800; color: #ffffff; letter-spacing: -0.5px; margin: 0; }}
    .logo-sub {{ font-size: 11px; color: #818cf8; text-transform: uppercase; font-weight: 700; letter-spacing: 1px; margin-top: 4px; }}
    .content {{ padding: 32px 24px; text-align: center; }}
    .headline {{ font-size: 20px; font-weight: 700; color: #ffffff; margin-bottom: 8px; }}
    .subheadline {{ font-size: 13px; color: #9ca3af; line-height: 1.5; margin-bottom: 28px; }}
    .otp-box {{ background: #1e1b4b; border: 2px dashed #6366f1; border-radius: 12px; padding: 18px 24px; display: inline-block; margin-bottom: 24px; }}
    .otp-code {{ font-size: 36px; font-weight: 800; color: #a5b4fc; letter-spacing: 8px; font-family: monospace; margin: 0; }}
    .warning {{ font-size: 12px; color: #ef4444; margin-top: 8px; font-weight: 500; }}
    .footer {{ padding: 20px 24px; text-align: center; border-top: 1px solid #1f2937; background: #0b0f19; font-size: 11px; color: #6b7280; }}
  </style>
</head>
<body>
  <div class="container">
    <div class="header">
      <div class="logo-text">⚡ Call Genie</div>
      <div class="logo-sub">BY MYBOT GENIE • INDORE</div>
    </div>
    <div class="content">
      <div class="headline">{headline}</div>
      <div class="subheadline">{subheadline}</div>
      <div class="otp-box">
        <div class="otp-code">{otp}</div>
      </div>
      <div class="warning">⏱ This code is valid for 10 minutes. Do not share it with anyone.</div>
    </div>
    <div class="footer">
      MyBot Genie, Indore (MP) 452010 | Helpline: +91 94795 80001<br/>
      Need help? Write to hello@mybotgenie.in
    </div>
  </div>
</body>
</html>"""

        # Priority 1: Hostinger HTTPS Relay (Port 443 - Never blocked on Render)
        sent = await EmailService.send_via_hostinger_relay(to_email, subject, html_content)
        if sent:
            return True

        # Priority 2: Resend API (if configured in Render)
        sent_resend = await EmailService.send_via_resend(to_email, subject, html_content)
        if sent_resend:
            return True

        # Priority 3: Direct SMTP Fallback
        return EmailService._send_smtp_sync(to_email, subject, html_content, f"Your verification code is: {otp}")

@app.post("/api/auth/register")
async def register(user_in: UserRegister, bg: BackgroundTasks, db: AsyncSession = Depends(get_db)):
    clean_email = user_in.email.strip().lower()
    if not is_valid_email(clean_email):
        raise HTTPException(status_code=400, detail="Please enter a valid email address with a domain (e.g. name@gmail.com). UPI handles are not allowed.")
    if len(user_in.password) < 6:
        raise HTTPException(status_code=400, detail="Password must be at least 6 characters long.")

    existing = (await db.execute(select(User).where(User.email == clean_email))).scalar_one_or_none()
    if existing and existing.is_verified:
        raise HTTPException(status_code=400, detail="This email is already registered and verified. Please go to Login.")

    # Generate 6-digit OTP
    otp = generate_6digit_otp()
    logger.info(f"==================================================")
    logger.info(f"🔑 [CALL GENIE OTP] Signup code for {clean_email}: {otp}")
    logger.info(f"==================================================")
    OTP_STORE[clean_email] = {
        "otp": otp,
        "purpose": "signup",
        "expires_at": datetime.utcnow() + timedelta(minutes=10),
        "data": {
            "company_name": user_in.company_name.strip(),
            "full_name": (user_in.full_name or "").strip(),
            "password": user_in.password
        }
    }
    
    # Send verification email asynchronously
    bg.add_task(EmailService.send_otp_email, clean_email, otp, "signup")
    return {"status": "otp_sent", "email": clean_email, "otp": otp, "message": f"A 6-digit verification code has been dispatched. (OTP: {otp})"}

@app.post("/api/auth/verify-signup-otp", response_model=TokenResponse)
async def verify_signup_otp(req: VerifyOTPOpRequest, db: AsyncSession = Depends(get_db)):
    clean_email = req.email.strip().lower()
    entry = OTP_STORE.get(clean_email)
    if not entry or entry.get("purpose") != "signup":
        raise HTTPException(status_code=400, detail="No pending signup found for this email. Please click Sign Up again.")
    if datetime.utcnow() > entry["expires_at"]:
        del OTP_STORE[clean_email]
        raise HTTPException(status_code=400, detail="Verification code has expired. Please request a new one.")
    if entry["otp"] != req.otp.strip():
        raise HTTPException(status_code=400, detail="Incorrect verification code. Please check your email and enter the 6-digit code.")

    temp_data = entry["data"]
    existing = (await db.execute(select(User).where(User.email == clean_email))).scalar_one_or_none()
    if existing:
        existing.hashed_password = get_password_hash(temp_data["password"])
        existing.full_name = temp_data["full_name"]
        existing.is_verified = True
        user = existing
        tenant = (await db.execute(select(Tenant).where(Tenant.id == user.tenant_id))).scalar_one_or_none()
    else:
        tenant = Tenant(name=temp_data["company_name"])
        db.add(tenant); await db.flush()
        user = User(tenant_id=tenant.id, email=clean_email, hashed_password=get_password_hash(temp_data["password"]), full_name=temp_data["full_name"], is_verified=True)
        db.add(user); db.add(Wallet(tenant_id=tenant.id, balance_inr=100.0))
        await db.commit(); await db.refresh(user)

    if clean_email in OTP_STORE:
        del OTP_STORE[clean_email]

    return {
        "access_token": create_access_token({"sub": user.id, "tenant_id": user.tenant_id}),
        "token_type": "bearer",
        "tenant_id": user.tenant_id,
        "email": user.email,
        "company_name": tenant.name if tenant else temp_data["company_name"]
    }

@app.post("/api/auth/resend-otp")
async def resend_otp(req: ResendOTPRequest, bg: BackgroundTasks):
    clean_email = req.email.strip().lower()
    if not is_valid_email(clean_email):
        raise HTTPException(status_code=400, detail="Invalid email address.")
    otp = generate_6digit_otp()
    if clean_email in OTP_STORE:
        OTP_STORE[clean_email]["otp"] = otp
        OTP_STORE[clean_email]["expires_at"] = datetime.utcnow() + timedelta(minutes=10)
    else:
        OTP_STORE[clean_email] = {
            "otp": otp,
            "purpose": req.purpose or "signup",
            "expires_at": datetime.utcnow() + timedelta(minutes=10),
            "data": {}
        }
    bg.add_task(EmailService.send_otp_email, clean_email, otp, req.purpose or "signup")
    return {"status": "otp_resent", "message": f"A new verification code has been dispatched to {clean_email}."}

@app.post("/api/auth/login", response_model=TokenResponse)
async def login(user_in: UserLogin, db: AsyncSession = Depends(get_db)):
    clean_email = user_in.email.strip().lower()
    if not is_valid_email(clean_email):
        raise HTTPException(status_code=400, detail="Please enter a valid email address.")

    user = (await db.execute(select(User).where(User.email == clean_email))).scalar_one_or_none()
    if not user or not verify_password(user_in.password, user.hashed_password):
        raise HTTPException(status_code=400, detail="Invalid email or password. Please check your credentials.")
    
    tenant = (await db.execute(select(Tenant).where(Tenant.id == user.tenant_id))).scalar_one_or_none()
    return {
        "access_token": create_access_token({"sub": user.id, "tenant_id": user.tenant_id}),
        "token_type": "bearer",
        "tenant_id": user.tenant_id,
        "email": user.email,
        "company_name": tenant.name if tenant else "Client"
    }

@app.post("/api/auth/forgot-password")
async def forgot_password(req: ForgotPasswordRequest, bg: BackgroundTasks, db: AsyncSession = Depends(get_db)):
    clean_email = req.email.strip().lower()
    if not is_valid_email(clean_email):
        raise HTTPException(status_code=400, detail="Please enter a valid email address.")
    user = (await db.execute(select(User).where(User.email == clean_email))).scalar_one_or_none()
    if not user:
        raise HTTPException(status_code=404, detail="No Call Genie account found with this email address.")

    otp = generate_6digit_otp()
    OTP_STORE[clean_email] = {
        "otp": otp,
        "purpose": "reset_password",
        "expires_at": datetime.utcnow() + timedelta(minutes=10),
        "data": {"user_id": user.id}
    }
    bg.add_task(EmailService.send_otp_email, clean_email, otp, "reset_password")
    return {"status": "reset_otp_sent", "email": clean_email, "message": f"Password reset code has been sent to {clean_email}."}

@app.post("/api/auth/reset-password")
async def reset_password(req: ResetPasswordRequest, db: AsyncSession = Depends(get_db)):
    clean_email = req.email.strip().lower()
    entry = OTP_STORE.get(clean_email)
    if not entry or entry.get("purpose") != "reset_password":
        raise HTTPException(status_code=400, detail="No active password reset request found. Please request a new code.")
    if datetime.utcnow() > entry["expires_at"]:
        del OTP_STORE[clean_email]
        raise HTTPException(status_code=400, detail="Password reset code has expired. Please request a new one.")
    if entry["otp"] != req.otp.strip():
        raise HTTPException(status_code=400, detail="Incorrect verification code. Please check your email.")
    if len(req.new_password) < 6:
        raise HTTPException(status_code=400, detail="New password must be at least 6 characters long.")

    user = (await db.execute(select(User).where(User.email == clean_email))).scalar_one_or_none()
    if not user:
        raise HTTPException(status_code=404, detail="User not found.")
    user.hashed_password = get_password_hash(req.new_password)
    await db.commit()

    if clean_email in OTP_STORE:
        del OTP_STORE[clean_email]
    return {"status": "password_reset_success", "message": "Password updated successfully! Please log in with your new password."}

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

@app.post("/api/integrations/crm")
async def save_crm_integration(crm_in: CRMIntegrationCreate, user: User = Depends(get_current_user), db: AsyncSession = Depends(get_db)):
    existing = (await db.execute(select(CRMIntegration).where(CRMIntegration.tenant_id == user.tenant_id))).scalar_one_or_none()
    if existing:
        for k, v in crm_in.model_dump(exclude_unset=True).items():
            setattr(existing, k, v)
        await db.commit(); await db.refresh(existing)
        return {"status": "updated", "crm": existing.name, "lookup_url": existing.lookup_url}
    else:
        new_int = CRMIntegration(tenant_id=user.tenant_id, **crm_in.model_dump())
        db.add(new_int); await db.commit(); await db.refresh(new_int)
        return {"status": "created", "crm": new_int.name, "lookup_url": new_int.lookup_url}

@app.get("/api/integrations/crm")
async def get_crm_integration(user: User = Depends(get_current_user), db: AsyncSession = Depends(get_db)):
    existing = (await db.execute(select(CRMIntegration).where(CRMIntegration.tenant_id == user.tenant_id))).scalar_one_or_none()
    if not existing:
        return {"status": "not_configured", "lookup_url": None, "sync_url": None}
    return {
        "status": "configured",
        "name": existing.name,
        "provider": existing.provider,
        "lookup_url": existing.lookup_url,
        "lookup_method": existing.lookup_method,
        "sync_url": existing.sync_url,
        "is_active": existing.is_active
    }

@app.post("/api/integrations/crm/test-lookup")
async def test_crm_lookup(req: CRMLookupTestRequest, user: User = Depends(get_current_user), db: AsyncSession = Depends(get_db)):
    res = await CRMService.lookup_customer_record(user.tenant_id, req.query_type, req.query_value, db)
    return {"query": req.model_dump(), "result": res or {"found": False, "message": "No record found for provided query."}}

@app.post("/api/integrations/api-keys")
async def make_key(user: User = Depends(get_current_user), db: AsyncSession = Depends(get_db)):
    raw = f"cg_live_{secrets.token_urlsafe(32)}"
    key_obj = ApiKey(tenant_id=user.tenant_id, key=raw)
    db.add(key_obj); await db.commit(); await db.refresh(key_obj)
    return {"key": raw}

# ==============================================================================
# 11. SAAS OWNER / MASTER SUPER ADMIN CONTROL PANEL
# ==============================================================================
ADMIN_EMAILS = [
    "admin@mybotgenie.in", "sarvesh@mybotgenie.in", "bot@mybotgenie.in",
    "sarveshawasthi@gmail.com", "ne.sarvesh@gmail.com"
]

async def get_super_admin_user(current_user: User = Depends(get_current_user)) -> User:
    if current_user.role == "super_admin" or current_user.email.lower() in ADMIN_EMAILS:
        return current_user
    raise HTTPException(status_code=403, detail="Access denied. Super Admin privileges required.")

@app.get("/api/admin/metrics")
async def get_admin_metrics(admin: User = Depends(get_super_admin_user), db: AsyncSession = Depends(get_db)):
    total_tenants = len((await db.execute(select(Tenant))).scalars().all())
    total_users = len((await db.execute(select(User))).scalars().all())
    calls = (await db.execute(select(CallLog))).scalars().all()
    total_calls = len(calls)
    total_billed_mins = sum(c.billed_minutes or 0 for c in calls)
    total_revenue_inr = sum(c.cost_inr or 0 for c in calls)
    wallets = (await db.execute(select(Wallet))).scalars().all()
    total_wallet_balances = sum(w.balance_inr or 0 for w in wallets)

    estimated_cogs_inr = total_billed_mins * 1.00
    estimated_profit_inr = max(0.0, total_revenue_inr - estimated_cogs_inr)

    return {
        "total_tenants": total_tenants,
        "total_users": total_users,
        "total_calls": total_calls,
        "total_billed_minutes": total_billed_mins,
        "total_revenue_inr": round(total_revenue_inr, 2),
        "total_cogs_inr": round(estimated_cogs_inr, 2),
        "net_profit_inr": round(estimated_profit_inr, 2),
        "total_wallet_pool_inr": round(total_wallet_balances, 2),
        "smtp_status": bool(settings.SMTP_USER and settings.SMTP_PASSWORD),
        "whatsapp_status": bool(settings.WHATSAPP_TOKEN and settings.WHATSAPP_PHONE_NUMBER_ID),
        "groq_status": bool(settings.GROQ_API_KEY),
        "sarvam_status": bool(settings.SARVAM_API_KEY)
    }

@app.get("/api/admin/tenants")
async def get_admin_tenants(admin: User = Depends(get_super_admin_user), db: AsyncSession = Depends(get_db)):
    tenants = (await db.execute(select(Tenant).order_by(desc(Tenant.created_at)))).scalars().all()
    results = []
    for t in tenants:
        u = (await db.execute(select(User).where(User.tenant_id == t.id))).scalar_one_or_none()
        w = (await db.execute(select(Wallet).where(Wallet.tenant_id == t.id))).scalar_one_or_none()
        c_count = len((await db.execute(select(CallLog).where(CallLog.tenant_id == t.id))).scalars().all())
        results.append({
            "id": t.id,
            "name": t.name,
            "email": u.email if u else "N/A",
            "owner_name": u.full_name if u else "N/A",
            "rate_per_min_inr": t.rate_per_min_inr,
            "wallet_balance_inr": round(w.balance_inr, 2) if w else 0.0,
            "total_calls": c_count,
            "is_active": t.is_active,
            "created_at": t.created_at
        })
    return results

class AdminWalletActionRequest(BaseModel):
    amount_inr: float
    action_type: str = "CREDIT"
    reference: Optional[str] = "Manual SaaS Owner Adjustment"

@app.post("/api/admin/tenants/{tenant_id}/wallet")
async def admin_adjust_wallet(tenant_id: str, req: AdminWalletActionRequest, admin: User = Depends(get_super_admin_user), db: AsyncSession = Depends(get_db)):
    w = (await db.execute(select(Wallet).where(Wallet.tenant_id == tenant_id))).scalar_one_or_none()
    if not w:
        w = Wallet(tenant_id=tenant_id, balance_inr=0.0)
        db.add(w)

    if req.action_type == "CREDIT":
        w.balance_inr += req.amount_inr
        w.total_recharged_inr += req.amount_inr
    else:
        w.balance_inr = max(0.0, w.balance_inr - req.amount_inr)

    tx = WalletTransaction(
        wallet_id=w.id,
        transaction_type=f"ADMIN_{req.action_type}",
        amount_inr=req.amount_inr,
        balance_after_inr=w.balance_inr,
        reference=req.reference
    )
    db.add(tx)
    await db.commit()
    return {"new_balance_inr": round(w.balance_inr, 2)}

class AdminRateChangeRequest(BaseModel):
    rate_per_min_inr: float

@app.put("/api/admin/tenants/{tenant_id}/rate")
async def admin_change_rate(tenant_id: str, req: AdminRateChangeRequest, admin: User = Depends(get_super_admin_user), db: AsyncSession = Depends(get_db)):
    t = (await db.execute(select(Tenant).where(Tenant.id == tenant_id))).scalar_one_or_none()
    if not t:
        raise HTTPException(status_code=404, detail="Tenant not found")
    t.rate_per_min_inr = req.rate_per_min_inr
    await db.commit()
    return {"rate_per_min_inr": t.rate_per_min_inr}


@app.post("/api/wallet/upi-recharge")
async def submit_upi_recharge(
    req: UpiRechargeSubmitRequest,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    utr = req.utr_number.strip().replace(" ", "")
    if len(utr) < 6:
        raise HTTPException(status_code=400, detail="कृपया मान्य 12-अंकों का UPI UTR / Reference नंबर दर्ज करें।")
    if req.amount_inr < 50:
        raise HTTPException(status_code=400, detail="न्यूनतम रीचार्ज राशि ₹50 है।")

    existing = (await db.execute(select(RechargeRequest).where(RechargeRequest.utr_number == utr))).scalar_one_or_none()
    if existing:
        raise HTTPException(status_code=400, detail=f"यह UTR नंबर ({utr}) पहले ही सबमिट किया जा चुका है (स्थिति: {existing.status})।")

    rec = RechargeRequest(
        tenant_id=user.tenant_id,
        user_id=user.id,
        amount_inr=float(req.amount_inr),
        utr_number=utr,
        payment_method="UPI_QR",
        status="PENDING",
        admin_notes=req.notes
    )
    db.add(rec)
    await db.commit()
    await db.refresh(rec)
    return {
        "status": "success",
        "message": "UPI रीचार्ज अनुरोध सफलतापूर्वक सबमिट किया गया। एडमिन द्वारा UTR सत्यापन के बाद राशि वॉलेट में जुड़ जाएगी।",
        "request_id": rec.id,
        "amount_inr": rec.amount_inr,
        "utr_number": rec.utr_number
    }

@app.get("/api/wallet/invoices")
async def list_user_invoices(
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    invoices = (await db.execute(
        select(Invoice).where(Invoice.tenant_id == user.tenant_id).order_by(desc(Invoice.created_at))
    )).scalars().all()
    return [
        {
            "id": inv.id,
            "invoice_number": inv.invoice_number,
            "amount_inr": inv.amount_inr,
            "total_amount_inr": inv.total_amount_inr,
            "payment_method": inv.payment_method,
            "utr_number": inv.utr_number,
            "status": inv.status,
            "created_at": inv.created_at.strftime("%Y-%m-%d %H:%M") if inv.created_at else "",
            "download_url": f"/api/wallet/invoices/{inv.id}/download"
        }
        for inv in invoices
    ]

@app.get("/api/wallet/invoices/{invoice_id}/download")
async def download_invoice_pdf(
    invoice_id: str,
    token: Optional[str] = Query(None),
    authorization: Optional[str] = Header(None),
    db: AsyncSession = Depends(get_db)
):
    user_email = None
    # Check Bearer token in header
    if authorization and authorization.startswith("Bearer "):
        auth_token = authorization.split(" ")[1]
    else:
        auth_token = token

    if auth_token:
        try:
            payload = jwt.decode(auth_token, settings.SECRET_KEY, algorithms=[settings.ALGORITHM])
            user_email = payload.get("sub")
        except Exception:
            pass

    if not user_email:
        raise HTTPException(status_code=401, detail="Authentication required to download invoice.")

    current_user = (await db.execute(select(User).where(User.email == user_email))).scalar_one_or_none()
    if not current_user:
        raise HTTPException(status_code=401, detail="User not found.")

    inv = (await db.execute(select(Invoice).where(Invoice.id == invoice_id))).scalar_one_or_none()
    if not inv:
        raise HTTPException(status_code=404, detail="Invoice not found.")

    if inv.tenant_id != current_user.tenant_id and current_user.role != "super_admin":
        raise HTTPException(status_code=403, detail="Not authorized to access this invoice.")

    inv_user = (await db.execute(select(User).where(User.id == inv.user_id))).scalar_one_or_none() or current_user
    inv_tenant = (await db.execute(select(Tenant).where(Tenant.id == inv.tenant_id))).scalar_one_or_none()

    pdf_bytes = generate_bill_of_supply_pdf(inv, inv_user, inv_tenant)
    filename = f"{inv.invoice_number}.pdf"
    return Response(
        content=pdf_bytes,
        media_type="application/pdf",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'}
    )

@app.get("/api/admin/recharges")
async def admin_list_recharges(
    admin: User = Depends(get_super_admin_user),
    db: AsyncSession = Depends(get_db)
):
    recharges = (await db.execute(select(RechargeRequest).order_by(desc(RechargeRequest.created_at)))).scalars().all()
    out = []
    for r in recharges:
        u = (await db.execute(select(User).where(User.id == r.user_id))).scalar_one_or_none()
        t = (await db.execute(select(Tenant).where(Tenant.id == r.tenant_id))).scalar_one_or_none()
        out.append({
            "id": r.id,
            "tenant_id": r.tenant_id,
            "company_name": t.name if t else "Unknown",
            "user_name": u.full_name if u else "Unknown",
            "user_email": u.email if u else "Unknown",
            "amount_inr": r.amount_inr,
            "utr_number": r.utr_number,
            "payment_method": r.payment_method,
            "status": r.status,
            "admin_notes": r.admin_notes,
            "created_at": r.created_at.strftime("%Y-%m-%d %H:%M") if r.created_at else ""
        })
    return out

@app.post("/api/admin/recharges/{request_id}/approve")
async def admin_approve_recharge(
    request_id: str,
    req: Optional[RechargeApprovalRequest] = None,
    admin: User = Depends(get_super_admin_user),
    db: AsyncSession = Depends(get_db)
):
    r = (await db.execute(select(RechargeRequest).where(RechargeRequest.id == request_id))).scalar_one_or_none()
    if not r:
        raise HTTPException(status_code=404, detail="Recharge request not found")
    if r.status == "APPROVED":
        return {"status": "already_approved", "message": "Request already approved"}

    r.status = "APPROVED"
    if req and req.notes:
        r.admin_notes = req.notes

    new_bal = await BillingService.credit_wallet(db, r.tenant_id, r.amount_inr, f"UPI Recharge Verified (UTR: {r.utr_number})")

    count_inv = (await db.execute(select(func.count(Invoice.id)))).scalar_one() or 0
    inv_number = f"INV-2026-{1001 + count_inv}"

    inv = Invoice(
        invoice_number=inv_number,
        tenant_id=r.tenant_id,
        user_id=r.user_id,
        recharge_request_id=r.id,
        amount_inr=r.amount_inr,
        gst_rate=0.0,
        gst_amount_inr=0.0,
        total_amount_inr=r.amount_inr,
        payment_method="UPI_QR",
        utr_number=r.utr_number,
        status="PAID"
    )
    db.add(inv)
    await db.commit()
    await db.refresh(inv)

    return {
        "status": "approved",
        "new_balance_inr": round(new_bal, 2),
        "invoice_id": inv.id,
        "invoice_number": inv.invoice_number,
        "amount_inr": inv.amount_inr
    }

@app.post("/api/admin/recharges/{request_id}/reject")
async def admin_reject_recharge(
    request_id: str,
    req: Optional[RechargeRejectRequest] = None,
    admin: User = Depends(get_super_admin_user),
    db: AsyncSession = Depends(get_db)
):
    r = (await db.execute(select(RechargeRequest).where(RechargeRequest.id == request_id))).scalar_one_or_none()
    if not r:
        raise HTTPException(status_code=404, detail="Recharge request not found")
    r.status = "REJECTED"
    if req and req.reason:
        r.admin_notes = req.reason
    await db.commit()
    return {"status": "rejected", "message": "Recharge request rejected"}
