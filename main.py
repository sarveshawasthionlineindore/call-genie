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
            # 1. Primary Port 465 (SSL)
            context = ssl.create_default_context()
            with smtplib.SMTP_SSL(settings.SMTP_HOST, settings.SMTP_PORT, context=context, timeout=12.0) as server:
                server.login(settings.SMTP_USER, settings.SMTP_PASSWORD)
                server.sendmail(settings.SMTP_USER, [to_email], msg.as_string())
            logger.info(f"Verification email delivered successfully to {to_email} via Port {settings.SMTP_PORT}")
            return True
        except Exception as e:
            logger.error(f"Primary SMTP (Port {settings.SMTP_PORT}) error to {to_email}: {e}. Retrying Port 587 TLS...")
            try:
                # 2. Fallback Port 587 (STARTTLS)
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
    .subhead {{ font-size: 14px; color: #9ca3af; line-height: 1.5; margin-bottom: 24px; }}
    .otp-card {{ background: #1e1b4b; border: 1px solid #4338ca; border-radius: 12px; padding: 18px 24px; margin: 16px auto; display: inline-block; min-width: 220px; }}
    .otp-code {{ font-size: 34px; font-weight: 800; color: #a5b4fc; letter-spacing: 8px; margin: 0; font-family: monospace; }}
    .expiry {{ font-size: 12px; color: #9ca3af; margin-top: 20px; }}
    .footer {{ padding: 20px; text-align: center; font-size: 11px; color: #6b7280; border-top: 1px solid #1f2937; background: #0b0f19; }}
  </style>
</head>
<body>
  <div class="container">
    <div class="header">
      <div class="logo-text">Call Genie</div>
      <div class="logo-sub">by MyBot Genie • Indore</div>
    </div>
    <div class="content">
      <div class="headline">{headline}</div>
      <div class="subhead">{subheadline}</div>
      <div class="otp-card">
        <div class="otp-code">{otp}</div>
      </div>
      <div class="expiry">This verification code is valid for <strong>10 minutes</strong>. Never share this code with anyone.</div>
    </div>
    <div class="footer">
      © 2026 MyBot Genie, Indore (M.P.), India. All rights reserved.<br>
      Automated Voice AI & WhatsApp Calling Solutions • call.mybotgenie.in
    </div>
  </div>
</body>
</html>"""
        text_content = f"{headline}\n\nYour 6-digit verification code is: {otp}\n\nValid for 10 minutes.\nTeam MyBot Genie, Indore"

        return await asyncio.to_thread(EmailService._send_smtp_sync, to_email, subject, html_content, text_content)

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

class CRMService:
    @staticmethod
    async def lookup_customer_record(tenant_id: Optional[str], query_type: str, query_value: str, db: Optional[AsyncSession] = None) -> Optional[Dict[str, Any]]:
        clean_val = "".join(filter(str.isdigit, str(query_value or "")))
        
        # 1. Check if tenant has a configured live CRM webhook URL
        if db and tenant_id:
            try:
                integration = (await db.execute(select(CRMIntegration).where(CRMIntegration.tenant_id == tenant_id, CRMIntegration.is_active == True))).scalar_one_or_none()
                if integration and integration.lookup_url:
                    headers = {"Content-Type": "application/json"}
                    if integration.auth_token:
                        headers[integration.auth_header_name or "Authorization"] = integration.auth_token
                    
                    async with httpx.AsyncClient(timeout=4.0) as client:
                        if integration.lookup_method.upper() == "POST":
                            res = await client.post(integration.lookup_url, json={"query_type": query_type, "query_value": clean_val}, headers=headers)
                        else:
                            res = await client.get(integration.lookup_url, params={"query_type": query_type, "value": clean_val}, headers=headers)
                        if res.status_code == 200:
                            return res.json()
            except Exception as e:
                logger.warning(f"Live CRM webhook lookup exception: {e}")
                
        # 2. Built-in Smart Registry for Zero-Failure Demo / Immediate Client Testing
        if len(clean_val) == 10:
            name = "सर्वेश अवस्थी" if clean_val.endswith("9851") else "राहुल शर्मा"
            return {
                "found": True,
                "customer_name": name,
                "registered_phone": clean_val,
                "record_type": "हॉस्पिटल लैब रिपोर्ट (Blood Test / CBC)",
                "status": "Ready (डॉक्टर द्वारा वेरिफ़ाई)",
                "summary": "सीबीसी और थाइरॉइड टेस्ट सामान्य है, हीमोग्लोबिन 13.5 है।",
                "doctor": "डॉक्टर शर्मा",
                "pdf_url": f"https://call.mybotgenie.in/reports/{clean_val[-4:]}.pdf",
                "pending_amount_inr": 0.0
            }
        elif len(clean_val) in [3, 4, 5]: # e.g. Bill / Job card 4092, 3021
            return {
                "found": True,
                "customer_name": "सर्वेश जी",
                "bill_no": clean_val,
                "record_type": "ऑटोमोबाइल सर्विस जॉब कार्ड",
                "status": "वाशिंग व फाइनल इंस्पेक्शन पूरा, डिलीवरी रेडी",
                "vehicle": "हुंडई क्रेटा (MP-09)",
                "total_amount_inr": 4850.0,
                "gatepass_url": f"https://call.mybotgenie.in/invoices/{clean_val}.pdf"
            }
            
        return None

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


def generate_bill_of_supply_pdf(invoice: Invoice, user: User, tenant: Optional[Tenant] = None) -> bytes:
    """
    Generates a legally compliant Bill of Supply & Payment Receipt for Indian Non-GST suppliers
    under Section 31(3)(c) and Section 22 of the CGST Act, 2017.
    """
    import io
    from reportlab.lib.pagesizes import A4
    from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, HRFlowable
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib import colors

    buf = io.BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=A4, leftMargin=36, rightMargin=36, topMargin=36, bottomMargin=36)
    styles = getSampleStyleSheet()

    legal_notice_style = ParagraphStyle('LegalNotice', parent=styles['Normal'], fontSize=8, leading=11, textColor=colors.HexColor('#475569'), fontName='Helvetica-Oblique')
    cell_bold = ParagraphStyle('CellBold', parent=styles['Normal'], fontSize=9, leading=12, fontName='Helvetica-Bold', textColor=colors.HexColor('#0f172a'))
    cell_norm = ParagraphStyle('CellNorm', parent=styles['Normal'], fontSize=9, leading=12, textColor=colors.HexColor('#334155'))
    cell_center = ParagraphStyle('CellCenter', parent=cell_norm, alignment=1)
    cell_right = ParagraphStyle('CellRight', parent=cell_norm, alignment=2)
    cell_bold_right = ParagraphStyle('CellBoldRight', parent=cell_bold, alignment=2)

    elements = []

    supplier_html = (
        '<b>MYBOT GENIE</b><br/>'
        'Indore, Madhya Pradesh - 452010, India<br/>'
        '<b>Helpline:</b> +91 94795 80001<br/>'
        '<b>Email:</b> hello@mybotgenie.in<br/>'
        '<b>UPI ID:</b> real.sarvesh@ybl | <b>UPI No:</b> 9425329851'
    )
    supplier_p = Paragraph(supplier_html, cell_norm)

    date_str = invoice.created_at.strftime('%d-%b-%Y') if invoice.created_at else datetime.utcnow().strftime('%d-%b-%Y')
    invoice_meta_html = (
        f"<font size='14' color='#1e1b4b'><b>BILL OF SUPPLY</b></font><br/>"
        f"<b>Invoice No:</b> {invoice.invoice_number}<br/>"
        f"<b>Date:</b> {date_str}<br/>"
        f"<b>Payment Mode:</b> UPI Transfer<br/>"
        f"<b>UTR / Ref:</b> {invoice.utr_number or 'Direct'}<br/>"
        f"<font color='#059669'><b>Status: PAID & VERIFIED</b></font>"
    )
    meta_p = Paragraph(invoice_meta_html, cell_right)

    header_table = Table([[supplier_p, meta_p]], colWidths=[280, 240])
    header_table.setStyle(TableStyle([
        ('VALIGN', (0,0), (-1,-1), 'TOP'),
        ('LEFTPADDING', (0,0), (-1,-1), 0),
        ('RIGHTPADDING', (0,0), (-1,-1), 0),
        ('BOTTOMPADDING', (0,0), (-1,-1), 10),
    ]))
    elements.append(header_table)
    elements.append(HRFlowable(width='100%', thickness=1.5, color=colors.HexColor('#4338ca'), spaceAfter=8))

    exempt_text = (
        '<b>Statutory Exemption Notice:</b> Composition / Non-GST Supply. Issued in accordance with Section 31(3)(c) of CGST Act, 2017. '
        'Turnover is below the registration threshold under Section 22 of the CGST Act, 2017. No tax is payable on a reverse charge basis.'
    )
    exempt_table = Table([[Paragraph(exempt_text, legal_notice_style)]], colWidths=[520])
    exempt_table.setStyle(TableStyle([
        ('BACKGROUND', (0,0), (-1,-1), colors.HexColor('#f8fafc')),
        ('BOX', (0,0), (-1,-1), 0.5, colors.HexColor('#cbd5e1')),
        ('LEFTPADDING', (0,0), (-1,-1), 10),
        ('RIGHTPADDING', (0,0), (-1,-1), 10),
        ('TOPPADDING', (0,0), (-1,-1), 6),
        ('BOTTOMPADDING', (0,0), (-1,-1), 6),
    ]))
    elements.append(exempt_table)
    elements.append(Spacer(1, 14))

    cust_name = user.full_name or 'Valued Client'
    comp_name = tenant.name if tenant else 'Client Workspace'
    bill_to_html = (
        f"<b>Billed To:</b><br/>"
        f"<b>Name:</b> {cust_name}<br/>"
        f"<b>Organization / Business:</b> {comp_name}<br/>"
        f"<b>Email:</b> {user.email}<br/>"
        f"<b>Account ID:</b> {user.id[:8]}..."
    )
    elements.append(Paragraph(bill_to_html, cell_norm))
    elements.append(Spacer(1, 14))

    amt = invoice.amount_inr
    items_data = [
        [Paragraph('<b>Sr.</b>', cell_center), Paragraph('<b>Description of Services</b>', cell_bold), Paragraph('<b>HSN/SAC</b>', cell_center), Paragraph('<b>Tax Rate</b>', cell_center), Paragraph('<b>Amount (INR)</b>', cell_bold_right)],
        [Paragraph('1', cell_center), Paragraph(f"<b>Call Genie AI Voice Minutes & Wallet Recharge</b><br/><font color='#64748b' size='8'>Prepaid calling minutes balance credited. UPI UTR: {invoice.utr_number or 'Direct'}</font>", cell_norm), Paragraph('998313', cell_center), Paragraph('0% (Exempt)', cell_center), Paragraph(f'INR {amt:,.2f}', cell_right)],
        ['', '', '', Paragraph('<b>Subtotal:</b>', cell_bold_right), Paragraph(f'INR {amt:,.2f}', cell_right)],
        ['', '', '', Paragraph('<b>CGST (0%):</b>', cell_bold_right), Paragraph('INR 0.00', cell_right)],
        ['', '', '', Paragraph('<b>SGST (0%):</b>', cell_bold_right), Paragraph('INR 0.00', cell_right)],
        ['', '', '', Paragraph('<b>Total Amount Paid:</b>', cell_bold_right), Paragraph(f'<b>INR {amt:,.2f}</b>', cell_bold_right)],
    ]
    item_table = Table(items_data, colWidths=[30, 240, 70, 80, 100], style=[
        ('BACKGROUND', (0,0), (-1,0), colors.HexColor('#f1f5f9')),
        ('GRID', (0,0), (-1,1), 0.5, colors.HexColor('#cbd5e1')),
        ('LINEBELOW', (0,1), (-1,1), 1, colors.HexColor('#94a3b8')),
        ('TOPPADDING', (0,0), (-1,-1), 6),
        ('BOTTOMPADDING', (0,0), (-1,-1), 6),
        ('BACKGROUND', (3,5), (4,5), colors.HexColor('#e0e7ff')),
        ('BOX', (3,5), (4,5), 1, colors.HexColor('#6366f1')),
    ])
    elements.append(item_table)
    elements.append(Spacer(1, 18))

    stamp_html = (
        "<font color='#059669'><b>✔ VERIFIED & DIGITALLY RECORDED</b></font><br/>"
        f"Credited to Call Genie Wallet: INR {amt:,.2f}<br/>"
        f"Reference UTR: {invoice.utr_number or 'N/A'}<br/>"
        "Issued by MyBot Genie, Indore (MP)"
    )
    terms_html = (
        "<b>Terms & Conditions:</b><br/>"
        "1. This is a computer-generated Bill of Supply and requires no physical signature.<br/>"
        "2. Calling minutes are non-transferable and valid across all configured AI campaigns.<br/>"
        "3. For billing support, write to <b>hello@mybotgenie.in</b> or call <b>+91 94795 80001</b>."
    )
    bottom_table = Table([[Paragraph(terms_html, legal_notice_style), Paragraph(stamp_html, cell_norm)]], colWidths=[320, 200])
    bottom_table.setStyle(TableStyle([
        ('VALIGN', (0,0), (-1,-1), 'TOP'),
        ('BACKGROUND', (1,0), (1,0), colors.HexColor('#ecfdf5')),
        ('BOX', (1,0), (1,0), 1, colors.HexColor('#10b981')),
        ('LEFTPADDING', (1,0), (1,0), 10),
        ('RIGHTPADDING', (1,0), (1,0), 10),
        ('TOPPADDING', (1,0), (1,0), 8),
        ('BOTTOMPADDING', (1,0), (1,0), 8),
    ]))
    elements.append(bottom_table)

    doc.build(elements)
    return buf.getvalue()

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
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.middleware("http")
async def add_cors_headers(request: Request, call_next):
    if request.method == "OPTIONS":
        response = Response(status_code=204)
        response.headers["Access-Control-Allow-Origin"] = "*"
        response.headers["Access-Control-Allow-Methods"] = "GET, POST, PUT, DELETE, OPTIONS, HEAD"
        response.headers["Access-Control-Allow-Headers"] = "*"
        return response
    response = await call_next(request)
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Methods"] = "GET, POST, PUT, DELETE, OPTIONS, HEAD"
    response.headers["Access-Control-Allow-Headers"] = "*"
    return response

@app.on_event("startup")
async def on_startup():
    global engine, AsyncSessionLocal
    logger.info("Checking Call Genie Database...")
    try:
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        logger.info("Primary database connected and tables verified.")
    except Exception as e:
        logger.warning(f"Primary database connection failed ({e}). Switching to local SQLite database fallback...")
        try:
            sqlite_url = "sqlite+aiosqlite:///./call_genie.db"
            engine = create_async_engine(sqlite_url, echo=False)
            AsyncSessionLocal = async_sessionmaker(
                bind=engine,
                class_=AsyncSession,
                expire_on_commit=False,
                autocommit=False,
                autoflush=False
            )
            async with engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)
            logger.info("Successfully initialized and switched to SQLite fallback in resilient mode.")
        except Exception as sq_err:
            logger.error(f"Failed to initialize SQLite fallback: {sq_err}")

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

def extract_lookup_identifier(text: str) -> Dict[str, Any]:
    raw = (text or "").strip()
    digits_only = "".join(filter(str.isdigit, raw))
    
    # 1. 10-digit Indian phone number
    phone_match = re.search(r'[6-9]\d{9}', digits_only)
    if phone_match:
        return {"type": "phone", "value": phone_match.group(0)}
        
    # 2. Bill / Job Card / Token / Patient ID
    id_match = re.search(r'(?:bill|token|patient|job\s*card|पर्ची|बिल|टोकन|आईडी|नंबर|क्रमांक)\s*(?:no|number|id)?\s*[:#-]?\s*(\d{3,6})', raw, re.IGNORECASE)
    if id_match:
        return {"type": "id", "value": id_match.group(1)}
        
    # 3. Caller is asking for report/account history without providing an ID yet
    asks_history = any(w in raw.lower() for w in [
        "report", "रिपोर्ट", "blood test", "ब्लड टेस्ट", "hisab", "हिसाब", "pichhla", "पिछला",
        "baki", "बाकी", "balance", "बैलेंस", "bill", "बिल", "status", "स्टेटस", "parcha", "पर्चा"
    ])
    return {"type": "inquiry_no_id" if asks_history else "none", "value": None}

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
    if any(w in msg for w in ["report", "रिपोर्ट", "blood test", "ब्लड टेस्ट", "test result"]):
        digits = "".join(filter(str.isdigit, msg))
        if len(digits) >= 10:
            return f"जी {c_name} जी! आपके नंबर पर सीबीसी व थाइरॉइड टेस्ट रिपोर्ट रेडी है और नॉर्मल है। क्या मैं यह आपके इसी नंबर पर व्हाट्सएप कर दूँ?"
        elif len(digits) >= 3:
            return f"जी {c_name} जी! बिल नंबर {digits} का रिकॉर्ड मिल गया है। यह पूरी तरह रेडी है। क्या मैं डिटेल्स व्हाट्सएप पर भेज दूँ?"
        return "जी बिल्कुल! क्या आप अपना 10 अंकों का रजिस्टर्ड मोबाइल नंबर या पेशेंट/बिल आईडी बता सकते हैं, ताकि मैं रिकॉर्ड चेक करके तुरंत बता सकूँ?"
        
    if any(w in msg for w in ["hisab", "हिसाब", "baki", "बाकी", "balance", "बैलेंस", "bill", "बिल"]):
        digits = "".join(filter(str.isdigit, msg))
        if len(digits) >= 10:
            return f"जी {c_name} जी! आपके नंबर पर रिकॉर्ड चेक कर लिया गया है। कुल बकाया राशि शून्य है। क्या मैं स्टेटमेंट व्हाट्सएप कर दूँ?"
        return "जी बिल्कुल! कृपया अपना 10 अंकों का रजिस्टर्ड मोबाइल नंबर या बिल नंबर बताइये, मैं तुरंत रिकॉर्ड चेक करके बताती हूँ।"

    if any(w in msg for w in ["dhanyawad", "shukriya", "bye", "alvida", "theek hai", "ok", "thank"]):
        return f"ओह, थैंक यू सो मच {c_name} जी! आपसे बात करके बहुत अच्छा लगा। मैं व्हाट्सएप पर सारी डिटेल्स भेज रही हूँ!"
    return f"जी {c_name} जी, मैंने समझ लिया! आपके बिज़नेस के लिए यह कॉलिंग ऑटोमेशन बहुत जबरदस्त काम करेगा। क्या मैं इस पर व्हाट्सएप डिटेल्स भेज दूँ?"

@app.post("/api/voice/chat")
async def voice_chat_endpoint(chat_in: VoiceChatRequest):
    """
    Energetic, cheerful telecaller 'Riya' with 1-turn discovery and Sarvam Bulbul native voice.
    """
    discovered = discover_caller_name(chat_in.message)
    lookup_info = extract_lookup_identifier(chat_in.message)
    crm_context = ""
    discovered_crm_record = None

    if lookup_info["type"] in ["phone", "id"]:
        discovered_crm_record = await CRMService.lookup_customer_record(
            tenant_id=None,
            query_type=lookup_info["type"],
            query_value=lookup_info["value"]
        )
        if discovered_crm_record:
            cust_name = discovered_crm_record.get("customer_name")
            if cust_name:
                discovered = cust_name
            crm_name = discovered_crm_record.get('customer_name', 'Customer')
            crm_type = discovered_crm_record.get('record_type', 'Record')
            crm_stat = discovered_crm_record.get('status', 'Ready')
            crm_summ = discovered_crm_record.get('summary', 'Normal')
            crm_link = discovered_crm_record.get('pdf_url', 'Ready')
            crm_val = lookup_info['value']
            crm_context = f"\n[LIVE CRM RECORD FOUND FOR {crm_val}]: Customer: {crm_name}, Record: {crm_type}, Status: {crm_stat}, Summary: {crm_summ}, Link: {crm_link}. INSTRUCTION: Greet them by name, confirm record found for {crm_val}, summarize status cheerfully in 15-20 Hindi words, and offer WhatsApp link."
    elif lookup_info["type"] == "inquiry_no_id":
        crm_context = "\n[CALLER ASKING FOR REPORT/BILL WITHOUT ID]: Politely ask caller to speak their 10-digit registered mobile number or Patient/Bill ID so you can check immediately."

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
        f"{crm_context}"
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

SAMPLE_AUDIO_CACHE = {}

@app.get("/api/voice/sample-audio/{sample_id}")
async def get_sample_audio(sample_id: str):
    """Generates and serves authentic studio-quality Sarvam Bulbul audio with in-memory caching."""
    sample_key = sample_id.lower().strip()
    profile = SAMPLE_SCRIPTS.get(sample_key, SAMPLE_SCRIPTS["automotive" if "auto" in sample_key else "realestate"])
    
    # Return from cache if already generated (instant response!)
    if sample_key in SAMPLE_AUDIO_CACHE and SAMPLE_AUDIO_CACHE[sample_key]:
        return {
            "sample_id": sample_id,
            "audio_base64": SAMPLE_AUDIO_CACHE[sample_key],
            "text": profile["text"],
            "speaker": profile["speaker"],
            "cached": True,
            "status": "ready"
        }
    
    audio_base64 = None
    if settings.SARVAM_API_KEY:
        try:
            async with httpx.AsyncClient(timeout=12.0) as client:
                res = await client.post(
                    "https://api.sarvam.ai/text-to-speech",
                    headers={"api-subscription-key": settings.SARVAM_API_KEY, "Content-Type": "application/json"},
                    json={
                        "inputs": [profile["text"]],
                        "target_language_code": "hi-IN",
                        "speaker": profile["speaker"],
                        "model": "bulbul:v4-flash",
                        "pace": 1.12,
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
                            "pace": 1.12,
                            "speech_sample_rate": 16000,
                            "enable_preprocessing": True
                        }
                    )
                    if res_fb.status_code == 200:
                        audio_base64 = res_fb.json().get("audios", [None])[0]
        except Exception as e:
            logger.warning(f"Sarvam sample audio error: {str(e)}")
            
    if audio_base64:
        SAMPLE_AUDIO_CACHE[sample_key] = audio_base64

    return {
        "sample_id": sample_id,
        "audio_base64": audio_base64,
        "text": profile["text"],
        "speaker": profile["speaker"],
        "cached": False,
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
# --- 10. AUTH & EMAIL VERIFICATION WORKFLOW ---
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
    return {"status": "otp_sent", "email": clean_email, "message": f"A 6-digit verification code has been sent to {clean_email} from bot@mybotgenie.in."}

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
