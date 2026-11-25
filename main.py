from __future__ import annotations

import logging
import os
import time
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel, Field
from pyrogram import Client
from pyrogram.errors import (
    BadRequest,
    FloodWait,
    PasswordHashInvalid,
    PhoneCodeExpired,
    PhoneCodeInvalid,
    PhoneNumberBanned,
    PhoneNumberInvalid,
    RPCError,
    SessionPasswordNeeded,
)

from userbot import SupabaseStore, UserbotManager, get_env_config

LOG_LEVEL_NAME = os.environ.get("LOG_LEVEL", "INFO").upper()
LOG_LEVEL = getattr(logging, LOG_LEVEL_NAME, logging.INFO)

logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
if LOG_LEVEL > logging.DEBUG:
    logging.getLogger("apscheduler.executors.default").setLevel(logging.WARNING)

logger = logging.getLogger("main")
logger.info("Logging initialized at level=%s", LOG_LEVEL_NAME)

PENDING_AUTH_TTL_SECONDS = 600.0  # 10 minutes


# ---------------------------------------------------------------------------
# Request Schemas
# ---------------------------------------------------------------------------


class SendCodeRequest(BaseModel):
    phone_number: str = Field(..., min_length=5, examples=["+8801700000000"])


class VerifyOtpRequest(BaseModel):
    auth_session_id: str = Field(..., min_length=1)
    code: str = Field(..., min_length=3, max_length=12)


class Verify2FARequest(BaseModel):
    auth_session_id: str = Field(..., min_length=1)
    password: str = Field(..., min_length=1)


# ---------------------------------------------------------------------------
# Stateful In-Memory Pending Auth Registry (10-Minute TTL)
# ---------------------------------------------------------------------------


@dataclass
class PendingAuth:
    client: Client
    phone_number: str
    phone_code_hash: str
    created_at: float


pending_auths: dict[str, PendingAuth] = {}

# Runtime singletons initialized in lifespan
config: dict[str, Any] = {}
scheduler: AsyncIOScheduler | None = None
db_store: SupabaseStore | None = None
userbot_manager: UserbotManager | None = None


async def _safe_disconnect(client: Client) -> None:
    try:
        if getattr(client, "is_connected", False):
            await client.disconnect()
    except Exception as exc:
        logger.warning("Error disconnecting temporary auth client: %s", exc)


async def cleanup_expired_pending_auths() -> None:
    """Disconnect and purge temporary auth clients older than 10 minutes."""
    now = time.monotonic()
    expired_ids = [
        sid
        for sid, entry in list(pending_auths.items())
        if now - entry.created_at > PENDING_AUTH_TTL_SECONDS
    ]
    for sid in expired_ids:
        entry = pending_auths.pop(sid, None)
        if entry is not None:
            await _safe_disconnect(entry.client)
            logger.info("Cleaned up expired pending auth session %s.", sid)


async def _get_valid_pending_auth(auth_session_id: str) -> PendingAuth:
    entry = pending_auths.get(auth_session_id)
    if entry is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Auth session not found or expired. Please start again with /auth/send-code.",
        )
    if time.monotonic() - entry.created_at > PENDING_AUTH_TTL_SECONDS:
        pending_auths.pop(auth_session_id, None)
        await _safe_disconnect(entry.client)
        raise HTTPException(
            status_code=status.HTTP_410_GONE,
            detail="Auth session expired (10-minute TTL). Please request a new code.",
        )
    return entry


async def _finalize_login(auth_session_id: str, entry: PendingAuth) -> dict[str, Any]:
    """Export session_string, persist to Supabase, tear down temp client, and start live userbot."""
    assert db_store is not None and userbot_manager is not None

    session_string = await entry.client.export_session_string()
    me = await entry.client.get_me()

    await db_store.upsert_user_session(
        telegram_user_id=me.id,
        phone_number=entry.phone_number,
        username=me.username,
        session_string=session_string,
        is_active=True,
    )

    pending_auths.pop(auth_session_id, None)
    await _safe_disconnect(entry.client)

    await userbot_manager.start_client(
        me.id,
        session_string,
        notify_connected=True,
        is_new_login=True,
    )
    logger.info("Authenticated and started userbot for user_id=%s (@%s)", me.id, me.username)

    return {
        "status": "authenticated",
        "telegram_user_id": me.id,
        "username": me.username,
    }


# ---------------------------------------------------------------------------
# FastAPI Lifespan
# ---------------------------------------------------------------------------


@asynccontextmanager
async def lifespan(_app: FastAPI):
    global config, scheduler, db_store, userbot_manager

    config = get_env_config()
    tz = config["timezone"]

    scheduler = AsyncIOScheduler(timezone=tz)
    db_store = SupabaseStore(config["supabase_url"], config["supabase_key"])
    userbot_manager = UserbotManager(
        db=db_store,
        scheduler=scheduler,
        api_id=config["api_id"],
        api_hash=config["api_hash"],
        tz=tz,
    )

    scheduler.start()
    scheduler.add_job(
        cleanup_expired_pending_auths,
        trigger=IntervalTrigger(seconds=60, timezone=tz),
        id="pending_auth_ttl_cleanup",
        replace_existing=True,
    )

    await userbot_manager.start_all_sessions()
    await userbot_manager.reload_scheduled_posts()
    logger.info("Telegram Userbot Automation Server is ready.")

    try:
        yield
    finally:
        logger.info("Shutting down Telegram Userbot Automation Server...")
        for sid in list(pending_auths.keys()):
            entry = pending_auths.pop(sid, None)
            if entry is not None:
                await _safe_disconnect(entry.client)

        if scheduler is not None and scheduler.running:
            scheduler.shutdown(wait=False)

        if userbot_manager is not None:
            await userbot_manager.stop_all_clients()
        logger.info("Shutdown complete.")


app = FastAPI(
    title="Telegram Userbot Automation Server",
    version="1.0.0",
    lifespan=lifespan,
)


# ---------------------------------------------------------------------------
# Health Check Endpoint
# ---------------------------------------------------------------------------


@app.get("/health")
async def health_check() -> dict[str, Any]:
    count = len(userbot_manager.active_clients) if userbot_manager else 0
    return {"status": "ok", "active_clients": count}


# ---------------------------------------------------------------------------
# Multi-Step Telegram Authentication API
# ---------------------------------------------------------------------------


@app.post("/auth/send-code")
async def auth_send_code(payload: SendCodeRequest) -> dict[str, Any]:
    phone = payload.phone_number.strip()
    temp_uuid = uuid.uuid4().hex
    temp_client = Client(
        name=f"temp_{temp_uuid}",
        api_id=config["api_id"],
        api_hash=config["api_hash"],
        in_memory=True,
    )

    try:
        await temp_client.connect()
        sent_code = await temp_client.send_code(phone)
    except FloodWait as exc:
        await _safe_disconnect(temp_client)
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail=f"Telegram rate limit hit. Retry after {exc.value} seconds.",
        ) from exc
    except PhoneNumberInvalid as exc:
        await _safe_disconnect(temp_client)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid phone number format. Include country code (e.g. +8801700000000).",
        ) from exc
    except PhoneNumberBanned as exc:
        await _safe_disconnect(temp_client)
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="This phone number is banned by Telegram.",
        ) from exc
    except BadRequest as exc:
        await _safe_disconnect(temp_client)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Telegram rejected send_code request: {exc}",
        ) from exc
    except RPCError as exc:
        await _safe_disconnect(temp_client)
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"Telegram RPC error: {exc}",
        ) from exc
    except Exception as exc:
        await _safe_disconnect(temp_client)
        logger.exception("Unexpected error in /auth/send-code: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to initiate Telegram login.",
        ) from exc

    auth_session_id = str(uuid.uuid4())
    pending_auths[auth_session_id] = PendingAuth(
        client=temp_client,
        phone_number=phone,
        phone_code_hash=sent_code.phone_code_hash,
        created_at=time.monotonic(),
    )

    return {
        "status": "code_sent",
        "auth_session_id": auth_session_id,
        "next_step": "/auth/verify-otp",
    }


@app.post("/auth/verify-otp")
async def auth_verify_otp(payload: VerifyOtpRequest) -> dict[str, Any]:
    entry = await _get_valid_pending_auth(payload.auth_session_id)
    code = payload.code.strip().replace("-", "").replace(" ", "")

    try:
        await entry.client.sign_in(
            phone_number=entry.phone_number,
            phone_code_hash=entry.phone_code_hash,
            phone_code=code,
        )
    except SessionPasswordNeeded:
        # Refresh TTL while waiting for 2FA password
        entry.created_at = time.monotonic()
        return {
            "status": "2fa_required",
            "auth_session_id": payload.auth_session_id,
            "next_step": "/auth/verify-2fa",
        }
    except PhoneCodeInvalid as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid OTP code. Please check the code and try again.",
        ) from exc
    except PhoneCodeExpired as exc:
        pending_auths.pop(payload.auth_session_id, None)
        await _safe_disconnect(entry.client)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="OTP code has expired. Please request a new code via /auth/send-code.",
        ) from exc
    except FloodWait as exc:
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail=f"Telegram rate limit hit. Retry after {exc.value} seconds.",
        ) from exc
    except BadRequest as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Telegram sign_in failed: {exc}",
        ) from exc
    except RPCError as exc:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"Telegram RPC error: {exc}",
        ) from exc

    return await _finalize_login(payload.auth_session_id, entry)


@app.post("/auth/verify-2fa")
async def auth_verify_2fa(payload: Verify2FARequest) -> dict[str, Any]:
    entry = await _get_valid_pending_auth(payload.auth_session_id)

    try:
        await entry.client.check_password(password=payload.password)
    except PasswordHashInvalid as exc:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid 2FA password. Please try again.",
        ) from exc
    except FloodWait as exc:
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail=f"Telegram rate limit hit. Retry after {exc.value} seconds.",
        ) from exc
    except BadRequest as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Telegram 2FA verification failed: {exc}",
        ) from exc
    except RPCError as exc:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"Telegram RPC error: {exc}",
        ) from exc

    return await _finalize_login(payload.auth_session_id, entry)
