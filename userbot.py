from __future__ import annotations

import asyncio
import binascii
import logging
import os
import re
import struct
import time
import uuid
from collections import deque
from datetime import datetime, timezone
from typing import Any
from zoneinfo import ZoneInfo

# Ensure an event loop exists before importing Pyrogram/Kurigram
# (protects Python 3.14+ environments where no default loop is set yet).
try:
    asyncio.get_event_loop()
except RuntimeError:
    asyncio.set_event_loop(asyncio.new_event_loop())

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.date import DateTrigger
from apscheduler.triggers.interval import IntervalTrigger
from pyrogram import Client, filters
from pyrogram.enums import MessageEntityType, ParseMode, UserStatus
from pyrogram.errors import (
    AuthKeyUnregistered,
    FloodPremiumWait,
    FloodWait,
    RPCError,
    SessionRevoked,
    UserDeactivated,
    UserDeactivatedBan,
)
from pyrogram.handlers import MessageHandler, UserStatusHandler
from pyrogram.types import Message, MessageEntity, ReplyParameters, User
from supabase import Client as SupabaseClient
from supabase import create_client

logger = logging.getLogger("userbot")

TEMPLATE_ID_RE = re.compile(r"^TPL-[0-9A-F]{6}$")
MENTION_COOLDOWN_SECONDS = 30.0
KEYWORD_COOLDOWN_SECONDS = 15.0
DEFAULT_OFFLINE_COOLDOWN_SECONDS = 300
MIN_OFFLINE_COOLDOWN_SECONDS = 10
MAX_OFFLINE_COOLDOWN_SECONDS = 86400
ONLINE_STATUS_CACHE_TTL_SECONDS = 20.0
CLIENT_SLEEP_THRESHOLD = 30
STARTUP_STAGGER_SECONDS = 1.0
TELEGRAM_SYSTEM_USER_IDS = frozenset({777000, 42777})

CLIENT_DEVICE_MODEL = os.environ.get("TG_DEVICE_MODEL", "Desktop")
CLIENT_SYSTEM_VERSION = os.environ.get("TG_SYSTEM_VERSION", "Linux")
CLIENT_APP_VERSION = os.environ.get("TG_APP_VERSION", "4.16.30")

_UNSET: Any = object()


# ---------------------------------------------------------------------------
# Environment Configuration
# ---------------------------------------------------------------------------


def get_env_config() -> dict[str, Any]:
    """Read and validate required environment variables."""
    api_id_raw = os.environ.get("API_ID", "").strip()
    api_hash = os.environ.get("API_HASH", "").strip()
    supabase_url = os.environ.get("SUPABASE_URL", "").strip()
    supabase_key = os.environ.get("SUPABASE_KEY", "").strip()
    tz_name = os.environ.get("TIMEZONE", "Asia/Dhaka").strip() or "Asia/Dhaka"

    missing = [
        name
        for name, val in [
            ("API_ID", api_id_raw),
            ("API_HASH", api_hash),
            ("SUPABASE_URL", supabase_url),
            ("SUPABASE_KEY", supabase_key),
        ]
        if not val
    ]
    if missing:
        raise RuntimeError(
            f"Missing required environment variables: {', '.join(missing)}"
        )

    try:
        api_id = int(api_id_raw)
    except ValueError as exc:
        raise RuntimeError("API_ID must be a valid integer") from exc

    try:
        tz = ZoneInfo(tz_name)
    except Exception as exc:
        raise RuntimeError(f"Invalid TIMEZONE '{tz_name}'") from exc

    return {
        "api_id": api_id,
        "api_hash": api_hash,
        "supabase_url": supabase_url,
        "supabase_key": supabase_key,
        "timezone": tz,
        "timezone_name": tz_name,
    }


# ---------------------------------------------------------------------------
# UTF-16 Offset & Premium Custom Emoji Serializer / Renderer
# ---------------------------------------------------------------------------


def utf16_len(text: str) -> int:
    """Return the exact UTF-16 code-unit length of a Python string (Telegram entity units)."""
    return len(text.encode("utf-16-le")) // 2


def serialize_entities_with_offset(
    full_text: str,
    entities: list[MessageEntity] | None,
    start_char_idx: int = 0,
) -> tuple[str, list[dict[str, Any]]]:
    """
    Slice `full_text[start_char_idx:]`, strip trailing whitespace, and shift all
    Telegram MessageEntity offsets by the exact UTF-16 length of the stripped prefix.
    """
    prefix = full_text[:start_char_idx]
    payload = full_text[start_char_idx:].rstrip()
    prefix_utf16 = utf16_len(prefix)
    payload_utf16 = utf16_len(payload)

    serialized: list[dict[str, Any]] = []
    if not entities or payload_utf16 == 0:
        return payload, serialized

    for entity in entities:
        new_offset = entity.offset - prefix_utf16
        new_length = entity.length

        # Skip entities entirely within the stripped command prefix
        if new_offset + new_length <= 0:
            continue

        # Clamp entities that partially overlap the boundary
        if new_offset < 0:
            new_length += new_offset
            new_offset = 0

        if new_offset >= payload_utf16:
            continue

        new_length = min(new_length, payload_utf16 - new_offset)
        if new_length <= 0:
            continue

        item: dict[str, Any] = {
            "type": entity.type.name,
            "offset": int(new_offset),
            "length": int(new_length),
        }
        if getattr(entity, "custom_emoji_id", None) is not None:
            item["custom_emoji_id"] = str(entity.custom_emoji_id)
        if getattr(entity, "url", None):
            item["url"] = str(entity.url)
        if getattr(entity, "language", None):
            item["language"] = str(entity.language)
        if getattr(entity, "expandable", None) is not None:
            item["expandable"] = bool(entity.expandable)
        if getattr(entity, "unix_time", None) is not None:
            item["unix_time"] = int(entity.unix_time)
        if getattr(entity, "date_time_format", None):
            item["date_time_format"] = str(entity.date_time_format)

        serialized.append(item)

    return payload, serialized


def deserialize_entities(entities_json: list[dict[str, Any]] | None) -> list[MessageEntity]:
    """Reconstruct Kurigram/Pyrogram MessageEntity objects (including Premium Custom Emojis)."""
    if not entities_json:
        return []

    unknown_type = getattr(MessageEntityType, "UNKNOWN", None)
    result: list[MessageEntity] = []
    for raw in entities_json:
        type_name = raw.get("type")
        if not type_name or not hasattr(MessageEntityType, type_name):
            continue

        entity_type = getattr(MessageEntityType, type_name)
        # TEXT_MENTION requires a live User object; UNKNOWN cannot be sent
        if entity_type == MessageEntityType.TEXT_MENTION or (
            unknown_type is not None and entity_type == unknown_type
        ):
            continue

        kwargs: dict[str, Any] = {
            "type": entity_type,
            "offset": int(raw["offset"]),
            "length": int(raw["length"]),
        }
        if raw.get("custom_emoji_id") is not None:
            kwargs["custom_emoji_id"] = str(raw["custom_emoji_id"])
        if raw.get("url"):
            kwargs["url"] = str(raw["url"])
        if raw.get("language"):
            kwargs["language"] = str(raw["language"])
        if raw.get("expandable") is not None:
            kwargs["expandable"] = bool(raw["expandable"])
        if raw.get("unix_time") is not None:
            kwargs["unix_time"] = int(raw["unix_time"])
        if raw.get("date_time_format"):
            kwargs["date_time_format"] = str(raw["date_time_format"])

        result.append(MessageEntity(**kwargs))

    return result


def count_custom_emojis(entities_json: list[dict[str, Any]] | None) -> int:
    """Count how many CUSTOM_EMOJI entities exist in a serialized entity list."""
    if not entities_json:
        return 0
    return sum(1 for e in entities_json if e.get("type") == "CUSTOM_EMOJI")


def extract_custom_emoji_ids(entities_json: list[dict[str, Any]] | None) -> list[str]:
    """Return all extracted custom_emoji_id values (as strings) from serialized entities."""
    if not entities_json:
        return []
    return [
        str(e["custom_emoji_id"])
        for e in entities_json
        if e.get("type") == "CUSTOM_EMOJI" and e.get("custom_emoji_id") is not None
    ]


def find_payload_start(full_text: str, cmd_end_idx: int) -> int:
    """Find the first non-whitespace index after a command token."""
    idx = cmd_end_idx
    length = len(full_text)
    while idx < length and full_text[idx].isspace():
        idx += 1
    return idx


async def send_template_message(
    client: Client,
    chat_id: int | str,
    template: dict[str, Any],
    reply_to_message_id: int | None = None,
) -> Message | None:
    """Send a saved template with ParseMode.DISABLED so all Premium Custom Emojis render natively."""
    text = template.get("text", "")
    entities = deserialize_entities(template.get("entities_json"))
    reply_params = (
        ReplyParameters(message_id=reply_to_message_id)
        if reply_to_message_id is not None
        else None
    )
    try:
        return await client.send_message(
            chat_id=chat_id,
            text=text,
            entities=entities or None,
            parse_mode=ParseMode.DISABLED,
            reply_parameters=reply_params,
        )
    except (FloodWait, FloodPremiumWait) as exc:
        wait_s = getattr(exc, "seconds", None) or getattr(exc, "value", 0)
        logger.warning(
            "FloodWait (%ss) while sending template %s to chat %s",
            wait_s,
            template.get("id"),
            chat_id,
        )
        return None
    except RPCError as exc:
        logger.error(
            "Telegram RPCError while sending template %s to chat %s: %s",
            template.get("id"),
            chat_id,
            exc,
        )
        return None


# ---------------------------------------------------------------------------
# Supabase Database Wrapper (Async-Safe via asyncio.to_thread)
# ---------------------------------------------------------------------------


class SupabaseStore:
    """Non-blocking wrapper around the official synchronous Supabase Python client."""

    def __init__(self, url: str, key: str) -> None:
        self._client: SupabaseClient = create_client(url, key)

    async def _run(self, fn: Any) -> Any:
        return await asyncio.to_thread(fn)

    # -- user_sessions --

    async def upsert_user_session(
        self,
        telegram_user_id: int,
        phone_number: str | None,
        username: str | None,
        session_string: str,
        is_active: bool = True,
    ) -> dict[str, Any]:
        payload = {
            "telegram_user_id": telegram_user_id,
            "phone_number": phone_number,
            "username": username,
            "session_string": session_string,
            "is_active": is_active,
        }
        res = await self._run(
            lambda: self._client.table("user_sessions")
            .upsert(payload, on_conflict="telegram_user_id")
            .execute()
        )
        return res.data[0] if res.data else payload

    async def get_active_sessions(self) -> list[dict[str, Any]]:
        res = await self._run(
            lambda: self._client.table("user_sessions")
            .select("*")
            .eq("is_active", True)
            .execute()
        )
        return res.data or []

    async def get_user_session(self, telegram_user_id: int) -> dict[str, Any] | None:
        res = await self._run(
            lambda: self._client.table("user_sessions")
            .select("*")
            .eq("telegram_user_id", telegram_user_id)
            .limit(1)
            .execute()
        )
        return res.data[0] if res.data else None

    async def set_user_active(self, telegram_user_id: int, is_active: bool) -> None:
        await self._run(
            lambda: self._client.table("user_sessions")
            .update({"is_active": is_active})
            .eq("telegram_user_id", telegram_user_id)
            .execute()
        )

    # -- manual_sessions --

    async def get_active_manual_sessions(self) -> list[dict[str, Any]]:
        res = await self._run(
            lambda: self._client.table("manual_sessions")
            .select("*")
            .eq("is_active", True)
            .order("id", desc=False)
            .execute()
        )
        return res.data or []

    async def update_manual_session_status(
        self,
        row_id: int,
        *,
        telegram_user_id: int | None = None,
        username: str | None = _UNSET,
        is_active: bool | None = None,
        last_error: Any = _UNSET,
    ) -> None:
        update_data: dict[str, Any] = {
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        if telegram_user_id is not None:
            update_data["telegram_user_id"] = telegram_user_id
        if username is not _UNSET:
            update_data["username"] = username
        if is_active is not None:
            update_data["is_active"] = is_active
        if last_error is not _UNSET:
            update_data["last_error"] = last_error

        await self._run(
            lambda: self._client.table("manual_sessions")
            .update(update_data)
            .eq("id", row_id)
            .execute()
        )

    # -- user_settings (unified for user_sessions & manual_sessions) --

    async def get_user_settings(self, telegram_user_id: int) -> dict[str, Any]:
        defaults: dict[str, Any] = {
            "telegram_user_id": telegram_user_id,
            "mention_enabled": False,
            "mention_template_id": None,
            "welcome_enabled": False,
            "welcome_template_id": None,
            "offline_enabled": False,
            "offline_mode": "manual",
            "offline_template_id": None,
            "offline_cooldown_seconds": DEFAULT_OFFLINE_COOLDOWN_SECONDS,
        }
        res = await self._run(
            lambda: self._client.table("user_settings")
            .select("*")
            .eq("telegram_user_id", telegram_user_id)
            .limit(1)
            .execute()
        )
        if res.data:
            row = res.data[0]
            merged = dict(defaults)
            for key in defaults:
                if row.get(key) is not None:
                    merged[key] = row[key]
            return merged

        # Graceful fallback for existing accounts before user_settings migration
        legacy = await self.get_user_session(telegram_user_id)
        if legacy:
            defaults["mention_enabled"] = bool(legacy.get("mention_enabled", False))
            defaults["mention_template_id"] = legacy.get("mention_template_id")
        return defaults

    async def upsert_user_settings(
        self, telegram_user_id: int, **fields: Any
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "telegram_user_id": telegram_user_id,
            "updated_at": datetime.now(timezone.utc).isoformat(),
            **fields,
        }
        res = await self._run(
            lambda: self._client.table("user_settings")
            .upsert(payload, on_conflict="telegram_user_id")
            .execute()
        )
        return res.data[0] if res.data else payload

    async def set_mention_config(
        self,
        telegram_user_id: int,
        enabled: bool,
        template_id: str | None = None,
    ) -> None:
        update_data: dict[str, Any] = {"mention_enabled": enabled}
        if template_id is not None:
            update_data["mention_template_id"] = template_id
        await self.upsert_user_settings(telegram_user_id, **update_data)

    async def set_welcome_config(
        self,
        telegram_user_id: int,
        enabled: bool,
        template_id: str | None = None,
    ) -> None:
        update_data: dict[str, Any] = {"welcome_enabled": enabled}
        if template_id is not None:
            update_data["welcome_template_id"] = template_id
        await self.upsert_user_settings(telegram_user_id, **update_data)

    async def set_offline_config(
        self,
        telegram_user_id: int,
        *,
        enabled: bool | None = None,
        mode: str | None = None,
        template_id: str | None = None,
        cooldown_seconds: int | None = None,
    ) -> None:
        update_data: dict[str, Any] = {}
        if enabled is not None:
            update_data["offline_enabled"] = enabled
        if mode is not None:
            update_data["offline_mode"] = mode
        if template_id is not None:
            update_data["offline_template_id"] = template_id
        if cooldown_seconds is not None:
            update_data["offline_cooldown_seconds"] = int(cooldown_seconds)
        if update_data:
            await self.upsert_user_settings(telegram_user_id, **update_data)

    # -- welcomed_users --

    async def list_welcomed_user_ids(self, telegram_user_id: int) -> set[int]:
        res = await self._run(
            lambda: self._client.table("welcomed_users")
            .select("sender_user_id")
            .eq("telegram_user_id", telegram_user_id)
            .execute()
        )
        return {int(row["sender_user_id"]) for row in (res.data or [])}

    async def mark_user_welcomed(
        self, telegram_user_id: int, sender_user_id: int
    ) -> None:
        payload = {
            "telegram_user_id": telegram_user_id,
            "sender_user_id": sender_user_id,
        }
        await self._run(
            lambda: self._client.table("welcomed_users")
            .upsert(payload, on_conflict="telegram_user_id,sender_user_id")
            .execute()
        )

    async def reset_welcomed_users(
        self, telegram_user_id: int, sender_user_id: int | None = None
    ) -> int:
        def _query() -> Any:
            q = (
                self._client.table("welcomed_users")
                .delete()
                .eq("telegram_user_id", telegram_user_id)
            )
            if sender_user_id is not None:
                q = q.eq("sender_user_id", sender_user_id)
            return q.execute()

        res = await self._run(_query)
        return len(res.data or [])

    # -- templates --

    async def create_template(
        self,
        telegram_user_id: int,
        text: str,
        entities_json: list[dict[str, Any]],
    ) -> dict[str, Any]:
        for _ in range(5):
            tpl_id = f"TPL-{uuid.uuid4().hex[:6].upper()}"
            existing = await self.get_template(telegram_user_id, tpl_id)
            if existing is None:
                row = {
                    "id": tpl_id,
                    "telegram_user_id": telegram_user_id,
                    "text": text,
                    "entities_json": entities_json,
                }
                res = await self._run(
                    lambda: self._client.table("templates").insert(row).execute()
                )
                return res.data[0] if res.data else row
        raise RuntimeError("Failed to generate a unique template ID")

    async def get_template(
        self, telegram_user_id: int, template_id: str
    ) -> dict[str, Any] | None:
        norm_id = template_id.strip().upper()
        res = await self._run(
            lambda: self._client.table("templates")
            .select("*")
            .eq("telegram_user_id", telegram_user_id)
            .eq("id", norm_id)
            .limit(1)
            .execute()
        )
        return res.data[0] if res.data else None

    async def list_templates(self, telegram_user_id: int) -> list[dict[str, Any]]:
        res = await self._run(
            lambda: self._client.table("templates")
            .select("*")
            .eq("telegram_user_id", telegram_user_id)
            .order("created_at", desc=True)
            .execute()
        )
        return res.data or []

    async def delete_template(self, telegram_user_id: int, template_id: str) -> bool:
        norm_id = template_id.strip().upper()
        res = await self._run(
            lambda: self._client.table("templates")
            .delete()
            .eq("telegram_user_id", telegram_user_id)
            .eq("id", norm_id)
            .execute()
        )
        return bool(res.data)

    # -- keyword_replies --

    async def upsert_keyword_reply(
        self, telegram_user_id: int, trigger_text: str, template_id: str
    ) -> dict[str, Any]:
        trigger_clean = trigger_text.strip()
        existing_rows = await self.list_keyword_replies(telegram_user_id)
        matched = next(
            (r for r in existing_rows if r["trigger_text"].lower() == trigger_clean.lower()),
            None,
        )
        if matched:
            res = await self._run(
                lambda: self._client.table("keyword_replies")
                .update({"trigger_text": trigger_clean, "template_id": template_id})
                .eq("id", matched["id"])
                .execute()
            )
            return res.data[0] if res.data else matched

        row = {
            "telegram_user_id": telegram_user_id,
            "trigger_text": trigger_clean,
            "template_id": template_id,
        }
        res = await self._run(
            lambda: self._client.table("keyword_replies").insert(row).execute()
        )
        return res.data[0] if res.data else row

    async def list_keyword_replies(self, telegram_user_id: int) -> list[dict[str, Any]]:
        res = await self._run(
            lambda: self._client.table("keyword_replies")
            .select("*")
            .eq("telegram_user_id", telegram_user_id)
            .order("created_at", desc=True)
            .execute()
        )
        return res.data or []

    async def delete_keyword_reply(self, telegram_user_id: int, trigger_text: str) -> bool:
        target = trigger_text.strip().lower()
        rows = await self.list_keyword_replies(telegram_user_id)
        matched_ids = [r["id"] for r in rows if r["trigger_text"].lower() == target]
        if not matched_ids:
            return False
        for row_id in matched_ids:
            await self._run(
                lambda rid=row_id: self._client.table("keyword_replies")
                .delete()
                .eq("id", rid)
                .eq("telegram_user_id", telegram_user_id)
                .execute()
            )
        return True

    # -- scheduled_posts --

    async def create_scheduled_post(
        self,
        telegram_user_id: int,
        template_id: str,
        target_chat_id: int,
        run_at: datetime | None = None,
        interval_minutes: int | None = None,
    ) -> dict[str, Any]:
        row: dict[str, Any] = {
            "telegram_user_id": telegram_user_id,
            "template_id": template_id,
            "target_chat_id": target_chat_id,
            "run_at": run_at.astimezone(timezone.utc).isoformat() if run_at else None,
            "interval_minutes": interval_minutes,
            "is_active": True,
        }
        res = await self._run(
            lambda: self._client.table("scheduled_posts").insert(row).execute()
        )
        return res.data[0] if res.data else row

    async def list_active_scheduled_posts(
        self, telegram_user_id: int | None = None
    ) -> list[dict[str, Any]]:
        def _query() -> Any:
            q = (
                self._client.table("scheduled_posts")
                .select("*")
                .eq("is_active", True)
                .order("id", desc=False)
            )
            if telegram_user_id is not None:
                q = q.eq("telegram_user_id", telegram_user_id)
            return q.execute()

        res = await self._run(_query)
        return res.data or []

    async def deactivate_scheduled_post(
        self, job_id: int, telegram_user_id: int | None = None
    ) -> bool:
        def _query() -> Any:
            q = (
                self._client.table("scheduled_posts")
                .update({"is_active": False})
                .eq("id", job_id)
            )
            if telegram_user_id is not None:
                q = q.eq("telegram_user_id", telegram_user_id)
            return q.execute()

        res = await self._run(_query)
        return bool(res.data)

    async def delete_scheduled_post(self, job_id: int, telegram_user_id: int) -> bool:
        res = await self._run(
            lambda: self._client.table("scheduled_posts")
            .delete()
            .eq("id", job_id)
            .eq("telegram_user_id", telegram_user_id)
            .execute()
        )
        return bool(res.data)


# ---------------------------------------------------------------------------
# Pyrogram / Kurigram In-Memory Userbot Manager
# ---------------------------------------------------------------------------


class UserbotManager:
    """Manages all in-memory Pyrogram/Kurigram userbot clients, handlers, and schedules."""

    def __init__(
        self,
        db: SupabaseStore,
        scheduler: AsyncIOScheduler,
        api_id: int,
        api_hash: str,
        tz: ZoneInfo,
    ) -> None:
        self.db = db
        self.scheduler = scheduler
        self.api_id = api_id
        self.api_hash = api_hash
        self.tz = tz

        # Active userbot registry: telegram_user_id -> Client
        self.active_clients: dict[int, Client] = {}
        # Source & account metadata: telegram_user_id -> {source, label, username, manual_session_id}
        self.session_metadata: dict[int, dict[str, Any]] = {}

        # Per-user in-memory caches for low-latency message handling
        self._mention_cache: dict[int, tuple[bool, str | None]] = {}
        self._welcome_cache: dict[int, tuple[bool, str | None]] = {}
        self._welcomed_users_cache: dict[int, set[int]] = {}
        # (enabled, mode, template_id, cooldown_seconds)
        self._offline_cache: dict[int, tuple[bool, str, str | None, int]] = {}
        self._keyword_cache: dict[int, list[dict[str, Any]]] = {}
        self._template_cache: dict[int, dict[str, dict[str, Any]]] = {}

        # Anti-spam / loop prevention cooldowns & online status cache
        self._mention_cooldowns: dict[tuple[int, int], float] = {}
        self._keyword_cooldowns: dict[tuple[int, int, str], float] = {}
        self._offline_cooldowns: dict[tuple[int, int], float] = {}
        self._online_status_cache: dict[int, tuple[bool, float]] = {}
        self._replied_messages: deque[tuple[int, int, int]] = deque(maxlen=500)

    # -- Lifecycle --

    def _build_client(self, name: str, session_string: str) -> Client:
        """Create an in-memory Kurigram Client configured for high-traffic resilience."""
        return Client(
            name=name,
            api_id=self.api_id,
            api_hash=self.api_hash,
            session_string=session_string,
            in_memory=True,
            sleep_threshold=CLIENT_SLEEP_THRESHOLD,
            skip_updates=True,
            fetch_stories=False,
            fetch_stickers=False,
            device_model=CLIENT_DEVICE_MODEL,
            system_version=CLIENT_SYSTEM_VERSION,
            app_version=CLIENT_APP_VERSION,
        )

    async def _send_connected_notice(
        self, client: Client, telegram_user_id: int, *, is_new_login: bool = False
    ) -> None:
        try:
            status_line = (
                "Your account is now connected!"
                if is_new_login
                else "Your userbot is online and connected!"
            )
            await client.send_message(
                chat_id="me",
                text=(
                    f"{status_line}\n"
                    f"Use /help here in Saved Messages to see all available commands."
                ),
                parse_mode=ParseMode.DISABLED,
            )
            logger.info(
                "Sent connection confirmation to Saved Messages for user_id=%s",
                telegram_user_id,
            )
        except Exception as exc:
            logger.warning(
                "Could not send connection confirmation to Saved Messages for user_id=%s: %s",
                telegram_user_id,
                exc,
            )

    async def start_all_sessions(self) -> None:
        """Load active sessions from user_sessions first, then manual_sessions, with deduplication."""
        seen_session_strings: set[str] = set()
        started_count = 0

        # 1. Endpoint-authenticated sessions (take precedence)
        sessions = await self.db.get_active_sessions()
        logger.info("Found %d active user_sessions row(s) in Supabase.", len(sessions))
        for row in sessions:
            user_id = int(row["telegram_user_id"])
            session_string = (row.get("session_string") or "").strip()
            if not session_string:
                continue
            if started_count > 0:
                await asyncio.sleep(STARTUP_STAGGER_SECONDS)
            try:
                await self.start_client(
                    user_id,
                    session_string,
                    source="user_sessions",
                    username=row.get("username"),
                )
                seen_session_strings.add(session_string)
                started_count += 1
            except (
                AuthKeyUnregistered,
                SessionRevoked,
                UserDeactivated,
                UserDeactivatedBan,
            ) as exc:
                logger.error(
                    "Session for user %s is invalid/revoked (%s); marking inactive.",
                    user_id,
                    exc,
                )
                await self.db.set_user_active(user_id, False)
            except Exception as exc:
                logger.exception("Failed to start userbot client for user %s: %s", user_id, exc)

        # 2. User-pasted manual_sessions rows
        try:
            manual_rows = await self.db.get_active_manual_sessions()
        except Exception as exc:
            logger.warning(
                "Could not load manual_sessions table (ensure SQL migration has been run): %s",
                exc,
            )
            return

        logger.info("Found %d active manual_sessions row(s) in Supabase.", len(manual_rows))
        for row in manual_rows:
            started = await self.start_manual_client(
                row,
                seen_session_strings=seen_session_strings,
                stagger=(started_count > 0),
            )
            if started:
                started_count += 1

    async def _safe_update_manual_status(self, row_id: int, **kwargs: Any) -> None:
        """Update manual_sessions status without letting a transient DB error abort startup."""
        try:
            await self.db.update_manual_session_status(row_id, **kwargs)
        except Exception as db_exc:
            logger.warning(
                "Failed to update manual_sessions status for id=%s: %s",
                row_id,
                db_exc,
            )

    async def start_manual_client(
        self,
        row: dict[str, Any],
        *,
        seen_session_strings: set[str] | None = None,
        stagger: bool = False,
        notify_connected: bool = True,
    ) -> bool:
        """Start a single client from manual_sessions with dedup, get_me() backfill, and error tracking."""
        row_id = int(row["id"])
        label = row.get("label")
        raw_session = (row.get("session_string") or "").strip()

        if not raw_session:
            logger.warning("manual_sessions id=%s has an empty session_string; marking inactive.", row_id)
            await self._safe_update_manual_status(
                row_id, is_active=False, last_error="Empty session_string"
            )
            return False

        if seen_session_strings is not None and raw_session in seen_session_strings:
            logger.info("manual_sessions id=%s skipped (duplicate session_string already active).", row_id)
            await self._safe_update_manual_status(
                row_id, last_error="Duplicate: session_string already active"
            )
            return False

        known_uid = row.get("telegram_user_id")
        if known_uid is not None and int(known_uid) in self.active_clients:
            logger.info(
                "manual_sessions id=%s skipped (telegram_user_id=%s already active).",
                row_id,
                known_uid,
            )
            await self._safe_update_manual_status(
                row_id, last_error="Duplicate: account already active"
            )
            return False

        if stagger:
            await asyncio.sleep(STARTUP_STAGGER_SECONDS)

        client: Client | None = None
        registered_uid: int | None = None
        try:
            client = self._build_client(f"manual_{row_id}", raw_session)
            await client.start()
            me = await client.get_me()
            telegram_user_id = int(me.id)
            username = getattr(me, "username", None)

            # Post-connect deduplication (handles two distinct strings for the same Telegram account)
            if telegram_user_id in self.active_clients:
                logger.info(
                    "manual_sessions id=%s resolved to user_id=%s which is already active; stopping duplicate.",
                    row_id,
                    telegram_user_id,
                )
                try:
                    await client.stop()
                except Exception:
                    pass
                await self._safe_update_manual_status(
                    row_id,
                    telegram_user_id=telegram_user_id,
                    username=username,
                    last_error="Duplicate: account already active",
                )
                return False

            await self.db.update_manual_session_status(
                row_id,
                telegram_user_id=telegram_user_id,
                username=username,
                is_active=True,
                last_error=None,
            )
            await self.refresh_user_cache(telegram_user_id)

            self._attach_handlers(client, telegram_user_id)
            self.active_clients[telegram_user_id] = client
            self.session_metadata[telegram_user_id] = {
                "source": "manual_sessions",
                "label": label,
                "username": username,
                "manual_session_id": row_id,
            }
            if seen_session_strings is not None:
                seen_session_strings.add(raw_session)
            registered_uid = telegram_user_id

            tpl_count = len(self._template_cache.get(telegram_user_id, {}))
            kw_count = len(self._keyword_cache.get(telegram_user_id, []))
            logger.info(
                "Started manual_sessions client id=%s for user_id=%s (@%s, label=%s, templates=%d, keywords=%d)",
                row_id,
                telegram_user_id,
                username,
                label,
                tpl_count,
                kw_count,
            )

            if notify_connected:
                await self._send_connected_notice(client, telegram_user_id, is_new_login=False)

            return True

        except (
            AuthKeyUnregistered,
            SessionRevoked,
            UserDeactivated,
            UserDeactivatedBan,
            ValueError,
            TypeError,
            binascii.Error,
            struct.error,
        ) as exc:
            if registered_uid is not None:
                if self.active_clients.get(registered_uid) is client:
                    self.active_clients.pop(registered_uid, None)
                    self.session_metadata.pop(registered_uid, None)
                if seen_session_strings is not None:
                    seen_session_strings.discard(raw_session)
            if client is not None:
                try:
                    if getattr(client, "is_connected", False):
                        await client.stop()
                except Exception:
                    pass
            err_msg = f"{type(exc).__name__}: {exc}"
            logger.error(
                "manual_sessions id=%s is invalid/revoked/corrupt (%s); marking inactive.",
                row_id,
                err_msg,
            )
            await self._safe_update_manual_status(
                row_id, is_active=False, last_error=err_msg
            )
            return False

        except Exception as exc:
            if registered_uid is not None:
                if self.active_clients.get(registered_uid) is client:
                    self.active_clients.pop(registered_uid, None)
                    self.session_metadata.pop(registered_uid, None)
                if seen_session_strings is not None:
                    seen_session_strings.discard(raw_session)
            if client is not None:
                try:
                    if getattr(client, "is_connected", False):
                        await client.stop()
                except Exception:
                    pass
            err_msg = f"{type(exc).__name__}: {exc}"
            logger.exception(
                "Transient error starting manual_sessions id=%s (keeping active): %s",
                row_id,
                err_msg,
            )
            await self._safe_update_manual_status(
                row_id, is_active=True, last_error=err_msg
            )
            return False

    async def start_client(
        self,
        telegram_user_id: int,
        session_string: str,
        notify_connected: bool = True,
        is_new_login: bool = False,
        source: str = "user_sessions",
        label: str | None = None,
        manual_session_id: int | None = None,
        username: str | None = None,
    ) -> Client:
        """Start (or restart) an in-memory Kurigram client with all automation handlers."""
        if telegram_user_id in self.active_clients:
            logger.debug("Stopping existing client for user_id=%s before restart", telegram_user_id)
            await self.stop_client(telegram_user_id)

        client = self._build_client(f"userbot_{telegram_user_id}", session_string.strip())

        self._attach_handlers(client, telegram_user_id)
        await client.start()

        resolved_username = username
        if resolved_username is None:
            resolved_username = getattr(getattr(client, "me", None), "username", None)

        self.active_clients[telegram_user_id] = client
        self.session_metadata[telegram_user_id] = {
            "source": source,
            "label": label,
            "username": resolved_username,
            "manual_session_id": manual_session_id,
        }
        await self.refresh_user_cache(telegram_user_id)

        tpl_count = len(self._template_cache.get(telegram_user_id, {}))
        kw_count = len(self._keyword_cache.get(telegram_user_id, []))
        mention_enabled, mention_tpl = self._mention_cache.get(telegram_user_id, (False, None))
        welcome_enabled, welcome_tpl = self._welcome_cache.get(telegram_user_id, (False, None))
        offline_enabled, offline_mode, offline_tpl, _ = self._offline_cache.get(
            telegram_user_id, (False, "manual", None, DEFAULT_OFFLINE_COOLDOWN_SECONDS)
        )
        logger.info(
            "Started in-memory userbot client for user_id=%s (source=%s, templates=%d, keywords=%d, mention=%s/%s, welcome=%s/%s, offline=%s[%s]/%s)",
            telegram_user_id,
            source,
            tpl_count,
            kw_count,
            mention_enabled,
            mention_tpl,
            welcome_enabled,
            welcome_tpl,
            offline_enabled,
            offline_mode,
            offline_tpl,
        )

        if notify_connected:
            await self._send_connected_notice(client, telegram_user_id, is_new_login=is_new_login)

        return client

    async def stop_client(self, telegram_user_id: int) -> None:
        client = self.active_clients.pop(telegram_user_id, None)
        self.session_metadata.pop(telegram_user_id, None)
        self._online_status_cache.pop(telegram_user_id, None)
        self._welcomed_users_cache.pop(telegram_user_id, None)
        self._mention_cache.pop(telegram_user_id, None)
        self._welcome_cache.pop(telegram_user_id, None)
        self._offline_cache.pop(telegram_user_id, None)
        self._keyword_cache.pop(telegram_user_id, None)
        self._template_cache.pop(telegram_user_id, None)
        for cooldown_map in (
            self._mention_cooldowns,
            self._keyword_cooldowns,
            self._offline_cooldowns,
        ):
            for cd_key in [k for k in cooldown_map if k[0] == telegram_user_id]:
                cooldown_map.pop(cd_key, None)
        if client is not None:
            try:
                await client.stop()
            except Exception as exc:
                logger.warning("Error stopping client for user_id=%s: %s", telegram_user_id, exc)

    async def stop_all_clients(self) -> None:
        user_ids = list(self.active_clients.keys())
        for user_id in user_ids:
            await self.stop_client(user_id)
        logger.info("Stopped %d userbot client(s).", len(user_ids))

    async def refresh_user_cache(self, telegram_user_id: int) -> None:
        """Sync mention, welcome, offline settings, welcomed users, keyword rules, and templates."""
        settings = await self.db.get_user_settings(telegram_user_id)
        self._mention_cache[telegram_user_id] = (
            bool(settings.get("mention_enabled", False)),
            settings.get("mention_template_id"),
        )
        self._welcome_cache[telegram_user_id] = (
            bool(settings.get("welcome_enabled", False)),
            settings.get("welcome_template_id"),
        )
        mode = str(settings.get("offline_mode") or "manual").lower()
        if mode not in ("manual", "auto"):
            mode = "manual"
        cooldown_s = int(settings.get("offline_cooldown_seconds") or DEFAULT_OFFLINE_COOLDOWN_SECONDS)
        self._offline_cache[telegram_user_id] = (
            bool(settings.get("offline_enabled", False)),
            mode,
            settings.get("offline_template_id"),
            cooldown_s,
        )

        try:
            self._welcomed_users_cache[telegram_user_id] = await self.db.list_welcomed_user_ids(
                telegram_user_id
            )
        except Exception as exc:
            logger.warning(
                "Could not load welcomed_users for user_id=%s: %s", telegram_user_id, exc
            )
            self._welcomed_users_cache.setdefault(telegram_user_id, set())

        templates = await self.db.list_templates(telegram_user_id)
        self._template_cache[telegram_user_id] = {t["id"]: t for t in templates}

        keywords = await self.db.list_keyword_replies(telegram_user_id)
        # Sort longest trigger first for more specific matching
        keywords.sort(key=lambda r: len(r.get("trigger_text", "")), reverse=True)
        self._keyword_cache[telegram_user_id] = keywords

    async def _get_template_cached(
        self, telegram_user_id: int, template_id: str | None
    ) -> dict[str, Any] | None:
        if not template_id:
            return None
        norm_id = template_id.strip().upper()
        user_tpls = self._template_cache.setdefault(telegram_user_id, {})
        if norm_id in user_tpls:
            return user_tpls[norm_id]
        tpl = await self.db.get_template(telegram_user_id, norm_id)
        if tpl:
            user_tpls[norm_id] = tpl
        return tpl

    # -- Scheduler Management --

    async def reload_scheduled_posts(self) -> None:
        """Reload all active one-time and recurring scheduled posts from Supabase into APScheduler."""
        rows = await self.db.list_active_scheduled_posts()
        now_utc = datetime.now(timezone.utc)
        loaded = 0

        for row in rows:
            job_id = int(row["id"])
            if row.get("interval_minutes"):
                self._register_interval_job(
                    job_id=job_id,
                    telegram_user_id=int(row["telegram_user_id"]),
                    template_id=row["template_id"],
                    target_chat_id=int(row["target_chat_id"]),
                    interval_minutes=int(row["interval_minutes"]),
                )
                loaded += 1
            elif row.get("run_at"):
                run_dt = datetime.fromisoformat(str(row["run_at"]).replace("Z", "+00:00"))
                if run_dt <= now_utc:
                    logger.info(
                        "Deactivating expired one-time schedule job_id=%s (run_at=%s).",
                        job_id,
                        row["run_at"],
                    )
                    await self.db.deactivate_scheduled_post(job_id)
                    continue
                self._register_onetime_job(
                    job_id=job_id,
                    telegram_user_id=int(row["telegram_user_id"]),
                    template_id=row["template_id"],
                    target_chat_id=int(row["target_chat_id"]),
                    run_dt=run_dt.astimezone(self.tz),
                )
                loaded += 1

        logger.info("Reloaded %d active scheduled post(s) into APScheduler.", loaded)

    def _register_onetime_job(
        self,
        job_id: int,
        telegram_user_id: int,
        template_id: str,
        target_chat_id: int,
        run_dt: datetime,
    ) -> None:
        self.scheduler.add_job(
            self._execute_scheduled_post,
            trigger=DateTrigger(run_date=run_dt, timezone=self.tz),
            id=f"sched_{job_id}",
            replace_existing=True,
            kwargs={
                "job_id": job_id,
                "telegram_user_id": telegram_user_id,
                "template_id": template_id,
                "target_chat_id": target_chat_id,
                "is_one_time": True,
            },
        )

    def _register_interval_job(
        self,
        job_id: int,
        telegram_user_id: int,
        template_id: str,
        target_chat_id: int,
        interval_minutes: int,
    ) -> None:
        self.scheduler.add_job(
            self._execute_scheduled_post,
            trigger=IntervalTrigger(minutes=interval_minutes, timezone=self.tz),
            id=f"sched_{job_id}",
            replace_existing=True,
            kwargs={
                "job_id": job_id,
                "telegram_user_id": telegram_user_id,
                "template_id": template_id,
                "target_chat_id": target_chat_id,
                "is_one_time": False,
            },
        )

    async def _execute_scheduled_post(
        self,
        job_id: int,
        telegram_user_id: int,
        template_id: str,
        target_chat_id: int,
        is_one_time: bool,
    ) -> None:
        client = self.active_clients.get(telegram_user_id)
        if client is None:
            logger.warning(
                "Skipping scheduled job %s: userbot %s is not active.",
                job_id,
                telegram_user_id,
            )
            return

        template = await self._get_template_cached(telegram_user_id, template_id)
        if not template:
            logger.warning(
                "Skipping scheduled job %s: template %s not found.",
                job_id,
                template_id,
            )
            if is_one_time:
                await self.db.deactivate_scheduled_post(job_id, telegram_user_id)
            return

        await send_template_message(client, target_chat_id, template)
        logger.info(
            "Executed scheduled job %s -> chat=%s template=%s (user=%s)",
            job_id,
            target_chat_id,
            template_id,
            telegram_user_id,
        )

        if is_one_time:
            await self.db.deactivate_scheduled_post(job_id, telegram_user_id)

    # -- Handler Registration --

    def _attach_handlers(self, client: Client, telegram_user_id: int) -> None:
        async def _on_saved_command(c: Client, m: Message) -> None:
            await self._handle_saved_messages_command(c, m, telegram_user_id)

        async def _on_mention(c: Client, m: Message) -> None:
            await self._handle_mention(c, m, telegram_user_id)

        async def _on_welcome(c: Client, m: Message) -> None:
            await self._handle_welcome(c, m, telegram_user_id)

        async def _on_keyword(c: Client, m: Message) -> None:
            await self._handle_keyword_reply(c, m, telegram_user_id)

        async def _on_offline(c: Client, m: Message) -> None:
            await self._handle_offline(c, m, telegram_user_id)

        async def _on_user_status(c: Client, user: User) -> None:
            if user and getattr(user, "id", None) == telegram_user_id:
                is_online = getattr(user, "status", None) == UserStatus.ONLINE
                self._online_status_cache[telegram_user_id] = (
                    is_online,
                    time.monotonic(),
                )
                logger.debug(
                    "[STATUS] Self status update for user=%s -> online=%s",
                    telegram_user_id,
                    is_online,
                )

        dm_filter = (
            filters.private
            & filters.incoming
            & ~filters.me
            & ~filters.bot
            & ~filters.service
        )

        # Group 0: Saved Messages control plane
        client.add_handler(
            MessageHandler(
                _on_saved_command,
                filters.me & filters.chat("me") & filters.text,
            ),
            group=0,
        )

        # Group 0 (UserStatus): Track self online/offline status for auto-offline mode
        client.add_handler(
            UserStatusHandler(_on_user_status),
            group=0,
        )

        # Group 1: Mention auto-reply
        client.add_handler(
            MessageHandler(
                _on_mention,
                filters.mentioned & ~filters.me,
            ),
            group=1,
        )

        # Group 2: Welcome DM auto-reply (first-time incoming private DM)
        client.add_handler(
            MessageHandler(
                _on_welcome,
                dm_filter,
            ),
            group=2,
        )

        # Group 3: Keyword custom text auto-reply
        client.add_handler(
            MessageHandler(
                _on_keyword,
                ~filters.me & filters.text,
            ),
            group=3,
        )

        # Group 4: Offline/Away DM auto-reply (fallback if Welcome & Keyword did not reply)
        client.add_handler(
            MessageHandler(
                _on_offline,
                dm_filter,
            ),
            group=4,
        )
        logger.debug("Attached async message & status handlers for user_id=%s", telegram_user_id)

    # -- Automation Handlers --

    @staticmethod
    def _is_valid_dm_sender(message: Message, telegram_user_id: int) -> int | None:
        """Return sender_id if this incoming DM is from a real human peer, else None."""
        if not message.chat or not message.from_user:
            return None
        if getattr(message.from_user, "is_bot", False):
            return None
        if getattr(message.from_user, "is_support", False):
            return None
        sender_id = int(message.from_user.id)
        if sender_id == telegram_user_id or sender_id in TELEGRAM_SYSTEM_USER_IDS:
            return None
        return sender_id

    async def _is_account_offline(self, client: Client, telegram_user_id: int) -> bool:
        """Check whether the userbot account is offline using UserStatusHandler / 20s TTL-cached get_me()."""
        now = time.monotonic()
        cached = self._online_status_cache.get(telegram_user_id)
        if cached is not None and (now - cached[1]) < ONLINE_STATUS_CACHE_TTL_SECONDS:
            return not cached[0]

        try:
            me = await client.get_me()
            is_online = getattr(me, "status", None) == UserStatus.ONLINE
            self._online_status_cache[telegram_user_id] = (is_online, time.monotonic())
            return not is_online
        except Exception as exc:
            logger.warning(
                "[OFFLINE] Failed to check online status via get_me() for user=%s (failing closed): %s",
                telegram_user_id,
                exc,
            )
            return False

    async def _handle_mention(
        self, client: Client, message: Message, telegram_user_id: int
    ) -> None:
        enabled, template_id = self._mention_cache.get(telegram_user_id, (False, None))
        if not enabled or not template_id or not message.chat:
            logger.debug(
                "[MENTION] Ignored for user=%s (enabled=%s, template_id=%s)",
                telegram_user_id,
                enabled,
                template_id,
            )
            return

        key = (telegram_user_id, message.chat.id)
        now = time.monotonic()
        last_sent = self._mention_cooldowns.get(key)
        if last_sent is not None and (now - last_sent) < MENTION_COOLDOWN_SECONDS:
            logger.debug(
                "[MENTION] Cooldown active for user=%s chat=%s",
                telegram_user_id,
                message.chat.id,
            )
            return

        template = await self._get_template_cached(telegram_user_id, template_id)
        if not template:
            logger.warning(
                "[MENTION] Template %s missing for user=%s",
                template_id,
                telegram_user_id,
            )
            return

        self._mention_cooldowns[key] = now
        self._replied_messages.append((telegram_user_id, message.chat.id, message.id))
        logger.info(
            "[MENTION] Replying in chat=%s msg=%s with template=%s (user=%s)",
            message.chat.id,
            message.id,
            template_id,
            telegram_user_id,
        )
        await send_template_message(
            client,
            message.chat.id,
            template,
            reply_to_message_id=message.id,
        )

    async def _handle_welcome(
        self, client: Client, message: Message, telegram_user_id: int
    ) -> None:
        enabled, template_id = self._welcome_cache.get(telegram_user_id, (False, None))
        if not enabled or not template_id:
            return

        sender_id = self._is_valid_dm_sender(message, telegram_user_id)
        if sender_id is None:
            return

        msg_key = (telegram_user_id, message.chat.id, message.id)
        if msg_key in self._replied_messages:
            return

        welcomed_set = self._welcomed_users_cache.setdefault(telegram_user_id, set())
        if sender_id in welcomed_set:
            return

        # Optimistic burst-safe reservation before any await
        welcomed_set.add(sender_id)
        self._replied_messages.append(msg_key)

        template = await self._get_template_cached(telegram_user_id, template_id)
        if not template:
            welcomed_set.discard(sender_id)
            try:
                self._replied_messages.remove(msg_key)
            except ValueError:
                pass
            logger.warning(
                "[WELCOME] Template %s missing for user=%s",
                template_id,
                telegram_user_id,
            )
            return

        logger.info(
            "[WELCOME] Sending welcome template=%s to sender=%s msg=%s (user=%s)",
            template_id,
            sender_id,
            message.id,
            telegram_user_id,
        )
        sent = await send_template_message(
            client,
            message.chat.id,
            template,
            reply_to_message_id=message.id,
        )
        if sent is not None:
            try:
                await self.db.mark_user_welcomed(telegram_user_id, sender_id)
            except Exception as exc:
                logger.warning(
                    "[WELCOME] Failed to persist welcomed sender=%s for user=%s: %s",
                    sender_id,
                    telegram_user_id,
                    exc,
                )
        else:
            # Rollback cache entry if Telegram send failed (e.g. FloodWait)
            welcomed_set.discard(sender_id)
            try:
                self._replied_messages.remove(msg_key)
            except ValueError:
                pass

    async def _handle_keyword_reply(
        self, client: Client, message: Message, telegram_user_id: int
    ) -> None:
        if not message.text or not message.chat:
            return

        # Avoid double-replying if Mention or Welcome already answered this exact message
        if (telegram_user_id, message.chat.id, message.id) in self._replied_messages:
            return

        rules = self._keyword_cache.get(telegram_user_id)
        if not rules:
            return

        incoming_lower = message.text.lower()
        now = time.monotonic()

        for rule in rules:
            trigger = rule.get("trigger_text", "")
            if not trigger:
                continue
            trigger_lower = trigger.lower()
            if trigger_lower in incoming_lower:
                cd_key = (telegram_user_id, message.chat.id, trigger_lower)
                last_sent = self._keyword_cooldowns.get(cd_key)
                if last_sent is not None and (now - last_sent) < KEYWORD_COOLDOWN_SECONDS:
                    logger.debug(
                        "[KEYWORD] Cooldown active for user=%s chat=%s trigger=%r",
                        telegram_user_id,
                        message.chat.id,
                        trigger,
                    )
                    return

                template = await self._get_template_cached(
                    telegram_user_id, rule.get("template_id")
                )
                if not template:
                    continue

                self._keyword_cooldowns[cd_key] = now
                self._replied_messages.append((telegram_user_id, message.chat.id, message.id))
                logger.info(
                    "[KEYWORD] Matched %r in chat=%s msg=%s -> template=%s (user=%s)",
                    trigger,
                    message.chat.id,
                    message.id,
                    rule.get("template_id"),
                    telegram_user_id,
                )
                await send_template_message(
                    client,
                    message.chat.id,
                    template,
                    reply_to_message_id=message.id,
                )
                return

    async def _handle_offline(
        self, client: Client, message: Message, telegram_user_id: int
    ) -> None:
        enabled, mode, template_id, cooldown_s = self._offline_cache.get(
            telegram_user_id,
            (False, "manual", None, DEFAULT_OFFLINE_COOLDOWN_SECONDS),
        )
        if not enabled or not template_id:
            return

        sender_id = self._is_valid_dm_sender(message, telegram_user_id)
        if sender_id is None:
            return

        msg_key = (telegram_user_id, message.chat.id, message.id)
        if msg_key in self._replied_messages:
            return

        cd_key = (telegram_user_id, sender_id)
        now = time.monotonic()
        last_sent = self._offline_cooldowns.get(cd_key)
        if last_sent is not None and (now - last_sent) < cooldown_s:
            logger.debug(
                "[OFFLINE] Cooldown active for user=%s sender=%s",
                telegram_user_id,
                sender_id,
            )
            return

        if mode == "auto":
            if not await self._is_account_offline(client, telegram_user_id):
                return
            # Re-check after await in case another message raced
            now = time.monotonic()
            if msg_key in self._replied_messages:
                return
            last_sent = self._offline_cooldowns.get(cd_key)
            if last_sent is not None and (now - last_sent) < cooldown_s:
                return

        template = await self._get_template_cached(telegram_user_id, template_id)
        if not template:
            logger.warning(
                "[OFFLINE] Template %s missing for user=%s",
                template_id,
                telegram_user_id,
            )
            return

        # Re-check and reserve synchronously before await send_template_message
        now = time.monotonic()
        if msg_key in self._replied_messages:
            return
        last_sent = self._offline_cooldowns.get(cd_key)
        if last_sent is not None and (now - last_sent) < cooldown_s:
            return

        self._offline_cooldowns[cd_key] = now
        self._replied_messages.append(msg_key)
        logger.info(
            "[OFFLINE] Sending offline[%s] template=%s to sender=%s msg=%s (user=%s)",
            mode,
            template_id,
            sender_id,
            message.id,
            telegram_user_id,
        )
        await send_template_message(
            client,
            message.chat.id,
            template,
            reply_to_message_id=message.id,
        )

    # -- Saved Messages Command Router --

    @staticmethod
    def _format_emoji_summary(entities_json: list[dict[str, Any]] | None) -> str:
        emoji_ids = extract_custom_emoji_ids(entities_json)
        if not emoji_ids:
            return "Custom Emojis: 0"
        return f"Custom Emojis: {len(emoji_ids)} (IDs: {', '.join(emoji_ids)})"

    async def _resolve_template_from_command(
        self,
        message: Message,
        telegram_user_id: int,
        usage_text: str,
    ) -> dict[str, Any] | None:
        """Resolve a template from `<TEMPLATE_ID>`, inline text with entities, or a replied message."""
        full_text = message.text or ""
        cmd_token = full_text.split(maxsplit=1)[0]
        start_idx = find_payload_start(full_text, len(cmd_token))

        if start_idx < len(full_text):
            raw_arg = full_text[start_idx:].strip()
            candidate_id = raw_arg.upper()
            if TEMPLATE_ID_RE.match(candidate_id):
                tpl = await self._get_template_cached(telegram_user_id, candidate_id)
                if not tpl:
                    await message.reply_text(
                        f"Template {candidate_id} not found. Create it first with /new.",
                        parse_mode=ParseMode.DISABLED,
                    )
                    return None
                return tpl

            payload_text, entities_json = serialize_entities_with_offset(
                full_text, message.entities, start_char_idx=start_idx
            )
        elif message.reply_to_message:
            replied = message.reply_to_message
            src_text = replied.text or replied.caption or ""
            src_entities = replied.entities or replied.caption_entities
            if not src_text.strip():
                await message.reply_text(
                    "The replied message has no text or caption to save.",
                    parse_mode=ParseMode.DISABLED,
                )
                return None
            payload_text, entities_json = serialize_entities_with_offset(
                src_text, src_entities, start_char_idx=0
            )
        else:
            await message.reply_text(usage_text, parse_mode=ParseMode.DISABLED)
            return None

        if not payload_text:
            await message.reply_text(
                "Template text cannot be empty.", parse_mode=ParseMode.DISABLED
            )
            return None

        tpl = await self.db.create_template(telegram_user_id, payload_text, entities_json)
        self._template_cache.setdefault(telegram_user_id, {})[tpl["id"]] = tpl
        return tpl

    async def _handle_saved_messages_command(
        self, client: Client, message: Message, telegram_user_id: int
    ) -> None:
        text = message.text or ""
        if not text.startswith("/"):
            return

        parts = text.split(maxsplit=1)
        cmd = parts[0].lower()
        logger.info(
            "[CMD] Received %s from Saved Messages (user=%s, msg_id=%s)",
            cmd,
            telegram_user_id,
            message.id,
        )

        try:
            if cmd == "/help":
                await self._cmd_help(message)
            elif cmd == "/ping":
                await self._cmd_ping(message)
            elif cmd == "/status":
                await self._cmd_status(client, message, telegram_user_id)
            elif cmd == "/preview":
                await self._cmd_preview_template(client, message, telegram_user_id)
            elif cmd == "/new":
                await self._cmd_new_template(client, message, telegram_user_id)
            elif cmd == "/templates":
                await self._cmd_list_templates(message, telegram_user_id)
            elif cmd == "/del_template":
                await self._cmd_del_template(message, telegram_user_id)
            elif cmd == "/set_mention":
                await self._cmd_set_mention(client, message, telegram_user_id)
            elif cmd == "/mention_off":
                await self._cmd_mention_off(message, telegram_user_id)
            elif cmd == "/set_welcome":
                await self._cmd_set_welcome(client, message, telegram_user_id)
            elif cmd == "/welcome":
                await self._cmd_welcome_status(client, message, telegram_user_id)
            elif cmd == "/welcome_on":
                await self._cmd_welcome_on(message, telegram_user_id)
            elif cmd == "/welcome_off":
                await self._cmd_welcome_off(message, telegram_user_id)
            elif cmd == "/welcome_reset":
                await self._cmd_welcome_reset(message, telegram_user_id)
            elif cmd == "/set_offline":
                await self._cmd_set_offline(client, message, telegram_user_id)
            elif cmd == "/offline":
                await self._cmd_offline(client, message, telegram_user_id)
            elif cmd == "/offline_on":
                await self._cmd_offline_on(message, telegram_user_id, mode="manual")
            elif cmd == "/offline_off":
                await self._cmd_offline_off(message, telegram_user_id)
            elif cmd == "/schedule":
                await self._cmd_schedule_onetime(message, telegram_user_id)
            elif cmd == "/schedule_every":
                await self._cmd_schedule_every(message, telegram_user_id)
            elif cmd == "/schedules":
                await self._cmd_list_schedules(message, telegram_user_id)
            elif cmd == "/cancel_schedule":
                await self._cmd_cancel_schedule(message, telegram_user_id)
            elif cmd == "/set_reply":
                await self._cmd_set_reply(message, telegram_user_id)
            elif cmd == "/list_replies":
                await self._cmd_list_replies(message, telegram_user_id)
            elif cmd == "/del_reply":
                await self._cmd_del_reply(message, telegram_user_id)
            else:
                logger.debug("[CMD] Unknown command %s from user=%s", cmd, telegram_user_id)
                await message.reply_text(
                    f"Unknown command '{cmd}'. Send /help to see all available commands.",
                    parse_mode=ParseMode.DISABLED,
                )
        except Exception as exc:
            logger.exception("Error handling command %s for user %s: %s", cmd, telegram_user_id, exc)
            await message.reply_text(
                f"Command failed: {exc}", parse_mode=ParseMode.DISABLED
            )

    async def _cmd_help(self, message: Message) -> None:
        help_text = (
            "Userbot Automation Commands (Saved Messages Only)\n"
            "=================================================\n\n"
            "System:\n"
            "• /status — Show live account, session source, automation & schedule stats\n"
            "• /ping — Check bot response latency\n"
            "• /help — Show this command reference\n\n"
            "Templates (Premium Custom Emojis Supported):\n"
            "• /new <text with emojis> — Save a new template (or reply to any message with /new)\n"
            "• /templates — List all saved templates & emoji counts\n"
            "• /preview <TEMPLATE_ID> — Preview a saved template with emojis\n"
            "• /del_template <TEMPLATE_ID> — Delete a template\n\n"
            "Mention Auto-Reply:\n"
            "• /set_mention <TEMPLATE_ID | text | reply> — Enable mention auto-reply\n"
            "• /mention_off — Disable mention auto-reply\n\n"
            "Welcome DM Auto-Reply (First-Time Incoming DMs):\n"
            "• /set_welcome <TEMPLATE_ID | text | reply> — Set & enable welcome DM\n"
            "• /welcome — Show welcome status, emoji IDs & preview\n"
            "• /welcome_on — Re-enable welcome DM with saved template\n"
            "• /welcome_off — Disable welcome DM\n"
            "• /welcome_reset [SENDER_USER_ID] — Reset welcomed users (all or one user)\n\n"
            "Offline / Away DM Auto-Reply:\n"
            "• /set_offline <TEMPLATE_ID | text | reply> — Set & enable offline DM\n"
            "• /offline — Show offline status, mode, cooldown & preview\n"
            "• /offline on (or /offline_on) — Enable manual offline mode\n"
            "• /offline auto — Enable automatic Telegram offline detection\n"
            "• /offline off (or /offline_off) — Disable offline auto-reply\n"
            "• /offline cooldown <SECONDS> — Set per-sender cooldown (10–86400s)\n\n"
            "Scheduled Posts:\n"
            f"• /schedule <TEMPLATE_ID> <CHAT_ID> <YYYY-MM-DD> <HH:MM> — One-time post ({self.tz.key})\n"
            "• /schedule_every <TEMPLATE_ID> <CHAT_ID> <MINUTES> — Recurring post\n"
            "• /schedules — List active scheduled jobs\n"
            "• /cancel_schedule <JOB_ID> — Cancel & remove a scheduled job\n\n"
            "Keyword Auto-Reply:\n"
            "• /set_reply <keyword> | <TEMPLATE_ID> — Bind keyword to a template\n"
            "• /set_reply <keyword> | <text with emojis> — Bind keyword to inline custom text\n"
            "• /list_replies — List all keyword auto-reply rules\n"
            "• /del_reply <keyword> — Delete a keyword rule"
        )
        await message.reply_text(help_text, parse_mode=ParseMode.DISABLED)

    async def _cmd_ping(self, message: Message) -> None:
        start = time.monotonic()
        sent = await message.reply_text("Pong!", parse_mode=ParseMode.DISABLED)
        elapsed_ms = int((time.monotonic() - start) * 1000)
        await sent.edit_text(f"Pong! ({elapsed_ms} ms)", parse_mode=ParseMode.DISABLED)

    async def _cmd_status(
        self, client: Client, message: Message, telegram_user_id: int
    ) -> None:
        me = await client.get_me()
        meta = self.session_metadata.get(telegram_user_id, {})
        source = meta.get("source", "user_sessions")
        label = meta.get("label")
        source_display = f"{source} ({label})" if label else source

        mention_enabled, mention_tpl = self._mention_cache.get(telegram_user_id, (False, None))
        welcome_enabled, welcome_tpl = self._welcome_cache.get(telegram_user_id, (False, None))
        welcomed_count = len(self._welcomed_users_cache.get(telegram_user_id, set()))
        offline_enabled, offline_mode, offline_tpl, offline_cd = self._offline_cache.get(
            telegram_user_id, (False, "manual", None, DEFAULT_OFFLINE_COOLDOWN_SECONDS)
        )
        tpl_count = len(self._template_cache.get(telegram_user_id, {}))
        kw_count = len(self._keyword_cache.get(telegram_user_id, []))
        active_jobs = await self.db.list_active_scheduled_posts(telegram_user_id)

        offline_display = (
            f"ON [{offline_mode}] ({offline_tpl}, cooldown {offline_cd}s)"
            if offline_enabled
            else f"OFF (mode: {offline_mode}, cooldown {offline_cd}s)"
        )

        status_text = (
            "Userbot Status (Kurigram 2.2.27)\n"
            "=================================\n"
            f"Account: {me.first_name or ''} (@{me.username or 'none'}) [{me.id}]\n"
            f"Session Source: {source_display}\n"
            f"Mention Auto-Reply: {'ON (' + str(mention_tpl) + ')' if mention_enabled else 'OFF'}\n"
            f"Welcome Auto-Reply: {'ON (' + str(welcome_tpl) + f', welcomed: {welcomed_count})' if welcome_enabled else f'OFF (welcomed: {welcomed_count})'}\n"
            f"Offline Auto-Reply: {offline_display}\n"
            f"Saved Templates: {tpl_count}\n"
            f"Keyword Rules: {kw_count}\n"
            f"Active Schedules: {len(active_jobs)}\n"
            f"Timezone: {self.tz.key}\n"
            f"Active Clients on Server: {len(self.active_clients)}"
        )
        await message.reply_text(status_text, parse_mode=ParseMode.DISABLED)

    async def _cmd_preview_template(
        self, client: Client, message: Message, telegram_user_id: int
    ) -> None:
        parts = (message.text or "").split(maxsplit=1)
        if len(parts) < 2 or not parts[1].strip():
            await message.reply_text(
                "Usage: /preview <TEMPLATE_ID>", parse_mode=ParseMode.DISABLED
            )
            return

        tpl_id = parts[1].strip().upper()
        tpl = await self._get_template_cached(telegram_user_id, tpl_id)
        if not tpl:
            await message.reply_text(
                f"Template {tpl_id} not found.", parse_mode=ParseMode.DISABLED
            )
            return

        await send_template_message(client, "me", tpl)

    async def _cmd_new_template(
        self, client: Client, message: Message, telegram_user_id: int
    ) -> None:
        full_text = message.text or ""
        cmd_token = full_text.split(maxsplit=1)[0]
        start_idx = find_payload_start(full_text, len(cmd_token))

        if start_idx < len(full_text):
            payload_text, entities_json = serialize_entities_with_offset(
                full_text, message.entities, start_char_idx=start_idx
            )
        elif message.reply_to_message:
            replied = message.reply_to_message
            src_text = replied.text or replied.caption or ""
            src_entities = replied.entities or replied.caption_entities
            if not src_text.strip():
                await message.reply_text(
                    "The replied message has no text or caption to save.",
                    parse_mode=ParseMode.DISABLED,
                )
                return
            payload_text, entities_json = serialize_entities_with_offset(
                src_text, src_entities, start_char_idx=0
            )
        else:
            await message.reply_text(
                "Usage: /new <message text with custom emojis>\n"
                "Or reply to any message in Saved Messages with /new.",
                parse_mode=ParseMode.DISABLED,
            )
            return

        if not payload_text:
            await message.reply_text(
                "Template text cannot be empty.", parse_mode=ParseMode.DISABLED
            )
            return

        tpl = await self.db.create_template(telegram_user_id, payload_text, entities_json)
        self._template_cache.setdefault(telegram_user_id, {})[tpl["id"]] = tpl
        emoji_summary = self._format_emoji_summary(entities_json)

        await message.reply_text(
            f"Template saved!\n"
            f"ID: {tpl['id']}\n"
            f"{emoji_summary}\n"
            f"Total Entities: {len(entities_json)}\n\n"
            f"Preview below:",
            parse_mode=ParseMode.DISABLED,
        )
        await send_template_message(client, "me", tpl)

    async def _cmd_list_templates(self, message: Message, telegram_user_id: int) -> None:
        templates = await self.db.list_templates(telegram_user_id)
        self._template_cache[telegram_user_id] = {t["id"]: t for t in templates}

        if not templates:
            await message.reply_text(
                "No templates saved yet. Use /new <text> to create one.",
                parse_mode=ParseMode.DISABLED,
            )
            return

        lines = [f"Saved Templates ({len(templates)}):"]
        for tpl in templates:
            emoji_cnt = count_custom_emojis(tpl.get("entities_json"))
            preview = tpl["text"].replace("\n", " ")
            if len(preview) > 45:
                preview = preview[:42] + "..."
            lines.append(f"• {tpl['id']} | Emojis: {emoji_cnt} | {preview}")

        await message.reply_text("\n".join(lines), parse_mode=ParseMode.DISABLED)

    async def _cmd_del_template(self, message: Message, telegram_user_id: int) -> None:
        parts = (message.text or "").split(maxsplit=1)
        if len(parts) < 2 or not parts[1].strip():
            await message.reply_text(
                "Usage: /del_template <TEMPLATE_ID>", parse_mode=ParseMode.DISABLED
            )
            return

        tpl_id = parts[1].strip().upper()
        deleted = await self.db.delete_template(telegram_user_id, tpl_id)
        self._template_cache.setdefault(telegram_user_id, {}).pop(tpl_id, None)

        if deleted:
            await message.reply_text(
                f"Deleted template {tpl_id}.", parse_mode=ParseMode.DISABLED
            )
        else:
            await message.reply_text(
                f"Template {tpl_id} not found.", parse_mode=ParseMode.DISABLED
            )

    async def _cmd_set_mention(
        self, client: Client, message: Message, telegram_user_id: int
    ) -> None:
        usage = (
            "Usage:\n"
            "• /set_mention <TEMPLATE_ID>\n"
            "• /set_mention <custom text with emojis>\n"
            "• Or reply to any message in Saved Messages with /set_mention"
        )
        tpl = await self._resolve_template_from_command(message, telegram_user_id, usage)
        if not tpl:
            return

        template_id = tpl["id"]
        await self.db.set_mention_config(
            telegram_user_id, enabled=True, template_id=template_id
        )
        self._mention_cache[telegram_user_id] = (True, template_id)
        emoji_summary = self._format_emoji_summary(tpl.get("entities_json"))

        await message.reply_text(
            f"Mention auto-reply ENABLED!\n"
            f"Template: {template_id}\n"
            f"{emoji_summary}\n"
            f"Cooldown: {int(MENTION_COOLDOWN_SECONDS)}s per chat\n\n"
            f"Preview below:",
            parse_mode=ParseMode.DISABLED,
        )
        await send_template_message(client, "me", tpl)

    async def _cmd_mention_off(self, message: Message, telegram_user_id: int) -> None:
        await self.db.set_mention_config(telegram_user_id, enabled=False)
        _, current_tpl = self._mention_cache.get(telegram_user_id, (False, None))
        self._mention_cache[telegram_user_id] = (False, current_tpl)
        await message.reply_text(
            "Mention auto-reply DISABLED.", parse_mode=ParseMode.DISABLED
        )

    # -- Welcome DM Commands --

    async def _cmd_set_welcome(
        self, client: Client, message: Message, telegram_user_id: int
    ) -> None:
        usage = (
            "Usage:\n"
            "• /set_welcome <TEMPLATE_ID>\n"
            "• /set_welcome <welcome text with custom emojis>\n"
            "• Or reply to any message in Saved Messages with /set_welcome"
        )
        tpl = await self._resolve_template_from_command(message, telegram_user_id, usage)
        if not tpl:
            return

        template_id = tpl["id"]
        await self.db.set_welcome_config(
            telegram_user_id, enabled=True, template_id=template_id
        )
        self._welcome_cache[telegram_user_id] = (True, template_id)
        emoji_summary = self._format_emoji_summary(tpl.get("entities_json"))

        await message.reply_text(
            f"Welcome DM auto-reply ENABLED!\n"
            f"Template: {template_id}\n"
            f"{emoji_summary}\n\n"
            f"Preview below:",
            parse_mode=ParseMode.DISABLED,
        )
        await send_template_message(client, "me", tpl)

    async def _cmd_welcome_status(
        self, client: Client, message: Message, telegram_user_id: int
    ) -> None:
        enabled, template_id = self._welcome_cache.get(telegram_user_id, (False, None))
        welcomed_count = len(self._welcomed_users_cache.get(telegram_user_id, set()))
        tpl = (
            await self._get_template_cached(telegram_user_id, template_id)
            if template_id
            else None
        )
        emoji_summary = (
            self._format_emoji_summary(tpl.get("entities_json"))
            if tpl
            else "Custom Emojis: 0"
        )
        status_text = (
            f"Welcome DM Auto-Reply\n"
            f"Status: {'ON' if enabled else 'OFF'}\n"
            f"Template: {template_id or 'None'}\n"
            f"{emoji_summary}\n"
            f"Welcomed Users: {welcomed_count}"
        )
        if tpl:
            status_text += "\n\nPreview below:"
        await message.reply_text(status_text, parse_mode=ParseMode.DISABLED)
        if tpl:
            await send_template_message(client, "me", tpl)

    async def _cmd_welcome_on(self, message: Message, telegram_user_id: int) -> None:
        _, template_id = self._welcome_cache.get(telegram_user_id, (False, None))
        if not template_id:
            await message.reply_text(
                "No welcome template configured yet. Use /set_welcome <TEMPLATE_ID | text> first.",
                parse_mode=ParseMode.DISABLED,
            )
            return
        await self.db.set_welcome_config(telegram_user_id, enabled=True)
        self._welcome_cache[telegram_user_id] = (True, template_id)
        await message.reply_text(
            f"Welcome DM auto-reply ENABLED (template: {template_id}).",
            parse_mode=ParseMode.DISABLED,
        )

    async def _cmd_welcome_off(self, message: Message, telegram_user_id: int) -> None:
        await self.db.set_welcome_config(telegram_user_id, enabled=False)
        _, current_tpl = self._welcome_cache.get(telegram_user_id, (False, None))
        self._welcome_cache[telegram_user_id] = (False, current_tpl)
        await message.reply_text(
            "Welcome DM auto-reply DISABLED.", parse_mode=ParseMode.DISABLED
        )

    async def _cmd_welcome_reset(self, message: Message, telegram_user_id: int) -> None:
        parts = (message.text or "").split(maxsplit=1)
        if len(parts) > 1 and parts[1].strip():
            try:
                sender_id = int(parts[1].strip())
            except ValueError:
                await message.reply_text(
                    "Usage: /welcome_reset [SENDER_USER_ID]\nSENDER_USER_ID must be a numeric user ID.",
                    parse_mode=ParseMode.DISABLED,
                )
                return
            removed = await self.db.reset_welcomed_users(telegram_user_id, sender_id)
            self._welcomed_users_cache.setdefault(telegram_user_id, set()).discard(sender_id)
            await message.reply_text(
                f"Reset welcome history for user {sender_id} (removed {removed} record(s)).",
                parse_mode=ParseMode.DISABLED,
            )
            return

        removed = await self.db.reset_welcomed_users(telegram_user_id)
        self._welcomed_users_cache[telegram_user_id] = set()
        await message.reply_text(
            f"Reset welcome history for all users (removed {removed} record(s)).",
            parse_mode=ParseMode.DISABLED,
        )

    # -- Offline / Away DM Commands --

    async def _cmd_set_offline(
        self, client: Client, message: Message, telegram_user_id: int
    ) -> None:
        usage = (
            "Usage:\n"
            "• /set_offline <TEMPLATE_ID>\n"
            "• /set_offline <offline text with custom emojis>\n"
            "• Or reply to any message in Saved Messages with /set_offline"
        )
        tpl = await self._resolve_template_from_command(message, telegram_user_id, usage)
        if not tpl:
            return

        template_id = tpl["id"]
        _, current_mode, _, current_cd = self._offline_cache.get(
            telegram_user_id,
            (False, "manual", None, DEFAULT_OFFLINE_COOLDOWN_SECONDS),
        )
        mode = current_mode if current_mode in ("manual", "auto") else "manual"

        await self.db.set_offline_config(
            telegram_user_id,
            enabled=True,
            mode=mode,
            template_id=template_id,
        )
        self._offline_cache[telegram_user_id] = (True, mode, template_id, current_cd)
        emoji_summary = self._format_emoji_summary(tpl.get("entities_json"))

        await message.reply_text(
            f"Offline auto-reply ENABLED [{mode}]!\n"
            f"Template: {template_id}\n"
            f"{emoji_summary}\n"
            f"Cooldown: {current_cd}s per sender\n\n"
            f"Preview below:",
            parse_mode=ParseMode.DISABLED,
        )
        await send_template_message(client, "me", tpl)

    async def _cmd_offline(
        self, client: Client, message: Message, telegram_user_id: int
    ) -> None:
        parts = (message.text or "").split()
        if len(parts) == 1:
            enabled, mode, template_id, cooldown_s = self._offline_cache.get(
                telegram_user_id,
                (False, "manual", None, DEFAULT_OFFLINE_COOLDOWN_SECONDS),
            )
            tpl = (
                await self._get_template_cached(telegram_user_id, template_id)
                if template_id
                else None
            )
            emoji_summary = (
                self._format_emoji_summary(tpl.get("entities_json"))
                if tpl
                else "Custom Emojis: 0"
            )
            status_label = f"ON [{mode}]" if enabled else f"OFF (mode: {mode})"
            status_text = (
                f"Offline / Away DM Auto-Reply\n"
                f"Status: {status_label}\n"
                f"Template: {template_id or 'None'}\n"
                f"{emoji_summary}\n"
                f"Cooldown: {cooldown_s}s per sender"
            )
            if tpl:
                status_text += "\n\nPreview below:"
            await message.reply_text(status_text, parse_mode=ParseMode.DISABLED)
            if tpl:
                await send_template_message(client, "me", tpl)
            return

        subcmd = parts[1].lower()
        if subcmd == "on":
            await self._cmd_offline_on(message, telegram_user_id, mode="manual")
        elif subcmd == "auto":
            await self._cmd_offline_on(message, telegram_user_id, mode="auto")
        elif subcmd == "off":
            await self._cmd_offline_off(message, telegram_user_id)
        elif subcmd == "cooldown":
            if len(parts) != 3:
                await message.reply_text(
                    f"Usage: /offline cooldown <SECONDS> ({MIN_OFFLINE_COOLDOWN_SECONDS}–{MAX_OFFLINE_COOLDOWN_SECONDS})",
                    parse_mode=ParseMode.DISABLED,
                )
                return
            try:
                seconds = int(parts[2])
            except ValueError:
                await message.reply_text(
                    "SECONDS must be an integer.", parse_mode=ParseMode.DISABLED
                )
                return
            if not (MIN_OFFLINE_COOLDOWN_SECONDS <= seconds <= MAX_OFFLINE_COOLDOWN_SECONDS):
                await message.reply_text(
                    f"Cooldown must be between {MIN_OFFLINE_COOLDOWN_SECONDS} and {MAX_OFFLINE_COOLDOWN_SECONDS} seconds.",
                    parse_mode=ParseMode.DISABLED,
                )
                return
            enabled, mode, template_id, _ = self._offline_cache.get(
                telegram_user_id,
                (False, "manual", None, DEFAULT_OFFLINE_COOLDOWN_SECONDS),
            )
            await self.db.set_offline_config(telegram_user_id, cooldown_seconds=seconds)
            self._offline_cache[telegram_user_id] = (enabled, mode, template_id, seconds)
            await message.reply_text(
                f"Offline auto-reply cooldown updated to {seconds}s per sender.",
                parse_mode=ParseMode.DISABLED,
            )
        else:
            await message.reply_text(
                "Usage:\n"
                "• /offline — Show status\n"
                "• /offline on — Enable manual offline mode\n"
                "• /offline auto — Enable automatic offline detection\n"
                "• /offline off — Disable offline auto-reply\n"
                "• /offline cooldown <SECONDS> — Set cooldown (10–86400s)",
                parse_mode=ParseMode.DISABLED,
            )

    async def _cmd_offline_on(
        self, message: Message, telegram_user_id: int, *, mode: str = "manual"
    ) -> None:
        _, _, template_id, cooldown_s = self._offline_cache.get(
            telegram_user_id,
            (False, "manual", None, DEFAULT_OFFLINE_COOLDOWN_SECONDS),
        )
        if not template_id:
            await message.reply_text(
                "No offline template configured yet. Use /set_offline <TEMPLATE_ID | text> first.",
                parse_mode=ParseMode.DISABLED,
            )
            return
        await self.db.set_offline_config(telegram_user_id, enabled=True, mode=mode)
        self._offline_cache[telegram_user_id] = (True, mode, template_id, cooldown_s)
        await message.reply_text(
            f"Offline auto-reply ENABLED [{mode}] (template: {template_id}, cooldown: {cooldown_s}s).",
            parse_mode=ParseMode.DISABLED,
        )

    async def _cmd_offline_off(self, message: Message, telegram_user_id: int) -> None:
        _, mode, template_id, cooldown_s = self._offline_cache.get(
            telegram_user_id,
            (False, "manual", None, DEFAULT_OFFLINE_COOLDOWN_SECONDS),
        )
        await self.db.set_offline_config(telegram_user_id, enabled=False)
        self._offline_cache[telegram_user_id] = (False, mode, template_id, cooldown_s)
        await message.reply_text(
            "Offline auto-reply DISABLED.", parse_mode=ParseMode.DISABLED
        )

    async def _cmd_schedule_onetime(
        self, message: Message, telegram_user_id: int
    ) -> None:
        parts = (message.text or "").split()
        if len(parts) != 5:
            await message.reply_text(
                f"Usage: /schedule <TEMPLATE_ID> <TARGET_CHAT_ID> <YYYY-MM-DD> <HH:MM>\n"
                f"Timezone: {self.tz.key}",
                parse_mode=ParseMode.DISABLED,
            )
            return

        tpl_id = parts[1].upper()
        try:
            target_chat_id = int(parts[2])
        except ValueError:
            await message.reply_text(
                "TARGET_CHAT_ID must be a numeric chat ID (e.g. -1001234567890).",
                parse_mode=ParseMode.DISABLED,
            )
            return

        try:
            naive_dt = datetime.strptime(f"{parts[3]} {parts[4]}", "%Y-%m-%d %H:%M")
            run_dt = naive_dt.replace(tzinfo=self.tz)
        except ValueError:
            await message.reply_text(
                "Invalid date/time format. Use YYYY-MM-DD HH:MM (24-hour).",
                parse_mode=ParseMode.DISABLED,
            )
            return

        if run_dt <= datetime.now(self.tz):
            await message.reply_text(
                f"Scheduled time must be in the future ({self.tz.key}).",
                parse_mode=ParseMode.DISABLED,
            )
            return

        tpl = await self._get_template_cached(telegram_user_id, tpl_id)
        if not tpl:
            await message.reply_text(
                f"Template {tpl_id} not found.", parse_mode=ParseMode.DISABLED
            )
            return

        row = await self.db.create_scheduled_post(
            telegram_user_id=telegram_user_id,
            template_id=tpl_id,
            target_chat_id=target_chat_id,
            run_at=run_dt,
            interval_minutes=None,
        )
        job_id = int(row["id"])
        self._register_onetime_job(
            job_id=job_id,
            telegram_user_id=telegram_user_id,
            template_id=tpl_id,
            target_chat_id=target_chat_id,
            run_dt=run_dt,
        )

        await message.reply_text(
            f"One-time post scheduled!\n"
            f"Job ID: {job_id}\n"
            f"Template: {tpl_id}\n"
            f"Target Chat: {target_chat_id}\n"
            f"Run At: {run_dt.strftime('%Y-%m-%d %H:%M %Z')}",
            parse_mode=ParseMode.DISABLED,
        )

    async def _cmd_schedule_every(
        self, message: Message, telegram_user_id: int
    ) -> None:
        parts = (message.text or "").split()
        if len(parts) != 4:
            await message.reply_text(
                "Usage: /schedule_every <TEMPLATE_ID> <TARGET_CHAT_ID> <MINUTES>",
                parse_mode=ParseMode.DISABLED,
            )
            return

        tpl_id = parts[1].upper()
        try:
            target_chat_id = int(parts[2])
        except ValueError:
            await message.reply_text(
                "TARGET_CHAT_ID must be a numeric chat ID (e.g. -1001234567890).",
                parse_mode=ParseMode.DISABLED,
            )
            return

        try:
            minutes = int(parts[3])
            if minutes < 1:
                raise ValueError
        except ValueError:
            await message.reply_text(
                "MINUTES must be an integer >= 1.", parse_mode=ParseMode.DISABLED
            )
            return

        tpl = await self._get_template_cached(telegram_user_id, tpl_id)
        if not tpl:
            await message.reply_text(
                f"Template {tpl_id} not found.", parse_mode=ParseMode.DISABLED
            )
            return

        row = await self.db.create_scheduled_post(
            telegram_user_id=telegram_user_id,
            template_id=tpl_id,
            target_chat_id=target_chat_id,
            run_at=None,
            interval_minutes=minutes,
        )
        job_id = int(row["id"])
        self._register_interval_job(
            job_id=job_id,
            telegram_user_id=telegram_user_id,
            template_id=tpl_id,
            target_chat_id=target_chat_id,
            interval_minutes=minutes,
        )

        await message.reply_text(
            f"Recurring post scheduled!\n"
            f"Job ID: {job_id}\n"
            f"Template: {tpl_id}\n"
            f"Target Chat: {target_chat_id}\n"
            f"Interval: every {minutes} minute(s)",
            parse_mode=ParseMode.DISABLED,
        )

    async def _cmd_list_schedules(
        self, message: Message, telegram_user_id: int
    ) -> None:
        rows = await self.db.list_active_scheduled_posts(telegram_user_id)
        if not rows:
            await message.reply_text(
                "No active scheduled jobs.", parse_mode=ParseMode.DISABLED
            )
            return

        lines = [f"Active Scheduled Jobs ({len(rows)}):"]
        for row in rows:
            job_id = row["id"]
            tpl_id = row["template_id"]
            chat_id = row["target_chat_id"]
            if row.get("interval_minutes"):
                timing = f"Every {row['interval_minutes']} min"
            elif row.get("run_at"):
                dt = datetime.fromisoformat(str(row["run_at"]).replace("Z", "+00:00"))
                timing = dt.astimezone(self.tz).strftime("%Y-%m-%d %H:%M %Z")
            else:
                timing = "Unknown"
            lines.append(f"• Job {job_id} | {tpl_id} -> {chat_id} | {timing}")

        await message.reply_text("\n".join(lines), parse_mode=ParseMode.DISABLED)

    async def _cmd_cancel_schedule(
        self, message: Message, telegram_user_id: int
    ) -> None:
        parts = (message.text or "").split()
        if len(parts) != 2:
            await message.reply_text(
                "Usage: /cancel_schedule <JOB_ID>", parse_mode=ParseMode.DISABLED
            )
            return

        try:
            job_id = int(parts[1])
        except ValueError:
            await message.reply_text(
                "JOB_ID must be an integer.", parse_mode=ParseMode.DISABLED
            )
            return

        deleted = await self.db.delete_scheduled_post(job_id, telegram_user_id)
        if deleted:
            sched_id = f"sched_{job_id}"
            if self.scheduler.get_job(sched_id):
                self.scheduler.remove_job(sched_id)
            await message.reply_text(
                f"Cancelled and deleted scheduled job {job_id}.",
                parse_mode=ParseMode.DISABLED,
            )
        else:
            await message.reply_text(
                f"Scheduled job {job_id} not found.", parse_mode=ParseMode.DISABLED
            )

    async def _cmd_set_reply(self, message: Message, telegram_user_id: int) -> None:
        full_text = message.text or ""
        cmd_token = full_text.split(maxsplit=1)[0]
        body_start = find_payload_start(full_text, len(cmd_token))
        pipe_idx = full_text.find("|", body_start)

        if pipe_idx == -1:
            await message.reply_text(
                "Usage:\n"
                "• /set_reply <trigger_keyword> | <TEMPLATE_ID>\n"
                "• /set_reply <trigger_keyword> | <direct reply text with custom emojis>",
                parse_mode=ParseMode.DISABLED,
            )
            return

        trigger_keyword = full_text[body_start:pipe_idx].strip()
        reply_start = find_payload_start(full_text, pipe_idx + 1)

        if not trigger_keyword or reply_start >= len(full_text):
            await message.reply_text(
                "Both trigger keyword and reply template/text are required.",
                parse_mode=ParseMode.DISABLED,
            )
            return

        raw_reply = full_text[reply_start:].strip()
        candidate_id = raw_reply.upper()

        if TEMPLATE_ID_RE.match(candidate_id):
            tpl = await self._get_template_cached(telegram_user_id, candidate_id)
            if not tpl:
                await message.reply_text(
                    f"Template {candidate_id} not found.", parse_mode=ParseMode.DISABLED
                )
                return
            template_id = tpl["id"]
        else:
            payload_text, entities_json = serialize_entities_with_offset(
                full_text, message.entities, start_char_idx=reply_start
            )
            tpl = await self.db.create_template(telegram_user_id, payload_text, entities_json)
            self._template_cache.setdefault(telegram_user_id, {})[tpl["id"]] = tpl
            template_id = tpl["id"]

        await self.db.upsert_keyword_reply(telegram_user_id, trigger_keyword, template_id)
        await self.refresh_user_cache(telegram_user_id)

        await message.reply_text(
            f"Keyword auto-reply saved!\n"
            f"Trigger: {trigger_keyword}\n"
            f"Template: {template_id}",
            parse_mode=ParseMode.DISABLED,
        )

    async def _cmd_list_replies(self, message: Message, telegram_user_id: int) -> None:
        rules = await self.db.list_keyword_replies(telegram_user_id)
        if not rules:
            await message.reply_text(
                "No keyword auto-reply rules configured.", parse_mode=ParseMode.DISABLED
            )
            return

        lines = [f"Keyword Auto-Replies ({len(rules)}):"]
        for rule in rules:
            lines.append(f"• \"{rule['trigger_text']}\" -> {rule['template_id']}")

        await message.reply_text("\n".join(lines), parse_mode=ParseMode.DISABLED)

    async def _cmd_del_reply(self, message: Message, telegram_user_id: int) -> None:
        parts = (message.text or "").split(maxsplit=1)
        if len(parts) < 2 or not parts[1].strip():
            await message.reply_text(
                "Usage: /del_reply <trigger_keyword>", parse_mode=ParseMode.DISABLED
            )
            return

        trigger = parts[1].strip()
        deleted = await self.db.delete_keyword_reply(telegram_user_id, trigger)
        await self.refresh_user_cache(telegram_user_id)

        if deleted:
            await message.reply_text(
                f"Deleted keyword rule for \"{trigger}\".",
                parse_mode=ParseMode.DISABLED,
            )
        else:
            await message.reply_text(
                f"No keyword rule found for \"{trigger}\".",
                parse_mode=ParseMode.DISABLED,
            )
