from __future__ import annotations

import asyncio
import logging
import os
import re
import time
import uuid
from collections import deque
from datetime import datetime, timezone
from typing import Any
from zoneinfo import ZoneInfo

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.date import DateTrigger
from apscheduler.triggers.interval import IntervalTrigger
from pyrogram import Client, filters
from pyrogram.enums import MessageEntityType, ParseMode
from pyrogram.errors import (
    AuthKeyUnregistered,
    FloodWait,
    RPCError,
    SessionRevoked,
    UserDeactivated,
    UserDeactivatedBan,
)
from pyrogram.handlers import MessageHandler
from pyrogram.types import Message, MessageEntity
from supabase import Client as SupabaseClient
from supabase import create_client

logger = logging.getLogger("userbot")

TEMPLATE_ID_RE = re.compile(r"^TPL-[0-9A-F]{6}$")
MENTION_COOLDOWN_SECONDS = 30.0
KEYWORD_COOLDOWN_SECONDS = 15.0


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
            item["custom_emoji_id"] = int(entity.custom_emoji_id)
        if getattr(entity, "url", None):
            item["url"] = str(entity.url)
        if getattr(entity, "language", None):
            item["language"] = str(entity.language)

        serialized.append(item)

    return payload, serialized


def deserialize_entities(entities_json: list[dict[str, Any]] | None) -> list[MessageEntity]:
    """Reconstruct Pyrogram MessageEntity objects (including Premium Custom Emojis)."""
    if not entities_json:
        return []

    result: list[MessageEntity] = []
    for raw in entities_json:
        type_name = raw.get("type")
        if not type_name or not hasattr(MessageEntityType, type_name):
            continue

        entity_type = getattr(MessageEntityType, type_name)
        # TEXT_MENTION requires a live User object; skip if not reconstructible
        if entity_type == MessageEntityType.TEXT_MENTION:
            continue

        kwargs: dict[str, Any] = {
            "type": entity_type,
            "offset": int(raw["offset"]),
            "length": int(raw["length"]),
        }
        if raw.get("custom_emoji_id") is not None:
            kwargs["custom_emoji_id"] = int(raw["custom_emoji_id"])
        if raw.get("url"):
            kwargs["url"] = str(raw["url"])
        if raw.get("language"):
            kwargs["language"] = str(raw["language"])

        result.append(MessageEntity(**kwargs))

    return result


def count_custom_emojis(entities_json: list[dict[str, Any]] | None) -> int:
    """Count how many CUSTOM_EMOJI entities exist in a serialized entity list."""
    if not entities_json:
        return 0
    return sum(1 for e in entities_json if e.get("type") == "CUSTOM_EMOJI")


def find_payload_start(full_text: str, cmd_end_idx: int) -> int:
    """Find the first non-whitespace index after a command token."""
    idx = cmd_end_idx
    length = len(full_text)
    while idx < length and full_text[idx] in (" ", "\t", "\n", "\r"):
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
    try:
        return await client.send_message(
            chat_id=chat_id,
            text=text,
            entities=entities or None,
            parse_mode=ParseMode.DISABLED,
            reply_to_message_id=reply_to_message_id,
        )
    except FloodWait as exc:
        logger.warning(
            "FloodWait (%ss) while sending template %s to chat %s",
            exc.value,
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

    async def set_mention_config(
        self,
        telegram_user_id: int,
        enabled: bool,
        template_id: str | None = None,
    ) -> None:
        update_data: dict[str, Any] = {"mention_enabled": enabled}
        if template_id is not None:
            update_data["mention_template_id"] = template_id
        await self._run(
            lambda: self._client.table("user_sessions")
            .update(update_data)
            .eq("telegram_user_id", telegram_user_id)
            .execute()
        )

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

        # Per-user in-memory caches for low-latency message handling
        self._mention_cache: dict[int, tuple[bool, str | None]] = {}
        self._keyword_cache: dict[int, list[dict[str, Any]]] = {}
        self._template_cache: dict[int, dict[str, dict[str, Any]]] = {}

        # Anti-spam / loop prevention cooldowns
        self._mention_cooldowns: dict[tuple[int, int], float] = {}
        self._keyword_cooldowns: dict[tuple[int, int, str], float] = {}
        self._replied_messages: deque[tuple[int, int, int]] = deque(maxlen=500)

    # -- Lifecycle --

    async def start_all_sessions(self) -> None:
        """Load every active session_string from Supabase and start its in-memory client."""
        sessions = await self.db.get_active_sessions()
        logger.info("Found %d active user session(s) in Supabase.", len(sessions))
        for row in sessions:
            user_id = int(row["telegram_user_id"])
            session_string = row.get("session_string")
            if not session_string:
                continue
            try:
                await self.start_client(user_id, session_string)
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

    async def start_client(
        self,
        telegram_user_id: int,
        session_string: str,
        notify_connected: bool = True,
        is_new_login: bool = False,
    ) -> Client:
        """Start (or restart) an in-memory Pyrogram client with all automation handlers."""
        if telegram_user_id in self.active_clients:
            logger.debug("Stopping existing client for user_id=%s before restart", telegram_user_id)
            await self.stop_client(telegram_user_id)

        client = Client(
            name=f"userbot_{telegram_user_id}",
            api_id=self.api_id,
            api_hash=self.api_hash,
            session_string=session_string,
            in_memory=True,
        )

        self._attach_handlers(client, telegram_user_id)
        await client.start()

        self.active_clients[telegram_user_id] = client
        await self.refresh_user_cache(telegram_user_id)

        tpl_count = len(self._template_cache.get(telegram_user_id, {}))
        kw_count = len(self._keyword_cache.get(telegram_user_id, []))
        mention_enabled, mention_tpl = self._mention_cache.get(telegram_user_id, (False, None))
        logger.info(
            "Started in-memory userbot client for user_id=%s (templates=%d, keywords=%d, mention_enabled=%s, mention_tpl=%s)",
            telegram_user_id,
            tpl_count,
            kw_count,
            mention_enabled,
            mention_tpl,
        )

        if notify_connected:
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
                logger.info("Sent connection confirmation to Saved Messages for user_id=%s", telegram_user_id)
            except Exception as exc:
                logger.warning(
                    "Could not send connection confirmation to Saved Messages for user_id=%s: %s",
                    telegram_user_id,
                    exc,
                )

        return client

    async def stop_client(self, telegram_user_id: int) -> None:
        client = self.active_clients.pop(telegram_user_id, None)
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
        """Sync mention settings, keyword rules, and templates from Supabase into memory."""
        session = await self.db.get_user_session(telegram_user_id)
        if session:
            self._mention_cache[telegram_user_id] = (
                bool(session.get("mention_enabled", False)),
                session.get("mention_template_id"),
            )
        else:
            self._mention_cache[telegram_user_id] = (False, None)

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

        async def _on_keyword(c: Client, m: Message) -> None:
            await self._handle_keyword_reply(c, m, telegram_user_id)

        # Group 0: Saved Messages control plane
        client.add_handler(
            MessageHandler(
                _on_saved_command,
                filters.me & filters.chat("me") & filters.text,
            ),
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

        # Group 2: Keyword custom text auto-reply
        client.add_handler(
            MessageHandler(
                _on_keyword,
                ~filters.me & filters.text,
            ),
            group=2,
        )
        logger.debug("Attached async message handlers for user_id=%s", telegram_user_id)

    # -- Automation Handlers --

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
        if now - self._mention_cooldowns.get(key, 0.0) < MENTION_COOLDOWN_SECONDS:
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

    async def _handle_keyword_reply(
        self, client: Client, message: Message, telegram_user_id: int
    ) -> None:
        if not message.text or not message.chat:
            return

        # Avoid double-replying if mention handler already answered this exact message
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
                if now - self._keyword_cooldowns.get(cd_key, 0.0) < KEYWORD_COOLDOWN_SECONDS:
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

    # -- Saved Messages Command Router --

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
            elif cmd == "/new":
                await self._cmd_new_template(client, message, telegram_user_id)
            elif cmd == "/templates":
                await self._cmd_list_templates(message, telegram_user_id)
            elif cmd == "/del_template":
                await self._cmd_del_template(message, telegram_user_id)
            elif cmd == "/set_mention":
                await self._cmd_set_mention(message, telegram_user_id)
            elif cmd == "/mention_off":
                await self._cmd_mention_off(message, telegram_user_id)
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
            "Templates (Premium Custom Emojis Supported):\n"
            "• /new <text with emojis> — Save a new template (or reply to any message with /new)\n"
            "• /templates — List all saved templates & emoji counts\n"
            "• /del_template <TEMPLATE_ID> — Delete a template\n\n"
            "Mention Auto-Reply:\n"
            "• /set_mention <TEMPLATE_ID> — Enable mention auto-reply with a template\n"
            "• /set_mention <text with emojis> — Auto-save text & enable mention auto-reply\n"
            "• /mention_off — Disable mention auto-reply\n\n"
            "Scheduled Posts:\n"
            f"• /schedule <TEMPLATE_ID> <CHAT_ID> <YYYY-MM-DD> <HH:MM> — One-time post ({self.tz.key})\n"
            "• /schedule_every <TEMPLATE_ID> <CHAT_ID> <MINUTES> — Recurring post\n"
            "• /schedules — List active scheduled jobs\n"
            "• /cancel_schedule <JOB_ID> — Cancel & remove a scheduled job\n\n"
            "Keyword Auto-Reply:\n"
            "• /set_reply <keyword> | <TEMPLATE_ID> — Bind keyword to a template\n"
            "• /set_reply <keyword> | <text with emojis> — Bind keyword to inline custom text\n"
            "• /list_replies — List all keyword auto-reply rules\n"
            "• /del_reply <keyword> — Delete a keyword rule\n\n"
            "• /help — Show this command reference"
        )
        await message.reply_text(help_text, parse_mode=ParseMode.DISABLED)

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
        emoji_count = count_custom_emojis(entities_json)

        await message.reply_text(
            f"Template saved!\n"
            f"ID: {tpl['id']}\n"
            f"Custom Emojis: {emoji_count}\n"
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

    async def _cmd_set_mention(self, message: Message, telegram_user_id: int) -> None:
        full_text = message.text or ""
        cmd_token = full_text.split(maxsplit=1)[0]
        start_idx = find_payload_start(full_text, len(cmd_token))

        if start_idx >= len(full_text):
            await message.reply_text(
                "Usage:\n"
                "• /set_mention <TEMPLATE_ID>\n"
                "• /set_mention <custom text with emojis>",
                parse_mode=ParseMode.DISABLED,
            )
            return

        raw_arg = full_text[start_idx:].strip()
        candidate_id = raw_arg.upper()

        if TEMPLATE_ID_RE.match(candidate_id):
            tpl = await self._get_template_cached(telegram_user_id, candidate_id)
            if not tpl:
                await message.reply_text(
                    f"Template {candidate_id} not found. Create it first with /new.",
                    parse_mode=ParseMode.DISABLED,
                )
                return
            template_id = tpl["id"]
        else:
            payload_text, entities_json = serialize_entities_with_offset(
                full_text, message.entities, start_char_idx=start_idx
            )
            tpl = await self.db.create_template(telegram_user_id, payload_text, entities_json)
            self._template_cache.setdefault(telegram_user_id, {})[tpl["id"]] = tpl
            template_id = tpl["id"]

        await self.db.set_mention_config(
            telegram_user_id, enabled=True, template_id=template_id
        )
        self._mention_cache[telegram_user_id] = (True, template_id)

        await message.reply_text(
            f"Mention auto-reply ENABLED using template {template_id} "
            f"(cooldown: {int(MENTION_COOLDOWN_SECONDS)}s per chat).",
            parse_mode=ParseMode.DISABLED,
        )

    async def _cmd_mention_off(self, message: Message, telegram_user_id: int) -> None:
        await self.db.set_mention_config(telegram_user_id, enabled=False)
        _, current_tpl = self._mention_cache.get(telegram_user_id, (False, None))
        self._mention_cache[telegram_user_id] = (False, current_tpl)
        await message.reply_text(
            "Mention auto-reply DISABLED.", parse_mode=ParseMode.DISABLED
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

        sched_id = f"sched_{job_id}"
        if self.scheduler.get_job(sched_id):
            self.scheduler.remove_job(sched_id)

        deleted = await self.db.delete_scheduled_post(job_id, telegram_user_id)
        if deleted:
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
