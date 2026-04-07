import logging
import os
from datetime import datetime
from zoneinfo import ZoneInfo

import psycopg2
from psycopg2.extras import RealDictCursor
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ChatType
from telegram.ext import (
    ApplicationBuilder,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    ConversationHandler,
    MessageHandler,
    filters,
)

# =========================================================
# CONFIG
# =========================================================
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
DATABASE_URL = os.getenv("DATABASE_URL", "")
ADMIN_IDS = {
    int(x.strip())
    for x in os.getenv("ADMIN_IDS", "").split(",")
    if x.strip().isdigit()
}

PACIFIC_TZ = ZoneInfo("America/Los_Angeles")
TG_SAFE = 3900

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# =========================================================
# STATES
# =========================================================
(
    WHALE_SELECT_MODEL,
    WHALE_NAME,
    WHALE_USER_ID,
    WHALE_STATUS,
    WHALE_LAST_CONVO,
    WHALE_NOTES,
    WHALE_COOLDOWN_REASON,
    ADD_NOTE_WAITING,
    QUICK_COOLDOWN_REASON,
) = range(8 + 1)

STATUSES = [
    "MAD",
    "UPSET",
    "WANT FREE SEXTING",
    "WANNA LEAVE",
    "NEED ATTENTION",
    "ASKING FOR CUSTOM",
    "HAPPY",
    "COOLDOWN",
]

URGENT_STATUSES = {"MAD", "UPSET", "WANNA LEAVE"}

# =========================================================
# DB
# =========================================================

def get_conn():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is missing")
    return psycopg2.connect(DATABASE_URL, sslmode="require")


def init_db():
    conn = get_conn()
    try:
        with conn, conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS whale_topics (
                    id SERIAL PRIMARY KEY,
                    model_name TEXT NOT NULL UNIQUE,
                    managers_chat_id BIGINT NOT NULL,
                    message_thread_id BIGINT NOT NULL,
                    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
                );
                """
            )

            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS whales (
                    id SERIAL PRIMARY KEY,
                    model_name TEXT NOT NULL,
                    whale_name TEXT NOT NULL,
                    whale_user_id TEXT NOT NULL,
                    current_status TEXT NOT NULL,
                    last_convo TEXT,
                    notes TEXT,
                    is_cooldown BOOLEAN NOT NULL DEFAULT FALSE,
                    cooldown_reason TEXT,
                    cooldown_started_at TIMESTAMP,
                    last_updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
                    last_updated_by_id BIGINT,
                    last_updated_by_name TEXT,
                    last_updated_by_username TEXT
                );
                """
            )

            cur.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS uq_whales_model_user
                ON whales(model_name, whale_user_id);
                """
            )

            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS whale_updates (
                    id SERIAL PRIMARY KEY,
                    whale_id BIGINT REFERENCES whales(id) ON DELETE CASCADE,
                    model_name TEXT NOT NULL,
                    whale_name TEXT NOT NULL,
                    whale_user_id TEXT NOT NULL,
                    status TEXT NOT NULL,
                    last_convo TEXT,
                    notes TEXT,
                    is_cooldown BOOLEAN NOT NULL DEFAULT FALSE,
                    cooldown_reason TEXT,
                    updated_by_id BIGINT,
                    updated_by_name TEXT,
                    updated_by_username TEXT,
                    created_at TIMESTAMP NOT NULL DEFAULT NOW()
                );
                """
            )
    finally:
        conn.close()


# =========================================================
# HELPERS
# =========================================================

def user_is_admin(user_id: int) -> bool:
    if not ADMIN_IDS:
        return True
    return user_id in ADMIN_IDS


def now_pst() -> datetime:
    return datetime.now(PACIFIC_TZ)


def fmt_dt_pst(dt_obj: datetime | None) -> str:
    if not dt_obj:
        return "-"
    if dt_obj.tzinfo is None:
        dt_obj = dt_obj.replace(tzinfo=PACIFIC_TZ)
    return dt_obj.astimezone(PACIFIC_TZ).strftime("%m/%d/%Y %I:%M %p PST")


def fmt_user(user) -> tuple[str, str]:
    full_name = user.full_name or "Unknown"
    username = f"@{user.username}" if user.username else "no_username"
    return full_name, username


def build_status_keyboard(prefix: str) -> InlineKeyboardMarkup:
    rows = [
        [
            InlineKeyboardButton("😡 MAD", callback_data=f"{prefix}:MAD"),
            InlineKeyboardButton("😞 UPSET", callback_data=f"{prefix}:UPSET"),
        ],
        [
            InlineKeyboardButton("🆓 FREE SEXTING", callback_data=f"{prefix}:WANT FREE SEXTING"),
            InlineKeyboardButton("⚠️ WANNA LEAVE", callback_data=f"{prefix}:WANNA LEAVE"),
        ],
        [
            InlineKeyboardButton("👀 NEED ATTENTION", callback_data=f"{prefix}:NEED ATTENTION"),
            InlineKeyboardButton("💸 ASKING CUSTOM", callback_data=f"{prefix}:ASKING FOR CUSTOM"),
        ],
        [
            InlineKeyboardButton("😊 HAPPY", callback_data=f"{prefix}:HAPPY"),
            InlineKeyboardButton("❄️ COOLDOWN", callback_data=f"{prefix}:COOLDOWN"),
        ],
    ]
    return InlineKeyboardMarkup(rows)


def split_text(text: str, chunk_size: int = TG_SAFE) -> list[str]:
    if len(text) <= chunk_size:
        return [text]
    parts = []
    current = []
    current_len = 0
    for line in text.splitlines(True):
        if current_len + len(line) > chunk_size and current:
            parts.append("".join(current))
            current = [line]
            current_len = len(line)
        else:
            current.append(line)
            current_len += len(line)
    if current:
        parts.append("".join(current))
    return parts


def get_registered_models() -> list[str]:
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT model_name FROM whale_topics ORDER BY lower(model_name) ASC")
            return [r[0] for r in cur.fetchall()]
    finally:
        conn.close()


def build_model_keyboard(models: list[str]) -> InlineKeyboardMarkup:
    rows = []
    row = []
    for model in models:
        row.append(InlineKeyboardButton(model.title(), callback_data=f"model:{model}"))
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    return InlineKeyboardMarkup(rows)


def get_topic_for_model(model_name: str):
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT managers_chat_id, message_thread_id, model_name
                FROM whale_topics
                WHERE lower(model_name) = lower(%s)
                """,
                (model_name,),
            )
            return cur.fetchone()
    finally:
        conn.close()


def get_model_by_topic(chat_id: int, thread_id: int | None):
    if not thread_id:
        return None
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT model_name
                FROM whale_topics
                WHERE managers_chat_id = %s AND message_thread_id = %s
                """,
                (chat_id, thread_id),
            )
            row = cur.fetchone()
            return row[0] if row else None
    finally:
        conn.close()


def get_whale_by_id(whale_id: int):
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM whales WHERE id = %s", (whale_id,))
            return cur.fetchone()
    finally:
        conn.close()


def upsert_whale_and_history(
    model_name: str,
    whale_name: str,
    whale_user_id: str,
    status: str,
    last_convo: str,
    notes: str,
    is_cooldown: bool,
    cooldown_reason: str | None,
    updated_by_id: int,
    updated_by_name: str,
    updated_by_username: str,
):
    conn = get_conn()
    try:
        with conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            cooldown_started_at = now_pst() if is_cooldown else None
            cur.execute(
                """
                INSERT INTO whales (
                    model_name, whale_name, whale_user_id, current_status,
                    last_convo, notes, is_cooldown, cooldown_reason,
                    cooldown_started_at, last_updated_at,
                    last_updated_by_id, last_updated_by_name, last_updated_by_username
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), %s, %s, %s)
                ON CONFLICT (model_name, whale_user_id)
                DO UPDATE SET
                    whale_name = EXCLUDED.whale_name,
                    current_status = EXCLUDED.current_status,
                    last_convo = EXCLUDED.last_convo,
                    notes = EXCLUDED.notes,
                    is_cooldown = EXCLUDED.is_cooldown,
                    cooldown_reason = EXCLUDED.cooldown_reason,
                    cooldown_started_at = EXCLUDED.cooldown_started_at,
                    last_updated_at = NOW(),
                    last_updated_by_id = EXCLUDED.last_updated_by_id,
                    last_updated_by_name = EXCLUDED.last_updated_by_name,
                    last_updated_by_username = EXCLUDED.last_updated_by_username
                RETURNING *
                """,
                (
                    model_name,
                    whale_name,
                    whale_user_id,
                    status,
                    last_convo,
                    notes,
                    is_cooldown,
                    cooldown_reason,
                    cooldown_started_at,
                    updated_by_id,
                    updated_by_name,
                    updated_by_username,
                ),
            )
            whale_row = cur.fetchone()
            cur.execute(
                """
                INSERT INTO whale_updates (
                    whale_id, model_name, whale_name, whale_user_id, status,
                    last_convo, notes, is_cooldown, cooldown_reason,
                    updated_by_id, updated_by_name, updated_by_username
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    whale_row["id"],
                    model_name,
                    whale_name,
                    whale_user_id,
                    status,
                    last_convo,
                    notes,
                    is_cooldown,
                    cooldown_reason,
                    updated_by_id,
                    updated_by_name,
                    updated_by_username,
                ),
            )
            return whale_row
    finally:
        conn.close()


def quick_update_whale(
    whale_id: int,
    status: str,
    updated_by_id: int,
    updated_by_name: str,
    updated_by_username: str,
    note_append: str | None = None,
    cooldown_reason: str | None = None,
    clear_cooldown: bool = False,
):
    conn = get_conn()
    try:
        with conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM whales WHERE id = %s", (whale_id,))
            existing = cur.fetchone()
            if not existing:
                return None

            notes = existing["notes"] or ""
            if note_append:
                if notes.strip():
                    notes = f"{notes}\n• {note_append}"
                else:
                    notes = note_append

            is_cooldown = status == "COOLDOWN"
            if clear_cooldown:
                is_cooldown = False
                cooldown_reason = None
                status = "HAPPY" if existing["current_status"] == "COOLDOWN" else existing["current_status"]

            cooldown_started_at = now_pst() if is_cooldown else None

            cur.execute(
                """
                UPDATE whales
                SET current_status = %s,
                    notes = %s,
                    is_cooldown = %s,
                    cooldown_reason = %s,
                    cooldown_started_at = %s,
                    last_updated_at = NOW(),
                    last_updated_by_id = %s,
                    last_updated_by_name = %s,
                    last_updated_by_username = %s
                WHERE id = %s
                RETURNING *
                """,
                (
                    status,
                    notes,
                    is_cooldown,
                    cooldown_reason,
                    cooldown_started_at,
                    updated_by_id,
                    updated_by_name,
                    updated_by_username,
                    whale_id,
                ),
            )
            whale_row = cur.fetchone()

            cur.execute(
                """
                INSERT INTO whale_updates (
                    whale_id, model_name, whale_name, whale_user_id, status,
                    last_convo, notes, is_cooldown, cooldown_reason,
                    updated_by_id, updated_by_name, updated_by_username
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    whale_row["id"],
                    whale_row["model_name"],
                    whale_row["whale_name"],
                    whale_row["whale_user_id"],
                    whale_row["current_status"],
                    whale_row["last_convo"],
                    whale_row["notes"],
                    whale_row["is_cooldown"],
                    whale_row["cooldown_reason"],
                    updated_by_id,
                    updated_by_name,
                    updated_by_username,
                ),
            )
            return whale_row
    finally:
        conn.close()


def fetch_whales_for_model(model_name: str, only_urgent: bool = False, only_cooldown: bool = False):
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            base_sql = """
                SELECT *
                FROM whales
                WHERE lower(model_name) = lower(%s)
            """
            params = [model_name]
            if only_urgent:
                base_sql += " AND current_status = ANY(%s)"
                params.append(list(URGENT_STATUSES))
            if only_cooldown:
                base_sql += " AND is_cooldown = TRUE"

            base_sql += """
                ORDER BY
                    CASE
                        WHEN current_status IN ('MAD','UPSET','WANNA LEAVE') THEN 1
                        WHEN is_cooldown = TRUE THEN 2
                        ELSE 3
                    END,
                    last_updated_at DESC
            """
            cur.execute(base_sql, tuple(params))
            return cur.fetchall()
    finally:
        conn.close()


def build_quick_update_keyboard(whale_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("😡 Mad", callback_data=f"quickstatus:{whale_id}:MAD"),
                InlineKeyboardButton("😞 Upset", callback_data=f"quickstatus:{whale_id}:UPSET"),
            ],
            [
                InlineKeyboardButton("😊 Happy", callback_data=f"quickstatus:{whale_id}:HAPPY"),
                InlineKeyboardButton("👀 Need Attention", callback_data=f"quickstatus:{whale_id}:NEED ATTENTION"),
            ],
            [
                InlineKeyboardButton("💸 Asking Custom", callback_data=f"quickstatus:{whale_id}:ASKING FOR CUSTOM"),
                InlineKeyboardButton("⚠️ Wanna Leave", callback_data=f"quickstatus:{whale_id}:WANNA LEAVE"),
            ],
            [
                InlineKeyboardButton("❄️ Cooldown", callback_data=f"quickcooldown:{whale_id}"),
                InlineKeyboardButton("✅ Clear Cooldown", callback_data=f"clearcooldown:{whale_id}"),
            ],
            [
                InlineKeyboardButton("📝 Add Note", callback_data=f"addnote:{whale_id}"),
            ],
        ]
    )


def format_whale_update_message(whale_row, title: str = "🐳 WHALE UPDATE") -> str:
    cooldown_line = ""
    if whale_row.get("is_cooldown"):
        cooldown_line = f"\n⏳ Cooldown Reason: {whale_row.get('cooldown_reason') or '-'}"

    return (
        f"{title}\n\n"
        f"👤 Model: {str(whale_row['model_name']).upper()}\n"
        f"💎 Whale: {whale_row['whale_name']}\n"
        f"🆔 User ID: {whale_row['whale_user_id']}\n\n"
        f"📌 Status: {whale_row['current_status']}{' ❄️' if whale_row.get('is_cooldown') else ''}\n"
        f"💬 Last Convo: {whale_row.get('last_convo') or '-'}\n"
        f"📝 Notes: {whale_row.get('notes') or '-'}"
        f"{cooldown_line}\n\n"
        f"👨 Updated by: {whale_row.get('last_updated_by_name') or '-'} ({whale_row.get('last_updated_by_username') or '-'})\n"
        f"🕒 {fmt_dt_pst(whale_row.get('last_updated_at'))}"
    )


async def send_to_registered_topic(bot, whale_row, extra_alert: bool = True):
    topic = get_topic_for_model(whale_row["model_name"])
    if not topic:
        logger.warning("No topic registered for model=%s", whale_row["model_name"])
        return

    await bot.send_message(
        chat_id=topic["managers_chat_id"],
        message_thread_id=topic["message_thread_id"],
        text=format_whale_update_message(whale_row),
        reply_markup=build_quick_update_keyboard(whale_row["id"]),
    )

    if extra_alert and whale_row["current_status"] in URGENT_STATUSES:
        await bot.send_message(
            chat_id=topic["managers_chat_id"],
            message_thread_id=topic["message_thread_id"],
            text=format_whale_update_message(whale_row, title="🚨 URGENT WHALE ALERT"),
        )


# =========================================================
# COMMANDS
# =========================================================
async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (
        "Whale Bot is online.\n\n"
        "Managers topic setup:\n"
        "/register carter  -> run inside the model topic\n\n"
        "Chatters:\n"
        "/whale -> create whale update\n\n"
        "Managers inside topic:\n"
        "/whales\n"
        "/handover\n"
        "/urgent\n"
        "/cooldowns"
    )
    await update.message.reply_text(text)


async def register_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not user_is_admin(user_id):
        await update.message.reply_text("You are not allowed to use /register.")
        return

    if not context.args:
        await update.message.reply_text("Usage: /register carter")
        return

    chat = update.effective_chat
    msg = update.effective_message
    model_name = context.args[0].strip().lower()
    thread_id = msg.message_thread_id

    if chat.type not in (ChatType.SUPERGROUP, ChatType.GROUP):
        await update.message.reply_text("Run /register inside the managers group topic.")
        return

    if not thread_id:
        await update.message.reply_text("Run /register inside the specific topic for that model.")
        return

    conn = get_conn()
    try:
        with conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO whale_topics (model_name, managers_chat_id, message_thread_id, created_at, updated_at)
                VALUES (%s, %s, %s, NOW(), NOW())
                ON CONFLICT (model_name)
                DO UPDATE SET
                    managers_chat_id = EXCLUDED.managers_chat_id,
                    message_thread_id = EXCLUDED.message_thread_id,
                    updated_at = NOW()
                """,
                (model_name, chat.id, thread_id),
            )
        await update.message.reply_text(
            f"✅ Registered model '{model_name}' to this topic.\n"
            f"Future /whale updates for {model_name.title()} will go here."
        )
    finally:
        conn.close()


async def whale_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    models = get_registered_models()
    if not models:
        await update.message.reply_text("No models registered yet. Use /register inside manager topics first.")
        return ConversationHandler.END

    context.user_data["whale_form"] = {}
    preview = ", ".join(m.title() for m in models[:20])
    extra = " ..." if len(models) > 20 else ""
    await update.message.reply_text(
        "Type the model name exactly as registered."
        f"Registered models: {preview}{extra}"
        "Example: carter"
    )
    return WHALE_SELECT_MODEL


async def whale_model_typed(update: Update, context: ContextTypes.DEFAULT_TYPE):
    typed_model = update.message.text.strip().lower()
    models = {m.lower(): m for m in get_registered_models()}

    if typed_model not in models:
        preview = ", ".join(sorted(models.keys())[:20])
        extra = " ..." if len(models) > 20 else ""
        await update.message.reply_text(
            "Model not found. Type the model name exactly as registered.

"
            f"Available models: {preview}{extra}"
        )
        return WHALE_SELECT_MODEL

    context.user_data.setdefault("whale_form", {})["model_name"] = typed_model
    await update.message.reply_text(f"Selected model: {typed_model.title()}

Send whale name:")
    return WHALE_NAME


async def whales_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    thread_id = update.effective_message.message_thread_id
    model_name = get_model_by_topic(chat_id, thread_id)

    if not model_name:
        await update.message.reply_text("Use /whales inside a registered model topic.")
        return

    rows = fetch_whales_for_model(model_name)
    if not rows:
        await update.message.reply_text(f"No whales found for {model_name.title()}.")
        return

    text = [f"🐳 ACTIVE WHALES — {model_name.upper()}\n"]
    for i, r in enumerate(rows, start=1):
        text.append(
            f"{i}. {r['whale_name']}\n"
            f"🆔 {r['whale_user_id']}\n"
            f"📌 {r['current_status']}{' ❄️' if r['is_cooldown'] else ''}\n"
            f"💬 {r.get('last_convo') or '-'}\n"
            f"📝 {r.get('notes') or '-'}\n"
            f"⏳ {r.get('cooldown_reason') or '-'}\n" if r['is_cooldown'] else
            f"{i}. {r['whale_name']}\n🆔 {r['whale_user_id']}\n📌 {r['current_status']}\n💬 {r.get('last_convo') or '-'}\n📝 {r.get('notes') or '-'}\n"
        )
        text.append(
            f"👨 {r.get('last_updated_by_username') or '-'}\n"
            f"🕒 {fmt_dt_pst(r.get('last_updated_at'))}\n"
        )

    final_text = "\n".join(text)
    for chunk in split_text(final_text):
        await update.message.reply_text(chunk)


async def handover_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    thread_id = update.effective_message.message_thread_id
    model_name = get_model_by_topic(chat_id, thread_id)

    if not model_name:
        await update.message.reply_text("Use /handover inside a registered model topic.")
        return

    rows = fetch_whales_for_model(model_name)
    if not rows:
        await update.message.reply_text(f"No whales found for {model_name.title()}.")
        return

    lines = [f"📋 WHALE HANDOVER — {model_name.upper()}\n"]
    for i, r in enumerate(rows, start=1):
        lines.append(
            f"{i}. {r['whale_name']} — {r['current_status']}{' ❄️' if r['is_cooldown'] else ''}\n"
            f"- user id: {r['whale_user_id']}\n"
            f"- last convo: {r.get('last_convo') or '-'}\n"
            f"- notes: {r.get('notes') or '-'}\n"
            f"- cooldown: {r.get('cooldown_reason') or '-'}\n" if r['is_cooldown'] else
            f"{i}. {r['whale_name']} — {r['current_status']}\n- user id: {r['whale_user_id']}\n- last convo: {r.get('last_convo') or '-'}\n- notes: {r.get('notes') or '-'}\n"
        )
        lines.append(
            f"- updated by: {r.get('last_updated_by_username') or '-'}\n"
            f"- updated at: {fmt_dt_pst(r.get('last_updated_at'))}\n"
        )

    final_text = "\n".join(lines)
    for chunk in split_text(final_text):
        await update.message.reply_text(chunk)


async def urgent_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    thread_id = update.effective_message.message_thread_id
    model_name = get_model_by_topic(chat_id, thread_id)

    if not model_name:
        await update.message.reply_text("Use /urgent inside a registered model topic.")
        return

    rows = fetch_whales_for_model(model_name, only_urgent=True)
    if not rows:
        await update.message.reply_text(f"No urgent whales for {model_name.title()}.")
        return

    lines = [f"🚨 URGENT WHALES — {model_name.upper()}\n"]
    for i, r in enumerate(rows, start=1):
        lines.append(
            f"{i}. {r['whale_name']}\n"
            f"🆔 {r['whale_user_id']}\n"
            f"📌 {r['current_status']}\n"
            f"💬 {r.get('last_convo') or '-'}\n"
            f"📝 {r.get('notes') or '-'}\n"
            f"👨 {r.get('last_updated_by_username') or '-'}\n"
            f"🕒 {fmt_dt_pst(r.get('last_updated_at'))}\n"
        )
    final_text = "\n".join(lines)
    for chunk in split_text(final_text):
        await update.message.reply_text(chunk)


async def cooldowns_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    thread_id = update.effective_message.message_thread_id
    model_name = get_model_by_topic(chat_id, thread_id)

    if not model_name:
        await update.message.reply_text("Use /cooldowns inside a registered model topic.")
        return

    rows = fetch_whales_for_model(model_name, only_cooldown=True)
    if not rows:
        await update.message.reply_text(f"No cooldown whales for {model_name.title()}.")
        return

    lines = [f"❄️ COOLDOWN WHALES — {model_name.upper()}\n"]
    for i, r in enumerate(rows, start=1):
        lines.append(
            f"{i}. {r['whale_name']}\n"
            f"🆔 {r['whale_user_id']}\n"
            f"⏳ {r.get('cooldown_reason') or '-'}\n"
            f"📝 {r.get('notes') or '-'}\n"
            f"👨 {r.get('last_updated_by_username') or '-'}\n"
            f"🕒 {fmt_dt_pst(r.get('last_updated_at'))}\n"
        )
    final_text = "\n".join(lines)
    for chunk in split_text(final_text):
        await update.message.reply_text(chunk)


async def cancel_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.pop("whale_form", None)
    context.user_data.pop("add_note_whale_id", None)
    context.user_data.pop("quick_cooldown_whale_id", None)
    await update.message.reply_text("Cancelled.")
    return ConversationHandler.END


# =========================================================
# /WHALE FLOW
# =========================================================
async def whale_name_received(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.setdefault("whale_form", {})["whale_name"] = update.message.text.strip()
    await update.message.reply_text("Send whale user ID:")
    return WHALE_USER_ID


async def whale_user_id_received(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.setdefault("whale_form", {})["whale_user_id"] = update.message.text.strip()
    await update.message.reply_text("Select status:", reply_markup=build_status_keyboard("newstatus"))
    return WHALE_STATUS


async def whale_status_selected(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    data = query.data or ""
    if not data.startswith("newstatus:"):
        return WHALE_STATUS

    status = data.split(":", 1)[1]
    context.user_data.setdefault("whale_form", {})["current_status"] = status
    await query.message.reply_text(f"Status selected: {status}\n\nSend last convo:")
    return WHALE_LAST_CONVO


async def whale_last_convo_received(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.setdefault("whale_form", {})["last_convo"] = update.message.text.strip()
    await update.message.reply_text("Send notes:\nExample: gfe him, do not upsell")
    return WHALE_NOTES


async def whale_notes_received(update: Update, context: ContextTypes.DEFAULT_TYPE):
    form = context.user_data.setdefault("whale_form", {})
    form["notes"] = update.message.text.strip()

    if form.get("current_status") == "COOLDOWN":
        await update.message.reply_text(
            "Send cooldown reason:\nExample: just milked him / said no more money"
        )
        return WHALE_COOLDOWN_REASON

    return await finalize_whale_submission(update, context, cooldown_reason=None)


async def whale_cooldown_reason_received(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cooldown_reason = update.message.text.strip()
    return await finalize_whale_submission(update, context, cooldown_reason=cooldown_reason)


async def finalize_whale_submission(update: Update, context: ContextTypes.DEFAULT_TYPE, cooldown_reason: str | None):
    form = context.user_data.get("whale_form", {})
    if not form:
        await update.message.reply_text("Form expired. Start again with /whale")
        return ConversationHandler.END

    full_name, username = fmt_user(update.effective_user)
    status = form["current_status"]
    whale_row = upsert_whale_and_history(
        model_name=form["model_name"],
        whale_name=form["whale_name"],
        whale_user_id=form["whale_user_id"],
        status=status,
        last_convo=form.get("last_convo", ""),
        notes=form.get("notes", ""),
        is_cooldown=(status == "COOLDOWN"),
        cooldown_reason=cooldown_reason,
        updated_by_id=update.effective_user.id,
        updated_by_name=full_name,
        updated_by_username=username,
    )

    await send_to_registered_topic(context.bot, whale_row, extra_alert=True)
    await update.message.reply_text(
        f"✅ Whale update sent to {form['model_name'].title()} topic."
    )
    context.user_data.pop("whale_form", None)
    return ConversationHandler.END


# =========================================================
# QUICK BUTTONS
# =========================================================
async def quick_status_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    try:
        _, whale_id_str, status = query.data.split(":", 2)
        whale_id = int(whale_id_str)
    except Exception:
        await query.message.reply_text("Invalid quick update data.")
        return

    full_name, username = fmt_user(query.from_user)
    whale_row = quick_update_whale(
        whale_id=whale_id,
        status=status,
        updated_by_id=query.from_user.id,
        updated_by_name=full_name,
        updated_by_username=username,
    )
    if not whale_row:
        await query.message.reply_text("Whale not found.")
        return

    await send_to_registered_topic(context.bot, whale_row, extra_alert=True)
    await query.message.reply_text(f"✅ Updated {whale_row['whale_name']} to {status}.")


async def add_note_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    try:
        whale_id = int(query.data.split(":", 1)[1])
    except Exception:
        await query.message.reply_text("Invalid note data.")
        return ConversationHandler.END

    context.user_data["add_note_whale_id"] = whale_id
    await query.message.reply_text("Send the note you want to add:")
    return ADD_NOTE_WAITING


async def add_note_received(update: Update, context: ContextTypes.DEFAULT_TYPE):
    whale_id = context.user_data.get("add_note_whale_id")
    if not whale_id:
        await update.message.reply_text("No whale selected for note. Use the button again.")
        return ConversationHandler.END

    note = update.message.text.strip()
    full_name, username = fmt_user(update.effective_user)
    existing = get_whale_by_id(whale_id)
    if not existing:
        await update.message.reply_text("Whale not found.")
        return ConversationHandler.END

    whale_row = quick_update_whale(
        whale_id=whale_id,
        status=existing["current_status"],
        updated_by_id=update.effective_user.id,
        updated_by_name=full_name,
        updated_by_username=username,
        note_append=note,
        cooldown_reason=existing.get("cooldown_reason"),
    )
    await send_to_registered_topic(context.bot, whale_row, extra_alert=False)
    await update.message.reply_text("✅ Note added.")
    context.user_data.pop("add_note_whale_id", None)
    return ConversationHandler.END


async def quick_cooldown_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    try:
        whale_id = int(query.data.split(":", 1)[1])
    except Exception:
        await query.message.reply_text("Invalid cooldown data.")
        return ConversationHandler.END

    context.user_data["quick_cooldown_whale_id"] = whale_id
    await query.message.reply_text(
        "Send cooldown reason:\nExample: just milked him / said no more money"
    )
    return QUICK_COOLDOWN_REASON


async def quick_cooldown_reason_received(update: Update, context: ContextTypes.DEFAULT_TYPE):
    whale_id = context.user_data.get("quick_cooldown_whale_id")
    if not whale_id:
        await update.message.reply_text("No whale selected for cooldown. Use the button again.")
        return ConversationHandler.END

    reason = update.message.text.strip()
    full_name, username = fmt_user(update.effective_user)
    whale_row = quick_update_whale(
        whale_id=whale_id,
        status="COOLDOWN",
        updated_by_id=update.effective_user.id,
        updated_by_name=full_name,
        updated_by_username=username,
        cooldown_reason=reason,
    )
    if not whale_row:
        await update.message.reply_text("Whale not found.")
        return ConversationHandler.END

    await send_to_registered_topic(context.bot, whale_row, extra_alert=False)
    await update.message.reply_text("✅ Cooldown added.")
    context.user_data.pop("quick_cooldown_whale_id", None)
    return ConversationHandler.END


async def clear_cooldown_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    try:
        whale_id = int(query.data.split(":", 1)[1])
    except Exception:
        await query.message.reply_text("Invalid clear cooldown data.")
        return

    full_name, username = fmt_user(query.from_user)
    existing = get_whale_by_id(whale_id)
    if not existing:
        await query.message.reply_text("Whale not found.")
        return

    whale_row = quick_update_whale(
        whale_id=whale_id,
        status=existing["current_status"],
        updated_by_id=query.from_user.id,
        updated_by_name=full_name,
        updated_by_username=username,
        clear_cooldown=True,
    )
    await send_to_registered_topic(context.bot, whale_row, extra_alert=False)
    await query.message.reply_text(f"✅ Cleared cooldown for {whale_row['whale_name']}.")


# =========================================================
# MAIN
# =========================================================
async def post_init(app):
    init_db()
    logger.info("Database initialized.")


def main():
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN is missing")

    app = ApplicationBuilder().token(BOT_TOKEN).post_init(post_init).build()

    whale_conv = ConversationHandler(
        entry_points=[CommandHandler("whale", whale_start)],
        states={
            WHALE_SELECT_MODEL: [MessageHandler(filters.TEXT & ~filters.COMMAND, whale_model_typed)],
            WHALE_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, whale_name_received)],
            WHALE_USER_ID: [MessageHandler(filters.TEXT & ~filters.COMMAND, whale_user_id_received)],
            WHALE_STATUS: [CallbackQueryHandler(whale_status_selected, pattern=r"^newstatus:")],
            WHALE_LAST_CONVO: [MessageHandler(filters.TEXT & ~filters.COMMAND, whale_last_convo_received)],
            WHALE_NOTES: [MessageHandler(filters.TEXT & ~filters.COMMAND, whale_notes_received)],
            WHALE_COOLDOWN_REASON: [MessageHandler(filters.TEXT & ~filters.COMMAND, whale_cooldown_reason_received)],
        },
        fallbacks=[CommandHandler("cancel", cancel_cmd)],
        allow_reentry=True,
    )

    add_note_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(add_note_callback, pattern=r"^addnote:")],
        states={
            ADD_NOTE_WAITING: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_note_received)],
        },
        fallbacks=[CommandHandler("cancel", cancel_cmd)],
        allow_reentry=True,
    )

    quick_cooldown_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(quick_cooldown_callback, pattern=r"^quickcooldown:")],
        states={
            QUICK_COOLDOWN_REASON: [MessageHandler(filters.TEXT & ~filters.COMMAND, quick_cooldown_reason_received)],
        },
        fallbacks=[CommandHandler("cancel", cancel_cmd)],
        allow_reentry=True,
    )

    app.add_handler(CommandHandler("start", start_cmd))
    app.add_handler(CommandHandler("register", register_cmd))
    app.add_handler(CommandHandler("whales", whales_cmd))
    app.add_handler(CommandHandler("handover", handover_cmd))
    app.add_handler(CommandHandler("urgent", urgent_cmd))
    app.add_handler(CommandHandler("cooldowns", cooldowns_cmd))
    app.add_handler(CommandHandler("cancel", cancel_cmd))

    app.add_handler(whale_conv)
    app.add_handler(add_note_conv)
    app.add_handler(quick_cooldown_conv)

    app.add_handler(CallbackQueryHandler(quick_status_callback, pattern=r"^quickstatus:"))
    app.add_handler(CallbackQueryHandler(clear_cooldown_callback, pattern=r"^clearcooldown:"))

    logger.info("Whale bot starting...")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
