import logging
import os
from datetime import datetime
from zoneinfo import ZoneInfo

import psycopg2
from psycopg2.extras import RealDictCursor
from telegram import Update
from telegram.constants import ChatType
from telegram.ext import (
    ApplicationBuilder,
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
(WHALE_TEMPLATE_INPUT,) = range(1)

VALID_PRIORITIES = {"HIGH", "MEDIUM", "LOW"}
VALID_STATUSES = {
    "CRITICAL",
    "AT RISK",
    "COOLING OFF",
    "NEEDS ATTENTION",
    "CUSTOM OPPORTUNITY",
    "STABLE",
}
URGENT_STATUSES = {"CRITICAL", "AT RISK"}

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
                    priority TEXT,
                    current_status TEXT NOT NULL,
                    last_convo TEXT,
                    notes TEXT,
                    action_needed TEXT,
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

            cur.execute("ALTER TABLE whales ADD COLUMN IF NOT EXISTS priority TEXT;")
            cur.execute("ALTER TABLE whales ADD COLUMN IF NOT EXISTS action_needed TEXT;")

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
                    priority TEXT,
                    status TEXT NOT NULL,
                    last_convo TEXT,
                    notes TEXT,
                    action_needed TEXT,
                    is_cooldown BOOLEAN NOT NULL DEFAULT FALSE,
                    cooldown_reason TEXT,
                    updated_by_id BIGINT,
                    updated_by_name TEXT,
                    updated_by_username TEXT,
                    created_at TIMESTAMP NOT NULL DEFAULT NOW()
                );
                """
            )

            cur.execute("ALTER TABLE whale_updates ADD COLUMN IF NOT EXISTS priority TEXT;")
            cur.execute("ALTER TABLE whale_updates ADD COLUMN IF NOT EXISTS action_needed TEXT;")
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


def fetch_whales_for_model(model_name: str, only_urgent: bool = False, only_cooldown: bool = False):
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            sql = """
                SELECT *
                FROM whales
                WHERE lower(model_name) = lower(%s)
            """
            params = [model_name]

            if only_urgent:
                sql += " AND current_status = ANY(%s)"
                params.append(list(URGENT_STATUSES))

            if only_cooldown:
                sql += " AND is_cooldown = TRUE"

            sql += """
                ORDER BY
                    CASE
                        WHEN current_status IN ('Critical','At Risk') THEN 1
                        WHEN is_cooldown = TRUE THEN 2
                        ELSE 3
                    END,
                    last_updated_at DESC
            """
            cur.execute(sql, tuple(params))
            return cur.fetchall()
    finally:
        conn.close()


def upsert_whale_and_history(
    model_name: str,
    whale_name: str,
    whale_user_id: str,
    priority: str,
    status: str,
    last_convo: str,
    notes: str,
    action_needed: str,
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
                    model_name, whale_name, whale_user_id, priority, current_status,
                    last_convo, notes, action_needed, is_cooldown, cooldown_reason,
                    cooldown_started_at, last_updated_at,
                    last_updated_by_id, last_updated_by_name, last_updated_by_username
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), %s, %s, %s)
                ON CONFLICT (model_name, whale_user_id)
                DO UPDATE SET
                    whale_name = EXCLUDED.whale_name,
                    priority = EXCLUDED.priority,
                    current_status = EXCLUDED.current_status,
                    last_convo = EXCLUDED.last_convo,
                    notes = EXCLUDED.notes,
                    action_needed = EXCLUDED.action_needed,
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
                    priority,
                    status,
                    last_convo,
                    notes,
                    action_needed,
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
                    whale_id, model_name, whale_name, whale_user_id, priority, status,
                    last_convo, notes, action_needed, is_cooldown, cooldown_reason,
                    updated_by_id, updated_by_name, updated_by_username
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    whale_row["id"],
                    model_name,
                    whale_name,
                    whale_user_id,
                    priority,
                    status,
                    last_convo,
                    notes,
                    action_needed,
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


def format_whale_update_message(whale_row, title: str = "🐳 WHALE UPDATE") -> str:
    return (
        f"{title}\n\n"
        f"👤 Model: {str(whale_row['model_name']).upper()}\n"
        f"💎 Whale: {whale_row['whale_name']}\n"
        f"🆔 User ID: {whale_row['whale_user_id']}\n"
        f"🚦 Priority: {whale_row.get('priority') or '-'}\n"
        f"📌 Status: {whale_row['current_status']}\n\n"
        f"💬 Last Convo:\n{whale_row.get('last_convo') or '-'}\n\n"
        f"📝 Notes:\n{whale_row.get('notes') or '-'}\n\n"
        f"🎯 Action:\n{whale_row.get('action_needed') or '-'}\n\n"
        f"⏳ Cooldown Reason:\n{whale_row.get('cooldown_reason') or '-'}\n\n"
        f"👤 Updated by:\n{whale_row.get('last_updated_by_name') or '-'} ({whale_row.get('last_updated_by_username') or '-'})\n\n"
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
    )

    if extra_alert and whale_row["current_status"] in ("Critical", "At Risk"):
        alert_text = (
            "🚨 WHALE ALERT\n\n"
            f"Model: {str(whale_row['model_name']).upper()}\n"
            f"Whale: {whale_row['whale_name']}\n"
            f"Status: {whale_row['current_status']}\n"
            f"Priority: {whale_row.get('priority') or '-'}\n"
            f"Updated by: {whale_row.get('last_updated_by_username') or '-'}"
        )
        await bot.send_message(
            chat_id=topic["managers_chat_id"],
            message_thread_id=topic["message_thread_id"],
            text=alert_text,
        )


def parse_template_message(text: str) -> dict[str, str]:
    result = {}
    lines = text.splitlines()

    for line in lines:
        if ":" not in line:
            continue
        key, value = line.split(":", 1)
        key = key.strip().lower()
        value = value.strip()

        if key == "model":
            result["model_name"] = value.lower()
        elif key == "whale":
            result["whale_name"] = value
        elif key == "user id":
            result["whale_user_id"] = value
        elif key == "priority":
            result["priority"] = value.upper()
        elif key == "status":
            result["current_status"] = value.upper()
        elif key == "last convo":
            result["last_convo"] = value
        elif key == "notes":
            result["notes"] = value
        elif key == "action":
            result["action_needed"] = value
        elif key == "cooldown reason":
            result["cooldown_reason"] = value

    return result


def validate_template_data(data: dict[str, str]) -> str | None:
    required = [
        "model_name",
        "whale_name",
        "whale_user_id",
        "priority",
        "current_status",
        "last_convo",
        "notes",
        "action_needed",
        "cooldown_reason",
    ]

    missing = [field for field in required if not data.get(field)]
    if missing:
        return f"Missing fields: {', '.join(missing)}"

    if data["priority"] not in VALID_PRIORITIES:
        return "Priority must be one of: High, Medium, Low"

    if data["current_status"] not in VALID_STATUSES:
        return (
            "Status must be one of: Critical, At Risk, Cooling Off, "
            "Needs Attention, Custom Opportunity, Stable"
        )

    return None


# =========================================================
# COMMANDS
# =========================================================
async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (
        "Whale Bot is online.\n\n"
        "Managers topic setup:\n"
        "/register carter  -> run inside the model topic\n\n"
        "Chatters:\n"
        "/whale -> send one full whale update template\n\n"
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
    template = (
        "Send whale update in this format:\n\n"
        "Model: carter\n"
        "Whale: John\n"
        "User ID: @9812377\n"
        "Priority: High\n"
        "Status: At Risk\n"
        "Last Convo: he's mad about the custom\n"
        "Notes: gif first\n"
        "Action: soft handle, no hard upsell\n"
        "Cooldown Reason: -\n\n"
        "Priority options:\n"
        "High\n"
        "Medium\n"
        "Low\n\n"
        "Status options:\n"
        "Critical\n"
        "At Risk\n"
        "Cooling Off\n"
        "Needs Attention\n"
        "Custom Opportunity\n"
        "Stable"
    )
    await update.message.reply_text(template)
    return WHALE_TEMPLATE_INPUT


async def whale_template_received(update: Update, context: ContextTypes.DEFAULT_TYPE):
    data = parse_template_message(update.message.text.strip())
    error = validate_template_data(data)

    if error:
        await update.message.reply_text(
            f"❌ {error}\n\n"
            "Please send again using this exact format:\n\n"
            "Model: carter\n"
            "Whale: John\n"
            "User ID: @9812377\n"
            "Priority: High\n"
            "Status: At Risk\n"
            "Last Convo: he's mad about the custom\n"
            "Notes: gif first\n"
            "Action: soft handle, no hard upsell\n"
            "Cooldown Reason: -"
        )
        return WHALE_TEMPLATE_INPUT

    full_name, username = fmt_user(update.effective_user)
    is_cooldown = data["current_status"] == "COOLING OFF"

    cooldown_reason = data["cooldown_reason"]
    if cooldown_reason == "-":
        cooldown_reason = None

    whale_row = upsert_whale_and_history(
        model_name=data["model_name"],
        whale_name=data["whale_name"],
        whale_user_id=data["whale_user_id"],
        priority=data["priority"].title(),
        status=data["current_status"].title(),
        last_convo=data["last_convo"],
        notes=data["notes"],
        action_needed=data["action_needed"],
        is_cooldown=is_cooldown,
        cooldown_reason=cooldown_reason,
        updated_by_id=update.effective_user.id,
        updated_by_name=full_name,
        updated_by_username=username,
    )

    await send_to_registered_topic(context.bot, whale_row, extra_alert=True)
    await update.message.reply_text(
        f"✅ Whale update sent to {data['model_name'].title()} topic."
    )
    return ConversationHandler.END


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

    parts = [f"🐳 ACTIVE WHALES — {model_name.upper()}\n"]
    for i, r in enumerate(rows, start=1):
        parts.append(
            f"{i}. {r['whale_name']}\n"
            f"🆔 {r['whale_user_id']}\n"
            f"🚦 {r.get('priority') or '-'}\n"
            f"📌 {r['current_status']}\n"
            f"💬 {r.get('last_convo') or '-'}\n"
            f"📝 {r.get('notes') or '-'}\n"
            f"🎯 {r.get('action_needed') or '-'}\n"
            f"⏳ {r.get('cooldown_reason') or '-'}\n"
            f"👨 {r.get('last_updated_by_username') or '-'}\n"
            f"🕒 {fmt_dt_pst(r.get('last_updated_at'))}\n"
        )

    final_text = "\n".join(parts)
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

    parts = [f"📋 WHALE HANDOVER — {model_name.upper()}\n"]
    for i, r in enumerate(rows, start=1):
        parts.append(
            f"{i}. {r['whale_name']} — {r['current_status']}\n"
            f"- user id: {r['whale_user_id']}\n"
            f"- priority: {r.get('priority') or '-'}\n"
            f"- last convo: {r.get('last_convo') or '-'}\n"
            f"- notes: {r.get('notes') or '-'}\n"
            f"- action: {r.get('action_needed') or '-'}\n"
            f"- cooldown: {r.get('cooldown_reason') or '-'}\n"
            f"- updated by: {r.get('last_updated_by_username') or '-'}\n"
            f"- updated at: {fmt_dt_pst(r.get('last_updated_at'))}\n"
        )

    final_text = "\n".join(parts)
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

    parts = [f"🚨 URGENT WHALES — {model_name.upper()}\n"]
    for i, r in enumerate(rows, start=1):
        parts.append(
            f"{i}. {r['whale_name']}\n"
            f"🆔 {r['whale_user_id']}\n"
            f"🚦 {r.get('priority') or '-'}\n"
            f"📌 {r['current_status']}\n"
            f"💬 {r.get('last_convo') or '-'}\n"
            f"📝 {r.get('notes') or '-'}\n"
            f"🎯 {r.get('action_needed') or '-'}\n"
            f"👨 {r.get('last_updated_by_username') or '-'}\n"
            f"🕒 {fmt_dt_pst(r.get('last_updated_at'))}\n"
        )

    final_text = "\n".join(parts)
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

    parts = [f"❄️ COOLDOWN WHALES — {model_name.upper()}\n"]
    for i, r in enumerate(rows, start=1):
        parts.append(
            f"{i}. {r['whale_name']}\n"
            f"🆔 {r['whale_user_id']}\n"
            f"🚦 {r.get('priority') or '-'}\n"
            f"⏳ {r.get('cooldown_reason') or '-'}\n"
            f"📝 {r.get('notes') or '-'}\n"
            f"🎯 {r.get('action_needed') or '-'}\n"
            f"👨 {r.get('last_updated_by_username') or '-'}\n"
            f"🕒 {fmt_dt_pst(r.get('last_updated_at'))}\n"
        )

    final_text = "\n".join(parts)
    for chunk in split_text(final_text):
        await update.message.reply_text(chunk)


async def cancel_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Cancelled.")
    return ConversationHandler.END


# =========================================================
# MAIN
# =========================================================
def main():
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN is missing")

    init_db()

    app = ApplicationBuilder().token(BOT_TOKEN).build()

    whale_conv = ConversationHandler(
        entry_points=[CommandHandler("whale", whale_start)],
        states={
            WHALE_TEMPLATE_INPUT: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, whale_template_received)
            ],
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

    logger.info("Whale bot starting...")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
