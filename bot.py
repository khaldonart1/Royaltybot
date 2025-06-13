# bot.py
# -*- coding: utf-8 -*-
import logging
import asyncio
import random
from enum import Enum, auto
from typing import Dict, Optional, Tuple, List

from telegram import (
    Update, InlineKeyboardButton, InlineKeyboardMarkup,
    KeyboardButton, ReplyKeyboardMarkup, ReplyKeyboardRemove, Chat
)
from telegram.ext import (
    Application, CommandHandler, MessageHandler, CallbackQueryHandler,
    ConversationHandler, ContextTypes, filters, ChatMemberHandler
)
from telegram.error import TelegramError
from supabase import create_client, Client

# --- Logging ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# --- States for ConversationHandler ---
MATH, PHONE, JOIN, ADMIN_BROADCAST = range(4)

# --- Configuration ---
class Config:
    BOT_TOKEN = "7950170561:AAFIrmAM5zzHQlvuUpR8KQeTSjRZ1_2Mi8M"
    SUPABASE_URL = "https://jofxsqsgarvzolgphqjg.supabase.co"
    SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImpvZnhzcXNnYXJ2em9sZ3BocWpnIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc0OTU5NTI4NiwiZXhwIjoyMDY1MTcxMjg2fQ.egB9qticc7ABgo6vmpsrPi3cOHooQmL5uQOKI4Jytqg"
    CHANNEL_ID  = -1002686156311
    GROUP_ID    = -1002472491601
    CHANNEL_URL = "https://t.me/Ry_Hub"
    GROUP_URL   = "https://t.me/joinchat/Rrx4fWReNLxlYWNk"
    OWNER_IDS = {596472053, 7164133014, 1971453570}
    ALLOWED_CODES = {
        "213","973","269","253","20","964","962","965","961",
        "218","222","212","968","970","974","966","252","249",
        "963","216","971","967"
    }
    CACHE_TTL_SECONDS = 90

# --- Initialize Supabase Client (sync) ---
supabase: Client = create_client(
    Config.SUPABASE_URL,
    Config.SUPABASE_KEY
)

# --- Helper to run sync DB calls in thread ---
async def run_sync_db(func):
    return await asyncio.to_thread(func)

# --- Helpers ---
def gen_math() -> Tuple[str,int]:
    a, b = random.randint(1,10), random.randint(1,10)
    return f"{a} + {b}", a+b

async def fetch_users(ctx: ContextTypes.DEFAULT_TYPE, force_refresh: bool = False) -> List[Dict]:
    cache = ctx.bot_data.get("users_cache")
    now = asyncio.get_event_loop().time()
    if cache and not force_refresh and now - cache["ts"] < Config.CACHE_TTL_SECONDS:
        return cache["data"]
    resp = await run_sync_db(lambda: supabase.table("users").select("*").execute())
    users = resp.data or []
    ctx.bot_data["users_cache"] = {"data": users, "ts": now}
    return users

async def get_user(uid: int) -> Optional[Dict]:
    resp = await run_sync_db(lambda: supabase
                              .table("users")
                              .select("*")
                              .eq("user_id", uid)
                              .single()
                              .execute())
    return resp.data

async def upsert_user(data: Dict):
    await run_sync_db(lambda: supabase
                      .table("users")
                      .upsert(data, on_conflict="user_id")
                      .execute())

# --- Keyboards ---
def main_kb(user_id: int):
    kb = [
        [InlineKeyboardButton("📊 إحصائياتي", callback_data="stats")],
        [InlineKeyboardButton("🔗 رابط الإحالة", callback_data="link")],
        [InlineKeyboardButton("🏆 أفضل 5", callback_data="top")],
    ]
    if user_id in Config.OWNER_IDS:
        kb.append([InlineKeyboardButton("👑 إدارة", callback_data="admin")])
    return InlineKeyboardMarkup(kb)

def join_kb():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("1. قناة", url=Config.CHANNEL_URL)],
        [InlineKeyboardButton("2. مجموعة", url=Config.GROUP_URL)],
        [InlineKeyboardButton("✅ تحقق", callback_data="confirm_join")],
    ])

# --- Handlers ---
async def start(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    if update.effective_chat.type != Chat.PRIVATE:
        return ConversationHandler.END
    user = update.effective_user
    await upsert_user({"user_id": user.id, "full_name": user.full_name})
    dbu = await get_user(user.id)
    if dbu and dbu.get("is_verified"):
        await update.message.reply_text("مرحباً مجدداً!", reply_markup=main_kb(user.id))
        return ConversationHandler.END
    q, ans = gen_math()
    ctx.user_data["math_ans"] = ans
    await update.message.reply_text(f"لحماية البوت: ما ناتج {q}؟")
    return MATH

async def math_answer(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    text = update.message.text.strip()
    if not text.isdigit() or int(text) != ctx.user_data.get("math_ans"):
        q, ans = gen_math()
        ctx.user_data["math_ans"] = ans
        return await update.message.reply_text(f"خطأ. حاول: ما ناتج {q}؟")
    kb = [[KeyboardButton("شارك رقمك", request_contact=True)]]
    await update.message.reply_text(
        "أرسل رقم هاتفك:", 
        reply_markup=ReplyKeyboardMarkup(kb, one_time_keyboard=True, resize_keyboard=True)
    )
    return PHONE

async def phone_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    contact = update.message.contact
    if not contact or contact.user_id != update.effective_user.id:
        return await update.message.reply_text("يرجى مشاركة رقمك من خلال الزر.")
    num = contact.phone_number.lstrip("+")
    if not any(num.startswith(code) for code in Config.ALLOWED_CODES):
        return await update.message.reply_text("عذراً، رقم غير مدعوم.", reply_markup=ReplyKeyboardRemove())
    await update.message.reply_text("✓ تم استلام الرقم.", reply_markup=ReplyKeyboardRemove())
    await update.message.reply_text("الخطوة الأخيرة، انضم ثم تحقق:", reply_markup=join_kb())
    return JOIN

async def confirm_join(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    uid = update.effective_user.id
    try:
        ch = await ctx.bot.get_chat_member(Config.CHANNEL_ID, uid)
        gr = await ctx.bot.get_chat_member(Config.GROUP_ID, uid)
        ok = ch.status in ("member","administrator","creator") and gr.status in ("member","administrator","creator")
    except TelegramError:
        ok = False
    if not ok:
        await update.callback_query.answer("لم تنضم بعد.", show_alert=True)
        return JOIN
    await upsert_user({"user_id": uid, "is_verified": True})
    await update.callback_query.edit_message_text("تم التحقق بنجاح!")
    await ctx.bot.send_message(uid, "أهلاً بك!", reply_markup=main_kb(uid))
    return ConversationHandler.END

async def admin_entry(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    await update.callback_query.edit_message_text("أرسل نص الإذاعة:")
    return ADMIN_BROADCAST

async def do_broadcast(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    text = update.message.text
    users = await fetch_users(ctx)
    verified = [u["user_id"] for u in users if u.get("is_verified")]
    sent = failed = 0
    for uid in verified:
        try:
            await ctx.bot.send_message(uid, text)
            sent += 1
        except TelegramError:
            failed += 1
        await asyncio.sleep(0.05)
    await update.message.reply_text(f"📤 تم الإرسال: {sent}\n❌ فشل: {failed}", reply_markup=main_kb(update.effective_user.id))
    return ConversationHandler.END

# --- Main ---
def main():
    app = Application.builder().token(Config.BOT_TOKEN).build()
    conv = ConversationHandler(
        entry_points=[CommandHandler("start", start)],
        states={
            MATH: [MessageHandler(filters.TEXT & ~filters.COMMAND, math_answer)],
            PHONE: [MessageHandler(filters.CONTACT, phone_handler)],
            JOIN: [CallbackQueryHandler(confirm_join, pattern="^confirm_join$")],
            ADMIN_BROADCAST: [MessageHandler(filters.TEXT & ~filters.COMMAND, do_broadcast)],
        },
        fallbacks=[]
    )
    app.add_handler(conv)
    app.add_handler(CallbackQueryHandler(admin_entry, pattern="^admin$"))
    logger.info("Bot is starting...")
    app.run_polling()

if __name__ == "__main__":
    main()
