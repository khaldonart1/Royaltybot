import asyncio
import logging
import math
import random
import re
import time
import requests
import json
from enum import Enum, auto
from typing import Any, Callable, Dict, List, Optional, Tuple

from telegram import (
    CallbackQuery,
    Chat,
    ChatMemberUpdated,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    Update,
    WebAppInfo,
    ReplyKeyboardRemove,
    ReplyKeyboardMarkup,
    Message
)
from telegram.constants import ParseMode
from telegram.error import BadRequest, TelegramError
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    ChatMemberHandler,
    CommandHandler,
    ContextTypes,
    JobQueue,
    MessageHandler,
    filters,
)

from supabase import Client, create_client

# --- إعدادات التسجيل (Logging) ---
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logging.getLogger("httpx").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)


# --- إعدادات البوت الرئيسية (Config) ---
class Config:
    BOT_TOKEN = "7950170561:AAH5OtiK38BBhAnVofqxnLWRYbaZaIaKY4s"
    SUPABASE_URL = "https://jofxsqsgarvzolgphqjg.supabase.co"
    SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImpvZnhzcXNnYXJ2em9sZ3BocWpnIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc0OTU5NTI4NiwiZXhwIjoyMDY1MTcxMjg2fQ.egB9qticc7ABgo6vmpsrPi3cOHooQmL5uQOKI4Jytqg"
    # !!! هام: يجب وضع الرابط العام من PythonAnywhere هنا !!!
    WEB_APP_URL = "https://khaldonart.pythonanywhere.com" 
    CHANNEL_ID = -1002686156311
    CHANNEL_URL = "https://t.me/Ry_Hub"
    BOT_OWNER_IDS = {596472053, 7164133014, 1971453570}
    ALLOWED_COUNTRY_CODES = {
        "213", "973", "269", "253", "20", "964", "962", "965", "961",
        "218", "222", "212", "968", "970", "974", "966", "252", "249",
        "963", "216", "971", "967"
    }
    USERS_PER_PAGE = 15
    MENTION_CACHE_TTL_SECONDS = 300 # Cache for user mentions (5 minutes)
    IPGEOLOCATION_API_KEY = None # أضف مفتاح API الخاص بك من api.ipgeolocation.io هنا إذا كنت تريد فحص VPN
    MAX_REFERRALS_PER_IP = 2

# --- حالات البوت (State) ---
class State(Enum):
    AWAITING_EDIT_USER_ID = auto()
    AWAITING_EDIT_AMOUNT = auto()
    AWAITING_WINNER_THRESHOLD = auto()
    AWAITING_BROADCAST_MESSAGE = auto()
    AWAITING_WEB_APP_VERIFICATION = auto() # Simplified flow, goes directly to web app
    AWAITING_UNIVERSAL_BROADCAST_MESSAGE = auto()


# --- تعريفات أزرار الكيبورد (Callback) ---
class Callback(Enum):
    MAIN_MENU = "main_menu"
    MY_REFERRALS = "my_referrals"
    MY_LINK = "my_link"
    TOP_5 = "top_5"
    CONFIRM_JOIN = "confirm_join"
    ADMIN_PANEL = "admin_panel"
    ADMIN_USER_COUNT = "admin_user_count"
    ADMIN_BOOO_MENU = "admin_booo_menu"
    ADMIN_USER_EDIT_MENU = "admin_user_edit_menu"
    USER_ADD_REAL = "user_add_real"
    USER_REMOVE_REAL = "user_remove_real"
    USER_ADD_FAKE = "user_add_fake"
    USER_REMOVE_FAKE = "user_remove_fake"
    REPORT_PAGE = "report_"
    DATA_MIGRATION = "data_migration"
    PICK_WINNER = "pick_winner"
    ADMIN_BROADCAST = "admin_broadcast"
    ADMIN_RESET_ALL = "admin_reset_all"
    ADMIN_RESET_CONFIRM = "admin_reset_confirm"
    REQUEST_PHONE_CONTACT = "request_phone_contact"
    ADMIN_FORMAT_BOT = "admin_format_bot"
    ADMIN_FORMAT_CONFIRM = "admin_format_confirm"
    ADMIN_FORCE_REVERIFICATION = "admin_force_reverification"
    ADMIN_UNIVERSAL_BROADCAST = "admin_universal_broadcast"

# --- رسائل البوت (Messages) ---
class Messages:
    VERIFIED_WELCOME = "أهلاً بك مجدداً! ✅\n\nاستخدم الأزرار أو الأوامر للتفاعل مع البوت."
    START_WELCOME = "أهلاً بك في البوت! 👋\n\nيجب عليك إتمام خطوات بسيطة للتحقق أولاً."
    WEB_VERIFY_PROMPT = "خطوة ممتازة! للتحقق من أنك لا تستخدم نفس الجهاز عدة مرات، الرجاء الضغط على الزر أدناه."
    JOIN_PROMPT = "ممتاز! الخطوة الأخيرة هي الانضمام إلى قناتنا. انضم ثم اضغط على الزر أدناه."
    JOIN_SUCCESS = "تهانينا! لقد تم التحقق منك بنجاح."
    JOIN_FAIL = "❌ لم تنضم بعد. الرجاء الانضمام إلى القناة ثم حاول مرة أخرى."
    GENERIC_ERROR = "حدث خطأ ما. يرجى المحاولة مرة أخرى لاحقاً."
    LOADING = "⏳ جاري التحميل..."
    ADMIN_WELCOME = "👑 أهلاً بك في لوحة تحكم المالك."
    INVALID_INPUT = "إدخال غير صالح. الرجاء المحاولة مرة أخرى."
    VPN_DETECTED = "تم اكتشاف استخدامك لـ VPN. يرجى تعطيله والمحاولة مرة أخرى."
    REFERRAL_ABUSE_DETECTED = "تم اكتشاف إساءة استخدام لنظام الإحالة. تم حظر هذه الإحالة لأن الجهاز تم استخدامه سابقاً."
    MATH_CORRECT = "إجابة صحيحة! لننتقل للخطوة التالية."

# --- الاتصال بقاعدة البيانات (Supabase) ---
try:
    supabase: Client = create_client(Config.SUPABASE_URL, Config.SUPABASE_KEY)
    logger.info("Successfully connected to Supabase.")
except Exception as e:
    logger.critical(f"FATAL: Failed to connect to Supabase. Error: {e}")
    exit(1)

# --- دوال مساعدة (Helper Functions) ---
def clean_name_for_markdown(name: str) -> str:
    if not name: return ""
    return re.sub(r"([*_`\[\]\(\)])", "", name)

async def get_user_mention(user_id: int, context: ContextTypes.DEFAULT_TYPE) -> str:
    cache = context.bot_data.setdefault('mention_cache', {})
    current_time = time.time()
    
    if user_id in cache and (current_time - cache[user_id].get('timestamp', 0) < Config.MENTION_CACHE_TTL_SECONDS):
        return cache[user_id]['mention']

    try:
        chat = await context.bot.get_chat(user_id)
        full_name = clean_name_for_markdown(chat.full_name)
        mention = f"[{full_name}](tg://user?id={user_id})"
        
        cache[user_id] = {'mention': mention, 'timestamp': current_time}
    except Exception:
        db_user_info = await get_user_from_db(user_id)
        full_name = "مستخدم غير معروف"
        if db_user_info:
            full_name = clean_name_for_markdown(db_user_info.get("full_name", f"User {user_id}"))
        mention = f"[{full_name}](tg://user?id={user_id})"
        cache[user_id] = {'mention': mention, 'timestamp': current_time}

    return mention

async def is_vpn(ip_address: str) -> bool:
    if not Config.IPGEOLOCATION_API_KEY:
        return False
    try:
        response = requests.get(f"https://api.ipgeolocation.io/ipgeo?apiKey={Config.IPGEOLOCATION_API_KEY}&ip={ip_address}&fields=security")
        data = response.json()
        return data.get("security", {}).get("is_vpn", False)
    except Exception as e:
        logger.error(f"VPN check failed for IP {ip_address}: {e}")
        return False

# --- دوال التعامل مع قاعدة البيانات (Database Functions) ---
async def run_sync_db(func: Callable[[], Any]) -> Any:
    return await asyncio.to_thread(func)

async def get_user_from_db(user_id: int) -> Optional[Dict[str, Any]]:
    try:
        res = await run_sync_db(
            lambda: supabase.table('users').select("*").eq('user_id', user_id).single().execute()
        )
        return res.data
    except Exception:
        return None

async def upsert_user_in_db(user_data: Dict[str, Any]) -> None:
    try:
        await run_sync_db(lambda: supabase.table('users').upsert(user_data, on_conflict='user_id').execute())
    except Exception as e:
        logger.error(f"DB_ERROR: Upserting user {user_data.get('user_id')}: {e}")

async def get_all_users_from_db() -> List[Dict[str, Any]]:
    try:
        res = await run_sync_db(
            lambda: supabase.table('users').select("*").execute()
        )
        return res.data or []
    except Exception as e:
        logger.error(f"DB_ERROR (get_all_users_from_db): {e}")
        return []

async def get_referrer(referred_id: int) -> Optional[int]:
    try:
        res = await run_sync_db(
            lambda: supabase.table('referrals').select('referrer_user_id').eq('referred_user_id', referred_id).single().execute()
        )
        return res.data.get('referrer_user_id') if res.data else None
    except Exception:
        return None

async def add_referral_mapping(referred_id: int, referrer_id: int, ip_address: str) -> bool:
    try:
        if ip_address != "UNKNOWN":
            res = await run_sync_db(
                lambda: supabase.table('referrals').select('ip_address', count='exact').eq('ip_address', ip_address).execute()
            )
            if res.count >= Config.MAX_REFERRALS_PER_IP:
                logger.warning(f"Referral abuse detected for IP {ip_address}. User {referred_id} blocked from referring {referrer_id}.")
                return False

        data = {'referred_user_id': referred_id, 'referrer_user_id': referrer_id, 'ip_address': ip_address}
        await run_sync_db(lambda: supabase.table('referrals').upsert(data, on_conflict='referred_user_id').execute())
        return True
    except Exception as e:
        logger.error(f"DB_ERROR: Adding referral map for {referred_id} by {referrer_id}: {e}")
        return False

async def reset_all_referrals_in_db() -> None:
    try:
        await run_sync_db(lambda: supabase.table('referrals').delete().gt('referred_user_id', 0).execute())
        update_payload = {"total_real": 0, "total_fake": 0}
        await run_sync_db(lambda: supabase.table('users').update(update_payload).gt('user_id', 0).execute())
        logger.info("All referrals have been reset in the database.")
    except Exception as e:
        logger.error(f"DB_ERROR: Resetting all referrals: {e}")

async def format_bot_in_db() -> None:
    try:
        await run_sync_db(lambda: supabase.table('referrals').delete().gt('referred_user_id', 0).execute())
        logger.info("All referrals have been deleted from the database.")
        await run_sync_db(lambda: supabase.table('users').delete().gt('user_id', 0).execute())
        logger.info("All users have been deleted from the database.")
        logger.info("BOT HAS BEEN FORMATTED.")
    except Exception as e:
        logger.error(f"DB_ERROR: Formatting bot: {e}")

async def unverify_all_users_in_db() -> None:
    try:
        update_payload = {"is_verified": False}
        await run_sync_db(lambda: supabase.table('users').update(update_payload).gt('user_id', 0).execute())
        logger.info("All users have been successfully marked as unverified.")
    except Exception as e:
        logger.error(f"DB_ERROR: Failed while un-verifying all users: {e}")

# --- دوال المنطق الأساسي (Core Logic) ---

async def modify_referral_count(user_id: int, real_delta: int = 0, fake_delta: int = 0) -> Optional[Dict[str, Any]]:
    if not user_id: return None
    user_data = await get_user_from_db(user_id)
    if not user_data:
        logger.warning(f"Attempted to modify counts for non-existent user {user_id}")
        return None
    current_real = int(user_data.get('total_real', 0) or 0)
    current_fake = int(user_data.get('total_fake', 0) or 0)
    new_real = max(0, current_real + real_delta)
    new_fake = max(0, current_fake + fake_delta)
    update_payload = {'user_id': user_id, 'total_real': new_real, 'total_fake': new_fake}
    await upsert_user_in_db(update_payload)
    logger.info(f"Updated counts for {user_id}: Real {current_real}->{new_real}, Fake {current_fake}->{new_fake}")
    return await get_user_from_db(user_id)

# --- دوال العرض (Display Functions) ---

def get_referral_stats_text(user_info: Optional[Dict[str, Any]]) -> str:
    if not user_info: return "لا توجد لديك بيانات بعد. حاول مرة أخرى."
    total_real = int(user_info.get("total_real", 0) or 0)
    total_fake = int(user_info.get("total_fake", 0) or 0)
    return f"📊 *إحصائيات إحالاتك:*\n\n✅ الإحالات الحقيقية: *{total_real}*\n⏳ الإحالات الوهمية: *{total_fake}*"

def get_referral_link_text(user_id: int, bot_username: str) -> str:
    return f"🔗 رابط الإحالة الخاص بك:\n`https://t.me/{bot_username}?start={user_id}`"

async def get_top_5_text(user_id: int, context: ContextTypes.DEFAULT_TYPE) -> str:
    msg = "🏆 *أفضل 5 متسابقين لدينا:*\n\n"
    all_users = await get_all_users_from_db()
    if not all_users:
        return msg + "لم يصل أحد إلى القائمة بعد. كن أنت الأول!\n\n---\n*ترتيبك الشخصي:*\nلا يمكن عرض ترتيبك حالياً."
    full_sorted_list = sorted(all_users, key=lambda u: u.get('total_real', 0), reverse=True)
    top_5_users = [u for u in full_sorted_list if u.get('total_real', 0) > 0][:5]
    if not top_5_users:
        msg += "لم يصل أحد إلى القائمة بعد. كن أنت الأول!\n"
    else:
        mentions = await asyncio.gather(*[get_user_mention(u['user_id'], context) for u in top_5_users])
        for i, u_info in enumerate(top_5_users):
            mention = mentions[i]
            count = u_info.get('total_real', 0)
            msg += f"{i+1}. {mention} - *{count}* إحالة\n"
    msg += "\n---\n*ترتيبك الشخصي:*\n"
    try:
        user_index = next((i for i, u in enumerate(full_sorted_list) if u.get('user_id') == user_id), -1)
        my_referrals = 0
        if user_index != -1:
            rank_str = f"#{user_index + 1}"
            my_referrals = full_sorted_list[user_index].get('total_real', 0)
        else:
            rank_str = "غير مصنف"
        msg += f"🎖️ ترتيبك: *{rank_str}*\n✅ رصيدك: *{my_referrals}* إحالة حقيقية."
    except Exception as e:
        logger.error(f"Error getting user rank for {user_id}: {e}")
        msg += "لا يمكن عرض ترتيبك حالياً."
    return msg

async def get_paginated_report(page: int, report_type: str, context: ContextTypes.DEFAULT_TYPE) -> Tuple[str, InlineKeyboardMarkup]:
    all_users = await get_all_users_from_db()
    if not all_users:
        return "لا يوجد أي مستخدمين في هذا التقرير حالياً.", get_admin_panel_keyboard()

    if report_type == 'real':
        filtered_users = [u for u in all_users if u.get('total_real', 0) > 0]
        filtered_users.sort(key=lambda u: u.get('total_real', 0), reverse=True)
        title = "✅ *تقرير الإحالات الحقيقية*"
        count_key = 'total_real'
    else: # fake
        filtered_users = [u for u in all_users if u.get('total_fake', 0) > 0]
        filtered_users.sort(key=lambda u: u.get('total_fake', 0), reverse=True)
        title = "⏳ *تقرير الإحالات الوهمية*"
        count_key = 'total_fake'

    if not filtered_users:
        return f"لا يوجد أي مستخدمين في هذا التقرير ({report_type}) حالياً.", get_admin_panel_keyboard()

    start_index = (page - 1) * Config.USERS_PER_PAGE
    end_index = start_index + Config.USERS_PER_PAGE
    page_users = filtered_users[start_index:end_index]
    total_pages = math.ceil(len(filtered_users) / Config.USERS_PER_PAGE)

    report = f"{title} (صفحة {page} من {total_pages}):\n\n"
    
    mentions = await asyncio.gather(*[get_user_mention(u['user_id'], context) for u in page_users])
    
    for i, u_data in enumerate(page_users):
        mention = mentions[i]
        count = u_data.get(count_key, 0)
        report += f"• {mention} - *{count}*\n"
        
    nav_buttons = []
    callback_prefix = f"{Callback.REPORT_PAGE.value}{report_type}_page_"
    if page > 1:
        nav_buttons.append(InlineKeyboardButton("⬅️ السابق", callback_data=f"{callback_prefix}{page-1}"))
    if page < total_pages:
        nav_buttons.append(InlineKeyboardButton("التالي ➡️", callback_data=f"{callback_prefix}{page+1}"))
    
    keyboard = [nav_buttons, [InlineKeyboardButton("🔙 العودة للوحة التحكم", callback_data=Callback.ADMIN_PANEL.value)]]
    return report, InlineKeyboardMarkup(keyboard)

# --- دوال إنشاء الكيبورد (Keyboard Functions) ---
def get_main_menu_keyboard(user_id: int) -> InlineKeyboardMarkup:
    keyboard = [
        [InlineKeyboardButton("إحصائياتي 📊", callback_data=Callback.MY_REFERRALS.value)],
        [InlineKeyboardButton("رابطي 🔗", callback_data=Callback.MY_LINK.value)],
        [InlineKeyboardButton("🏆 أفضل 5 متسابقين", callback_data=Callback.TOP_5.value)],
    ]
    if user_id in Config.BOT_OWNER_IDS:
        keyboard.append([InlineKeyboardButton("👑 لوحة تحكم المالك 👑", callback_data=Callback.ADMIN_PANEL.value)])
    return InlineKeyboardMarkup(keyboard)

def get_admin_panel_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📊 تقرير حقيقي", callback_data=f"{Callback.REPORT_PAGE.value}real_page_1"), InlineKeyboardButton("⏳ تقرير وهمي", callback_data=f"{Callback.REPORT_PAGE.value}fake_page_1")],
        [InlineKeyboardButton("👥 عدد المستخدمين", callback_data=Callback.ADMIN_USER_COUNT.value), InlineKeyboardButton("🏆 اختيار فائز", callback_data=Callback.PICK_WINNER.value)],
        [InlineKeyboardButton("Booo 👾 (تعديل يدوي)", callback_data=Callback.ADMIN_BOOO_MENU.value)],
        [InlineKeyboardButton("📢 إذاعة للموثقين", callback_data=Callback.ADMIN_BROADCAST.value)],
        [InlineKeyboardButton("📢 إذاعة للكل", callback_data=Callback.ADMIN_UNIVERSAL_BROADCAST.value)],
        [InlineKeyboardButton("🔄 فرض إعادة التحقق", callback_data=Callback.ADMIN_FORCE_REVERIFICATION.value)],
        [InlineKeyboardButton("⚠️ تصفير كل الإحالات", callback_data=Callback.ADMIN_RESET_ALL.value)],
        [InlineKeyboardButton("⚙️ ترحيل وإعادة حساب البيانات", callback_data=Callback.DATA_MIGRATION.value)],
        [InlineKeyboardButton("💀 فورمات البوت (حذف كل شيء)", callback_data=Callback.ADMIN_FORMAT_BOT.value)],
        [InlineKeyboardButton("⬅️ العودة للقائمة الرئيسية", callback_data=Callback.MAIN_MENU.value)],
    ])

def get_booo_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✍️ تعديل إحصائيات مستخدم", callback_data=Callback.ADMIN_USER_EDIT_MENU.value)],
        [InlineKeyboardButton("🔙 العودة للوحة التحكم", callback_data=Callback.ADMIN_PANEL.value)]
    ])

def get_user_edit_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("➕ إضافة إحالات حقيقية", callback_data=Callback.USER_ADD_REAL.value)],
        [InlineKeyboardButton("➖ خصم إحالات حقيقية", callback_data=Callback.USER_REMOVE_REAL.value)],
        [InlineKeyboardButton("➕ إضافة إحالات وهمية", callback_data=Callback.USER_ADD_FAKE.value)],
        [InlineKeyboardButton("➖ خصم إحالات وهمية", callback_data=Callback.USER_REMOVE_FAKE.value)],
        [InlineKeyboardButton("🔙 العودة لقائمة Booo", callback_data=Callback.ADMIN_BOOO_MENU.value)]
    ])

def get_reset_confirmation_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ نعم، قم بالتصفير", callback_data=Callback.ADMIN_RESET_CONFIRM.value)],
        [InlineKeyboardButton("❌ لا، الغِ الأمر", callback_data=Callback.ADMIN_PANEL.value)]
    ])

# --- دوال التحقق من الانضمام والتحقق (Verification Handlers) ---
async def is_user_in_channel(user_id: int, context: ContextTypes.DEFAULT_TYPE) -> bool:
    try:
        ch_mem = await context.bot.get_chat_member(chat_id=Config.CHANNEL_ID, user_id=user_id)
        return ch_mem.status in {'member', 'administrator', 'creator'}
    except TelegramError as e:
        logger.warning(f"Error checking membership for {user_id}: {e}")
        return False
    except Exception as e:
        logger.error(f"An unexpected error occurred while checking membership for {user_id}: {e}")
        return False

def generate_math_question() -> Tuple[str, int]:
    num1, num2 = random.randint(1, 10), random.randint(1, 10)
    return f"{num1} + {num2}", num1 + num2

# --- معالجات الأوامر (Command Handlers) ---
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or not update.message or update.effective_chat.type != Chat.PRIVATE:
        return
    
    user = update.effective_user
    user_id = user.id
    
    db_user = await get_user_from_db(user_id)
    if db_user and db_user.get("is_verified"):
        await update.message.reply_text(Messages.VERIFIED_WELCOME, reply_markup=get_main_menu_keyboard(user_id))
        return

    if not db_user:
        await upsert_user_in_db({'user_id': user_id, 'full_name': user.full_name, 'username': user.username, 'total_real': 0, 'total_fake': 0, 'is_verified': False})

    args = context.args
    if args:
        try:
            referrer_id = int(args[0])
            if referrer_id != user_id:
                context.user_data['referrer_id'] = referrer_id
        except (ValueError, IndexError):
            pass
            
    await update.message.reply_text(Messages.START_WELCOME)
    await ask_math_question(update, context)

async def my_referrals_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_user or not update.message: return
    msg = await update.message.reply_text(Messages.LOADING)
    user_info = await get_user_from_db(update.effective_user.id)
    text = get_referral_stats_text(user_info)
    await msg.edit_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=get_main_menu_keyboard(update.effective_user.id))

async def link_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_user or not update.message: return
    user_id = update.effective_user.id
    text = get_referral_link_text(user_id, context.bot.username)
    await update.message.reply_text(
        text, 
        parse_mode=ParseMode.MARKDOWN, 
        reply_markup=get_main_menu_keyboard(user_id),
        disable_web_page_preview=True
    )

async def top_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_user or not update.message: return
    user_id = update.effective_user.id
    msg = await update.message.reply_text(Messages.LOADING)
    text = await get_top_5_text(user_id, context)
    await msg.edit_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=get_main_menu_keyboard(user_id), disable_web_page_preview=True)

async def ask_math_question(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    question, answer = generate_math_question()
    context.user_data['math_answer'] = answer
    await update.message.reply_text(f"للتحقق، ما هو ناتج {question}؟")
    
async def ask_web_verification(message: Message, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Sends a message with a button to open the web app for IP verification."""
    keyboard = InlineKeyboardMarkup.from_button(
        InlineKeyboardButton(
            text="🔒 اضغط هنا للتحقق من جهازك",
            web_app=WebAppInfo(url=Config.WEB_APP_URL),
        )
    )
    await message.reply_text(
        Messages.WEB_VERIFY_PROMPT,
        reply_markup=keyboard,
    )

# --- معالجات الرسائل والمدخلات (Input Handlers) ---
async def handle_verification_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_user or not update.message or update.effective_chat.type != Chat.PRIVATE: return
    
    user_id = update.effective_user.id
    
    if user_id in Config.BOT_OWNER_IDS and context.user_data.get('state'):
        await handle_admin_messages(update, context)
        return
        
    db_user = await get_user_from_db(user_id)
    if db_user and db_user.get('is_verified'):
        await update.message.reply_text(Messages.VERIFIED_WELCOME, reply_markup=get_main_menu_keyboard(user_id))
        return

    if 'math_answer' in context.user_data:
        try:
            if int(update.message.text) == context.user_data['math_answer']:
                del context.user_data['math_answer']
                context.user_data['state'] = State.AWAITING_WEB_APP_VERIFICATION
                await update.message.reply_text(Messages.MATH_CORRECT)
                await ask_web_verification(update.message, context)
            else:
                await update.message.reply_text("إجابة خاطئة. حاول مرة اخرى.")
                await ask_math_question(update, context)
        except (ValueError, TypeError):
            await update.message.reply_text("من فضلك أدخل رقماً صحيحاً كإجابة.")

async def web_app_data_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handles data sent from the web app with enhanced logging and user feedback."""
    logger.info("--- web_app_data_handler TRIGGERED ---")
    
    if not update.message or not update.effective_user:
        logger.error("web_app_data_handler was triggered but 'update.message' or 'update.effective_user' is missing.")
        return

    user_id = update.effective_user.id
    
    try:
        await context.bot.send_message(
            chat_id=user_id,
            text="⏳ استلمت البيانات من صفحة التحقق، جاري المعالجة..."
        )
    except Exception as e:
        logger.error(f"Failed to send initial confirmation message to user {user_id}: {e}")
        
    try:
        logger.info(f"Processing web app data for user_id: {user_id}")

        if not update.message.web_app_data:
            logger.warning(f"Handler triggered for user {user_id}, but no web_app_data found in the message.")
            await context.bot.send_message(chat_id=user_id, text="لم أجد بيانات التحقق. يرجى المحاولة مرة أخرى.")
            return
            
        raw_data = update.message.web_app_data.data
        logger.info(f"Raw data received for user {user_id}: {raw_data}")
        
        try:
            data = json.loads(raw_data)
        except json.JSONDecodeError:
            logger.error(f"Failed to decode JSON for user {user_id}. Raw data: {raw_data}")
            await context.bot.send_message(chat_id=user_id, text="حدث خطأ في تنسيق بيانات التحقق. حاول مرة أخرى.")
            return

        if data.get("error"):
            logger.error(f"Web app reported a client-side error for user {user_id}: {data.get('details')}")
            await context.bot.send_message(chat_id=user_id, text="أبلغت صفحة التحقق عن وجود خطأ. يرجى التأكد من اتصالك بالإنترنت والمحاولة مرة أخرى.")
            return
        
        ip_address = data.get("ip")
        if not ip_address:
            logger.error(f"IP address is missing in parsed data for user {user_id}. Data: {data}")
            await context.bot.send_message(chat_id=user_id, text="لم يتم العثور على عنوان IP في بيانات التحقق. يرجى المحاولة مرة أخرى.")
            return
            
        logger.info(f"Successfully parsed IP address '{ip_address}' for user {user_id}")

        context.bot_data[f'ip_{user_id}'] = ip_address
        
        referrer_id = context.user_data.get('referrer_id')
        if referrer_id and not await get_referrer(user_id):
            logger.info(f"Processing referral for {user_id} by referrer {referrer_id} with IP {ip_address}")
            if not await add_referral_mapping(user_id, referrer_id, ip_address):
                logger.warning(f"Referral abuse detected for user {user_id} from IP {ip_address}.")
                await context.bot.send_message(chat_id=user_id, text=Messages.REFERRAL_ABUSE_DETECTED)
                return
            else:
                await modify_referral_count(user_id=referrer_id, fake_delta=1)
                logger.info(f"Referral mapping for {user_id} successful.")
        
        if await is_vpn(ip_address):
            logger.warning(f"VPN detected for user {user_id} with IP {ip_address}.")
            await context.bot.send_message(chat_id=user_id, text=Messages.VPN_DETECTED)
            return

        logger.info(f"All IP checks passed for user {user_id}. Proceeding to phone verification.")
        keyboard = InlineKeyboardMarkup.from_button(
            InlineKeyboardButton(
                text="📱 مشاركة رقم الهاتف",
                callback_data=Callback.REQUEST_PHONE_CONTACT.value
            )
        )
        await context.bot.send_message(
            chat_id=user_id,
            text="الآن، من فضلك شارك رقم هاتفك لإكمال العملية بالضغط على الزر أدناه.",
            reply_markup=keyboard
        )

    except Exception as e:
        logger.error(f"An unexpected error occurred in web_app_data_handler for user {user_id}: {e}", exc_info=True)
        try:
            await context.bot.send_message(chat_id=user_id, text=Messages.GENERIC_ERROR)
        except Exception as send_error:
            logger.error(f"Could not even send a final error message to user {user_id}: {send_error}")
    finally:
        logger.info(f"--- web_app_data_handler finished for user {user_id} ---")

async def request_phone_handler(query: CallbackQuery, context: ContextTypes.DEFAULT_TYPE) -> None:
    await query.answer()
    phone_button = [[KeyboardButton("اضغط هنا لمشاركة رقم هاتفك", request_contact=True)]]
    await query.message.reply_text(
        "الرجاء الضغط على الزر الذي سيظهر في الأسفل لمشاركة رقم هاتفك.",
        reply_markup=ReplyKeyboardMarkup(phone_button, resize_keyboard=True, one_time_keyboard=True)
    )

async def handle_contact(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.message.contact or update.effective_chat.type != Chat.PRIVATE:
        return
    
    contact = update.message.contact
    if contact.user_id != update.effective_user.id:
        await update.message.reply_text("الرجاء مشاركة جهة الاتصال الخاصة بك فقط.", reply_markup=ReplyKeyboardRemove())
        return

    phone_number = contact.phone_number.lstrip('+')
    if any(phone_number.startswith(code) for code in Config.ALLOWED_COUNTRY_CODES):
        await update.message.reply_text("تم استلام الرقم بنجاح.", reply_markup=ReplyKeyboardRemove())
        keyboard = [
            [InlineKeyboardButton("1. الانضمام للقناة", url=Config.CHANNEL_URL)],
            [InlineKeyboardButton("✅ لقد انضممت، تحقق الآن", callback_data=Callback.CONFIRM_JOIN.value)]
        ]
        await update.message.reply_text(Messages.JOIN_PROMPT, reply_markup=InlineKeyboardMarkup(keyboard))
    else:
        await update.message.reply_text("عذراً، هذا البوت مخصص فقط للمستخدمين من الدول العربية. رقمك غير مدعوم.", reply_markup=ReplyKeyboardRemove())
        context.user_data['state'] = State.AWAITING_WEB_APP_VERIFICATION
        await ask_web_verification(update.message, context)

async def handle_confirm_join(query: CallbackQuery, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = query.from_user
    await query.edit_message_text(Messages.LOADING)
    
    if await is_user_in_channel(user.id, context):
        db_user = await get_user_from_db(user.id)
        if not db_user or not db_user.get('is_verified'):
            await upsert_user_in_db({'user_id': user.id, 'is_verified': True, 'full_name': user.full_name, 'username': user.username})
            referrer_id = await get_referrer(user.id)
            if referrer_id:
                try:
                    updated_referrer = await modify_referral_count(user_id=referrer_id, real_delta=1, fake_delta=-1)
                    if updated_referrer:
                        new_real_count = updated_referrer.get('total_real', 0)
                        mention = await get_user_mention(user.id, context)
                        await context.bot.send_message(
                            chat_id=referrer_id,
                            text=f"🎉 تهانينا! لقد انضم مستخدم جديد ({mention}) عن طريق رابطك.\n\n"
                                 f"رصيدك المحدث هو: *{new_real_count}* إحالة حقيقية.",
                            parse_mode=ParseMode.MARKDOWN
                        )
                except TelegramError as e:
                    logger.warning(f"Could not send notification to referrer {referrer_id}: {e}")
        await query.edit_message_text(Messages.JOIN_SUCCESS)
        await query.message.reply_text(Messages.VERIFIED_WELCOME, reply_markup=get_main_menu_keyboard(user.id))
    else:
        await query.answer(text=Messages.JOIN_FAIL, show_alert=True)
        keyboard = [
            [InlineKeyboardButton("1. الانضمام للقناة", url=Config.CHANNEL_URL)],
            [InlineKeyboardButton("✅ لقد انضممت، تحقق الآن", callback_data=Callback.CONFIRM_JOIN.value)]
        ]
        await query.edit_message_text(Messages.JOIN_PROMPT, reply_markup=InlineKeyboardMarkup(keyboard))

async def handle_button_press_my_referrals(query: CallbackQuery) -> None:
    await query.edit_message_text(Messages.LOADING)
    user_info = await get_user_from_db(query.from_user.id)
    text = get_referral_stats_text(user_info)
    await query.edit_message_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=get_main_menu_keyboard(query.from_user.id))
    
async def handle_button_press_top5(query: CallbackQuery, context: ContextTypes.DEFAULT_TYPE) -> None:
    await query.edit_message_text(Messages.LOADING)
    text = await get_top_5_text(query.from_user.id, context)
    try:
        await query.edit_message_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=get_main_menu_keyboard(query.from_user.id), disable_web_page_preview=True)
    except BadRequest as e:
        if "message is not modified" in str(e).lower():
            await query.answer()
        else: raise e

async def handle_button_press_link(query: CallbackQuery, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = query.from_user.id
    text = get_referral_link_text(user_id, context.bot.username)
    await query.edit_message_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=get_main_menu_keyboard(user_id), disable_web_page_preview=True)

# --- معالجات لوحة التحكم (Admin Panel Handlers) ---
async def handle_admin_panel(query: CallbackQuery) -> None:
    await query.edit_message_text(text=Messages.ADMIN_WELCOME, reply_markup=get_admin_panel_keyboard())

async def handle_admin_user_count(query: CallbackQuery) -> None:
    all_users = await get_all_users_from_db()
    total = len(all_users)
    verified = sum(1 for u in all_users if u.get('is_verified'))
    text = f"📈 *إحصائيات مستخدمي البوت:*\n\n▫️ إجمالي المستخدمين: *{total}*\n✅ المستخدمون الموثقون: *{verified}*"
    await query.edit_message_text(text=text, parse_mode=ParseMode.MARKDOWN, reply_markup=get_admin_panel_keyboard())

async def handle_admin_broadcast(query: CallbackQuery, context: ContextTypes.DEFAULT_TYPE) -> None:
    context.user_data['state'] = State.AWAITING_BROADCAST_MESSAGE
    await query.edit_message_text(text="أرسل الآن الرسالة التي تريد إذاعتها لجميع المستخدمين الموثقين.")

async def handle_broadcast_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message: return
    context.user_data['state'] = None
    await update.message.reply_text("⏳ جاري إرسال الإذاعة للمستخدمين الموثقين...")

    all_users = await get_all_users_from_db()
    verified_users = [u for u in all_users if u.get('is_verified')]
    
    sent_count = 0
    failed_count = 0

    for user in verified_users:
        try:
            await context.bot.copy_message(
                chat_id=user['user_id'],
                from_chat_id=update.effective_chat.id,
                message_id=update.message.message_id
            )
            sent_count += 1
        except TelegramError as e:
            logger.error(f"Failed to send broadcast to {user['user_id']}: {e}")
            failed_count += 1
        await asyncio.sleep(0.1)

    await update.message.reply_text(
        f"✅ اكتملت الإذاعة للموثقين!\n\n"
        f"تم الإرسال بنجاح إلى: {sent_count} مستخدم\n"
        f"فشل الإرسال إلى: {failed_count} مستخدم"
    )

async def handle_admin_universal_broadcast(query: CallbackQuery, context: ContextTypes.DEFAULT_TYPE) -> None:
    context.user_data['state'] = State.AWAITING_UNIVERSAL_BROADCAST_MESSAGE
    await query.edit_message_text(text="أرسل الآن الرسالة التي تريد إذاعتها لجميع المستخدمين المسجلين (موثقين وغير موثقين).")

async def handle_universal_broadcast_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message: return
    context.user_data['state'] = None
    await update.message.reply_text("⏳ جاري إرسال الإذاعة الشاملة لجميع المستخدمين...")
    all_users = await get_all_users_from_db()
    
    sent_count = 0
    failed_count = 0

    for user in all_users:
        try:
            await context.bot.copy_message(
                chat_id=user['user_id'],
                from_chat_id=update.effective_chat.id,
                message_id=update.message.message_id
            )
            sent_count += 1
        except TelegramError as e:
            logger.error(f"Failed to send universal broadcast to {user['user_id']}: {e}")
            failed_count += 1
        await asyncio.sleep(0.1)

    await update.message.reply_text(
        f"✅ اكتملت الإذاعة الشاملة!\n\n"
        f"✔️ تم الإرسال بنجاح إلى: {sent_count} مستخدم\n"
        f"❌ فشل الإرسال إلى: {failed_count} مستخدم"
    )

async def handle_admin_reset_all(query: CallbackQuery) -> None:
    await query.edit_message_text(
        text="⚠️ *تأكيد الإجراء* ⚠️\n\nهل أنت متأكد من أنك تريد تصفير *جميع* الإحالات؟ هذا سيحذف سجلات الإحالات و يصفر العدادات.",
        parse_mode=ParseMode.MARKDOWN, 
        reply_markup=get_reset_confirmation_keyboard()
    )

async def handle_admin_reset_confirm(query: CallbackQuery) -> None:
    await query.edit_message_text(text="⏳ جاري تصفير جميع الإحالات...")
    await reset_all_referrals_in_db()
    await query.edit_message_text(text="✅ تم تصفير جميع إحصائيات الإحالات بنجاح.", reply_markup=get_admin_panel_keyboard())

async def handle_admin_format_bot(query: CallbackQuery) -> None:
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("‼️ نعم، قم بحذف كل شيء ‼️", callback_data=Callback.ADMIN_FORMAT_CONFIRM.value)],
        [InlineKeyboardButton("❌ لا، إلغاء الأمر", callback_data=Callback.ADMIN_PANEL.value)]
    ])
    await query.edit_message_text(
        text="⚠️⚠️⚠️ *تحذير خطير جداً* ⚠️⚠️⚠️\n\n"
             "أنت على وشك حذف **جميع بيانات البوت بشكل نهائي**.\n"
             "سيتم حذف:\n"
             "- جميع المستخدمين المسجلين.\n"
             "- جميع الإحالات (الحقيقية والوهمية).\n"
             "- كل شيء حرفياً.\n\n"
             "**هذا الإجراء لا يمكن التراجع عنه.** هل أنت متأكد تماماً؟",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=keyboard
    )

async def handle_admin_format_confirm(query: CallbackQuery) -> None:
    await query.edit_message_text(text="⏳ جاري تنفيذ الفورمات...")
    await format_bot_in_db()
    await query.edit_message_text(text="✅ تم عمل فورمات للبوت بنجاح. لقد عاد إلى حالة المصنع.", reply_markup=get_admin_panel_keyboard())

async def handle_force_reverification(query: CallbackQuery) -> None:
    await query.edit_message_text(text="⏳ جاري إلغاء تحقق جميع المستخدمين...")
    await unverify_all_users_in_db()
    await query.edit_message_text(text="✅ تم إلغاء تحقق جميع المستخدمين بنجاح. سيُطلب منهم إتمام التحقق مرة أخرى عند استخدام الأمر /start.", reply_markup=get_admin_panel_keyboard())

async def handle_booo_menu(query: CallbackQuery) -> None:
    await query.edit_message_text(text="👾 *Booo*\n\nاختر الأداة التي تريد استخدامها:", parse_mode=ParseMode.MARKDOWN, reply_markup=get_booo_menu_keyboard())

async def handle_user_edit_menu(query: CallbackQuery) -> None:
    await query.edit_message_text(text="👤 *تعديل إحصائيات المستخدم*\n\nاختر الإجراء المطلوب:", parse_mode=ParseMode.MARKDOWN, reply_markup=get_user_edit_keyboard())

async def handle_user_edit_action(query: CallbackQuery, context: ContextTypes.DEFAULT_TYPE) -> None:
    context.user_data['state'] = State.AWAITING_EDIT_USER_ID
    context.user_data['action_type'] = query.data
    await query.edit_message_text(text="الرجاء إرسال الـ ID الرقمي للمستخدم لتنفيذ الإجراء.")

async def handle_report_pagination(query: CallbackQuery, context: ContextTypes.DEFAULT_TYPE) -> None:
    try:
        data_parts = query.data.split('_')
        if not (len(data_parts) == 4 and data_parts[0] == 'report' and data_parts[2] == 'page'):
            return
        
        report_type = data_parts[1]
        page = int(data_parts[3])
        
        await query.edit_message_text(Messages.LOADING)
        text, keyboard = await get_paginated_report(page, report_type, context)
        await query.edit_message_text(text=text, parse_mode=ParseMode.MARKDOWN, reply_markup=keyboard)

    except BadRequest as e:
        if "message is not modified" in str(e).lower():
            await query.answer()
        else: raise e
    except (ValueError, IndexError) as e:
        logger.error(f"Error in report pagination: {e}")
        await query.answer("خطأ في البيانات.", show_alert=True)

async def handle_data_migration(query: CallbackQuery, context: ContextTypes.DEFAULT_TYPE):
    await query.edit_message_text("⏳ **بدء عملية إعادة حساب وترحيل البيانات...**\nهذه العملية ستقوم بحساب جميع الإحالات من جديد. لا تقم بتشغيلها مرة أخرى.", parse_mode=ParseMode.MARKDOWN)
    
    try:
        all_users_res = await run_sync_db(lambda: supabase.table('users').select("user_id, is_verified").execute())
        all_mappings_res = await run_sync_db(lambda: supabase.table('referrals').select("referrer_user_id, referred_user_id").execute())

        all_users = all_users_res.data
        all_mappings = all_mappings_res.data

        if not all_users:
            await query.edit_message_text("لا يوجد مستخدمون لترحيل بياناتهم.", reply_markup=get_admin_panel_keyboard())
            return
            
        verified_ids = {u['user_id'] for u in all_users if u.get('is_verified')}
        
        user_counts = {u['user_id']: {'total_real': 0, 'total_fake': 0} for u in all_users}
        
        for mapping in all_mappings:
            referrer_id = mapping.get('referrer_user_id')
            referred_id = mapping.get('referred_user_id')
            
            if referrer_id in user_counts:
                if referred_id in verified_ids:
                    user_counts[referrer_id]['total_real'] += 1
                else:
                    user_counts[referrer_id]['total_fake'] += 1

        users_to_update = [
            {'user_id': uid, 'total_real': counts['total_real'], 'total_fake': counts['total_fake']}
            for uid, counts in user_counts.items()
        ]
            
        if users_to_update:
            chunk_size = 100
            for i in range(0, len(users_to_update), chunk_size):
                chunk = users_to_update[i:i + chunk_size]
                await run_sync_db(lambda: supabase.table('users').upsert(chunk).execute())
                await asyncio.sleep(0.5) 
            
        await query.edit_message_text(f"✅ **اكتملت عملية إعادة الحساب بنجاح!**\nتم تحديث بيانات *{len(users_to_update)}* مستخدم.\n\nالآن البيانات في البوت وقاعدة البيانات متطابقة.",
                                      reply_markup=get_admin_panel_keyboard(), parse_mode=ParseMode.MARKDOWN)
    except Exception as e:
        logger.error(f"Data migration failed: {e}", exc_info=True)
        await query.edit_message_text(f"❌ فشلت عملية الترحيل.\nالرجاء التأكد من أن الأعمدة `total_real` و `total_fake` موجودة في جدول `users`.\n\nالخطأ الفني: `{e}`", 
                                      reply_markup=get_admin_panel_keyboard())

# --- معالج الأزرار الرئيسي (Main Button Handler) ---
async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query or not query.data: return
    
    try:
        await query.answer()
    except BadRequest as e:
        if "Query is too old" in str(e):
            logger.warning(f"Could not answer callback query {query.id}: {e}")
            return
        else:
            raise
    
    action = query.data
    if action == Callback.MAIN_MENU.value: await query.edit_message_text(text=Messages.VERIFIED_WELCOME, reply_markup=get_main_menu_keyboard(query.from_user.id))
    elif action == Callback.MY_REFERRALS.value: await handle_button_press_my_referrals(query)
    elif action == Callback.MY_LINK.value: await handle_button_press_link(query, context)
    elif action == Callback.TOP_5.value: await handle_button_press_top5(query, context)
    elif action == Callback.CONFIRM_JOIN.value: await handle_confirm_join(query, context)
    elif action == Callback.ADMIN_PANEL.value: await handle_admin_panel(query)
    elif action == Callback.ADMIN_USER_COUNT.value: await handle_admin_user_count(query)
    elif action == Callback.ADMIN_BOOO_MENU.value: await handle_booo_menu(query)
    elif action == Callback.ADMIN_USER_EDIT_MENU.value: await handle_user_edit_menu(query)
    elif action == Callback.ADMIN_RESET_ALL.value: await handle_admin_reset_all(query)
    elif action == Callback.ADMIN_RESET_CONFIRM.value: await handle_admin_reset_confirm(query)
    elif action == Callback.DATA_MIGRATION.value: await handle_data_migration(query, context)
    elif action == Callback.REQUEST_PHONE_CONTACT.value: await request_phone_handler(query, context)
    elif action == Callback.ADMIN_BROADCAST.value: await handle_admin_broadcast(query, context)
    elif action == Callback.ADMIN_UNIVERSAL_BROADCAST.value: await handle_admin_universal_broadcast(query, context)
    elif action == Callback.ADMIN_FORMAT_BOT.value: await handle_admin_format_bot(query)
    elif action == Callback.ADMIN_FORMAT_CONFIRM.value: await handle_admin_format_confirm(query)
    elif action == Callback.ADMIN_FORCE_REVERIFICATION.value: await handle_force_reverification(query)
    elif action in [c.value for c in [Callback.USER_ADD_REAL, Callback.USER_REMOVE_REAL, Callback.USER_ADD_FAKE, Callback.USER_REMOVE_FAKE]]: await handle_user_edit_action(query, context)
    elif action.startswith(Callback.REPORT_PAGE.value): await handle_report_pagination(query, context)

# --- معالج رسائل المالك (Admin Message Handler) ---
async def handle_admin_messages(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    state = context.user_data.get('state')
    if not state or not update.message or not update.message.text: return
    text = update.message.text

    if state == State.AWAITING_BROADCAST_MESSAGE:
        await handle_broadcast_message(update, context)
        return

    if state == State.AWAITING_UNIVERSAL_BROADCAST_MESSAGE:
        await handle_universal_broadcast_message(update, context)
        return

    if state == State.AWAITING_EDIT_USER_ID:
        try:
            target_user_id = int(text)
            user_to_fix = await get_user_from_db(target_user_id)
            if not user_to_fix:
                await update.message.reply_text("لم يتم العثور على مستخدم بهذا الـ ID.", reply_markup=get_admin_panel_keyboard())
                context.user_data.clear()
                return

            context.user_data['state'] = State.AWAITING_EDIT_AMOUNT
            context.user_data['target_id'] = target_user_id
            
            action_map = {
                Callback.USER_ADD_REAL.value: "زيادة إحالات حقيقية",
                Callback.USER_REMOVE_REAL.value: "خصم إحالات حقيقية",
                Callback.USER_ADD_FAKE.value: "زيادة إحالات وهمية",
                Callback.USER_REMOVE_FAKE.value: "خصم إحالات وهمية",
            }
            action_type = context.user_data.get('action_type')
            mention = await get_user_mention(target_user_id, context)
            prompt = (f"المستخدم: {mention}\n"
                      f"الإجراء: *{action_map.get(action_type, 'غير معروف')}*\n\n"
                      "الرجاء إرسال العدد الذي تريد تطبيقه.")
            await update.message.reply_text(prompt, parse_mode=ParseMode.MARKDOWN)
        except (ValueError, TypeError):
            await update.message.reply_text(Messages.INVALID_INPUT, reply_markup=get_admin_panel_keyboard())
    
    elif state == State.AWAITING_EDIT_AMOUNT:
        target_user_id = context.user_data.get('target_id')
        action_type = context.user_data.get('action_type')
        
        if not target_user_id or not action_type:
            context.user_data.clear()
            await update.message.reply_text("حدث خطأ في السياق. الرجاء البدء من جديد.", reply_markup=get_admin_panel_keyboard())
            return
            
        try:
            amount = int(text)
            if amount <= 0:
                await update.message.reply_text("الرجاء إرسال عدد صحيح أكبر من صفر.")
                return
            
            real_delta = 0
            fake_delta = 0
            
            if action_type == Callback.USER_ADD_REAL.value: real_delta = amount
            elif action_type == Callback.USER_REMOVE_REAL.value: real_delta = -amount
            elif action_type == Callback.USER_ADD_FAKE.value: fake_delta = amount
            elif action_type == Callback.USER_REMOVE_FAKE.value: fake_delta = -amount

            updated_user = await modify_referral_count(target_user_id, real_delta, fake_delta)

            if updated_user:
                mention = await get_user_mention(target_user_id, context)
                final_text = (f"✅ تم التعديل بنجاح.\n\n"
                              f"المستخدم: {mention}\n"
                              f"الرصيد الجديد:\n"
                              f"✅ *{updated_user.get('total_real', 0)}* إحالة حقيقية\n"
                              f"⏳ *{updated_user.get('total_fake', 0)}* إحالة وهمية")
                await update.message.reply_text(final_text, parse_mode=ParseMode.MARKDOWN, reply_markup=get_admin_panel_keyboard())
            else:
                await update.message.reply_text("فشل تحديث المستخدم.", reply_markup=get_admin_panel_keyboard())

            context.user_data.clear()

        except (ValueError, TypeError):
            context.user_data['state'] = State.AWAITING_EDIT_AMOUNT
            await update.message.reply_text(Messages.INVALID_INPUT + "\nيرجى إدخال رقم صحيح فقط.")
            
    else:
        context.user_data.clear()

# --- معالج مغادرة الأعضاء (Chat Member Handler) ---
async def handle_chat_member_updates(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    result = update.chat_member
    if not result: return
    
    user = result.new_chat_member.user
    was_member = result.old_chat_member.status in {'member', 'administrator', 'creator'}
    is_no_longer_member = result.new_chat_member.status in {'left', 'kicked'}
    
    if was_member and is_no_longer_member:
        logger.info(f"User {user.full_name} ({user.id}) left/was kicked from chat {result.chat.title}.")
        await upsert_user_in_db({'user_id': user.id, 'is_verified': False})
        
        referrer_id = await get_referrer(user.id)
        if referrer_id:
            try:
                updated_referrer = await modify_referral_count(user_id=referrer_id, real_delta=-1, fake_delta=1)
                if updated_referrer:
                    new_real_count = updated_referrer.get('total_real', 'N/A')
                    mention = await get_user_mention(user.id, context)
                    await context.bot.send_message(
                        chat_id=referrer_id,
                        text=f"⚠️ تنبيه! أحد المستخدمين الذين دعوتهم ({mention}) غادر.\n\n"
                             f"تم تحديث رصيدك. رصيدك الحالي هو: *{new_real_count}* إحالة حقيقية.",
                        parse_mode=ParseMode.MARKDOWN
                    )
            except TelegramError as e:
                logger.warning(f"Could not send leave notification to referrer {referrer_id}: {e}")

# --- الدالة الرئيسية (Main Function) ---
def main() -> None:
    """بدء تشغيل البوت."""
    application = Application.builder().token(Config.BOT_TOKEN).job_queue(JobQueue()).build()

    # Handlers
    application.add_handler(ChatMemberHandler(handle_chat_member_updates, ChatMemberHandler.CHAT_MEMBER), group=0)
    
    application.add_handler(CommandHandler("start", start_command), group=1)
    application.add_handler(CommandHandler("invites", my_referrals_command), group=1)
    application.add_handler(CommandHandler("link", link_command), group=1)
    application.add_handler(CommandHandler("top", top_command), group=1)
    application.add_handler(CallbackQueryHandler(button_handler), group=1)

    private_chat_filter = filters.ChatType.PRIVATE
    application.add_handler(MessageHandler(filters.CONTACT & private_chat_filter, handle_contact), group=2)
    application.add_handler(MessageHandler(filters.StatusUpdate.WEB_APP_DATA, web_app_data_handler), group=2)
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND & private_chat_filter, handle_verification_text), group=2)
    
    logger.info("Bot is starting...")
    application.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()
