import asyncio
import logging
import json
import re
import time
import math
import html
from enum import Enum, auto
from typing import Any, Callable, Dict, List, Optional, Tuple

from telegram import (
    Update,
    User,
    Chat,
    Message,
    CallbackQuery,
    ChatMemberUpdated,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
    WebAppInfo,
    ChatMember
)
from telegram.constants import ParseMode
from telegram.error import BadRequest, TelegramError
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    ChatMemberHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

from supabase import Client, create_client

# --- Configuration ---
class Config:
    BOT_TOKEN = "7950170561:AAH5OtiK38BBhAnVofqxnLWRYbaZaIaKY4s"
    SUPABASE_URL = "https://jofxsqsgarvzolgphqjg.supabase.co"
    SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImpvZnhzcXNnYXJ2em9sZ3BocWpnIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc0OTU5NTI4NiwiZXhwIjoyMDY1MTcxMjg2fQ.egB9qticc7ABgo6vmpsrPi3cOHooQmL5uQOKI4Jytqg"
    WEB_APP_URL = "https://heartfelt-biscuit-5489cd.netlify.app"
    CHANNEL_ID = -1002686156311
    CHANNEL_URL = "https://t.me/Ry_Hub"
    BOT_OWNER_IDS = {596472053, 7164133014, 1971453570}
    ALLOWED_COUNTRY_CODES = {
        "213", "973", "269", "253", "20", "964", "962", "965", "961",
        "218", "222", "212", "968", "970", "974", "966", "252", "249",
        "963", "216", "971", "967"
    }
    USERS_PER_PAGE = 15
    MENTION_CACHE_TTL_SECONDS = 300

# --- Bot States ---
class State(Enum):
    AWAITING_EDIT_USER_ID = auto()
    AWAITING_EDIT_AMOUNT = auto()
    AWAITING_BROADCAST_MESSAGE = auto()
    AWAITING_UNIVERSAL_BROADCAST_MESSAGE = auto()
    AWAITING_INSPECT_USER_ID = auto()

# --- Callback Data Definitions ---
class Callback(str, Enum):
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
    REPORT_PAGE = "report"
    DATA_MIGRATION = "data_migration"
    ADMIN_BROADCAST = "admin_broadcast"
    ADMIN_RESET_ALL = "admin_reset_all"
    ADMIN_RESET_CONFIRM = "admin_reset_confirm"
    ADMIN_FORMAT_BOT = "admin_format_bot"
    ADMIN_FORMAT_CONFIRM = "admin_format_confirm"
    ADMIN_FORCE_REVERIFICATION = "admin_force_reverification"
    ADMIN_UNIVERSAL_BROADCAST = "admin_universal_broadcast"
    ADMIN_INSPECT_REFERRALS = "admin_inspect_referrals"
    INSPECT_LOG = "inspect_log"

# --- Bot Messages (User-facing text in Arabic) ---
class Messages:
    VERIFIED_WELCOME = "أهلاً بك مجدداً! ✅\n\nاستخدم الأزرار أو الأوامر للتفاعل مع البوت."
    START_WELCOME = "أهلاً بك في البوت! 👋\n\nللبدء، نحتاج للتحقق من جهازك. الرجاء الضغط على الزر أدناه."
    WEB_VERIFY_PROMPT = "للتحقق من أنك لا تستخدم نفس الجهاز عدة مرات، الرجاء الضغط على الزر أدناه."
    PHONE_PROMPT = "تم التحقق من جهازك بنجاح! الآن، من فضلك شارك رقم هاتفك لإكمال العملية."
    PHONE_SUCCESS = "تم استلام الرقم بنجاح."
    PHONE_INVALID = "الرجاء مشاركة جهة الاتصال الخاصة بك فقط."
    COUNTRY_NOT_ALLOWED = "عذراً، هذا البوت مخصص فقط للمستخدمين من الدول العربية. رقمك غير مدعوم."
    JOIN_PROMPT = "ممتاز! الخطوة الأخيرة هي الانضمام إلى قناتنا. انضم ثم اضغط على الزر أدناه."
    JOIN_SUCCESS = "تهانينا! لقد تم التحقق منك بنجاح."
    JOIN_FAIL = "❌ لم تنضم بعد. الرجاء الانضمام إلى القناة ثم حاول مرة أخرى."
    GENERIC_ERROR = "حدث خطأ ما. يرجى المحاولة مرة أخرى لاحقاً."
    LOADING = "⏳ جاري التحميل..."
    ADMIN_WELCOME = "👑 أهلاً بك في لوحة تحكم المالك."
    INVALID_INPUT = "إدخال غير صالح. الرجاء المحاولة مرة أخرى."
    REFERRAL_ABUSE_DEVICE_USED = "تم اكتشاف إساءة استخدام لنظام الإحالة. تم حظر هذه الإحالة لأن هذا الجهاز تم استخدامه سابقاً للتسجيل."
    REFERRAL_EXISTING_MEMBER = "💡 تنبيه: المستخدم الذي دعوته عضو بالفعل في القناة. سيتم احتساب هذه الإحالة كإحالة وهمية."
    REFERRAL_SUCCESS = "🎉 تهانينا! لقد انضم مستخدم جديد ({mention}) عن طريق رابطك.\n\n" \
                       "رصيدك المحدث هو: *{new_real_count}* إحالة حقيقية."
    LEAVE_NOTIFICATION = "⚠️ تنبيه! أحد المستخدمين الذين دعوتهم ({mention}) غادر القناة.\n\n" \
                         "تم تحديث رصيدك. رصيدك الحالي هو: *{new_real_count}* إحالة حقيقية."
    NO_REFERRALS_YET = "لم تقم بدعوة أي مستخدم بعد."
    USER_HAS_NO_REFERRALS = "هذا المستخدم لم يقم بدعوة أي شخص بعد."
    USER_NOT_FOUND = "لم يتم العثور على مستخدم بهذا الـ ID."

# --- Logging Setup ---
logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO)
logging.getLogger("httpx").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

# --- Supabase Database Connection ---
try:
    supabase: Client = create_client(Config.SUPABASE_URL, Config.SUPABASE_KEY)
    logger.info("Successfully connected to Supabase.")
except Exception as e:
    logger.critical(f"FATAL: Failed to connect to Supabase. Error: {e}")
    exit(1)

# --- Helper Functions ---
def clean_name_for_html(name: str) -> str:
    """Escapes characters for HTML parsing."""
    if not name: return ""
    return html.escape(name)

async def get_user_mention(user_id: int, context: ContextTypes.DEFAULT_TYPE) -> str:
    """Gets an HTML-safe user mention, using a cache to reduce API calls."""
    cache = context.bot_data.setdefault('mention_cache', {})
    current_time = time.time()
    if user_id in cache and (current_time - cache[user_id].get('timestamp', 0) < Config.MENTION_CACHE_TTL_SECONDS):
        return cache[user_id]['mention']
    try:
        chat = await context.bot.get_chat(user_id)
        full_name = clean_name_for_html(chat.full_name or f"User {user_id}")
        mention = f'<a href="tg://user?id={user_id}">{full_name}</a>'
    except (TelegramError, BadRequest):
        db_user_info = await get_user_from_db(user_id)
        full_name = "Unknown User"
        if db_user_info:
            full_name = clean_name_for_html(db_user_info.get("full_name", f"User {user_id}"))
        mention = f'<a href="tg://user?id={user_id}">{full_name}</a>'
    cache[user_id] = {'mention': mention, 'timestamp': current_time}
    return mention

# --- Database Functions ---
async def run_sync_db(func: Callable[[], Any]) -> Any:
    """Runs a synchronous database function in a separate thread."""
    return await asyncio.to_thread(func)

async def get_user_from_db(user_id: int) -> Optional[Dict[str, Any]]:
    """Fetches a single user's data from the database."""
    try:
        res = await run_sync_db(lambda: supabase.table('users').select("*").eq('user_id', user_id).single().execute())
        return res.data
    except Exception:
        return None

async def upsert_user_in_db(user_data: Dict[str, Any]) -> None:
    """Inserts or updates a user's data in the database."""
    try:
        await run_sync_db(lambda: supabase.table('users').upsert(user_data).execute())
    except Exception as e:
        logger.error(f"DB_ERROR: Upserting user {user_data.get('user_id')}: {e}")

async def get_all_users_from_db() -> List[Dict[str, Any]]:
    """Fetches all users from the database."""
    try:
        res = await run_sync_db(lambda: supabase.table('users').select("*").execute())
        return res.data or []
    except Exception as e:
        logger.error(f"DB_ERROR (get_all_users_from_db): {e}")
        return []

async def get_referrer(referred_id: int) -> Optional[int]:
    """Finds the referrer for a given referred user."""
    try:
        res = await run_sync_db(lambda: supabase.table('referrals').select('referrer_user_id').eq('referred_user_id', referred_id).execute())
        return res.data[0].get('referrer_user_id') if res.data else None
    except Exception:
        return None

async def add_referral_mapping_in_db(referred_id: int, referrer_id: Optional[int], device_id: str) -> None:
    """Adds a referral relationship and device ID to the database."""
    try:
        data = {'referred_user_id': referred_id, 'referrer_user_id': referrer_id, 'device_id': device_id}
        await run_sync_db(lambda: supabase.table('referrals').upsert(data, on_conflict='referred_user_id').execute())
    except Exception as e:
        logger.error(f"DB_ERROR: Adding referral map for {referred_id}: {e}")

async def get_my_referrals_details(user_id: int) -> Tuple[List[int], List[int]]:
    """Gets lists of real and fake referral IDs for a user."""
    try:
        all_refs_res = await run_sync_db(lambda: supabase.table('referrals').select('referred_user_id').eq('referrer_user_id', user_id).execute())
        if not all_refs_res.data: return [], []
        referred_ids = [ref['referred_user_id'] for ref in all_refs_res.data]
        verified_status_res = await run_sync_db(lambda: supabase.table('users').select('user_id, is_verified').in_('user_id', referred_ids).execute())
        verified_map = {u['user_id']: u.get('is_verified', False) for u in verified_status_res.data}
        real_referrals = sorted([uid for uid in referred_ids if verified_map.get(uid, False)])
        fake_referrals = sorted([uid for uid in referred_ids if not verified_map.get(uid, False)])
        return real_referrals, fake_referrals
    except Exception as e:
        logger.error(f"Error fetching referral details for user {user_id}: {e}")
        return [], []

async def reset_all_referrals_in_db() -> None:
    """Resets all referral stats for all users to zero."""
    try:
        await run_sync_db(lambda: supabase.table('referrals').delete().gt('referred_user_id', 0).execute())
        await run_sync_db(lambda: supabase.table('users').update({"total_real": 0, "total_fake": 0}).gt('user_id', 0).execute())
        logger.info("All referrals have been reset.")
    except Exception as e:
        logger.error(f"DB_ERROR: Resetting all referrals: {e}")
        raise

async def format_bot_in_db() -> None:
    """Deletes all user and referral data from the database."""
    try:
        await run_sync_db(lambda: supabase.table('referrals').delete().gt('referred_user_id', 0).execute())
        await run_sync_db(lambda: supabase.table('users').delete().gt('user_id', 0).execute())
        logger.info("BOT HAS BEEN FORMATTED.")
    except Exception as e:
        logger.error(f"DB_ERROR: Formatting bot: {e}")
        raise

async def unverify_all_users_in_db() -> None:
    """Sets the is_verified flag to False for all users."""
    try:
        await run_sync_db(lambda: supabase.table('users').update({"is_verified": False}).gt('user_id', 0).execute())
        logger.info("All users have been un-verified.")
    except Exception as e:
        logger.error(f"DB_ERROR: Un-verifying all users: {e}")

# --- Display Functions ---
def get_referral_stats_text(user_info: Optional[Dict[str, Any]]) -> str:
    if not user_info: return Messages.NO_REFERRALS_YET
    total_real = int(user_info.get("total_real", 0) or 0)
    total_fake = int(user_info.get("total_fake", 0) or 0)
    return f"📊 <b>إحصائيات إحالاتك:</b>\n\n✅ الإحالات الحقيقية: <code>{total_real}</code>\n⏳ الإحالات الوهمية: <code>{total_fake}</code>"

def get_referral_link_text(user_id: int, bot_username: str) -> str:
    return f"🔗 رابط الإحالة الخاص بك:\n<code>https://t.me/{bot_username}?start={user_id}</code>"

async def get_top_5_text(user_id: int, context: ContextTypes.DEFAULT_TYPE) -> str:
    msg = "🏆 <b>أفضل 5 متسابقين لدينا:</b>\n\n"
    try:
        all_users = await get_all_users_from_db()
        if not all_users:
            return msg + "لم يصل أحد إلى القائمة بعد.\n\n---\n<b>ترتيبك الشخصي:</b>\nلا يمكن عرض ترتيبك حالياً."

        full_sorted_list = sorted([u for u in all_users if u.get('total_real', 0) > 0], key=lambda u: u.get('total_real', 0), reverse=True)
        top_5_users = full_sorted_list[:5]

        if not top_5_users:
            msg += "لم يصل أحد إلى القائمة بعد.\n"
        else:
            mentions = await asyncio.gather(*[get_user_mention(u['user_id'], context) for u in top_5_users])
            for i, u_info in enumerate(top_5_users):
                mention = mentions[i]
                count = u_info.get('total_real', 0)
                msg += f"{i+1}. {mention} - <b>{count}</b> إحالة\n"

        msg += "\n---\n<b>ترتيبك الشخصي:</b>\n"
        user_index = next((i for i, u in enumerate(full_sorted_list) if u.get('user_id') == user_id), -1)
        my_referrals = 0
        rank_str = "غير مصنف"
        if user_index != -1:
            my_info = full_sorted_list[user_index]
            rank_str = f"#{user_index + 1}"
            my_referrals = my_info.get('total_real', 0)
        else:
            my_info = await get_user_from_db(user_id)
            if my_info: my_referrals = my_info.get('total_real', 0)

        msg += f"🎖️ ترتيبك: <b>{rank_str}</b>\n✅ رصيدك: <b>{my_referrals}</b> إحالة حقيقية."
    except Exception as e:
        logger.error(f"Error getting top 5 text for {user_id}: {e}")
        msg = Messages.GENERIC_ERROR
    return msg

# --- Core Logic & Keyboard Functions ---
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

async def is_user_in_channel(user_id: int, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """Checks if a user is a member of the channel."""
    try:
        member = await context.bot.get_chat_member(chat_id=Config.CHANNEL_ID, user_id=user_id)
        # FIX: Corrected CREATOR to OWNER
        return member.status in {ChatMember.MEMBER, ChatMember.ADMINISTRATOR, ChatMember.OWNER}
    except BadRequest as e:
        if "user not found" in str(e).lower():
            return False
        else:
            logger.error(f"Telegram BadRequest checking membership for {user_id} in channel {Config.CHANNEL_ID}: {e}. The bot might lack admin rights in the channel.")
            raise
    except TelegramError as e:
        logger.warning(f"TelegramError checking membership for {user_id}: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error checking membership for {user_id}: {e}")
        raise
    return False

# --- Keyboard Functions ---
def get_main_menu_keyboard(user_id: int) -> InlineKeyboardMarkup:
    keyboard = [
        [InlineKeyboardButton("إحصائياتي 📊", callback_data=Callback.MY_REFERRALS)],
        [InlineKeyboardButton("رابطي 🔗", callback_data=Callback.MY_LINK)],
        [InlineKeyboardButton("🏆 أفضل 5 متسابقين", callback_data=Callback.TOP_5)],
    ]
    if user_id in Config.BOT_OWNER_IDS:
        keyboard.append([InlineKeyboardButton("👑 لوحة تحكم المالك", callback_data=Callback.ADMIN_PANEL)])
    return InlineKeyboardMarkup(keyboard)

def get_admin_panel_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📊 تقرير حقيقي", callback_data=f"{Callback.REPORT_PAGE}_real_page_1"), InlineKeyboardButton("⏳ تقرير وهمي", callback_data=f"{Callback.REPORT_PAGE}_fake_page_1")],
        [InlineKeyboardButton("🔍 فحص إحالات مستخدم", callback_data=Callback.ADMIN_INSPECT_REFERRALS)],
        [InlineKeyboardButton("👥 عدد المستخدمين", callback_data=Callback.ADMIN_USER_COUNT)],
        [InlineKeyboardButton("Booo 👾 (تعديل يدوي)", callback_data=Callback.ADMIN_BOOO_MENU)],
        [InlineKeyboardButton("📢 إذاعة للموثقين", callback_data=Callback.ADMIN_BROADCAST)],
        [InlineKeyboardButton("📢 إذاعة للكل", callback_data=Callback.ADMIN_UNIVERSAL_BROADCAST)],
        [InlineKeyboardButton("🔄 فرض إعادة التحقق", callback_data=Callback.ADMIN_FORCE_REVERIFICATION)],
        [InlineKeyboardButton("⚠️ تصفير كل الإحالات", callback_data=Callback.ADMIN_RESET_ALL)],
        [InlineKeyboardButton("⚙️ ترحيل وإعادة حساب البيانات", callback_data=Callback.DATA_MIGRATION)],
        [InlineKeyboardButton("💀 فورمات البوت (حذف كل شيء)", callback_data=Callback.ADMIN_FORMAT_BOT)],
        [InlineKeyboardButton("⬅️ العودة للقائمة الرئيسية", callback_data=Callback.MAIN_MENU)],
    ])

def get_booo_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("✍️ تعديل إحصائيات مستخدم", callback_data=Callback.ADMIN_USER_EDIT_MENU)], [InlineKeyboardButton("🔙 العودة للوحة التحكم", callback_data=Callback.ADMIN_PANEL)]])

def get_user_edit_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("➕ إضافة حقيقي", callback_data=Callback.USER_ADD_REAL), InlineKeyboardButton("➖ خصم حقيقي", callback_data=Callback.USER_REMOVE_REAL)],
        [InlineKeyboardButton("➕ إضافة وهمي", callback_data=Callback.USER_ADD_FAKE), InlineKeyboardButton("➖ خصم وهمي", callback_data=Callback.USER_REMOVE_FAKE)],
        [InlineKeyboardButton("🔙 العودة", callback_data=Callback.ADMIN_BOOO_MENU)]
    ])

def get_reset_confirmation_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("✅ نعم، قم بالتصفير", callback_data=Callback.ADMIN_RESET_CONFIRM)], [InlineKeyboardButton("❌ لا، الغِ الأمر", callback_data=Callback.ADMIN_PANEL)]])

def get_format_confirmation_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("‼️ نعم، قم بحذف كل شيء ‼️", callback_data=Callback.ADMIN_FORMAT_CONFIRM)], [InlineKeyboardButton("❌ لا، إلغاء الأمر", callback_data=Callback.ADMIN_PANEL)]])


# --- Handlers ---
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.info(f"Received /start command from user {update.effective_user.id}")
    if not update.message or not update.effective_user or update.effective_chat.type != Chat.PRIVATE: return
    user = update.effective_user
    db_user = await get_user_from_db(user.id)
    if db_user and db_user.get("is_verified"):
        logger.info(f"User {user.id} is already verified. Sending main menu.")
        await update.message.reply_text(Messages.VERIFIED_WELCOME, reply_markup=get_main_menu_keyboard(user.id))
        return
    args = context.args
    if args:
        try:
            referrer_id = int(args[0])
            if referrer_id != user.id:
                context.user_data['referrer_id'] = referrer_id
                logger.info(f"User {user.id} was referred by {referrer_id}")
        except (ValueError, IndexError): pass
    await upsert_user_in_db({'user_id': user.id, 'full_name': user.full_name, 'username': user.username})
    try:
        logger.info(f"Checking channel membership for user {user.id}")
        context.user_data['was_already_member'] = await is_user_in_channel(user.id, context)
        logger.info(f"User {user.id} was_already_member: {context.user_data['was_already_member']}")
    except (TelegramError, BadRequest) as e:
        logger.error(f"Could not check channel membership for {user.id} on start: {e}")
        context.user_data['was_already_member'] = False
    await update.message.reply_text(Messages.START_WELCOME, reply_markup=ReplyKeyboardRemove())
    await ask_web_verification(update.message)

async def my_referrals_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_user or not update.message: return
    logger.info(f"Received /invites command from user {update.effective_user.id}")
    msg = await update.message.reply_text(Messages.LOADING)
    user_info = await get_user_from_db(update.effective_user.id)
    text = get_referral_stats_text(user_info)
    await msg.edit_text(text, parse_mode=ParseMode.HTML, reply_markup=get_main_menu_keyboard(update.effective_user.id))

async def link_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_user or not update.message or not context.bot.username: return
    logger.info(f"Received /link command from user {update.effective_user.id}")
    user_id = update.effective_user.id
    text = get_referral_link_text(user_id, context.bot.username)
    await update.message.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=get_main_menu_keyboard(user_id), disable_web_page_preview=True)

async def top_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_user or not update.message: return
    logger.info(f"Received /top command from user {update.effective_user.id}")
    user_id = update.effective_user.id
    msg = await update.message.reply_text(Messages.LOADING)
    text = await get_top_5_text(user_id, context)
    await msg.edit_text(text, parse_mode=ParseMode.HTML, reply_markup=get_main_menu_keyboard(user_id), disable_web_page_preview=True)

async def ask_web_verification(message: Message) -> None:
    logger.info(f"Asking for web verification from user {message.from_user.id}")
    keyboard = ReplyKeyboardMarkup.from_button(KeyboardButton(text="🔒 اضغط هنا للتحقق من جهازك", web_app=WebAppInfo(url=Config.WEB_APP_URL)), resize_keyboard=True)
    await message.reply_text(Messages.WEB_VERIFY_PROMPT, reply_markup=keyboard)

async def web_app_data_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_user or not update.message or not update.message.web_app_data: return
    user_id = update.effective_user.id
    logger.info(f"Received web_app_data from user {user_id}")
    try:
        data = json.loads(update.message.web_app_data.data)
        device_id = data.get("visitorId")
    except (json.JSONDecodeError, AttributeError):
        logger.error(f"Failed to parse web_app_data from user {user_id}")
        await update.message.reply_text(Messages.GENERIC_ERROR + " (بيانات تحقق تالفة)", reply_markup=ReplyKeyboardRemove())
        return
    if not device_id:
        logger.error(f"No visitorId in web_app_data from user {user_id}")
        await update.message.reply_text(Messages.GENERIC_ERROR + " (لم يتم استلام بصمة الجهاز)", reply_markup=ReplyKeyboardRemove())
        return

    logger.info(f"Received device_id {device_id} for user {user_id}. Verifying uniqueness...")
    try:
        device_usage_res = await run_sync_db(lambda: supabase.table('referrals').select('referred_user_id').eq('device_id', device_id).neq('referred_user_id', user_id).limit(1).execute())
        if device_usage_res.data:
            original_user_id = device_usage_res.data[0].get('referred_user_id')
            logger.warning(f"Abuse: User {user_id} trying to use device {device_id} already registered to {original_user_id}.")
            await update.message.reply_text(Messages.REFERRAL_ABUSE_DEVICE_USED, reply_markup=ReplyKeyboardRemove())
            return

        referrer_id = context.user_data.get('referrer_id')
        existing_ref_res = await run_sync_db(lambda: supabase.table('referrals').select('referred_user_id').eq('referred_user_id', user_id).execute())

        if not existing_ref_res.data and referrer_id:
            await modify_referral_count(user_id=referrer_id, fake_delta=1)
            logger.info(f"New user {user_id} under referrer {referrer_id}. Added +1 fake referral.")

        await add_referral_mapping_in_db(user_id, referrer_id, device_id)
        # MODIFICATION: Skip phone verification and go straight to join prompt
        logger.info(f"User {user_id} passed web verification. Skipping phone verification and asking to join channel.")
        await update.message.reply_text(Messages.PHONE_SUCCESS, reply_markup=ReplyKeyboardRemove()) # Re-using this message
        keyboard = [[InlineKeyboardButton("1. الانضمام للقناة", url=Config.CHANNEL_URL)], [InlineKeyboardButton("✅ لقد انضممت، تحقق الآن", callback_data=Callback.CONFIRM_JOIN)]]
        await update.message.reply_text(Messages.JOIN_PROMPT, reply_markup=InlineKeyboardMarkup(keyboard))
    except Exception as e:
        logger.error(f"Error in web_app_data_handler for user {user_id}: {e}", exc_info=True)
        await update.message.reply_text(Messages.GENERIC_ERROR, reply_markup=ReplyKeyboardRemove())

async def handle_chat_member_updates(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    result = update.chat_member
    if not result: return

    user = result.new_chat_member.user
    chat_id = result.chat.id

    if chat_id != Config.CHANNEL_ID:
        return

    # FIX: Corrected CREATOR to OWNER
    was_member = result.old_chat_member.status in {ChatMember.MEMBER, ChatMember.ADMINISTRATOR, ChatMember.OWNER}
    is_member = result.new_chat_member.status in {ChatMember.MEMBER, ChatMember.ADMINISTRATOR, ChatMember.OWNER}

    logger.info(f"Chat member update for user {user.id} in chat {chat_id}. Was member: {was_member}, Is member: {is_member}")

    if not was_member and is_member:
        logger.info(f"User {user.id} joined channel {chat_id}.")
        # This logic is now handled in `handle_confirm_join` to prevent race conditions
        pass

    elif was_member and not is_member:
        logger.info(f"User {user.id} left/was kicked from chat {chat_id}.")
        db_user = await get_user_from_db(user.id)
        if db_user and db_user.get('is_verified'):
            await upsert_user_in_db({'user_id': user.id, 'is_verified': False})
            referrer_id = await get_referrer(user.id)
            if referrer_id:
                try:
                    updated_referrer = await modify_referral_count(user_id=referrer_id, real_delta=-1, fake_delta=1)
                    if updated_referrer:
                        # The user might have blocked the bot, so wrap in try/except
                        try:
                            mention = await get_user_mention(user.id, context)
                            new_real_count = updated_referrer.get('total_real', 0)
                            text = f"⚠️ تنبيه! أحد المستخدمين الذين دعوتهم ({mention}) غادر القناة.\n\n" \
                                   f"تم تحديث رصيدك. رصيدك الحالي هو: <b>{new_real_count}</b> إحالة حقيقية."
                            await context.bot.send_message(
                                chat_id=referrer_id,
                                text=text,
                                parse_mode=ParseMode.HTML
                            )
                        except TelegramError as e:
                            logger.warning(f"Could not send leave notification to referrer {referrer_id}: {e}")
                except Exception as e:
                    logger.error(f"Error updating referrer counts after user {user.id} left: {e}")


async def handle_admin_messages(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_user or update.effective_user.id not in Config.BOT_OWNER_IDS or not update.message: return
    user_id = update.effective_user.id
    state = context.user_data.get('state')
    text = update.message.text
    logger.info(f"Admin {user_id} sent message in state {state}: {text}")
    if not state or not text: return

    if state == State.AWAITING_BROADCAST_MESSAGE:
        await handle_broadcast_message(update, context)
    elif state == State.AWAITING_UNIVERSAL_BROADCAST_MESSAGE:
        await handle_universal_broadcast_message(update, context)
    elif state == State.AWAITING_INSPECT_USER_ID:
        try:
            target_user_id = int(text)
            target_user_info = await get_user_from_db(target_user_id)
            if not target_user_info:
                await update.message.reply_text(Messages.USER_NOT_FOUND, reply_markup=get_admin_panel_keyboard())
            else:
                # Start with the 'real' report by default
                await display_target_referrals_log(update.message, None, context, target_user_id, 'real', 1)
        except (ValueError, TypeError):
            await update.message.reply_text(Messages.INVALID_INPUT, reply_markup=get_admin_panel_keyboard())
        finally:
            context.user_data.clear()
    elif state == State.AWAITING_EDIT_USER_ID:
        try:
            target_user_id = int(text)
            if not await get_user_from_db(target_user_id):
                await update.message.reply_text(Messages.USER_NOT_FOUND, reply_markup=get_admin_panel_keyboard())
                context.user_data.clear()
                return
            context.user_data['state'] = State.AWAITING_EDIT_AMOUNT
            context.user_data['target_id'] = target_user_id
            action_map = {
                Callback.USER_ADD_REAL: "زيادة إحالات حقيقية", Callback.USER_REMOVE_REAL: "خصم إحالات حقيقية",
                Callback.USER_ADD_FAKE: "زيادة إحالات وهمية", Callback.USER_REMOVE_FAKE: "خصم إحالات وهمية"
            }
            action_text = action_map.get(context.user_data.get('action_type'), 'N/A')
            mention = await get_user_mention(target_user_id, context)
            prompt = (f"المستخدم: {mention}\n"
                      f"الإجراء: <b>{action_text}</b>\n\n"
                      "الرجاء إرسال العدد الذي تريد تطبيقه.")
            await update.message.reply_text(prompt, parse_mode=ParseMode.HTML)
        except (ValueError, TypeError):
            await update.message.reply_text(Messages.INVALID_INPUT, reply_markup=get_admin_panel_keyboard())
            context.user_data.clear()
    elif state == State.AWAITING_EDIT_AMOUNT:
        target_user_id = context.user_data.get('target_id')
        action_type = context.user_data.get('action_type')
        if not target_user_id or not action_type:
            context.user_data.clear()
            await update.message.reply_text(Messages.GENERIC_ERROR, reply_markup=get_admin_panel_keyboard())
            return
        try:
            amount = int(text)
            if amount <= 0:
                await update.message.reply_text("الرجاء إدخال عدد صحيح موجب.", reply_markup=get_admin_panel_keyboard())
                return

            real_delta, fake_delta = 0, 0
            if action_type == Callback.USER_ADD_REAL: real_delta = amount
            elif action_type == Callback.USER_REMOVE_REAL: real_delta = -amount
            elif action_type == Callback.USER_ADD_FAKE: fake_delta = amount
            elif action_type == Callback.USER_REMOVE_FAKE: fake_delta = -amount

            updated_user = await modify_referral_count(user_id=target_user_id, real_delta=real_delta, fake_delta=fake_delta)
            if updated_user:
                mention = await get_user_mention(target_user_id, context)
                new_stats = get_referral_stats_text(updated_user)
                await update.message.reply_text(f"✅ تم تحديث المستخدم {mention} بنجاح.\n\n{new_stats}", parse_mode=ParseMode.HTML, reply_markup=get_admin_panel_keyboard())
            else:
                await update.message.reply_text(Messages.USER_NOT_FOUND, reply_markup=get_admin_panel_keyboard())

        except (ValueError, TypeError):
            await update.message.reply_text(Messages.INVALID_INPUT, reply_markup=get_admin_panel_keyboard())
        finally:
            context.user_data.clear()


# --- Callback Helper Functions ---
async def handle_button_press_my_referrals(query: CallbackQuery) -> None:
    if not query.from_user: return
    logger.info(f"Handling '{query.data}' callback from user {query.from_user.id}")
    await query.edit_message_text(Messages.LOADING)
    user_info = await get_user_from_db(query.from_user.id)
    text = get_referral_stats_text(user_info)
    await query.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=get_main_menu_keyboard(query.from_user.id))

async def handle_button_press_top5(query: CallbackQuery, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not query.from_user: return
    logger.info(f"Handling '{query.data}' callback from user {query.from_user.id}")
    try:
        await query.edit_message_text(Messages.LOADING)
        text = await get_top_5_text(query.from_user.id, context)
        await query.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=get_main_menu_keyboard(query.from_user.id), disable_web_page_preview=True)
    except BadRequest as e:
        if "message is not modified" not in str(e).lower():
            logger.warning(f"BadRequest in top5 handler for {query.from_user.id}: {e}")
    except TelegramError as e:
        logger.error(f"TelegramError in top5 handler for {query.from_user.id}: {e}")


async def handle_button_press_link(query: CallbackQuery, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not query.from_user or not context.bot.username: return
    logger.info(f"Handling '{query.data}' callback from user {query.from_user.id}")
    user_id = query.from_user.id
    text = get_referral_link_text(user_id, context.bot.username)
    await query.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=get_main_menu_keyboard(user_id), disable_web_page_preview=True)

async def handle_confirm_join(query: CallbackQuery, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = query.from_user
    logger.info(f"Handling '{query.data}' callback from user {user.id}")
    await query.edit_message_text(Messages.LOADING)
    try:
        if await is_user_in_channel(user.id, context):
            logger.info(f"User {user.id} confirmed channel join.")
            await upsert_user_in_db({'user_id': user.id, 'is_verified': True})

            referrer_id = await get_referrer(user.id)
            was_already_member = context.user_data.get('was_already_member', False)

            if referrer_id and not was_already_member:
                logger.info(f"User {user.id} was a valid new referral for {referrer_id}.")
                updated_referrer = await modify_referral_count(user_id=referrer_id, real_delta=1, fake_delta=-1)
                if updated_referrer:
                    try:
                        mention = await get_user_mention(user.id, context)
                        new_real_count = updated_referrer.get('total_real', 0)
                        text = f"🎉 تهانينا! لقد انضم مستخدم جديد ({mention}) عن طريق رابطك.\n\n" \
                               f"رصيدك المحدث هو: <b>{new_real_count}</b> إحالة حقيقية."
                        await context.bot.send_message(chat_id=referrer_id, text=text, parse_mode=ParseMode.HTML)
                    except TelegramError as e:
                        logger.warning(f"Could not send join notification to referrer {referrer_id}: {e}")
            elif referrer_id and was_already_member:
                 logger.info(f"User {user.id} was already a member. Notifying referrer {referrer_id} of fake referral.")
                 try:
                    mention = await get_user_mention(user.id, context)
                    await context.bot.send_message(chat_id=referrer_id, text=Messages.REFERRAL_EXISTING_MEMBER.format(mention=mention), parse_mode=ParseMode.HTML)
                 except TelegramError as e:
                    logger.warning(f"Could not send existing member notification to referrer {referrer_id}: {e}")

            await query.edit_message_text(Messages.JOIN_SUCCESS + "\n" + Messages.VERIFIED_WELCOME, reply_markup=get_main_menu_keyboard(user.id))
        else:
            logger.warning(f"User {user.id} clicked confirm but is not in channel.")
            keyboard = [[InlineKeyboardButton("1. الانضمام للقناة", url=Config.CHANNEL_URL)], [InlineKeyboardButton("✅ لقد انضممت، تحقق الآن", callback_data=Callback.CONFIRM_JOIN)]]
            await query.edit_message_text(Messages.JOIN_FAIL, reply_markup=InlineKeyboardMarkup(keyboard))
    except (TelegramError, BadRequest) as e:
        logger.error(f"Error in confirm_join for user {user.id}: {e}")
        await query.edit_message_text(Messages.GENERIC_ERROR)


# --- Admin Panel Callback Functions ---
async def display_report_page(query: CallbackQuery, context: ContextTypes.DEFAULT_TYPE, report_type: str, page: int):
    """Displays a paginated report of users based on referral counts."""
    await query.edit_message_text(Messages.LOADING)
    try:
        all_users = await get_all_users_from_db()
        sort_key = f"total_{report_type}"
        report_title = "الحقيقي" if report_type == "real" else "الوهمي"

        relevant_users = sorted(
            [u for u in all_users if u.get(sort_key, 0) > 0],
            key=lambda u: u.get(sort_key, 0),
            reverse=True
        )

        if not relevant_users:
            await query.edit_message_text(
                f"لا يوجد مستخدمون في تقرير الإحالات {report_title} حالياً.",
                reply_markup=get_admin_panel_keyboard()
            )
            return

        start_index = (page - 1) * Config.USERS_PER_PAGE
        end_index = start_index + Config.USERS_PER_PAGE
        users_on_page = relevant_users[start_index:end_index]
        total_pages = math.ceil(len(relevant_users) / Config.USERS_PER_PAGE)

        message_text = f"📄 <b>تقرير الإحالات ({report_title}) - صفحة {page}/{total_pages}</b>\n\n"
        mentions = await asyncio.gather(*[get_user_mention(u['user_id'], context) for u in users_on_page])

        for i, user_data in enumerate(users_on_page):
            mention = mentions[i]
            count = user_data.get(sort_key, 0)
            message_text += f"▪️ {mention} - <code>{count}</code>\n"

        keyboard = []
        row = []
        if page > 1:
            row.append(InlineKeyboardButton("⬅️ السابق", callback_data=f"{Callback.REPORT_PAGE}_{report_type}_page_{page-1}"))
        if page < total_pages:
            row.append(InlineKeyboardButton("التالي ➡️", callback_data=f"{Callback.REPORT_PAGE}_{report_type}_page_{page+1}"))
        if row: keyboard.append(row)
        keyboard.append([InlineKeyboardButton("🔙 العودة للوحة التحكم", callback_data=Callback.ADMIN_PANEL)])

        await query.edit_message_text(
            message_text,
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(keyboard),
            disable_web_page_preview=True
        )
    except Exception as e:
        logger.error(f"Error generating report page: {e}", exc_info=True)
        await query.edit_message_text(Messages.GENERIC_ERROR, reply_markup=get_admin_panel_keyboard())


async def display_target_referrals_log(message: Optional[Message], query: Optional[CallbackQuery], context: ContextTypes.DEFAULT_TYPE, target_user_id: int, report_type: str, page: int):
    """Displays a paginated log of a specific user's referrals."""
    if query: await query.edit_message_text(Messages.LOADING)
    try:
        real_refs, fake_refs = await get_my_referrals_details(target_user_id)
        user_list, title = (real_refs, "الحقيقيين") if report_type == 'real' else (fake_refs, "الوهميين")

        if not user_list:
            text = Messages.USER_HAS_NO_REFERRALS
            if message: await message.reply_text(text, reply_markup=get_admin_panel_keyboard())
            elif query: await query.edit_message_text(text, reply_markup=get_admin_panel_keyboard())
            return

        start_index = (page - 1) * Config.USERS_PER_PAGE
        end_index = start_index + Config.USERS_PER_PAGE
        users_on_page = user_list[start_index:end_index]
        total_pages = math.ceil(len(user_list) / Config.USERS_PER_PAGE)
        target_mention = await get_user_mention(target_user_id, context)
        message_text = f"🔍 <b>سجل إحالات {target_mention} ({title}) - صفحة {page}/{total_pages}</b>\n\n"

        if not users_on_page:
            message_text += "لا يوجد إحالات لعرضها في هذه الصفحة."
        else:
            mentions = await asyncio.gather(*[get_user_mention(uid, context) for uid in users_on_page])
            for i, mention in enumerate(mentions):
                message_text += f"▪️ {mention} (ID: <code>{users_on_page[i]}</code>)\n"

        keyboard = []
        row = []
        if page > 1: row.append(InlineKeyboardButton("⬅️ السابق", callback_data=f"{Callback.INSPECT_LOG}_{target_user_id}_{report_type}_{page-1}"))
        if page < total_pages: row.append(InlineKeyboardButton("التالي ➡️", callback_data=f"{Callback.INSPECT_LOG}_{target_user_id}_{report_type}_{page+1}"))
        if row: keyboard.append(row)

        switch_row = []
        if report_type == 'real': switch_row.append(InlineKeyboardButton("عرض الوهمي", callback_data=f"{Callback.INSPECT_LOG}_{target_user_id}_fake_1"))
        else: switch_row.append(InlineKeyboardButton("عرض الحقيقي", callback_data=f"{Callback.INSPECT_LOG}_{target_user_id}_real_1"))
        keyboard.append(switch_row)
        keyboard.append([InlineKeyboardButton("🔙 العودة للوحة التحكم", callback_data=Callback.ADMIN_PANEL)])

        reply_markup = InlineKeyboardMarkup(keyboard)
        if query: await query.edit_message_text(message_text, parse_mode=ParseMode.HTML, reply_markup=reply_markup, disable_web_page_preview=True)
        elif message: await message.reply_text(message_text, parse_mode=ParseMode.HTML, reply_markup=reply_markup, disable_web_page_preview=True)
    except Exception as e:
        logger.error(f"Error displaying target referrals log: {e}", exc_info=True)
        error_text = Messages.GENERIC_ERROR
        if query: await query.edit_message_text(error_text, reply_markup=get_admin_panel_keyboard())
        elif message: await message.reply_text(error_text, reply_markup=get_admin_panel_keyboard())

async def handle_admin_user_count(query: CallbackQuery):
    """Handles the admin button for showing user counts."""
    all_users = await get_all_users_from_db()
    total_users = len(all_users)
    verified_users = sum(1 for u in all_users if u.get('is_verified'))
    text = f"👥 <b>إحصائيات المستخدمين:</b>\n\n" \
           f"▫️ إجمالي المستخدمين في البوت: <b>{total_users}</b>\n" \
           f"▫️ المستخدمون الموثقون حالياً: <b>{verified_users}</b>"
    await query.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=get_admin_panel_keyboard())

async def handle_reset_all_confirm(query: CallbackQuery):
    """Handles the confirmation for resetting all referral data."""
    await query.edit_message_text("⏳ جاري تصفير جميع الإحالات...")
    try:
        await reset_all_referrals_in_db()
        await query.edit_message_text("✅ تم تصفير جميع إحصائيات الإحالة بنجاح.", reply_markup=get_admin_panel_keyboard())
    except Exception as e:
        await query.edit_message_text(f"❌ حدث خطأ أثناء التصفير: {e}", reply_markup=get_admin_panel_keyboard())

async def handle_format_bot_confirm(query: CallbackQuery):
    """Handles the confirmation for formatting the bot (deleting all data)."""
    await query.edit_message_text("💀💀💀 جاري حذف جميع البيانات...")
    try:
        await format_bot_in_db()
        await query.edit_message_text("✅ تم حذف جميع بيانات البوت بنجاح.", reply_markup=get_admin_panel_keyboard())
    except Exception as e:
        await query.edit_message_text(f"❌ حدث خطأ أثناء الفورمات: {e}", reply_markup=get_admin_panel_keyboard())

async def handle_force_reverification(query: CallbackQuery):
    """Handles forcing all users to re-verify."""
    await query.edit_message_text("⏳ جاري فرض إعادة التحقق على جميع المستخدمين...")
    try:
        await unverify_all_users_in_db()
        await query.edit_message_text("✅ تم إلغاء توثيق جميع المستخدمين. سيُطلب منهم التحقق مرة أخرى عند التفاعل التالي.", reply_markup=get_admin_panel_keyboard())
    except Exception as e:
        await query.edit_message_text(f"❌ حدث خطأ: {e}", reply_markup=get_admin_panel_keyboard())

async def handle_data_migration(query: CallbackQuery, context: ContextTypes.DEFAULT_TYPE):
    """Recalculates all referral counts for all users."""
    msg = await query.edit_message_text("⏳ جاري ترحيل البيانات وإعادة حساب الإحالات... هذه العملية قد تستغرق وقتاً.")
    try:
        all_users = await get_all_users_from_db()
        all_referrals_res = await run_sync_db(lambda: supabase.table('referrals').select('referred_user_id, referrer_user_id').execute())
        all_referrals_map = {ref['referred_user_id']: ref['referrer_user_id'] for ref in all_referrals_res.data}
        user_updates = []

        for user in all_users:
            user_id = user['user_id']
            referred_by_user = [k for k, v in all_referrals_map.items() if v == user_id]
            if not referred_by_user:
                user_updates.append({'user_id': user_id, 'total_real': 0, 'total_fake': 0})
                continue

            verified_status_res = await run_sync_db(lambda: supabase.table('users').select('user_id, is_verified').in_('user_id', referred_by_user).execute())
            verified_map = {u['user_id']: u.get('is_verified', False) for u in verified_status_res.data}
            real_refs = [ref_id for ref_id in referred_by_user if verified_map.get(ref_id, False)]
            fake_refs = [ref_id for ref_id in referred_by_user if not verified_map.get(ref_id, False)]
            user_updates.append({'user_id': user_id, 'total_real': len(real_refs), 'total_fake': len(fake_refs)})

        if user_updates:
            await run_sync_db(lambda: supabase.table('users').upsert(user_updates).execute())
        await msg.edit_text("✅ تم ترحيل وإعادة حساب جميع البيانات بنجاح!", reply_markup=get_admin_panel_keyboard())
    except Exception as e:
        logger.error(f"Error during data migration: {e}", exc_info=True)
        await msg.edit_text(f"❌ حدث خطأ أثناء ترحيل البيانات: {e}", reply_markup=get_admin_panel_keyboard())

async def handle_broadcast_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles sending a broadcast message to verified users."""
    if not update.message or not update.message.text_html: return
    message_text = update.message.text_html
    context.user_data.clear()
    await update.message.reply_text("⏳ جاري إرسال الإذاعة للمستخدمين الموثقين...")
    all_users = await get_all_users_from_db()
    verified_users = [u for u in all_users if u.get('is_verified')]
    sent_count, failed_count = 0, 0
    for user in verified_users:
        try:
            await context.bot.send_message(chat_id=user['user_id'], text=message_text, parse_mode=ParseMode.HTML)
            sent_count += 1
            await asyncio.sleep(0.1)
        except (BadRequest, TelegramError):
            failed_count += 1
    await update.message.reply_text(f"✅ تم إرسال الإذاعة.\n\n✔️ أُرسلت إلى: {sent_count} مستخدم\n✖️ فشل الإرسال إلى: {failed_count} مستخدم", reply_markup=get_admin_panel_keyboard())

async def handle_universal_broadcast_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles sending a broadcast message to ALL users."""
    if not update.message or not update.message.text_html: return
    message_text = update.message.text_html
    context.user_data.clear()
    await update.message.reply_text("⏳ جاري إرسال الإذاعة لجميع المستخدمين...")
    all_users = await get_all_users_from_db()
    sent_count, failed_count = 0, 0
    for user in all_users:
        try:
            await context.bot.send_message(chat_id=user['user_id'], text=message_text, parse_mode=ParseMode.HTML)
            sent_count += 1
            await asyncio.sleep(0.1)
        except (BadRequest, TelegramError):
            failed_count += 1
    await update.message.reply_text(f"✅ تم إرسال الإذاعة الشاملة.\n\n✔️ أُرسلت إلى: {sent_count} مستخدم\n✖️ فشل الإرسال إلى: {failed_count} مستخدم", reply_markup=get_admin_panel_keyboard())

# --- Central Callback Query Handler ---
async def callback_query_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handles all callback queries from inline keyboards."""
    query = update.callback_query
    if not query or not query.data or not query.from_user: return
    await query.answer()
    user_id = query.from_user.id
    data = query.data
    logger.info(f"Callback received from user {user_id}: {data}")

    # --- Main Menu Callbacks ---
    if data == Callback.MAIN_MENU: await query.edit_message_text(Messages.VERIFIED_WELCOME, reply_markup=get_main_menu_keyboard(user_id))
    elif data == Callback.MY_REFERRALS: await handle_button_press_my_referrals(query)
    elif data == Callback.MY_LINK: await handle_button_press_link(query, context)
    elif data == Callback.TOP_5: await handle_button_press_top5(query, context)
    elif data == Callback.CONFIRM_JOIN: await handle_confirm_join(query, context)

    # --- Admin Panel Callbacks ---
    elif user_id in Config.BOT_OWNER_IDS:
        if data == Callback.ADMIN_PANEL: await query.edit_message_text(Messages.ADMIN_WELCOME, reply_markup=get_admin_panel_keyboard())
        elif data.startswith(f"{Callback.REPORT_PAGE}_"):
            try:
                _, report_type, _, page_str = data.split('_')
                await display_report_page(query, context, report_type, int(page_str))
            except (ValueError, IndexError) as e: logger.error(f"Could not parse report callback data '{data}': {e}")
        elif data == Callback.ADMIN_USER_COUNT: await handle_admin_user_count(query)
        elif data == Callback.ADMIN_BOOO_MENU: await query.edit_message_text("👾 قائمة التعديل اليدوي:", reply_markup=get_booo_menu_keyboard())
        elif data == Callback.ADMIN_USER_EDIT_MENU:
            context.user_data['state'] = State.AWAITING_EDIT_USER_ID
            await query.edit_message_text("✍️ <b>تعديل إحصائيات مستخدم</b>\n\nالرجاء إرسال ID المستخدم الذي تريد تعديل إحصائياته.", parse_mode=ParseMode.HTML)
        elif data in {Callback.USER_ADD_REAL, Callback.USER_REMOVE_REAL, Callback.USER_ADD_FAKE, Callback.USER_REMOVE_FAKE}:
            context.user_data['action_type'] = data
            context.user_data['state'] = State.AWAITING_EDIT_USER_ID
            await query.edit_message_text("✍️ <b>تعديل إحصائيات مستخدم</b>\n\nالرجاء إرسال ID المستخدم.", parse_mode=ParseMode.HTML)
        elif data == Callback.ADMIN_BROADCAST:
            context.user_data['state'] = State.AWAITING_BROADCAST_MESSAGE
            await query.edit_message_text("📢 الرجاء إرسال رسالة الإذاعة التي تريد إرسالها لجميع المستخدمين <b>الموثقين</b>. يمكنك استخدام تنسيق HTML.", parse_mode=ParseMode.HTML)
        elif data == Callback.ADMIN_UNIVERSAL_BROADCAST:
            context.user_data['state'] = State.AWAITING_UNIVERSAL_BROADCAST_MESSAGE
            await query.edit_message_text("📢 الرجاء إرسال رسالة الإذاعة التي تريد إرسالها لـ <b>جميع</b> المستخدمين في قاعدة البيانات. يمكنك استخدام تنسيق HTML.", parse_mode=ParseMode.HTML)
        elif data == Callback.ADMIN_INSPECT_REFERRALS:
            context.user_data['state'] = State.AWAITING_INSPECT_USER_ID
            await query.edit_message_text("🔍 الرجاء إرسال ID المستخدم الذي تريد فحص إحالاته.")
        elif data == Callback.DATA_MIGRATION: await handle_data_migration(query, context)
        elif data == Callback.ADMIN_RESET_ALL: await query.edit_message_text("⚠️ <b>تحذير!</b>\n\nأنت على وشك تصفير جميع إحصائيات الإحالة. هذا الإجراء لا يمكن التراجع عنه.\n\nهل أنت متأكد؟", parse_mode=ParseMode.HTML, reply_markup=get_reset_confirmation_keyboard())
        elif data == Callback.ADMIN_RESET_CONFIRM: await handle_reset_all_confirm(query)
        elif data == Callback.ADMIN_FORMAT_BOT: await query.edit_message_text("💀 <b>تحذير خطير!</b>\n\nأنت على وشك حذف <b>جميع</b> بيانات المستخدمين والإحالات. هذا الإجراء لا يمكن التراجع عنه.\n\nهل أنت متأكد تماماً؟", parse_mode=ParseMode.HTML, reply_markup=get_format_confirmation_keyboard())
        elif data == Callback.ADMIN_FORMAT_CONFIRM: await handle_format_bot_confirm(query)
        elif data == Callback.ADMIN_FORCE_REVERIFICATION: await handle_force_reverification(query)
        elif data.startswith(f"{Callback.INSPECT_LOG}_"):
             try:
                _, target_user_id_str, report_type, page_str = data.split('_')
                await display_target_referrals_log(None, query, context, int(target_user_id_str), report_type, int(page_str))
             except (ValueError, IndexError) as e: logger.error(f"Could not parse inspect log callback data '{data}': {e}")
    else:
        await query.answer("You are not authorized for this action.", show_alert=True)

# --- Main Application Setup ---
def main() -> None:
    """Start the bot."""
    application = Application.builder().token(Config.BOT_TOKEN).build()

    # Command Handlers
    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CommandHandler("invites", my_referrals_command))
    application.add_handler(CommandHandler("link", link_command))
    application.add_handler(CommandHandler("top", top_command))

    # Message Handlers
    application.add_handler(MessageHandler(filters.StatusUpdate.WEB_APP_DATA, web_app_data_handler))
    application.add_handler(MessageHandler(filters.TEXT & filters.ChatType.PRIVATE & filters.User(Config.BOT_OWNER_IDS), handle_admin_messages))

    # Callback Query Handler
    application.add_handler(CallbackQueryHandler(callback_query_handler))

    # Chat Member Handler
    application.add_handler(ChatMemberHandler(handle_chat_member_updates, ChatMemberHandler.CHAT_MEMBER))

    logger.info("Bot is starting...")
    application.run_polling()

if __name__ == "__main__":
    main()
