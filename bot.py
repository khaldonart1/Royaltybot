import asyncio
import logging
import json
import re
import time
import math
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
    # REMOVED ALLOWED_COUNTRY_CODES as phone verification is disabled
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
    START_WELCOME = "أهلاً بك في البوت! �\n\nللبدء، نحتاج للتحقق من جهازك. الرجاء الضغط على الزر أدناه."
    WEB_VERIFY_PROMPT = "للتحقق من أنك لا تستخدم نفس الجهاز عدة مرات، الرجاء الضغط على الزر أدناه."
    WEB_VERIFY_SUCCESS = "تم التحقق من جهازك بنجاح!" # New message
    # REMOVED PHONE RELATED MESSAGES
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

# --- Helper Functions ---
def get_db_client(context: ContextTypes.DEFAULT_TYPE) -> Client:
    """Helper to get the Supabase client from bot_data."""
    return context.bot_data['db_client']

def clean_name_for_markdown(name: str) -> str:
    """Escapes characters for MarkdownV2 parsing."""
    if not name: return ""
    escape_chars = r"([_*\[\]()~`>#\+\-=|{}\.!\\])"
    return re.sub(escape_chars, r"\\\1", name)

async def get_user_mention(user_id: int, context: ContextTypes.DEFAULT_TYPE, full_name: Optional[str] = None) -> str:
    """Gets a Markdown-safe user mention, using a cache to reduce API calls."""
    cache = context.bot_data.setdefault('mention_cache', {})
    current_time = time.time()
    if user_id in cache and (current_time - cache[user_id].get('timestamp', 0) < Config.MENTION_CACHE_TTL_SECONDS):
        return cache[user_id]['mention']
    mention_name = "Unknown User"
    try:
        chat = await context.bot.get_chat(user_id)
        mention_name = chat.full_name or f"User {user_id}"
    except (TelegramError, BadRequest):
        if full_name:
            mention_name = full_name
        else:
            db_user_info = await get_user_from_db(user_id, context)
            if db_user_info:
                mention_name = db_user_info.get("full_name", f"User {user_id}")
    cleaned_name = clean_name_for_markdown(mention_name)
    mention = f"[{cleaned_name}](tg://user?id={user_id})"
    cache[user_id] = {'mention': mention, 'timestamp': current_time}
    return mention

# --- Database Functions ---
async def run_sync_db(func: Callable[[], Any]) -> Any:
    return await asyncio.to_thread(func)

async def get_user_from_db(user_id: int, context: ContextTypes.DEFAULT_TYPE) -> Optional[Dict[str, Any]]:
    db = get_db_client(context)
    try:
        res = await run_sync_db(lambda: db.table('users').select("*").eq('user_id', user_id).single().execute())
        return res.data
    except Exception:
        return None

async def upsert_user_in_db(user_data: Dict[str, Any], context: ContextTypes.DEFAULT_TYPE) -> None:
    db = get_db_client(context)
    try:
        await run_sync_db(lambda: db.table('users').upsert(user_data).execute())
    except Exception as e:
        logger.error(f"DB_ERROR: Upserting user {user_data.get('user_id')}: {e}", exc_info=True)

async def get_all_users_from_db(context: ContextTypes.DEFAULT_TYPE) -> List[Dict[str, Any]]:
    db = get_db_client(context)
    all_users = []
    current_page = 0
    page_size = 1000
    try:
        while True:
            start_index = current_page * page_size
            res = await run_sync_db(lambda: db.table('users').select("*", count='exact').range(start_index, start_index + page_size - 1).execute())
            if res.data:
                all_users.extend(res.data)
                if res.count is None or len(all_users) >= res.count:
                    break
            else:
                break
            current_page += 1
        return all_users
    except Exception as e:
        logger.error(f"DB_ERROR (get_all_users_from_db): {e}", exc_info=True)
        return []

async def get_referrer(referred_id: int, context: ContextTypes.DEFAULT_TYPE) -> Optional[int]:
    db = get_db_client(context)
    try:
        res = await run_sync_db(lambda: db.table('referrals').select('referrer_user_id').eq('referred_user_id', referred_id).execute())
        return res.data[0].get('referrer_user_id') if res.data else None
    except Exception:
        return None

async def add_referral_mapping_in_db(referred_id: int, referrer_id: Optional[int], device_id: str, context: ContextTypes.DEFAULT_TYPE) -> None:
    db = get_db_client(context)
    try:
        data = {'referred_user_id': referred_id, 'referrer_user_id': referrer_id, 'device_id': device_id}
        await run_sync_db(lambda: db.table('referrals').upsert(data, on_conflict='referred_user_id').execute())
    except Exception as e:
        logger.error(f"DB_ERROR: Adding referral map for {referred_id}: {e}", exc_info=True)

async def get_my_referrals_details(user_id: int, context: ContextTypes.DEFAULT_TYPE) -> Tuple[List[int], List[int]]:
    db = get_db_client(context)
    try:
        all_refs_res = await run_sync_db(lambda: db.table('referrals').select('referred_user_id').eq('referrer_user_id', user_id).execute())
        if not all_refs_res.data: return [], []
        referred_ids = [ref['referred_user_id'] for ref in all_refs_res.data]
        verified_status_res = await run_sync_db(lambda: db.table('users').select('user_id, is_verified').in_('user_id', referred_ids).execute())
        verified_map = {u['user_id']: u.get('is_verified', False) for u in verified_status_res.data}
        real_referrals = sorted([uid for uid in referred_ids if verified_map.get(uid, False)])
        fake_referrals = sorted([uid for uid in referred_ids if not verified_map.get(uid, False)])
        return real_referrals, fake_referrals
    except Exception as e:
        logger.error(f"Error fetching referral details for user {user_id}: {e}", exc_info=True)
        return [], []

async def reset_all_referrals_in_db(context: ContextTypes.DEFAULT_TYPE) -> None:
    db = get_db_client(context)
    try:
        await run_sync_db(lambda: db.table('referrals').delete().gt('referred_user_id', -1).execute())
        await run_sync_db(lambda: db.table('users').update({"total_real": 0, "total_fake": 0}).gt('user_id', -1).execute())
        logger.info("All referrals have been reset.")
    except Exception as e:
        logger.error(f"DB_ERROR: Resetting all referrals: {e}", exc_info=True)
        raise

async def format_bot_in_db(context: ContextTypes.DEFAULT_TYPE) -> None:
    db = get_db_client(context)
    try:
        logger.info("Attempting to delete from 'referrals' table.")
        await run_sync_db(lambda: db.table('referrals').delete().gt('referred_user_id', -1).execute())
        logger.info("Successfully deleted from 'referrals'. Attempting to delete from 'users'.")
        await run_sync_db(lambda: db.table('users').delete().gt('user_id', -1).execute())
        logger.info("BOT HAS BEEN FORMATTED.")
    except Exception as e:
        logger.error(f"DB_ERROR: Formatting bot: {e}", exc_info=True)
        raise

async def unverify_all_users_in_db(context: ContextTypes.DEFAULT_TYPE) -> None:
    db = get_db_client(context)
    try:
        await run_sync_db(lambda: db.table('users').update({"is_verified": False}).gt('user_id', 0).execute())
        logger.info("All users have been un-verified.")
    except Exception as e:
        logger.error(f"DB_ERROR: Un-verifying all users: {e}", exc_info=True)

# --- Display Functions ---
def get_referral_stats_text(user_info: Optional[Dict[str, Any]]) -> str:
    if not user_info: return Messages.NO_REFERRALS_YET
    total_real = int(user_info.get("total_real", 0) or 0)
    total_fake = int(user_info.get("total_fake", 0) or 0)
    return f"📊 *إحصائيات إحالاتك:*\n\n✅ الإحالات الحقيقية: `{total_real}`\n⏳ الإحالات الوهمية: `{total_fake}`"

def get_referral_link_text(user_id: int, bot_username: str) -> str:
    return f"🔗 رابط الإحالة الخاص بك:\n`https://t.me/{bot_username}?start={user_id}`"

async def get_top_5_text(user_id: int, context: ContextTypes.DEFAULT_TYPE) -> str:
    db = get_db_client(context)
    msg = "🏆 *أفضل 5 متسابقين لدينا:*\n\n"
    try:
        top_5_res = await run_sync_db(lambda: db.table('users').select('user_id, full_name, total_real').gt('total_real', 0).order('total_real', desc=True).limit(5).execute())
        top_5_users = top_5_res.data or []
        if not top_5_users:
            msg += "لم يصل أحد إلى القائمة بعد.\n"
        else:
            mentions = await asyncio.gather(*[get_user_mention(u['user_id'], context, u.get('full_name')) for u in top_5_users])
            for i, u_info in enumerate(top_5_users):
                msg += f"{i+1}\\. {mentions[i]} \\- *{u_info.get('total_real', 0)}* إحالة\n"
        msg += "\n---\n*ترتيبك الشخصي:*\n"
        my_info = await get_user_from_db(user_id, context)
        my_referrals = my_info.get('total_real', 0) if my_info else 0
        rank_str = "غير مصنف"
        if my_info and my_referrals > 0:
            try:
                count_res = await run_sync_db(lambda: db.table('users').select('user_id', count='exact').gt('total_real', my_referrals).execute())
                my_rank = (count_res.count or 0) + 1
                rank_str = f"\\#{my_rank}"
            except Exception as e:
                logger.error(f"Could not calculate rank for user {user_id}: {e}", exc_info=True)
                rank_str = "خطأ في الحساب"
        msg += f"🎖️ ترتيبك: *{rank_str}*\n✅ رصيدك: *{my_referrals}* إحالة حقيقية\\."
    except Exception as e:
        logger.error(f"Error getting top 5 text for {user_id}: {e}", exc_info=True)
        msg = Messages.GENERIC_ERROR
    return msg

# --- Core Logic & Keyboard Functions ---
async def modify_referral_count(user_id: int, context: ContextTypes.DEFAULT_TYPE, real_delta: int = 0, fake_delta: int = 0) -> Optional[Dict[str, Any]]:
    if not user_id: return None
    user_data = await get_user_from_db(user_id, context)
    if not user_data:
        logger.warning(f"Attempted to modify counts for non-existent user {user_id}")
        return None
    current_real = int(user_data.get('total_real', 0) or 0)
    current_fake = int(user_data.get('total_fake', 0) or 0)
    new_real = max(0, current_real + real_delta)
    new_fake = max(0, current_fake + fake_delta)
    update_payload = {'user_id': user_id, 'total_real': new_real, 'total_fake': new_fake}
    await upsert_user_in_db(update_payload, context)
    logger.info(f"Updated counts for {user_id}: Real {current_real}->{new_real}, Fake {current_fake}->{new_fake}")
    return await get_user_from_db(user_id, context)

async def is_user_in_channel(user_id: int, context: ContextTypes.DEFAULT_TYPE) -> bool:
    try:
        member = await context.bot.get_chat_member(chat_id=Config.CHANNEL_ID, user_id=user_id)
        return member.status in {ChatMember.MEMBER, ChatMember.ADMINISTRATOR, ChatMember.OWNER}
    except BadRequest as e:
        if "user not found" in str(e).lower():
            return False
        else:
            logger.error(f"Telegram BadRequest checking membership for {user_id}: {e}", exc_info=True)
            raise
    except Exception as e:
        logger.error(f"Unexpected error checking membership for {user_id}: {e}", exc_info=True)
        raise

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
        [InlineKeyboardButton("⚙️ ترحيل وحساب البيانات", callback_data=Callback.DATA_MIGRATION)],
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
    db_user = await get_user_from_db(user.id, context)
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
    await upsert_user_in_db({'user_id': user.id, 'full_name': user.full_name, 'username': user.username}, context)
    try:
        context.user_data['was_already_member'] = await is_user_in_channel(user.id, context)
    except (TelegramError, BadRequest) as e:
        logger.error(f"Could not check channel membership for {user.id} on start: {e}", exc_info=True)
        context.user_data['was_already_member'] = False
    await update.message.reply_text(Messages.START_WELCOME, reply_markup=ReplyKeyboardRemove())
    await ask_web_verification(update.message)

async def my_referrals_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_user or not update.message: return
    msg = await update.message.reply_text(Messages.LOADING)
    user_info = await get_user_from_db(update.effective_user.id, context)
    text = get_referral_stats_text(user_info)
    await msg.edit_text(text, parse_mode=ParseMode.MARKDOWN_V2, reply_markup=get_main_menu_keyboard(update.effective_user.id))

async def link_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_user or not update.message or not context.bot.username: return
    user_id = update.effective_user.id
    text = get_referral_link_text(user_id, context.bot.username)
    await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN_V2, reply_markup=get_main_menu_keyboard(user_id), disable_web_page_preview=True)

async def top_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_user or not update.message: return
    user_id = update.effective_user.id
    msg = await update.message.reply_text(Messages.LOADING)
    text = await get_top_5_text(user_id, context)
    await msg.edit_text(text, parse_mode=ParseMode.MARKDOWN_V2, reply_markup=get_main_menu_keyboard(user_id), disable_web_page_preview=True)

async def ask_web_verification(message: Message) -> None:
    logger.info(f"Asking for web verification from user {message.from_user.id}")
    keyboard = ReplyKeyboardMarkup.from_button(KeyboardButton(text="🔒 اضغط هنا للتحقق من جهازك", web_app=WebAppInfo(url=Config.WEB_APP_URL)), resize_keyboard=True)
    await message.reply_text(Messages.WEB_VERIFY_PROMPT, reply_markup=keyboard)

async def web_app_data_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_user or not update.message or not update.message.web_app_data: return
    user_id = update.effective_user.id
    db = get_db_client(context)
    try:
        data = json.loads(update.message.web_app_data.data)
        device_id = data.get("visitorId")
    except (json.JSONDecodeError, AttributeError):
        logger.error(f"Failed to parse web_app_data from user {user_id}", exc_info=True)
        await update.message.reply_text(Messages.GENERIC_ERROR, reply_markup=ReplyKeyboardRemove())
        return
    if not device_id:
        logger.error(f"No visitorId in web_app_data from user {user_id}")
        await update.message.reply_text(Messages.GENERIC_ERROR, reply_markup=ReplyKeyboardRemove())
        return

    try:
        device_usage_res = await run_sync_db(lambda: db.table('referrals').select('referred_user_id', count='exact').eq('device_id', device_id).neq('referred_user_id', user_id).execute())
        if device_usage_res.count and device_usage_res.count > 0:
            await update.message.reply_text(Messages.REFERRAL_ABUSE_DEVICE_USED, reply_markup=ReplyKeyboardRemove())
            return

        referrer_id = context.user_data.get('referrer_id')
        existing_ref_res = await run_sync_db(lambda: db.table('referrals').select('referred_user_id', count='exact').eq('referred_user_id', user_id).execute())
        if (existing_ref_res.count is None or existing_ref_res.count == 0) and referrer_id:
            await modify_referral_count(referrer_id, context, fake_delta=1)
            logger.info(f"New user {user_id} under referrer {referrer_id}. Added +1 fake referral.")

        await add_referral_mapping_in_db(user_id, referrer_id, device_id, context)
        
        # --- REMOVED PHONE VERIFICATION ---
        # Directly ask to join the channel
        await update.message.reply_text(Messages.WEB_VERIFY_SUCCESS, reply_markup=ReplyKeyboardRemove())
        keyboard = [[InlineKeyboardButton("1. الانضمام للقناة", url=Config.CHANNEL_URL)], [InlineKeyboardButton("✅ لقد انضممت، تحقق الآن", callback_data=Callback.CONFIRM_JOIN)]]
        await update.message.reply_text(Messages.JOIN_PROMPT, reply_markup=InlineKeyboardMarkup(keyboard))

    except Exception as e:
        logger.error(f"Error in web_app_data_handler for user {user_id}: {e}", exc_info=True)
        await update.message.reply_text(Messages.GENERIC_ERROR, reply_markup=ReplyKeyboardRemove())

async def handle_chat_member_updates(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    result = update.chat_member
    if not result: return
    user, chat_id = result.new_chat_member.user, result.chat.id
    if chat_id != Config.CHANNEL_ID: return

    was_member = result.old_chat_member.status in {ChatMember.MEMBER, ChatMember.ADMINISTRATOR, ChatMember.OWNER}
    is_member = result.new_chat_member.status in {ChatMember.MEMBER, ChatMember.ADMINISTRATOR, ChatMember.OWNER}
    logger.info(f"Chat member update for user {user.id}: Was member: {was_member}, Is member: {is_member}")

    if was_member and not is_member:
        logger.info(f"User {user.id} left channel.")
        db_user = await get_user_from_db(user.id, context)
        if db_user and db_user.get('is_verified'):
            await upsert_user_in_db({'user_id': user.id, 'is_verified': False}, context)
            referrer_id = await get_referrer(user.id, context)
            if referrer_id:
                try:
                    updated_referrer = await modify_referral_count(referrer_id, context, real_delta=-1, fake_delta=1)
                    if updated_referrer:
                        mention = await get_user_mention(user.id, context)
                        await context.bot.send_message(
                            chat_id=referrer_id,
                            text=Messages.LEAVE_NOTIFICATION.format(mention=mention, new_real_count=updated_referrer.get('total_real', 0)),
                            parse_mode=ParseMode.MARKDOWN_V2
                        )
                except TelegramError as e:
                    logger.warning(f"Could not send leave notification to referrer {referrer_id}: {e}")

async def handle_admin_messages(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_user or update.effective_user.id not in Config.BOT_OWNER_IDS or not update.message: return
    user_id, state, text = update.effective_user.id, context.user_data.get('state'), update.message.text
    if not state or not text: return
    logger.info(f"Admin {user_id} sent message in state {state}: {text}")

    if state == State.AWAITING_BROADCAST_MESSAGE: await handle_broadcast_message(update, context)
    elif state == State.AWAITING_UNIVERSAL_BROADCAST_MESSAGE: await handle_universal_broadcast_message(update, context)
    elif state == State.AWAITING_INSPECT_USER_ID:
        try:
            target_user_id = int(text)
            if not await get_user_from_db(target_user_id, context):
                await update.message.reply_text(Messages.USER_NOT_FOUND, reply_markup=get_admin_panel_keyboard())
            else:
                await display_target_referrals_log(update.message, None, context, target_user_id, 'real', 1)
        except (ValueError, TypeError):
            await update.message.reply_text(Messages.INVALID_INPUT, reply_markup=get_admin_panel_keyboard())
        finally:
            context.user_data.clear()
    elif state == State.AWAITING_EDIT_USER_ID:
        try:
            target_user_id = int(text)
            if not await get_user_from_db(target_user_id, context):
                await update.message.reply_text(Messages.USER_NOT_FOUND, reply_markup=get_admin_panel_keyboard())
                context.user_data.clear()
                return
            context.user_data.update({'state': State.AWAITING_EDIT_AMOUNT, 'target_id': target_user_id})
            action_map = {
                Callback.USER_ADD_REAL: "زيادة إحالات حقيقية", Callback.USER_REMOVE_REAL: "خصم إحالات حقيقية",
                Callback.USER_ADD_FAKE: "زيادة إحالات وهمية", Callback.USER_REMOVE_FAKE: "خصم إحالات وهمية"
            }
            mention = await get_user_mention(target_user_id, context)
            prompt = f"المستخدم: {mention}\nالإجراء: *{action_map.get(context.user_data.get('action_type'), 'N/A')}*\n\nالرجاء إرسال العدد."
            await update.message.reply_text(prompt, parse_mode=ParseMode.MARKDOWN_V2)
        except (ValueError, TypeError):
            await update.message.reply_text(Messages.INVALID_INPUT, reply_markup=get_admin_panel_keyboard())
            context.user_data.clear()
    elif state == State.AWAITING_EDIT_AMOUNT:
        target_user_id, action_type = context.user_data.get('target_id'), context.user_data.get('action_type')
        if not target_user_id or not action_type:
            context.user_data.clear(); return
        try:
            amount = int(text)
            if amount <= 0: raise ValueError("Amount must be positive")
            deltas = {
                Callback.USER_ADD_REAL: (amount, 0), Callback.USER_REMOVE_REAL: (-amount, 0),
                Callback.USER_ADD_FAKE: (0, amount), Callback.USER_REMOVE_FAKE: (0, -amount)
            }
            real_delta, fake_delta = deltas.get(action_type, (0, 0))
            updated_user = await modify_referral_count(target_user_id, context, real_delta, fake_delta)
            if updated_user:
                mention = await get_user_mention(target_user_id, context)
                final_text = (f"✅ تم التعديل بنجاح.\n\nالمستخدم: {mention}\nالرصيد الجديد:\n"
                              f"✅ *{updated_user.get('total_real', 0)}* حقيقي\n"
                              f"⏳ *{updated_user.get('total_fake', 0)}* وهمي")
                await update.message.reply_text(final_text, parse_mode=ParseMode.MARKDOWN_V2, reply_markup=get_admin_panel_keyboard())
        except (ValueError, TypeError):
            await update.message.reply_text(Messages.INVALID_INPUT + "\nيرجى إدخال رقم صحيح فقط.")
        finally:
            context.user_data.clear()

# --- Callback Helper Functions ---
async def handle_button_press_my_referrals(query: CallbackQuery, context: ContextTypes.DEFAULT_TYPE) -> None:
    await query.edit_message_text(Messages.LOADING)
    user_info = await get_user_from_db(query.from_user.id, context)
    text = get_referral_stats_text(user_info)
    await query.edit_message_text(text, parse_mode=ParseMode.MARKDOWN_V2, reply_markup=get_main_menu_keyboard(query.from_user.id))

async def handle_button_press_top5(query: CallbackQuery, context: ContextTypes.DEFAULT_TYPE) -> None:
    try:
        await query.edit_message_text(Messages.LOADING)
        text = await get_top_5_text(query.from_user.id, context)
        await query.edit_message_text(text, parse_mode=ParseMode.MARKDOWN_V2, reply_markup=get_main_menu_keyboard(query.from_user.id), disable_web_page_preview=True)
    except BadRequest as e:
        if "message is not modified" not in str(e).lower():
            logger.error(f"Top5 BadRequest: {e}", exc_info=True)
            await query.message.reply_text(Messages.GENERIC_ERROR)
    except Exception as e:
        logger.error(f"Top5 Exception: {e}", exc_info=True)
        await query.message.reply_text(Messages.GENERIC_ERROR)

async def handle_button_press_link(query: CallbackQuery, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = query.from_user.id
    text = get_referral_link_text(user_id, context.bot.username)
    await query.edit_message_text(text, parse_mode=ParseMode.MARKDOWN_V2, reply_markup=get_main_menu_keyboard(user_id), disable_web_page_preview=True)

async def handle_confirm_join(query: CallbackQuery, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = query.from_user
    await query.edit_message_text(Messages.LOADING)
    try:
        if await is_user_in_channel(user.id, context):
            db_user = await get_user_from_db(user.id, context)
            if not db_user or not db_user.get('is_verified'):
                await upsert_user_in_db({'user_id': user.id, 'is_verified': True}, context)
                referrer_id = await get_referrer(user.id, context)
                if referrer_id:
                    was_already_member = context.user_data.get('was_already_member', False)
                    try:
                        if was_already_member:
                            await context.bot.send_message(chat_id=referrer_id, text=Messages.REFERRAL_EXISTING_MEMBER)
                        else:
                            updated_referrer = await modify_referral_count(referrer_id, context, real_delta=1, fake_delta=-1)
                            if updated_referrer:
                                mention = await get_user_mention(user.id, context)
                                await context.bot.send_message(chat_id=referrer_id, text=Messages.REFERRAL_SUCCESS.format(mention=mention, new_real_count=updated_referrer.get('total_real', 0)), parse_mode=ParseMode.MARKDOWN_V2)
                    except TelegramError as e:
                        logger.warning(f"Could not send notification to referrer {referrer_id}: {e}")
            await query.edit_message_text(Messages.JOIN_SUCCESS)
            await query.message.reply_text(Messages.VERIFIED_WELCOME, reply_markup=get_main_menu_keyboard(user.id))
        else:
            await query.answer(text=Messages.JOIN_FAIL, show_alert=True)
            keyboard = [[InlineKeyboardButton("1. الانضمام للقناة", url=Config.CHANNEL_URL)], [InlineKeyboardButton("✅ لقد انضممت، تحقق الآن", callback_data=Callback.CONFIRM_JOIN)]]
            await query.edit_message_text(Messages.JOIN_PROMPT, reply_markup=InlineKeyboardMarkup(keyboard))
    except (TelegramError, BadRequest) as e:
        logger.error(f"Error during join confirmation for user {user.id}: {e}", exc_info=True)
        await query.edit_message_text(Messages.GENERIC_ERROR + "\n\nتأكد من أن البوت لديه صلاحيات الأدمن في القناة.")

# --- Admin Panel Callback Functions ---
async def handle_admin_panel(query: CallbackQuery) -> None:
    await query.edit_message_text(text=Messages.ADMIN_WELCOME, reply_markup=get_admin_panel_keyboard())

async def handle_admin_user_count(query: CallbackQuery, context: ContextTypes.DEFAULT_TYPE) -> None:
    db = get_db_client(context)
    try:
        await query.edit_message_text(Messages.LOADING)
        total_res = await run_sync_db(lambda: db.table('users').select('user_id', count='exact').execute())
        verified_res = await run_sync_db(lambda: db.table('users').select('user_id', count='exact').eq('is_verified', True).execute())
        text = f"📈 *إحصائيات البوت:*\n\n▫️ إجمالي المستخدمين: `{total_res.count or 0}`\n✅ الموثقون: `{verified_res.count or 0}`"
        await query.edit_message_text(text=text, parse_mode=ParseMode.MARKDOWN_V2, reply_markup=get_admin_panel_keyboard())
    except Exception as e:
        logger.error(f"Error counting users: {e}", exc_info=True)
        await query.edit_message_text(Messages.GENERIC_ERROR, reply_markup=get_admin_panel_keyboard())

async def handle_report_pagination(query: CallbackQuery, context: ContextTypes.DEFAULT_TYPE) -> None:
    db = get_db_client(context)
    try:
        parts = query.data.split('_'); report_type, page = parts[1], int(parts[3])
        await query.edit_message_text(Messages.LOADING)
        count_key = 'total_real' if report_type == 'real' else 'total_fake'
        title = "✅ *تقرير الإحالات الحقيقية*" if report_type == 'real' else "⏳ *تقرير الإحالات الوهمية*"
        start_index = (page - 1) * Config.USERS_PER_PAGE
        
        count_res = await run_sync_db(lambda: db.table('users').select('user_id', count='exact').gt(count_key, 0).execute())
        total_users = count_res.count or 0
        
        if total_users == 0:
            await query.edit_message_text(f"لا يوجد مستخدمون في هذا التقرير.", reply_markup=get_admin_panel_keyboard()); return

        users_res = await run_sync_db(lambda: db.table('users').select(f"user_id, full_name, {count_key}").gt(count_key, 0).order(count_key, desc=True).range(start_index, start_index + Config.USERS_PER_PAGE - 1).execute())
        page_users = users_res.data or []
        total_pages = math.ceil(total_users / Config.USERS_PER_PAGE)
        mentions = await asyncio.gather(*[get_user_mention(u['user_id'], context, u.get('full_name')) for u in page_users])

        report_lines = [f"• {mention} \\- *{u_data.get(count_key, 0)}*" for mention, u_data in zip(mentions, page_users)]
        report = f"{title} (صفحة {page} من {total_pages}):\n\n" + "\n".join(report_lines)

        nav_buttons = []
        cb_prefix = f"{Callback.REPORT_PAGE}_{report_type}_page_"
        if page > 1: nav_buttons.append(InlineKeyboardButton("⬅️ السابق", callback_data=f"{cb_prefix}{page-1}"))
        if page < total_pages: nav_buttons.append(InlineKeyboardButton("التالي ➡️", callback_data=f"{cb_prefix}{page+1}"))
        keyboard = [nav_buttons] if nav_buttons else []
        keyboard.append([InlineKeyboardButton("🔙 العودة", callback_data=Callback.ADMIN_PANEL)])
        await query.edit_message_text(text=report, parse_mode=ParseMode.MARKDOWN_V2, reply_markup=InlineKeyboardMarkup(keyboard), disable_web_page_preview=True)
    except Exception as e:
        logger.error(f"Error generating report {query.data}: {e}", exc_info=True)
        await query.edit_message_text(Messages.GENERIC_ERROR, reply_markup=get_admin_panel_keyboard())

async def handle_admin_broadcast(query: CallbackQuery, context: ContextTypes.DEFAULT_TYPE) -> None:
    context.user_data['state'] = State.AWAITING_BROADCAST_MESSAGE
    await query.edit_message_text(text="أرسل الآن الرسالة التي تريد إذاعتها لجميع المستخدمين الموثقين.")

async def handle_broadcast_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message: return
    context.user_data['state'] = None
    await update.message.reply_text("⏳ جاري إرسال الإذاعة...")
    db = get_db_client(context)
    sent, failed, current_page, page_size = 0, 0, 0, 500
    while True:
        start_index = current_page * page_size
        res = await run_sync_db(lambda: db.table('users').select('user_id').eq('is_verified', True).range(start_index, start_index + page_size - 1).execute())
        if not res.data: break
        for user in res.data:
            if not (user_id := user.get('user_id')): continue
            try:
                await context.bot.copy_message(chat_id=user_id, from_chat_id=update.effective_chat.id, message_id=update.message.message_id)
                sent += 1
            except TelegramError: failed += 1
            await asyncio.sleep(0.05)
        current_page += 1
    await update.message.reply_text(f"✅ اكتملت الإذاعة!\n\nتم الإرسال إلى: {sent}\nفشل: {failed}")

async def handle_admin_universal_broadcast(query: CallbackQuery, context: ContextTypes.DEFAULT_TYPE) -> None:
    context.user_data['state'] = State.AWAITING_UNIVERSAL_BROADCAST_MESSAGE
    await query.edit_message_text(text="أرسل الآن الرسالة التي تريد إذاعتها لجميع المستخدمين المسجلين.")

async def handle_universal_broadcast_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message: return
    context.user_data['state'] = None
    await update.message.reply_text("⏳ جاري إرسال الإذاعة الشاملة...")
    all_users = await get_all_users_from_db(context)
    sent, failed = 0, 0
    for user in all_users:
        if not (user_id := user.get('user_id')): continue
        try:
            await context.bot.copy_message(chat_id=user_id, from_chat_id=update.effective_chat.id, message_id=update.message.message_id)
            sent += 1
        except TelegramError: failed += 1
        await asyncio.sleep(0.05)
    await update.message.reply_text(f"✅ اكتملت الإذاعة الشاملة!\n\nتم الإرسال إلى: {sent}\nفشل: {failed}")

async def handle_booo_menu(query: CallbackQuery) -> None:
    await query.edit_message_text("👾 *Booo*\n\nاختر الأداة:", parse_mode=ParseMode.MARKDOWN_V2, reply_markup=get_booo_menu_keyboard())

async def handle_user_edit_menu(query: CallbackQuery) -> None:
    await query.edit_message_text("👤 *تعديل إحصائيات المستخدم*", parse_mode=ParseMode.MARKDOWN_V2, reply_markup=get_user_edit_keyboard())

async def handle_user_edit_action(query: CallbackQuery, context: ContextTypes.DEFAULT_TYPE) -> None:
    context.user_data.update({'state': State.AWAITING_EDIT_USER_ID, 'action_type': query.data})
    await query.edit_message_text(text="الرجاء إرسال الـ ID الرقمي للمستخدم.")

async def handle_admin_inspect_request(query: CallbackQuery, context: ContextTypes.DEFAULT_TYPE) -> None:
    context.user_data['state'] = State.AWAITING_INSPECT_USER_ID
    await query.edit_message_text(text="🔍 الرجاء إرسال الـ ID الرقمي للمستخدم الذي تريد فحص إحالاته.")

async def display_target_referrals_log(message: Optional[Message], query: Optional[CallbackQuery], context: ContextTypes.DEFAULT_TYPE, target_user_id: int, log_type: str, page: int) -> None:
    target = query.message if query else message
    if not target: return
    await target.edit_text(Messages.LOADING)
    real_ids, fake_ids = await get_my_referrals_details(target_user_id, context)
    target_mention = await get_user_mention(target_user_id, context)

    if not real_ids and not fake_ids:
        await target.edit_text(f"📜 سجل {target_mention}\n\n" + Messages.USER_HAS_NO_REFERRALS, parse_mode=ParseMode.MARKDOWN_V2, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙", callback_data=Callback.ADMIN_PANEL)]]))
        return

    id_list, title = (real_ids, "✅ الحقيقية") if log_type == 'real' else (fake_ids, "⏳ الوهمية")
    if not id_list:
        text = f"📜 سجل {target_mention}\n\n{title}:\n\nلا يوجد."
    else:
        start_index, end_index = (page - 1) * Config.USERS_PER_PAGE, page * Config.USERS_PER_PAGE
        page_ids = id_list[start_index:end_index]
        mentions = await asyncio.gather(*[get_user_mention(uid, context) for uid in page_ids])
        user_list_text = "\n".join(f"• {mention} (`{uid}`)" for mention, uid in zip(mentions, page_ids))
        text = f"📜 سجل {target_mention}\n\n{title} (صفحة {page}):\n{user_list_text}"

    nav, total_pages = [], math.ceil(len(id_list) / Config.USERS_PER_PAGE)
    cb = f"{Callback.INSPECT_LOG}_{target_user_id}_{log_type}_"
    if page > 1: nav.append(InlineKeyboardButton("⬅️", callback_data=f"{cb}{page-1}"))
    if page < total_pages: nav.append(InlineKeyboardButton("➡️", callback_data=f"{cb}{page+1}"))
    toggle_cb = f"{Callback.INSPECT_LOG}_{target_user_id}_{'fake' if log_type == 'real' else 'real'}_1"
    toggle_text = "عرض الوهمية ⏳" if log_type == 'real' else "عرض الحقيقية ✅"
    keyboard = [nav] if nav else []
    keyboard.extend([[InlineKeyboardButton(toggle_text, callback_data=toggle_cb)], [InlineKeyboardButton("🔙", callback_data=Callback.ADMIN_PANEL)]])
    await target.edit_text(text, parse_mode=ParseMode.MARKDOWN_V2, reply_markup=InlineKeyboardMarkup(keyboard), disable_web_page_preview=True)

async def handle_inspect_log_pagination(query: CallbackQuery, context: ContextTypes.DEFAULT_TYPE) -> None:
    try:
        _, target_id_str, log_type, page_str = query.data.split('_')
        await display_target_referrals_log(None, query, context, int(target_id_str), log_type, int(page_str))
    except (ValueError, IndexError): return

async def handle_data_migration(query: CallbackQuery, context: ContextTypes.DEFAULT_TYPE) -> None:
    await query.edit_message_text("⏳ **بدء عملية إعادة الحساب...**\nقد تستغرق هذه العملية بعض الوقت.", parse_mode=ParseMode.MARKDOWN_V2)
    db = get_db_client(context)
    try:
        all_users = await get_all_users_from_db(context)
        if not all_users:
            await query.edit_message_text("لا يوجد مستخدمون.", reply_markup=get_admin_panel_keyboard()); return

        all_mappings, current_page, page_size = [], 0, 1000
        while True:
            start_index = current_page * page_size
            res = await run_sync_db(lambda: db.table('referrals').select("referrer_user_id, referred_user_id").range(start_index, start_index + page_size - 1).execute())
            if res.data: all_mappings.extend(res.data)
            else: break
            current_page += 1
        
        verified_ids = {u['user_id'] for u in all_users if u.get('is_verified') and u.get('user_id')}
        user_counts = {u['user_id']: {'total_real': 0, 'total_fake': 0} for u in all_users if u.get('user_id')}

        for mapping in all_mappings:
            ref_id, red_id = mapping.get('referrer_user_id'), mapping.get('referred_user_id')
            if ref_id in user_counts:
                user_counts[ref_id]['total_real' if red_id in verified_ids else 'total_fake'] += 1
        
        users_to_update = [{'user_id': uid, **counts} for uid, counts in user_counts.items()]
        if users_to_update:
            chunk_size = 200
            for i in range(0, len(users_to_update), chunk_size):
                await run_sync_db(lambda: db.table('users').upsert(users_to_update[i:i + chunk_size]).execute())
                logger.info(f"Data migration: updated chunk {i//chunk_size + 1}")
                await asyncio.sleep(0.5)

        await query.edit_message_text(f"✅ **اكتملت!** تم تحديث *{len(users_to_update)}* مستخدم.", reply_markup=get_admin_panel_keyboard(), parse_mode=ParseMode.MARKDOWN_V2)
    except Exception as e:
        logger.error(f"Data migration failed: {e}", exc_info=True)
        await query.edit_message_text(f"❌ فشلت العملية.\n`{e}`", reply_markup=get_admin_panel_keyboard())

async def handle_admin_reset_all(query: CallbackQuery, context: ContextTypes.DEFAULT_TYPE) -> None:
    await query.edit_message_text("⚠️ *تأكيد* ⚠️\nهل أنت متأكد أنك تريد تصفير *جميع* الإحالات؟", parse_mode=ParseMode.MARKDOWN_V2, reply_markup=get_reset_confirmation_keyboard())

async def handle_admin_reset_confirm(query: CallbackQuery, context: ContextTypes.DEFAULT_TYPE) -> None:
    try:
        await query.edit_message_text(text="⏳ جاري تصفير جميع الإحالات...")
        await reset_all_referrals_in_db(context)
        await query.edit_message_text("✅ تم تصفير جميع الإحالات بنجاح.", reply_markup=get_admin_panel_keyboard())
    except Exception as e:
        logger.error(f"Failed to reset all referrals: {e}", exc_info=True)
        await query.edit_message_text(f"❌ فشل تصفير الإحالات.\n`{e}`", reply_markup=get_admin_panel_keyboard())

async def handle_admin_format_bot(query: CallbackQuery, context: ContextTypes.DEFAULT_TYPE) -> None:
    await query.edit_message_text("⚠️⚠️⚠️ *تحذير خطير جداً* ⚠️⚠️⚠️\n\nأنت على وشك حذف **جميع بيانات البوت بشكل نهائي**.\nهذا الإجراء لا يمكن التراجع عنه.", parse_mode=ParseMode.MARKDOWN_V2, reply_markup=get_format_confirmation_keyboard())

async def handle_admin_format_confirm(query: CallbackQuery, context: ContextTypes.DEFAULT_TYPE) -> None:
    try:
        await query.edit_message_text(text="⏳ جاري تنفيذ الفورمات...")
        await format_bot_in_db(context)
        await query.edit_message_text("✅ تم عمل فورمات للبوت بنجاح.", reply_markup=get_admin_panel_keyboard())
    except Exception as e:
        logger.error(f"Failed to format bot: {e}", exc_info=True)
        await query.edit_message_text(f"❌ فشل الفورمات.\n`{e}`", reply_markup=get_admin_panel_keyboard())

async def handle_force_reverification(query: CallbackQuery, context: ContextTypes.DEFAULT_TYPE) -> None:
    await query.edit_message_text(text="⏳ جاري إلغاء تحقق جميع المستخدمين...")
    await unverify_all_users_in_db(context)
    await query.edit_message_text("✅ تم إلغاء تحقق جميع المستخدمين.", reply_markup=get_admin_panel_keyboard())

# --- Main Callback Router ---
async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query or not query.data or not query.from_user: return
    try:
        await query.answer()
    except BadRequest: return

    action, user_id = query.data, query.from_user.id
    logger.info(f"Button press from user {user_id}: {action}")

    try:
        if action == Callback.MAIN_MENU: await query.edit_message_text(text=Messages.VERIFIED_WELCOME, reply_markup=get_main_menu_keyboard(user_id))
        elif action == Callback.MY_REFERRALS: await handle_button_press_my_referrals(query, context)
        elif action == Callback.MY_LINK: await handle_button_press_link(query, context)
        elif action == Callback.TOP_5: await handle_button_press_top5(query, context)
        elif action == Callback.CONFIRM_JOIN: await handle_confirm_join(query, context)
        elif user_id in Config.BOT_OWNER_IDS:
            if action == Callback.ADMIN_PANEL: await handle_admin_panel(query)
            elif action == Callback.ADMIN_USER_COUNT: await handle_admin_user_count(query, context)
            elif action.startswith(f"{Callback.REPORT_PAGE}_"): await handle_report_pagination(query, context)
            elif action == Callback.ADMIN_INSPECT_REFERRALS: await handle_admin_inspect_request(query, context)
            elif action.startswith(f"{Callback.INSPECT_LOG}_"): await handle_inspect_log_pagination(query, context)
            elif action == Callback.ADMIN_BOOO_MENU: await handle_booo_menu(query)
            elif action == Callback.ADMIN_USER_EDIT_MENU: await handle_user_edit_menu(query)
            elif action in {Callback.USER_ADD_REAL, Callback.USER_REMOVE_REAL, Callback.USER_ADD_FAKE, Callback.USER_REMOVE_FAKE}: await handle_user_edit_action(query, context)
            elif action == Callback.ADMIN_BROADCAST: await handle_admin_broadcast(query, context)
            elif action == Callback.ADMIN_UNIVERSAL_BROADCAST: await handle_admin_universal_broadcast(query, context)
            elif action == Callback.ADMIN_FORCE_REVERIFICATION: await handle_force_reverification(query, context)
            elif action == Callback.ADMIN_RESET_ALL: await handle_admin_reset_all(query, context)
            elif action == Callback.ADMIN_RESET_CONFIRM: await handle_admin_reset_confirm(query, context)
            elif action == Callback.DATA_MIGRATION: await handle_data_migration(query, context)
            elif action == Callback.ADMIN_FORMAT_BOT: await handle_admin_format_bot(query, context)
            elif action == Callback.ADMIN_FORMAT_CONFIRM: await handle_admin_format_confirm(query, context)
    except Exception as e:
        logger.error(f"Error in button_handler for action {action} by user {user_id}: {e}", exc_info=True)
        try:
            if query.message: await query.edit_message_text(Messages.GENERIC_ERROR)
            else: await context.bot.send_message(chat_id=user_id, text=Messages.GENERIC_ERROR)
        except Exception as inner_e:
            logger.error(f"Failed to send generic error message: {inner_e}", exc_info=True)

# --- Post Init Function ---
async def post_init(application: Application) -> None:
    """Initialize things after the bot starts."""
    # Create the Supabase client and store it in bot_data
    application.bot_data['db_client'] = create_client(Config.SUPABASE_URL, Config.SUPABASE_KEY)
    logger.info("Supabase client created and stored in bot_data.")
    # Send startup message to owner
    for owner_id in Config.BOT_OWNER_IDS:
        try:
            await application.bot.send_message(chat_id=owner_id, text=f"✅ RoyaltyBot has started successfully.")
        except Exception as e:
            logger.error(f"Could not send startup message to owner {owner_id}: {e}")

# --- Main Function ---
def main() -> None:
    """Starts the bot."""
    application = (Application.builder().token(Config.BOT_TOKEN).post_init(post_init).build())

    # Handler group 0
    application.add_handler(ChatMemberHandler(handle_chat_member_updates, ChatMemberHandler.CHAT_MEMBER), group=0)

    # Handler group 1
    application.add_handler(CommandHandler("start", start_command), group=1)
    application.add_handler(CommandHandler("invites", my_referrals_command), group=1)
    application.add_handler(CommandHandler("link", link_command), group=1)
    application.add_handler(CommandHandler("top", top_command), group=1)
    application.add_handler(CallbackQueryHandler(button_handler), group=1)

    # Handler group 2
    private_filter = filters.ChatType.PRIVATE
    application.add_handler(MessageHandler(filters.StatusUpdate.WEB_APP_DATA & private_filter, web_app_data_handler), group=2)
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND & private_filter, handle_admin_messages), group=2)

    logger.info("Bot is starting...")
    application.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()
