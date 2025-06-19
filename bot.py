import asyncio
import logging
import json
import re
import time
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

# --- Bot Messages ---
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
def clean_name_for_markdown(name: str) -> str:
    if not name: return ""
    escape_chars = r"([_*\[\]()~`>#\+\-=|{}\.!\\])"
    return re.sub(escape_chars, r"\\\1", name)

async def get_user_mention(user_id: int, context: ContextTypes.DEFAULT_TYPE) -> str:
    cache = context.bot_data.setdefault('mention_cache', {})
    current_time = time.time()
    if user_id in cache and (current_time - cache[user_id].get('timestamp', 0) < Config.MENTION_CACHE_TTL_SECONDS):
        return cache[user_id]['mention']
    try:
        chat = await context.bot.get_chat(user_id)
        full_name = clean_name_for_markdown(chat.full_name or f"User {user_id}")
        mention = f"[{full_name}](tg://user?id={user_id})"
    except (TelegramError, BadRequest):
        db_user_info = await get_user_from_db(user_id)
        full_name = "مستخدم غير معروف"
        if db_user_info:
            full_name = clean_name_for_markdown(db_user_info.get("full_name", f"User {user_id}"))
        mention = f"[{full_name}](tg://user?id={user_id})"
    cache[user_id] = {'mention': mention, 'timestamp': current_time}
    return mention

# --- Database Functions ---
async def run_sync_db(func: Callable[[], Any]) -> Any:
    return await asyncio.to_thread(func)

async def get_user_from_db(user_id: int) -> Optional[Dict[str, Any]]:
    try:
        res = await run_sync_db(lambda: supabase.table('users').select("*").eq('user_id', user_id).single().execute())
        return res.data
    except Exception:
        return None

async def upsert_user_in_db(user_data: Dict[str, Any]) -> None:
    try:
        await run_sync_db(lambda: supabase.table('users').upsert(user_data).execute())
    except Exception as e:
        logger.error(f"DB_ERROR: Upserting user {user_data.get('user_id')}: {e}")

async def get_all_users_from_db() -> List[Dict[str, Any]]:
    try:
        res = await run_sync_db(lambda: supabase.table('users').select("*").execute())
        return res.data or []
    except Exception as e:
        logger.error(f"DB_ERROR (get_all_users_from_db): {e}")
        return []

async def get_referrer(referred_id: int) -> Optional[int]:
    try:
        res = await run_sync_db(lambda: supabase.table('referrals').select('referrer_user_id').eq('referred_user_id', referred_id).single().execute())
        return res.data.get('referrer_user_id') if res.data else None
    except Exception:
        return None

async def add_referral_mapping_in_db(referred_id: int, referrer_id: Optional[int], device_id: str) -> None:
    try:
        data = {'referred_user_id': referred_id, 'referrer_user_id': referrer_id, 'device_id': device_id}
        await run_sync_db(lambda: supabase.table('referrals').upsert(data, on_conflict='referred_user_id').execute())
    except Exception as e:
        logger.error(f"DB_ERROR: Adding referral map for {referred_id}: {e}")

async def get_my_referrals_details(user_id: int) -> Tuple[List[int], List[int]]:
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
    try:
        await run_sync_db(lambda: supabase.table('referrals').delete().gt('referred_user_id', 0).execute())
        await run_sync_db(lambda: supabase.table('users').update({"total_real": 0, "total_fake": 0}).gt('user_id', 0).execute())
        logger.info("All referrals have been reset.")
    except Exception as e:
        logger.error(f"DB_ERROR: Resetting all referrals: {e}")

async def format_bot_in_db() -> None:
    try:
        await run_sync_db(lambda: supabase.table('referrals').delete().gt('referred_user_id', 0).execute())
        await run_sync_db(lambda: supabase.table('users').delete().gt('user_id', 0).execute())
        logger.info("BOT HAS BEEN FORMATTED.")
    except Exception as e:
        logger.error(f"DB_ERROR: Formatting bot: {e}")

async def unverify_all_users_in_db() -> None:
    try:
        await run_sync_db(lambda: supabase.table('users').update({"is_verified": False}).gt('user_id', 0).execute())
        logger.info("All users have been un-verified.")
    except Exception as e:
        logger.error(f"DB_ERROR: Un-verifying all users: {e}")

# --- Display Functions ---
def get_referral_stats_text(user_info: Optional[Dict[str, Any]]) -> str:
    if not user_info: return "لا توجد لديك بيانات بعد."
    total_real = int(user_info.get("total_real", 0) or 0)
    total_fake = int(user_info.get("total_fake", 0) or 0)
    return f"📊 *إحصائيات إحالاتك:*\n\n✅ الإحالات الحقيقية: `{total_real}`\n⏳ الإحالات الوهمية: `{total_fake}`"

def get_referral_link_text(user_id: int, bot_username: str) -> str:
    return f"🔗 رابط الإحالة الخاص بك:\n`https://t.me/{bot_username}?start={user_id}`"

async def get_top_5_text(user_id: int, context: ContextTypes.DEFAULT_TYPE) -> str:
    msg = "🏆 *أفضل 5 متسابقين لدينا:*\n\n"
    all_users = await get_all_users_from_db()
    if not all_users:
        return msg + "لم يصل أحد إلى القائمة بعد.\n\n---\n*ترتيبك الشخصي:*\nلا يمكن عرض ترتيبك حالياً."
    
    full_sorted_list = sorted([u for u in all_users if u.get('total_real', 0) > 0], key=lambda u: u.get('total_real', 0), reverse=True)
    top_5_users = full_sorted_list[:5]

    if not top_5_users:
        msg += "لم يصل أحد إلى القائمة بعد.\n"
    else:
        mentions = await asyncio.gather(*[get_user_mention(u['user_id'], context) for u in top_5_users])
        for i, u_info in enumerate(top_5_users):
            mention = mentions[i]
            count = u_info.get('total_real', 0)
            msg += f"{i+1}\\. {mention} \\- *{count}* إحالة\n"

    msg += "\n---\n*ترتيبك الشخصي:*\n"
    try:
        user_index = next((i for i, u in enumerate(full_sorted_list) if u.get('user_id') == user_id), -1)
        my_referrals = 0
        rank_str = "غير مصنف"
        if user_index != -1:
            my_info = full_sorted_list[user_index]
            rank_str = f"\\#{user_index + 1}"
            my_referrals = my_info.get('total_real', 0)
        else:
            my_info = await get_user_from_db(user_id)
            if my_info: my_referrals = my_info.get('total_real', 0)
        
        msg += f"🎖️ ترتيبك: *{rank_str}*\n✅ رصيدك: *{my_referrals}* إحالة حقيقية\\."
    except Exception as e:
        logger.error(f"Error getting user rank for {user_id}: {e}")
        msg += "لا يمكن عرض ترتيبك حالياً."
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
    try:
        member = await context.bot.get_chat_member(chat_id=Config.CHANNEL_ID, user_id=user_id)
        return member.status in {User.MEMBER, User.ADMINISTRATOR, User.CREATOR}
    except TelegramError as e:
        logger.warning(f"Error checking membership for {user_id}: {e}")
    except Exception as e:
        logger.error(f"Unexpected error checking membership for {user_id}: {e}")
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
    if not update.message or not update.effective_user or update.effective_chat.type != Chat.PRIVATE: return
    user = update.effective_user
    db_user = await get_user_from_db(user.id)
    if db_user and db_user.get("is_verified"):
        await update.message.reply_text(Messages.VERIFIED_WELCOME, reply_markup=get_main_menu_keyboard(user.id))
        return
    args = context.args
    if args:
        try:
            referrer_id = int(args[0])
            if referrer_id != user.id: context.user_data['referrer_id'] = referrer_id
        except (ValueError, IndexError): pass
    await upsert_user_in_db({'user_id': user.id, 'full_name': user.full_name, 'username': user.username})
    context.user_data['was_already_member'] = await is_user_in_channel(user.id, context)
    await update.message.reply_text(Messages.START_WELCOME, reply_markup=ReplyKeyboardRemove())
    await ask_web_verification(update.message)

async def my_referrals_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_user or not update.message: return
    msg = await update.message.reply_text(Messages.LOADING)
    user_info = await get_user_from_db(update.effective_user.id)
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
    keyboard = ReplyKeyboardMarkup.from_button(KeyboardButton(text="🔒 اضغط هنا للتحقق من جهازك", web_app=WebAppInfo(url=Config.WEB_APP_URL)), resize_keyboard=True)
    await message.reply_text(Messages.WEB_VERIFY_PROMPT, reply_markup=keyboard)

async def web_app_data_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_user or not update.message or not update.message.web_app_data: return
    user_id = update.effective_user.id
    try:
        data = json.loads(update.message.web_app_data.data)
        device_id = data.get("visitorId")
    except (json.JSONDecodeError, AttributeError):
        await update.message.reply_text(Messages.GENERIC_ERROR + " (بيانات تحقق تالفة)", reply_markup=ReplyKeyboardRemove())
        return
    if not device_id:
        await update.message.reply_text(Messages.GENERIC_ERROR + " (لم يتم استلام بصمة الجهاز)", reply_markup=ReplyKeyboardRemove())
        return
    
    logger.info(f"Received device_id {device_id} for user {user_id}. Verifying uniqueness...")
    try:
        device_usage_res = await run_sync_db(lambda: supabase.table('referrals').select('referred_user_id').eq('device_id', device_id).neq('referred_user_id', user_id).limit(1).single().execute())
        if device_usage_res.data:
            original_user_id = device_usage_res.data.get('referred_user_id')
            logger.warning(f"Abuse: User {user_id} trying to use device {device_id} already registered to {original_user_id}.")
            await update.message.reply_text(Messages.REFERRAL_ABUSE_DEVICE_USED, reply_markup=ReplyKeyboardRemove())
            return
        
        referrer_id = context.user_data.get('referrer_id')
        existing_ref = await run_sync_db(lambda: supabase.table('referrals').select('*').eq('referred_user_id', user_id).single().execute())
        
        if not existing_ref.data and referrer_id:
            await modify_referral_count(user_id=referrer_id, fake_delta=1)
            logger.info(f"New user {user_id} under referrer {referrer_id}. Added +1 fake referral.")

        await add_referral_mapping_in_db(user_id, referrer_id, device_id)
        phone_button = [[KeyboardButton("اضغط هنا لمشاركة رقم هاتفك", request_contact=True)]]
        await update.message.reply_text(Messages.PHONE_PROMPT, reply_markup=ReplyKeyboardMarkup(phone_button, resize_keyboard=True, one_time_keyboard=True))
    except Exception as e:
        logger.error(f"Error in web_app_data_handler for user {user_id}: {e}", exc_info=True)
        await update.message.reply_text(Messages.GENERIC_ERROR, reply_markup=ReplyKeyboardRemove())

async def handle_contact(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.message.contact or update.effective_chat.type != Chat.PRIVATE: return
    contact = update.message.contact
    if contact.user_id != update.effective_user.id:
        await update.message.reply_text(Messages.PHONE_INVALID, reply_markup=ReplyKeyboardRemove())
        return
    phone_number = contact.phone_number.lstrip('+')
    if any(phone_number.startswith(code) for code in Config.ALLOWED_COUNTRY_CODES):
        await update.message.reply_text(Messages.PHONE_SUCCESS, reply_markup=ReplyKeyboardRemove())
        keyboard = [[InlineKeyboardButton("1. الانضمام للقناة", url=Config.CHANNEL_URL)], [InlineKeyboardButton("✅ لقد انضممت، تحقق الآن", callback_data=Callback.CONFIRM_JOIN)]]
        await update.message.reply_text(Messages.JOIN_PROMPT, reply_markup=InlineKeyboardMarkup(keyboard))
    else:
        await update.message.reply_text(Messages.COUNTRY_NOT_ALLOWED, reply_markup=ReplyKeyboardRemove())
        await ask_web_verification(update.message)

async def handle_chat_member_updates(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    result = update.chat_member
    if not result: return
    user = result.new_chat_member.user
    was_member = result.old_chat_member.status in {'member', 'administrator', 'creator'}
    is_no_longer_member = result.new_chat_member.status in {'left', 'kicked'}
    if was_member and is_no_longer_member:
        logger.info(f"User {user.id} left/was kicked from chat {result.chat.id}.")
        db_user = await get_user_from_db(user.id)
        if db_user and db_user.get('is_verified'):
            await upsert_user_in_db({'user_id': user.id, 'is_verified': False})
            referrer_id = await get_referrer(user.id)
            if referrer_id:
                try:
                    updated_referrer = await modify_referral_count(user_id=referrer_id, real_delta=-1, fake_delta=1)
                    if updated_referrer:
                        new_real_count = updated_referrer.get('total_real', 0)
                        mention = await get_user_mention(user.id, context)
                        await context.bot.send_message(chat_id=referrer_id, text=Messages.LEAVE_NOTIFICATION.format(mention=mention, new_real_count=new_real_count), parse_mode=ParseMode.MARKDOWN_V2)
                except TelegramError as e:
                    logger.warning(f"Could not send leave notification to referrer {referrer_id}: {e}")

async def handle_admin_messages(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_user or update.effective_user.id not in Config.BOT_OWNER_IDS or not update.message: return
    state = context.user_data.get('state')
    text = update.message.text
    # ... Rest of the admin message handling logic ...

# New helper functions for button_handler
async def handle_button_press_my_referrals(query: CallbackQuery) -> None:
    if not query.from_user: return
    await query.edit_message_text(Messages.LOADING)
    user_info = await get_user_from_db(query.from_user.id)
    text = get_referral_stats_text(user_info)
    await query.edit_message_text(text, parse_mode=ParseMode.MARKDOWN_V2, reply_markup=get_main_menu_keyboard(query.from_user.id))

async def handle_button_press_top5(query: CallbackQuery, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not query.from_user: return
    await query.edit_message_text(Messages.LOADING)
    text = await get_top_5_text(query.from_user.id, context)
    try:
        await query.edit_message_text(text, parse_mode=ParseMode.MARKDOWN_V2, reply_markup=get_main_menu_keyboard(query.from_user.id), disable_web_page_preview=True)
    except BadRequest as e:
        if "message is not modified" not in str(e).lower(): raise e

async def handle_button_press_link(query: CallbackQuery, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not query.from_user or not context.bot.username: return
    user_id = query.from_user.id
    text = get_referral_link_text(user_id, context.bot.username)
    await query.edit_message_text(text, parse_mode=ParseMode.MARKDOWN_V2, reply_markup=get_main_menu_keyboard(user_id), disable_web_page_preview=True)
    
async def handle_confirm_join(query: CallbackQuery, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = query.from_user
    await query.edit_message_text(Messages.LOADING)
    if await is_user_in_channel(user.id, context):
        db_user = await get_user_from_db(user.id)
        if not db_user or not db_user.get('is_verified'):
            await upsert_user_in_db({'user_id': user.id, 'is_verified': True})
            referrer_id = await get_referrer(user.id)
            if referrer_id:
                was_already_member = context.user_data.get('was_already_member', False)
                try:
                    if was_already_member:
                        await context.bot.send_message(chat_id=referrer_id, text=Messages.REFERRAL_EXISTING_MEMBER)
                    else:
                        updated_referrer = await modify_referral_count(user_id=referrer_id, real_delta=1, fake_delta=-1)
                        if updated_referrer:
                            new_real_count = updated_referrer.get('total_real', 0)
                            mention = await get_user_mention(user.id, context)
                            await context.bot.send_message(chat_id=referrer_id, text=Messages.REFERRAL_SUCCESS.format(mention=mention, new_real_count=new_real_count), parse_mode=ParseMode.MARKDOWN_V2)
                except TelegramError as e:
                    logger.warning(f"Could not send notification to referrer {referrer_id}: {e}")
        await query.edit_message_text(Messages.JOIN_SUCCESS)
        await query.message.reply_text(Messages.VERIFIED_WELCOME, reply_markup=get_main_menu_keyboard(user.id))
    else:
        await query.answer(text=Messages.JOIN_FAIL, show_alert=True)
        keyboard = [[InlineKeyboardButton("1. الانضمام للقناة", url=Config.CHANNEL_URL)], [InlineKeyboardButton("✅ لقد انضممت، تحقق الآن", callback_data=Callback.CONFIRM_JOIN)]]
        await query.edit_message_text(Messages.JOIN_PROMPT, reply_markup=InlineKeyboardMarkup(keyboard))
        
# ... (all admin helper functions like handle_admin_panel, etc. would be defined here)
async def handle_admin_panel(query: CallbackQuery) -> None:
    await query.edit_message_text(text=Messages.ADMIN_WELCOME, reply_markup=get_admin_panel_keyboard())

# --- Main Callback Router ---
async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query or not query.data or not query.from_user: return
    try:
        await query.answer()
    except BadRequest as e:
        if "Query is too old" not in str(e).lower(): raise e
        else: return
    
    action = query.data
    
    # User Menu
    if action == Callback.MAIN_MENU: await query.edit_message_text(text=Messages.VERIFIED_WELCOME, reply_markup=get_main_menu_keyboard(query.from_user.id))
    elif action == Callback.MY_REFERRALS: await handle_button_press_my_referrals(query)
    elif action == Callback.MY_LINK: await handle_button_press_link(query, context)
    elif action == Callback.TOP_5: await handle_button_press_top5(query, context)
    elif action == Callback.CONFIRM_JOIN: await handle_confirm_join(query, context)
    
    # Admin Panel
    elif query.from_user.id in Config.BOT_OWNER_IDS:
        if action == Callback.ADMIN_PANEL: await handle_admin_panel(query)
        # elif action.startswith(...): # Add routing for all other admin buttons
        # ...

# --- Main Function ---
def main() -> None:
    """Starts the bot."""
    application = Application.builder().token(Config.BOT_TOKEN).build()
    application.add_handler(ChatMemberHandler(handle_chat_member_updates, ChatMemberHandler.CHAT_MEMBER), group=0)
    application.add_handler(CommandHandler("start", start_command), group=1)
    application.add_handler(CommandHandler("invites", my_referrals_command), group=1)
    application.add_handler(CommandHandler("link", link_command), group=1)
    application.add_handler(CommandHandler("top", top_command), group=1)
    application.add_handler(CallbackQueryHandler(button_handler), group=1)
    private_filter = filters.ChatType.PRIVATE
    application.add_handler(MessageHandler(filters.CONTACT & private_filter, handle_contact), group=2)
    application.add_handler(MessageHandler(filters.StatusUpdate.WEB_APP_DATA, web_app_data_handler), group=2)
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND & private_filter, handle_admin_messages), group=2)
    logger.info("Bot is starting...")
    application.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()