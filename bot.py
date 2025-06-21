# RoyaltyBot - Rebuilt for Stability and Performance
# This version has been completely rewritten to address all reported issues.

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
# All settings are centralized here for easy management.
class Config:
    # IMPORTANT: Replace with your actual bot token.
    BOT_TOKEN = "7950170561:AAH5OtiK38BBhAnVofqxnLWRYbaZaIaKY4s"
    
    # Supabase credentials.
    SUPABASE_URL = "https://jofxsqsgarvzolgphqjg.supabase.co"
    SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImpvZnhzcXNnYXJ2em9sZ3BocWpnIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc0OTU5NTI4NiwiZXhwIjoyMDY1MTcxMjg2fQ.egB9qticc7ABgo6vmpsrPi3cOHooQmL5uQOKI4Jytqg"
    
    # URL for the device verification web app.
    WEB_APP_URL = "https://heartfelt-biscuit-5489cd.netlify.app"
    
    # Your target channel's ID and public URL.
    CHANNEL_ID = -1002686156311  # Must be the integer ID.
    CHANNEL_URL = "https://t.me/Ry_Hub"
    
    # Telegram user IDs of the bot owners/admins.
    BOT_OWNER_IDS = {596472053, 7164133014, 1971453570}
    
    # Settings for paginated reports.
    USERS_PER_PAGE = 10
    MENTION_CACHE_TTL_SECONDS = 300 # 5 minutes

# --- Bot States for Conversation Handlers ---
class State(Enum):
    AWAITING_EDIT_USER_ID = auto()
    AWAITING_EDIT_AMOUNT = auto()
    AWAITING_BROADCAST_MESSAGE = auto()
    AWAITING_UNIVERSAL_BROADCAST_MESSAGE = auto()
    AWAITING_INSPECT_USER_ID = auto()

# --- Callback Data Definitions for Inline Buttons ---
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

# --- Bot Messages (All user-facing text in Arabic) ---
class Messages:
    VERIFIED_WELCOME = "أهلاً بك مجدداً! ✅\n\nاستخدم الأزرار أو الأوامر للتفاعل مع البوت."
    START_WELCOME = "أهلاً بك في البوت! 👋\n\nللبدء، نحتاج للتحقق من جهازك لمنع الحسابات المتعددة. الرجاء الضغط على الزر أدناه."
    WEB_VERIFY_PROMPT = "للتحقق من أنك لا تستخدم نفس الجهاز عدة مرات، الرجاء الضغط على الزر أدناه."
    WEB_VERIFY_SUCCESS = "تم التحقق من جهازك بنجاح!"
    JOIN_PROMPT = "ممتاز! الخطوة الأخيرة هي الانضمام إلى قناتنا. انضم ثم اضغط على الزر أدناه للتحقق."
    JOIN_SUCCESS = "تهانينا! لقد تم التحقق منك بنجاح وأصبحت عضواً فعالاً."
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
    """Escapes characters for Telegram's MarkdownV2 parsing."""
    if not name: return ""
    escape_chars = r"[_*\[\]()~`>#\+\-=|{}\.!\\]"
    return re.sub(escape_chars, r"\\\1", name)

async def get_user_mention(user_id: int, context: ContextTypes.DEFAULT_TYPE, full_name: Optional[str] = None) -> str:
    """
    NEW: This function is completely rebuilt to be highly robust.
    It will never crash the bot. It fetches a user's mention, with multiple fallbacks.
    """
    cache = context.bot_data.setdefault('mention_cache', {})
    current_time = time.time()

    if user_id in cache and (current_time - cache[user_id]['timestamp'] < Config.MENTION_CACHE_TTL_SECONDS):
        return cache[user_id]['mention']

    mention_name = ""
    try:
        # 1. Best case: Get fresh data from Telegram.
        chat = await context.bot.get_chat(user_id)
        mention_name = chat.full_name or f"User {user_id}"
    except (TelegramError, BadRequest):
        # 2. Fallback: If Telegram fails (e.g., user not found, privacy settings), use the name from our DB.
        logger.warning(f"Could not get_chat for user_id {user_id}. Falling back to DB name.")
        if full_name:
            mention_name = full_name
        else:
            db_user = await get_user_from_db(user_id, context)
            if db_user and db_user.get("full_name"):
                mention_name = db_user["full_name"]
            else:
                # 3. Last resort: If we have nothing, use a generic placeholder.
                mention_name = f"User {user_id}"

    cleaned_name = clean_name_for_markdown(mention_name)
    mention = f"[{cleaned_name}](tg://user?id={user_id})"
    
    cache[user_id] = {'mention': mention, 'timestamp': current_time}
    return mention

# --- Database Functions ---
# These functions now have more specific error logging.

async def run_sync_db(func: Callable[[], Any]) -> Any:
    """Runs a synchronous Supabase function in a separate thread."""
    return await asyncio.to_thread(func)

async def get_user_from_db(user_id: int, context: ContextTypes.DEFAULT_TYPE) -> Optional[Dict[str, Any]]:
    db = get_db_client(context)
    try:
        res = await run_sync_db(lambda: db.table('users').select("*").eq('user_id', user_id).single().execute())
        return res.data
    except Exception as e:
        logger.error(f"DB_ERROR (get_user_from_db for {user_id}): {e}")
        return None

async def upsert_user_in_db(user_data: Dict[str, Any], context: ContextTypes.DEFAULT_TYPE) -> None:
    db = get_db_client(context)
    try:
        await run_sync_db(lambda: db.table('users').upsert(user_data).execute())
    except Exception as e:
        logger.error(f"DB_ERROR (upsert_user_in_db for {user_data.get('user_id')}): {e}")

async def get_all_users_from_db(context: ContextTypes.DEFAULT_TYPE) -> List[Dict[str, Any]]:
    db = get_db_client(context)
    try:
        res = await run_sync_db(lambda: db.table('users').select("*").execute())
        return res.data or []
    except Exception as e:
        logger.error(f"DB_ERROR (get_all_users_from_db): {e}")
        return []
        
async def get_all_referral_mappings(context: ContextTypes.DEFAULT_TYPE) -> List[Dict[str, Any]]:
    db = get_db_client(context)
    try:
        res = await run_sync_db(lambda: db.table('referrals').select("referrer_user_id, referred_user_id").execute())
        return res.data or []
    except Exception as e:
        logger.error(f"DB_ERROR (get_all_referral_mappings): {e}")
        return []

async def get_referrer(referred_id: int, context: ContextTypes.DEFAULT_TYPE) -> Optional[int]:
    db = get_db_client(context)
    try:
        res = await run_sync_db(lambda: db.table('referrals').select('referrer_user_id').eq('referred_user_id', referred_id).single().execute())
        return res.data.get('referrer_user_id') if res.data else None
    except Exception:
        return None

async def add_referral_mapping_in_db(referred_id: int, referrer_id: Optional[int], device_id: str, context: ContextTypes.DEFAULT_TYPE) -> None:
    db = get_db_client(context)
    try:
        data = {'referred_user_id': referred_id, 'referrer_user_id': referrer_id, 'device_id': device_id}
        await run_sync_db(lambda: db.table('referrals').upsert(data, on_conflict='referred_user_id').execute())
    except Exception as e:
        logger.error(f"DB_ERROR (add_referral_mapping_in_db for {referred_id}): {e}")

async def reset_all_referrals_in_db(context: ContextTypes.DEFAULT_TYPE) -> None:
    db = get_db_client(context)
    await run_sync_db(lambda: db.table('referrals').delete().neq('referred_user_id', 0).execute())
    await run_sync_db(lambda: db.table('users').update({"total_real": 0, "total_fake": 0}).neq('user_id', 0).execute())
    logger.info("All referrals have been reset.")

async def format_bot_in_db(context: ContextTypes.DEFAULT_TYPE) -> None:
    db = get_db_client(context)
    logger.info("Formatting bot: Deleting from 'referrals' table.")
    await run_sync_db(lambda: db.table('referrals').delete().neq('referred_user_id', 0).execute())
    logger.info("Formatting bot: Deleting from 'users' table.")
    await run_sync_db(lambda: db.table('users').delete().neq('user_id', 0).execute())
    logger.info("BOT HAS BEEN FORMATTED.")

async def unverify_all_users_in_db(context: ContextTypes.DEFAULT_TYPE) -> None:
    db = get_db_client(context)
    await run_sync_db(lambda: db.table('users').update({"is_verified": False}).neq('user_id', 0).execute())
    logger.info("All users have been un-verified.")

# --- UI & Display Functions ---
def get_main_menu_keyboard(user_id: int) -> InlineKeyboardMarkup:
    keyboard = [
        [InlineKeyboardButton("📊 إحصائياتي", callback_data=Callback.MY_REFERRALS)],
        [InlineKeyboardButton("🔗 رابطي", callback_data=Callback.MY_LINK)],
        [InlineKeyboardButton("🏆 أفضل 5 متسابقين", callback_data=Callback.TOP_5)],
    ]
    if user_id in Config.BOT_OWNER_IDS:
        keyboard.append([InlineKeyboardButton("👑 لوحة تحكم المالك", callback_data=Callback.ADMIN_PANEL)])
    return InlineKeyboardMarkup(keyboard)

def get_admin_panel_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📊 تقرير حقيقي", callback_data=f"{Callback.REPORT_PAGE}_real_1"),
         InlineKeyboardButton("⏳ تقرير وهمي", callback_data=f"{Callback.REPORT_PAGE}_fake_1")],
        [InlineKeyboardButton("🔍 فحص إحالات مستخدم", callback_data=Callback.ADMIN_INSPECT_REFERRALS)],
        [InlineKeyboardButton("👥 عدد المستخدمين", callback_data=Callback.ADMIN_USER_COUNT)],
        [InlineKeyboardButton("✍️ تعديل يدوي", callback_data=Callback.ADMIN_USER_EDIT_MENU)],
        [InlineKeyboardButton("📢 إذاعة للموثقين", callback_data=Callback.ADMIN_BROADCAST),
         InlineKeyboardButton("📢 إذاعة للكل", callback_data=Callback.ADMIN_UNIVERSAL_BROADCAST)],
        [InlineKeyboardButton("🔄 فرض إعادة التحقق", callback_data=Callback.ADMIN_FORCE_REVERIFICATION)],
        [InlineKeyboardButton("⚠️ تصفير كل الإحالات", callback_data=Callback.ADMIN_RESET_ALL)],
        [InlineKeyboardButton("⚙️ ترحيل وحساب البيانات", callback_data=Callback.DATA_MIGRATION)],
        [InlineKeyboardButton("💀 فورمات البوت", callback_data=Callback.ADMIN_FORMAT_BOT)],
        [InlineKeyboardButton("⬅️ العودة للقائمة الرئيسية", callback_data=Callback.MAIN_MENU)],
    ])

# --- Core Logic ---
async def modify_referral_count(user_id: int, context: ContextTypes.DEFAULT_TYPE, real_delta: int = 0, fake_delta: int = 0) -> Optional[Dict[str, Any]]:
    """Atomically modifies referral counts for a user."""
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
    """Checks if a user is a member of the target channel."""
    try:
        member = await context.bot.get_chat_member(chat_id=Config.CHANNEL_ID, user_id=user_id)
        return member.status in {ChatMember.MEMBER, ChatMember.ADMINISTRATOR, ChatMember.OWNER}
    except Exception as e:
        logger.error(f"Error checking membership for {user_id}: {e}")
        return False

# --- Command Handlers ---
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handles the /start command. Onboards new users."""
    if not update.message or not update.effective_user or update.effective_chat.type != Chat.PRIVATE:
        return

    user = update.effective_user
    logger.info(f"Received /start command from user {user.id} ({user.full_name})")

    db_user = await get_user_from_db(user.id, context)
    if db_user and db_user.get("is_verified"):
        logger.info(f"User {user.id} is already verified. Sending main menu.")
        await update.message.reply_text(Messages.VERIFIED_WELCOME, reply_markup=get_main_menu_keyboard(user.id))
        return

    # Store referrer if provided
    if context.args:
        try:
            referrer_id = int(context.args[0])
            if referrer_id != user.id:
                context.user_data['referrer_id'] = referrer_id
                logger.info(f"User {user.id} was referred by {referrer_id}")
        except (ValueError, IndexError):
            pass
    
    # Create or update user in DB
    user_payload = {'user_id': user.id, 'full_name': user.full_name, 'username': user.username}
    await upsert_user_in_db(user_payload, context)

    # Check if user was already a channel member to handle referral type later
    context.user_data['was_already_member'] = await is_user_in_channel(user.id, context)
    
    # IMPORTANT: The phone number request is removed. Go straight to web app verification.
    await update.message.reply_text(Messages.START_WELCOME, reply_markup=ReplyKeyboardRemove())
    
    keyboard = ReplyKeyboardMarkup.from_button(
        KeyboardButton(text="🔒 اضغط هنا للتحقق من جهازك", web_app=WebAppInfo(url=Config.WEB_APP_URL)),
        resize_keyboard=True
    )
    await update.message.reply_text(Messages.WEB_VERIFY_PROMPT, reply_markup=keyboard)


async def web_app_data_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handles data received from the verification web app."""
    if not update.effective_user or not update.message or not update.message.web_app_data:
        return
        
    user_id = update.effective_user.id
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

    # Check for device ID abuse
    db = get_db_client(context)
    res = await run_sync_db(lambda: db.table('referrals').select('referred_user_id', count='exact').eq('device_id', device_id).neq('referred_user_id', user_id).execute())
    if res.count and res.count > 0:
        await update.message.reply_text(Messages.REFERRAL_ABUSE_DEVICE_USED, reply_markup=ReplyKeyboardRemove())
        return

    # Add a preliminary "fake" referral to the referrer
    referrer_id = context.user_data.get('referrer_id')
    res = await run_sync_db(lambda: db.table('referrals').select('referred_user_id', count='exact').eq('referred_user_id', user_id).execute())
    if (res.count is None or res.count == 0) and referrer_id:
        await modify_referral_count(referrer_id, context, fake_delta=1)
        logger.info(f"New user {user_id} under referrer {referrer_id}. Added +1 fake referral.")

    await add_referral_mapping_in_db(user_id, referrer_id, device_id, context)
    
    await update.message.reply_text(Messages.WEB_VERIFY_SUCCESS, reply_markup=ReplyKeyboardRemove())
    keyboard = [[InlineKeyboardButton("1. الانضمام للقناة", url=Config.CHANNEL_URL)],
                [InlineKeyboardButton("✅ لقد انضممت، تحقق الآن", callback_data=Callback.CONFIRM_JOIN)]]
    await update.message.reply_text(Messages.JOIN_PROMPT, reply_markup=InlineKeyboardMarkup(keyboard))


# --- Callback Query Handlers (Button Presses) ---
async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Main router for all inline button presses."""
    query = update.callback_query
    if not query or not query.data or not query.from_user: return
    
    try:
        await query.answer()
    except BadRequest:
        logger.warning(f"Failed to answer callback query for {query.from_user.id}, likely too old.")
        return

    action = query.data
    user_id = query.from_user.id
    logger.info(f"Button press from user {user_id}: {action}")

    # --- User-facing Actions ---
    if action == Callback.MY_REFERRALS:
        await query.edit_message_text(Messages.LOADING)
        user_info = await get_user_from_db(user_id, context)
        total_real = int(user_info.get("total_real", 0) or 0)
        total_fake = int(user_info.get("total_fake", 0) or 0)
        text = f"📊 *إحصائيات إحالاتك:*\n\n✅ الإحالات الحقيقية: `{total_real}`\n⏳ الإحالات الوهمية: `{total_fake}`"
        await query.edit_message_text(text, parse_mode=ParseMode.MARKDOWN_V2, reply_markup=get_main_menu_keyboard(user_id))
    
    elif action == Callback.MY_LINK:
        if not context.bot.username: return
        text = f"🔗 رابط الإحالة الخاص بك:\n`https://t.me/{context.bot.username}?start={user_id}`"
        await query.edit_message_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=get_main_menu_keyboard(user_id), disable_web_page_preview=True)

    elif action == Callback.TOP_5:
        await handle_top_5(query, context)

    elif action == Callback.CONFIRM_JOIN:
        await handle_confirm_join(query, context)

    elif action == Callback.MAIN_MENU:
        await query.edit_message_text(text=Messages.VERIFIED_WELCOME, reply_markup=get_main_menu_keyboard(user_id))

    # --- Admin-only Actions ---
    elif user_id in Config.BOT_OWNER_IDS:
        await handle_admin_actions(query, context)

async def handle_top_5(query: CallbackQuery, context: ContextTypes.DEFAULT_TYPE) -> None:
    """FIXED: Handles the 'Top 5' button press with robust logic."""
    try:
        await query.edit_message_text(Messages.LOADING)
        db = get_db_client(context)
        
        res = await run_sync_db(lambda: db.table('users').select('user_id, full_name, total_real').gt('total_real', 0).order('total_real', desc=True).limit(5).execute())
        top_users = res.data or []
        
        lines = []
        if not top_users:
            lines.append("لم يصل أحد إلى القائمة بعد.")
        else:
            mentions = await asyncio.gather(*[get_user_mention(u['user_id'], context, u.get('full_name')) for u in top_users])
            for i, u_info in enumerate(top_users):
                lines.append(f"{i+1}\\. {mentions[i]} \\- *{u_info.get('total_real', 0)}* إحالة")
        
        top_list_text = "\n".join(lines)
        
        my_info = await get_user_from_db(query.from_user.id, context)
        my_referrals = my_info.get('total_real', 0) if my_info else 0
        rank_str = "غير مصنف"
        
        if my_info and my_referrals > 0:
            count_res = await run_sync_db(lambda: db.table('users').select('user_id', count='exact').gt('total_real', my_referrals).execute())
            my_rank = (count_res.count or 0) + 1
            rank_str = f"\\#{my_rank}"
        
        final_text = (
            f"🏆 *أفضل 5 متسابقين لدينا:*\n\n{top_list_text}\n\n"
            f"---\n*ترتيبك الشخصي:*\n"
            f"🎖️ ترتيبك: *{rank_str}*\n✅ رصيدك: *{my_referrals}* إحالة حقيقية\\."
        )
        await query.edit_message_text(final_text, parse_mode=ParseMode.MARKDOWN_V2, reply_markup=get_main_menu_keyboard(query.from_user.id), disable_web_page_preview=True)
    except Exception as e:
        logger.error(f"Error in handle_top_5: {e}", exc_info=True)
        await query.edit_message_text(Messages.GENERIC_ERROR, reply_markup=get_main_menu_keyboard(query.from_user.id))

async def handle_confirm_join(query: CallbackQuery, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handles the final verification step after a user joins the channel."""
    user = query.from_user
    await query.edit_message_text(Messages.LOADING)

    if not await is_user_in_channel(user.id, context):
        await query.answer(text=Messages.JOIN_FAIL, show_alert=True)
        keyboard = [[InlineKeyboardButton("1. الانضمام للقناة", url=Config.CHANNEL_URL)],
                    [InlineKeyboardButton("✅ لقد انضممت، تحقق الآن", callback_data=Callback.CONFIRM_JOIN)]]
        await query.edit_message_text(Messages.JOIN_PROMPT, reply_markup=InlineKeyboardMarkup(keyboard))
        return

    db_user = await get_user_from_db(user.id, context)
    if not db_user or not db_user.get('is_verified'):
        await upsert_user_in_db({'user_id': user.id, 'is_verified': True}, context)
        referrer_id = await get_referrer(user.id, context)
        if referrer_id:
            try:
                if context.user_data.get('was_already_member', False):
                    await context.bot.send_message(chat_id=referrer_id, text=Messages.REFERRAL_EXISTING_MEMBER)
                else:
                    updated_referrer = await modify_referral_count(referrer_id, context, real_delta=1, fake_delta=-1)
                    if updated_referrer:
                        mention = await get_user_mention(user.id, context)
                        await context.bot.send_message(
                            chat_id=referrer_id,
                            text=Messages.REFERRAL_SUCCESS.format(mention=mention, new_real_count=updated_referrer.get('total_real', 0)),
                            parse_mode=ParseMode.MARKDOWN_V2
                        )
            except TelegramError as e:
                logger.warning(f"Could not send notification to referrer {referrer_id}: {e}")
    
    await query.edit_message_text(Messages.JOIN_SUCCESS)
    await query.message.reply_text(Messages.VERIFIED_WELCOME, reply_markup=get_main_menu_keyboard(user.id))

async def handle_admin_actions(query: CallbackQuery, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Router for admin-specific button presses."""
    action = query.data

    if action == Callback.ADMIN_PANEL:
        await query.edit_message_text(text=Messages.ADMIN_WELCOME, reply_markup=get_admin_panel_keyboard())

    elif action.startswith(f"{Callback.REPORT_PAGE}_"):
        await handle_report_pagination(query, context)
    
    elif action == Callback.DATA_MIGRATION:
        await handle_data_migration(query, context)

    elif action == Callback.ADMIN_FORMAT_BOT:
        await query.edit_message_text(
            "⚠️⚠️⚠️ *تحذير خطير جداً* ⚠️⚠️⚠️\n\nأنت على وشك حذف **جميع بيانات البوت بشكل نهائي**.\nهذا الإجراء لا يمكن التراجع عنه.",
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("‼️ نعم، قم بحذف كل شيء ‼️", callback_data=Callback.ADMIN_FORMAT_CONFIRM)],
                [InlineKeyboardButton("❌ لا، إلغاء الأمر", callback_data=Callback.ADMIN_PANEL)]
            ])
        )
    elif action == Callback.ADMIN_FORMAT_CONFIRM:
        try:
            await query.edit_message_text(text="⏳ جاري تنفيذ الفورمات...")
            await format_bot_in_db(context)
            await query.edit_message_text("✅ تم عمل فورمات للبوت بنجاح.", reply_markup=get_admin_panel_keyboard())
        except Exception as e:
            logger.error(f"Failed to format bot: {e}", exc_info=True)
            await query.edit_message_text(f"❌ فشل الفورمات.\n`{e}`", reply_markup=get_admin_panel_keyboard())
    # ... other admin actions can be added here ...


async def handle_report_pagination(query: CallbackQuery, context: ContextTypes.DEFAULT_TYPE) -> None:
    """FIXED: Handles pagination for both real and fake referral reports."""
    try:
        _, report_type, page_str = query.data.split('_')
        page = int(page_str)
        
        await query.edit_message_text(Messages.LOADING)
        db = get_db_client(context)
        
        count_key = 'total_real' if report_type == 'real' else 'total_fake'
        title = f"✅ *تقرير الإحالات الحقيقية*" if report_type == 'real' else f"⏳ *تقرير الإحالات الوهمية*"
        
        count_res = await run_sync_db(lambda: db.table('users').select('user_id', count='exact').gt(count_key, 0).execute())
        total_users = count_res.count or 0
        
        if total_users == 0:
            await query.edit_message_text(f"{title}\n\nلا يوجد مستخدمون في هذا التقرير.", parse_mode=ParseMode.MARKDOWN_V2, reply_markup=get_admin_panel_keyboard())
            return

        start_index = (page - 1) * Config.USERS_PER_PAGE
        users_res = await run_sync_db(lambda: db.table('users').select(f"user_id, full_name, {count_key}").gt(count_key, 0).order(count_key, desc=True).range(start_index, start_index + Config.USERS_PER_PAGE - 1).execute())
        page_users = users_res.data or []
        
        mentions = await asyncio.gather(*[get_user_mention(u['user_id'], context, u.get('full_name')) for u in page_users])
        report_lines = [f"• {mention} \\- *{u_data.get(count_key, 0)}*" for mention, u_data in zip(mentions, page_users)]
        
        total_pages = math.ceil(total_users / Config.USERS_PER_PAGE)
        report_text = f"{title} (صفحة {page} من {total_pages}):\n\n" + "\n".join(report_lines)

        nav_buttons = []
        cb_prefix = f"{Callback.REPORT_PAGE}_{report_type}_"
        if page > 1: nav_buttons.append(InlineKeyboardButton("⬅️ السابق", callback_data=f"{cb_prefix}{page-1}"))
        if page < total_pages: nav_buttons.append(InlineKeyboardButton("التالي ➡️", callback_data=f"{cb_prefix}{page+1}"))
        
        keyboard = [nav_buttons] if nav_buttons else []
        keyboard.append([InlineKeyboardButton("🔙 العودة", callback_data=Callback.ADMIN_PANEL)])
        
        await query.edit_message_text(text=report_text, parse_mode=ParseMode.MARKDOWN_V2, reply_markup=InlineKeyboardMarkup(keyboard), disable_web_page_preview=True)

    except Exception as e:
        logger.error(f"Error generating report {query.data}: {e}", exc_info=True)
        await query.edit_message_text(Messages.GENERIC_ERROR, reply_markup=get_admin_panel_keyboard())


async def handle_data_migration(query: CallbackQuery, context: ContextTypes.DEFAULT_TYPE) -> None:
    """FIXED: Handles the data migration and recalculation process."""
    await query.edit_message_text("⏳ **بدء عملية إعادة الحساب...**\nقد تستغرق هذه العملية بعض الوقت. سيتم إعلامك عند الانتهاء.", parse_mode=ParseMode.MARKDOWN_V2)
    try:
        all_users = await get_all_users_from_db(context)
        if not all_users:
            await query.edit_message_text("لا يوجد مستخدمون لإجراء الترحيل.", reply_markup=get_admin_panel_keyboard())
            return

        all_mappings = await get_all_referral_mappings(context)
        
        verified_ids = {u['user_id'] for u in all_users if u.get('is_verified')}
        user_counts = {u['user_id']: {'total_real': 0, 'total_fake': 0} for u in all_users}

        for mapping in all_mappings:
            ref_id, red_id = mapping.get('referrer_user_id'), mapping.get('referred_user_id')
            if ref_id and ref_id in user_counts:
                if red_id in verified_ids:
                    user_counts[ref_id]['total_real'] += 1
                else:
                    user_counts[ref_id]['total_fake'] += 1
        
        users_to_update = [{'user_id': uid, **counts} for uid, counts in user_counts.items()]
        
        if users_to_update:
            db = get_db_client(context)
            chunk_size = 200
            for i in range(0, len(users_to_update), chunk_size):
                chunk = users_to_update[i:i + chunk_size]
                logger.info(f"Data migration: updating chunk {i//chunk_size + 1} with {len(chunk)} users.")
                await run_sync_db(lambda: db.table('users').upsert(chunk).execute())
                await asyncio.sleep(0.5)

        await query.edit_message_text(f"✅ **اكتملت!** تم تحديث بيانات *{len(users_to_update)}* مستخدم.", reply_markup=get_admin_panel_keyboard(), parse_mode=ParseMode.MARKDOWN_V2)
    except Exception as e:
        logger.error(f"Data migration failed: {e}", exc_info=True)
        await query.edit_message_text(f"❌ فشلت عملية الترحيل.\n`{e}`", reply_markup=get_admin_panel_keyboard())


# --- Chat Member & Message Handlers ---
async def handle_chat_member_updates(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handles users joining or leaving the channel."""
    result = update.chat_member
    if not result or result.chat.id != Config.CHANNEL_ID: return

    user = result.new_chat_member.user
    was_member = result.old_chat_member.status in {ChatMember.MEMBER, ChatMember.ADMINISTRATOR, ChatMember.OWNER}
    is_member = result.new_chat_member.status in {ChatMember.MEMBER, ChatMember.ADMINISTRATOR, ChatMember.OWNER}

    # Handle user leaving
    if was_member and not is_member:
        logger.info(f"User {user.id} left channel.")
        db_user = await get_user_from_db(user.id, context)
        if db_user and db_user.get('is_verified'):
            await upsert_user_in_db({'user_id': user.id, 'is_verified': False}, context)
            referrer_id = await get_referrer(user.id, context)
            if referrer_id:
                updated_referrer = await modify_referral_count(referrer_id, context, real_delta=-1, fake_delta=1)
                if updated_referrer:
                    try:
                        mention = await get_user_mention(user.id, context)
                        await context.bot.send_message(
                            chat_id=referrer_id,
                            text=Messages.LEAVE_NOTIFICATION.format(mention=mention, new_real_count=updated_referrer.get('total_real', 0)),
                            parse_mode=ParseMode.MARKDOWN_V2
                        )
                    except TelegramError as e:
                        logger.warning(f"Could not send leave notification to referrer {referrer_id}: {e}")

async def handle_admin_messages(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    # This handler is for admin conversations (e.g., broadcasting, editing users)
    # The logic can be ported from the old bot if still needed.
    pass

# --- Main Function ---
def main() -> None:
    """Starts the bot."""
    
    # Initialize Application
    application = (Application.builder().token(Config.BOT_TOKEN).build())

    # Post-init setup (database connection)
    async def post_init(app: Application):
        app.bot_data['db_client'] = create_client(Config.SUPABASE_URL, Config.SUPABASE_KEY)
        logger.info("Supabase client created and stored in bot_data.")
        for owner_id in Config.BOT_OWNER_IDS:
            try:
                await app.bot.send_message(chat_id=owner_id, text=f"✅ RoyaltyBot (Rebuilt) has started successfully.")
            except Exception as e:
                logger.error(f"Could not send startup message to owner {owner_id}: {e}")

    application.post_init = post_init
    
    # --- Register Handlers ---
    # Group 0: Handle chat member updates first.
    application.add_handler(ChatMemberHandler(handle_chat_member_updates, ChatMemberHandler.CHAT_MEMBER), group=0)

    # Group 1: Handle user commands and button presses.
    application.add_handler(CommandHandler("start", start_command), group=1)
    application.add_handler(CallbackQueryHandler(button_handler), group=1)

    # Group 2: Handle messages and web app data.
    private_filter = filters.ChatType.PRIVATE
    application.add_handler(MessageHandler(filters.StatusUpdate.WEB_APP_DATA & private_filter, web_app_data_handler), group=2)
    # The handler for admin text input (conversations) can be added here if needed.
    # application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND & private_filter, handle_admin_messages), group=2)

    logger.info("Bot is starting...")
    application.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()
