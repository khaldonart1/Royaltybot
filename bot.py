import asyncio
import logging
import math
import random
import re
import time
from enum import Enum, auto
from typing import Any, Awaitable, Callable, Dict, List, Optional, Set, Tuple

from telegram import (
    CallbackQuery,
    Chat,
    ChatMemberUpdated,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
    Update,
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
    CHANNEL_ID = -1002686156311
    GROUP_ID = -1002472491601
    CHANNEL_URL = "https://t.me/Ry_Hub"
    GROUP_URL = "https://t.me/joinchat/Rrx4fWReNLxlYWNk"
    BOT_OWNER_IDS = {596472053, 7164133014, 1971453570}
    ALLOWED_COUNTRY_CODES = {
        "213", "973", "269", "253", "20", "964", "962", "965", "961",
        "218", "222", "212", "968", "970", "974", "966", "252", "249",
        "963", "216", "971", "967"
    }
    USERS_PER_PAGE = 15
    CACHE_TTL_SECONDS = 60
    MENTION_CACHE_TTL_SECONDS = 300 # Cache for user mentions (5 minutes)


# --- حالات البوت (State) ---
class State(Enum):
    AWAITING_BROADCAST_MESSAGE = auto()
    AWAITING_WINNER_THRESHOLD = auto()
    AWAITING_CHECK_USER_ID = auto()
    AWAITING_EDIT_USER_ID = auto()
    AWAITING_EDIT_AMOUNT = auto()
    AWAITING_REAL_REFERRAL_LIST_USER_ID = auto()
    AWAITING_FAKE_REFERRAL_LIST_USER_ID = auto()

# --- تعريفات أزرار الكيبورد (Callback) ---
class Callback(Enum):
    MAIN_MENU = "main_menu"
    MY_REFERRALS = "my_referrals"
    MY_LINK = "my_link"
    TOP_5 = "top_5"
    CONFIRM_JOIN = "confirm_join"
    ADMIN_PANEL = "admin_panel"
    ADMIN_USER_COUNT = "admin_user_count"
    PICK_WINNER = "pick_winner"
    ADMIN_CHECKER = "admin_checker"
    ADMIN_BOOO_MENU = "admin_booo_menu"
    ADMIN_BROADCAST = "admin_broadcast"
    ADMIN_RESET_ALL = "admin_reset_all"
    ADMIN_RESET_CONFIRM = "admin_reset_confirm"
    ADMIN_CHECK_ALL = "admin_check_all"
    ADMIN_CHECK_ONE = "admin_check_one"
    ADMIN_RECHECK_LEAVERS = "admin_recheck_leavers"
    ADMIN_USER_EDIT_MENU = "admin_user_edit_menu"
    USER_ADD_MANUAL = "user_add_manual"
    USER_REMOVE_MANUAL = "user_remove_manual"
    ADMIN_GET_REAL_REFERRALS_LIST = "admin_get_real_referrals"
    ADMIN_GET_FAKE_REFERRALS_LIST = "admin_get_fake_referrals"
    REPORT_PAGE = "report_"

# --- رسائل البوت (Messages) ---
class Messages:
    VERIFIED_WELCOME = "أهلاً بك مجدداً! ✅\n\nاستخدم الأزرار أو الأوامر (/) للتفاعل مع البوت."
    START_WELCOME = "أهلاً بك في البوت! 👋\n\nيجب عليك إتمام خطوات بسيطة للتحقق أولاً."
    MATH_QUESTION = "الرجاء حل هذه المسألة الرياضية البسيطة للمتابعة:"
    PHONE_REQUEST = "رائع! الآن، من فضلك شارك رقم هاتفك لإكمال عملية التحقق."
    JOIN_PROMPT = "ممتاز! الخطوة الأخيرة هي الانضمام إلى قناتنا ومجموعتنا. انضم ثم اضغط على الزر أدناه."
    JOIN_SUCCESS = "تهانينا! لقد تم التحقق منك بنجاح."
    JOIN_FAIL = "❌ لم تنضم بعد. الرجاء الانضمام إلى القناة والمجموعة ثم حاول مرة أخرى."
    INVALID_COUNTRY_CODE = "عذراً، هذا البوت مخصص فقط للمستخدمين من الدول العربية. رقمك غير مدعوم."
    GENERIC_ERROR = "حدث خطأ ما. يرجى المحاولة مرة أخرى لاحقاً."
    LOADING = "⏳ جاري التحميل..."
    ADMIN_WELCOME = "👑 أهلاً بك في لوحة تحكم المالك."
    INVALID_INPUT = "إدخال غير صالح. الرجاء المحاولة مرة أخرى."

# --- الاتصال بقاعدة البيانات (Supabase) ---
try:
    supabase: Client = create_client(Config.SUPABASE_URL, Config.SUPABASE_KEY)
    logger.info("Successfully connected to Supabase.")
except Exception as e:
    logger.critical(f"FATAL: Failed to connect to Supabase. Error: {e}")
    exit(1)

# --- دوال مساعدة (Helper Functions) ---
def clean_name_for_markdown(name: str) -> str:
    """إزالة الحروف الخاصة من الاسم لمنع أخطاء الماركداون"""
    if not name: return ""
    return re.sub(r"([*_`\[\]\(\)])", "", name)

async def get_user_mention(user_id: int, context: ContextTypes.DEFAULT_TYPE) -> str:
    """جلب منشن للمستخدم مع استخدام الكاش لتحسين الأداء"""
    cache = context.bot_data.setdefault('mention_cache', {})
    current_time = time.time()
    
    if user_id in cache and (current_time - cache[user_id]['timestamp'] < Config.MENTION_CACHE_TTL_SECONDS):
        return cache[user_id]['mention']

    try:
        chat = await context.bot.get_chat(user_id)
        full_name = clean_name_for_markdown(chat.full_name)
        mention = f"[{full_name}](tg://user?id={user_id})"
        
        cache[user_id] = {'mention': mention, 'timestamp': current_time}

        db_user_info = await get_user_from_db(user_id)
        if db_user_info and (chat.full_name != db_user_info.get('full_name') or chat.username != db_user_info.get('username')):
            context.job_queue.run_once(lambda _: upsert_user_in_db({'user_id': user_id, 'full_name': chat.full_name, 'username': chat.username}), 0)
        
        return mention
    except Exception:
        db_user_info = await get_user_from_db(user_id)
        if db_user_info:
            full_name = clean_name_for_markdown(db_user_info.get("full_name", f"User {user_id}"))
            mention = f"[{full_name}](tg://user?id={user_id})"
            cache[user_id] = {'mention': mention, 'timestamp': current_time}
            return mention
        return f"[User {user_id}](tg://user?id={user_id})"


# --- دوال التعامل مع قاعدة البيانات (Database Functions) ---
async def run_sync_db(func: Callable[[], Any]) -> Any:
    """تشغيل دوال Supabase المتزامنة في خيط منفصل لتجنب حظر الحلقة"""
    return await asyncio.to_thread(func)

async def get_user_from_db(user_id: int) -> Optional[Dict[str, Any]]:
    """جلب مستخدم واحد من قاعدة البيانات"""
    try:
        res = await run_sync_db(
            lambda: supabase.table('users').select("user_id, full_name, username, real_referrals, fake_referrals, is_verified, manual_referrals").eq('user_id', user_id).single().execute()
        )
        return res.data
    except Exception:
        return None

async def upsert_user_in_db(user_data: Dict[str, Any]) -> None:
    """إضافة أو تحديث مستخدم في قاعدة البيانات"""
    try:
        await run_sync_db(lambda: supabase.table('users').upsert(user_data, on_conflict='user_id').execute())
    except Exception as e:
        logger.error(f"DB_ERROR: Upserting user {user_data.get('user_id')}: {e}")

async def upsert_users_batch(users_data: List[Dict[str, Any]]) -> None:
    """إضافة أو تحديث دفعة من المستخدمين"""
    if not users_data: return
    try:
        await run_sync_db(lambda: supabase.table('users').upsert(users_data, on_conflict='user_id').execute())
    except Exception as e:
        logger.error(f"DB_ERROR: Batch upserting {len(users_data)} users: {e}")

async def get_all_users_from_db() -> List[Dict[str, Any]]:
    """جلب كل المستخدمين من قاعدة البيانات"""
    try:
        res = await run_sync_db(
            lambda: supabase.table('users').select(
                "user_id, full_name, username, real_referrals, fake_referrals, is_verified, manual_referrals"
            ).execute()
        )
        return res.data or []
    except Exception as e:
        logger.error(f"DB_ERROR (get_all_users_from_db): {e}")
        return []

async def get_referrer(referred_id: int) -> Optional[int]:
    """جلب المُحيل لمستخدم معين"""
    try:
        res = await run_sync_db(
            lambda: supabase.table('referrals').select('referrer_user_id').eq('referred_user_id', referred_id).single().execute()
        )
        return res.data.get('referrer_user_id') if res.data else None
    except Exception:
        return None
    
async def get_referrals_for_user(referrer_id: int) -> List[Dict[str, Any]]:
    """جلب كل الإحالات لمستخدم معين"""
    try:
        res = await run_sync_db(
            lambda: supabase.table('referrals')
            .select('referred_user_id')
            .eq('referrer_user_id', referrer_id)
            .execute()
        )
        return res.data or []
    except Exception as e:
        logger.error(f"DB_ERROR (get_referrals_for_user for {referrer_id}): {e}")
        return []

async def get_all_referral_mappings() -> List[Dict[str, Any]]:
    """جلب كل علاقات الإحالة من قاعدة البيانات"""
    try:
        res = await run_sync_db(
            lambda: supabase.table('referrals').select("referrer_user_id, referred_user_id").execute()
        )
        return res.data or []
    except Exception as e:
        logger.error(f"DB_ERROR (get_all_referral_mappings): {e}")
        return []

async def add_referral_mapping(referred_id: int, referrer_id: int) -> None:
    """إضافة علاقة إحالة جديدة"""
    try:
        data = {'referred_user_id': referred_id, 'referrer_user_id': referrer_id}
        await run_sync_db(lambda: supabase.table('referrals').upsert(data, on_conflict='referred_user_id').execute())
    except Exception as e:
        logger.error(f"DB_ERROR: Adding referral map for {referred_id} by {referrer_id}: {e}")

async def reset_all_referrals_in_db() -> None:
    """تصفير كل الإحالات في قاعدة البيانات"""
    try:
        # We only reset the referral mappings. The periodic job will fix the counts.
        await run_sync_db(lambda: supabase.table('referrals').delete().gt('referred_user_id', 0).execute())
        # Also reset manual referrals
        await run_sync_db(lambda: supabase.table('users').update({"manual_referrals": 0, "real_referrals": 0, "fake_referrals": 0}).gt('user_id', 0).execute())
        logger.info("All referrals have been reset in the database.")
    except Exception as e:
        logger.error(f"DB_ERROR: Resetting all referrals: {e}")

# --- دوال العرض والمنطق الأساسي (Core Logic & Display Functions) ---

async def get_accurate_referral_counts(context: ContextTypes.DEFAULT_TYPE) -> Dict[int, Dict[str, int]]:
    """
    **دالة جديدة ومحورية**
    تحسب عدد الإحالات الحقيقي والدقيق لكل مستخدم مباشرة من مصدر البيانات الأساسي.
    هذا يضمن أن التقارير دقيقة دائمًا.
    """
    all_users = await get_all_users_from_db()
    all_mappings = await get_all_referral_mappings()
    
    if not all_users:
        return {}

    verified_ids = {u['user_id'] for u in all_users if u.get('is_verified')}
    
    # بناء قاموس لحسابات الإحالات
    accurate_counts = {}
    for user in all_users:
        user_id = user['user_id']
        manual_referrals = int(user.get('manual_referrals', 0) or 0)
        accurate_counts[user_id] = {
            'organic_real': 0,
            'fake': 0,
            'manual': manual_referrals,
            'total_real': manual_referrals,
            'user_info': user # Keep original user info
        }

    # حساب الإحالات العضوية (الحقيقية والوهمية)
    for mapping in all_mappings:
        referrer_id = mapping.get('referrer_user_id')
        referred_id = mapping.get('referred_user_id')
        
        if referrer_id and referrer_id in accurate_counts:
            if referred_id in verified_ids:
                accurate_counts[referrer_id]['organic_real'] += 1
            else:
                accurate_counts[referrer_id]['fake'] += 1
    
    # حساب المجموع النهائي
    for user_id, counts in accurate_counts.items():
        counts['total_real'] = counts['organic_real'] + counts['manual']
        
    return accurate_counts


def get_referral_stats_text(user_id: int, accurate_counts: Dict[int, Dict[str, int]]) -> str:
    """توليد نص إحصائيات الإحالة للمستخدم"""
    user_stats = accurate_counts.get(user_id)
    if not user_stats: return "لا توجد لديك بيانات بعد. حاول مرة أخرى."
    
    total_real = user_stats.get("total_real", 0)
    fake = user_stats.get("fake", 0)
    return f"📊 *إحصائيات إحالاتك (دقيقة):*\n\n✅ الإحالات الحقيقية: *{total_real}*\n⏳ الإحالات الوهمية: *{fake}*"

def get_referral_link_text(user_id: int, bot_username: str) -> str:
    """توليد رابط الإحالة"""
    return f"🔗 رابط الإحالة الخاص بك:\n`https://t.me/{bot_username}?start={user_id}`"

async def get_top_5_text(user_id: int, context: ContextTypes.DEFAULT_TYPE) -> str:
    """
    **تم التعديل**
    تستخدم الآن الدالة الجديدة `get_accurate_referral_counts` لضمان دقة القائمة.
    """
    msg = "🏆 *أفضل 5 متسابقين لدينا (بيانات حقيقية):*\n\n"
    
    accurate_counts = await get_accurate_referral_counts(context)
    if not accurate_counts:
        return msg + "لم يصل أحد إلى القائمة بعد. كن أنت الأول!\n\n---\n*ترتيبك الشخصي:*\nلا يمكن عرض ترتيبك حالياً."

    # تحويل القاموس إلى قائمة للفرز
    full_sorted_list = sorted(accurate_counts.values(), key=lambda u: u['total_real'], reverse=True)
    
    top_5_users = [u for u in full_sorted_list if u['total_real'] > 0][:5]

    if not top_5_users:
        msg += "لم يصل أحد إلى القائمة بعد. كن أنت الأول!\n"
    else:
        mentions = await asyncio.gather(*[get_user_mention(u['user_info']['user_id'], context) for u in top_5_users])
        for i, u_data in enumerate(top_5_users):
            mention = mentions[i]
            count = u_data['total_real']
            msg += f"{i+1}. {mention} - *{count}* إحالة\n"
    
    msg += "\n---\n*ترتيبك الشخصي:*\n"
    try:
        user_index = next((i for i, u in enumerate(full_sorted_list) if u['user_info'].get('user_id') == user_id), -1)
        my_referrals = 0
        if user_index != -1:
            rank_str = f"#{user_index + 1}"
            my_referrals = full_sorted_list[user_index]['total_real']
        else:
            rank_str = "غير مصنف"
        
        msg += f"🎖️ ترتيبك: *{rank_str}*\n✅ رصيدك: *{my_referrals}* إحالة حقيقية."
    except Exception as e:
        logger.error(f"Error getting user rank for {user_id}: {e}")
        msg += "لا يمكن عرض ترتيبك حالياً."
        
    return msg

async def get_paginated_report(page: int, report_type: str, context: ContextTypes.DEFAULT_TYPE) -> Tuple[str, InlineKeyboardMarkup]:
    """
    **تم التعديل**
    تستخدم الآن الدالة الجديدة `get_accurate_referral_counts` لضمان دقة التقارير.
    """
    accurate_counts = await get_accurate_referral_counts(context)
    if not accurate_counts:
        return "لا يوجد أي مستخدمين في هذا التقرير حالياً.", get_admin_panel_keyboard()
    
    all_users_data = list(accurate_counts.values())

    if report_type == 'real':
        filtered_users = [u for u in all_users_data if u['total_real'] > 0]
        filtered_users.sort(key=lambda u: u['total_real'], reverse=True)
        title = "📊 *تقرير الإحالات الحقيقية (محسوب بدقة)*"
    else: # fake
        filtered_users = [u for u in all_users_data if u['fake'] > 0]
        filtered_users.sort(key=lambda u: u['fake'], reverse=True)
        title = "⏳ *تقرير الإحالات الوهمية (محسوب بدقة)*"

    if not filtered_users:
        return f"لا يوجد أي مستخدمين في هذا التقرير ({report_type}) حالياً.", get_admin_panel_keyboard()

    start_index = (page - 1) * Config.USERS_PER_PAGE
    end_index = start_index + Config.USERS_PER_PAGE
    page_users = filtered_users[start_index:end_index]
    total_pages = math.ceil(len(filtered_users) / Config.USERS_PER_PAGE)

    report = f"{title} (صفحة {page} من {total_pages}):\n\n"
    
    mentions = await asyncio.gather(*[get_user_mention(u['user_info']['user_id'], context) for u in page_users])
    
    for i, u_data in enumerate(page_users):
        mention = mentions[i]
        count = u_data['total_real'] if report_type == 'real' else u_data['fake']
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
        [InlineKeyboardButton("📊 تقرير الإحالات الحقيقية", callback_data=f"{Callback.REPORT_PAGE.value}real_page_1")],
        [InlineKeyboardButton("⏳ تقرير الإحالات الوهمية", callback_data=f"{Callback.REPORT_PAGE.value}fake_page_1")],
        [InlineKeyboardButton("👥 عدد مستخدمي البوت", callback_data=Callback.ADMIN_USER_COUNT.value)],
        [InlineKeyboardButton("🏆 اختيار فائز عشوائي", callback_data=Callback.PICK_WINNER.value)],
        [
            InlineKeyboardButton("📜 عرض حقيقي", callback_data=Callback.ADMIN_GET_REAL_REFERRALS_LIST.value),
            InlineKeyboardButton("📜 عرض وهمي", callback_data=Callback.ADMIN_GET_FAKE_REFERRALS_LIST.value)
        ],
        [InlineKeyboardButton("Checker 🔫", callback_data=Callback.ADMIN_CHECKER.value)],
        [InlineKeyboardButton("Booo 👾", callback_data=Callback.ADMIN_BOOO_MENU.value)],
        [InlineKeyboardButton("📢 إرسال رسالة للجميع", callback_data=Callback.ADMIN_BROADCAST.value)],
        [InlineKeyboardButton("⚠️ تصفير كل الإحالات ⚠️", callback_data=Callback.ADMIN_RESET_ALL.value)],
        [InlineKeyboardButton("⬅️ العودة للقائمة الرئيسية", callback_data=Callback.MAIN_MENU.value)],
    ])

def get_checker_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔄 مزامنة إحصائيات الجميع", callback_data=Callback.ADMIN_CHECK_ALL.value)],
        [InlineKeyboardButton("👤 فحص مستخدم محدد", callback_data=Callback.ADMIN_CHECK_ONE.value)],
        [InlineKeyboardButton("🔙 العودة للوحة التحكم", callback_data=Callback.ADMIN_PANEL.value)]
    ])

def get_booo_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔄 إعادة فحص المغادرين", callback_data=Callback.ADMIN_RECHECK_LEAVERS.value)],
        [InlineKeyboardButton("✍️ تعديل إحالات مستخدم (يدوي)", callback_data=Callback.ADMIN_USER_EDIT_MENU.value)],
        [InlineKeyboardButton("🔙 العودة للوحة التحكم", callback_data=Callback.ADMIN_PANEL.value)]
    ])

def get_user_edit_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("➕ زيادة إحالات (يدوي)", callback_data=Callback.USER_ADD_MANUAL.value)],
        [InlineKeyboardButton("➖ خصم إحالات (يدوي)", callback_data=Callback.USER_REMOVE_MANUAL.value)],
        [InlineKeyboardButton("🔙 العودة لقائمة Booo", callback_data=Callback.ADMIN_BOOO_MENU.value)]
    ])

def get_reset_confirmation_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ نعم، قم بالتصفير", callback_data=Callback.ADMIN_RESET_CONFIRM.value)],
        [InlineKeyboardButton("❌ لا، الغِ الأمر", callback_data=Callback.ADMIN_PANEL.value)]
    ])

# --- دوال التحقق من الانضمام والتحقق (Verification Handlers) ---
async def is_user_in_channel_and_group(user_id: int, context: ContextTypes.DEFAULT_TYPE) -> bool:
    try:
        ch_mem = await context.bot.get_chat_member(chat_id=Config.CHANNEL_ID, user_id=user_id)
        if ch_mem.status not in {'member', 'administrator', 'creator'}:
            return False
        
        gr_mem = await context.bot.get_chat_member(chat_id=Config.GROUP_ID, user_id=user_id)
        return gr_mem.status in {'member', 'administrator', 'creator'}
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
    
    user_data = { 
        'user_id': user_id, 
        'full_name': user.full_name,
        'username': user.username,
        'is_verified': False # Ensure is_verified is set initially
    }
    await upsert_user_in_db(user_data)
    
    db_user = await get_user_from_db(user_id)
    
    if db_user and db_user.get("is_verified"):
        await update.message.reply_text(Messages.VERIFIED_WELCOME, reply_markup=get_main_menu_keyboard(user_id))
        return

    args = context.args
    if args:
        try:
            referrer_id = int(args[0])
            if referrer_id != user_id and not await get_referrer(user_id):
                # Only add mapping if it doesn't exist
                await add_referral_mapping(user_id, referrer_id)
                logger.info(f"Referral link used: {user_id} was referred by {referrer_id}")
                # We no longer increment fake referrals here, the reconciliation job will handle it
        except (ValueError, IndexError):
            pass # No valid referrer ID
            
    await update.message.reply_text(Messages.START_WELCOME)
    await ask_math_question(update, context)

async def my_referrals_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_user or not update.message: return
    msg = await update.message.reply_text(Messages.LOADING)
    user_id = update.effective_user.id
    accurate_counts = await get_accurate_referral_counts(context)
    text = get_referral_stats_text(user_id, accurate_counts)
    await msg.edit_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=get_main_menu_keyboard(user_id))

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
    await update.message.reply_text(f"{Messages.MATH_QUESTION}\n\nما هو ناتج {question}؟")

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
                phone_button = [[KeyboardButton("اضغط هنا لمشاركة رقم هاتفك", request_contact=True)]]
                await update.message.reply_text(
                    Messages.PHONE_REQUEST, 
                    reply_markup=ReplyKeyboardMarkup(phone_button, resize_keyboard=True, one_time_keyboard=True)
                )
            else:
                await update.message.reply_text("إجابة خاطئة. حاول مرة اخرى.")
                await ask_math_question(update, context)
        except (ValueError, TypeError):
            await update.message.reply_text("من فضلك أدخل رقماً صحيحاً كإجابة.")

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
            [InlineKeyboardButton("2. الانضمام للمجموعة", url=Config.GROUP_URL)],
            [InlineKeyboardButton("✅ لقد انضممت، تحقق الآن", callback_data=Callback.CONFIRM_JOIN.value)]
        ]
        await update.message.reply_text(Messages.JOIN_PROMPT, reply_markup=InlineKeyboardMarkup(keyboard))
    else:
        await update.message.reply_text(Messages.INVALID_COUNTRY_CODE, reply_markup=ReplyKeyboardRemove())
        await ask_math_question(update, context)

async def reconcile_single_user(user_id: int, context: ContextTypes.DEFAULT_TYPE) -> int:
    """
    **مزامنة مستخدم واحد**
    تقوم بحساب الإحالات الدقيقة لمستخدم واحد وتحديثها في قاعدة البيانات.
    """
    accurate_counts_all = await get_accurate_referral_counts(context)
    user_counts = accurate_counts_all.get(user_id)
    
    if not user_counts: return 0

    user_info = user_counts['user_info']
    db_real = int(user_info.get('real_referrals', 0) or 0)
    db_fake = int(user_info.get('fake_referrals', 0) or 0)
    
    calculated_real = user_counts['organic_real']
    calculated_fake = user_counts['fake']

    changes_made = 0
    if calculated_real != db_real or calculated_fake != db_fake:
        await upsert_user_in_db({"user_id": user_id, "real_referrals": calculated_real, "fake_referrals": calculated_fake})
        changes_made = abs(calculated_real - db_real) + abs(calculated_fake - db_fake)
        logger.info(f"Reconciled user {user_id}. DB: {db_real}R/{db_fake}F -> Correct: {calculated_real}R/{calculated_fake}F.")
    return changes_made


async def reconcile_all_referrals_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    **مهمة المزامنة الشاملة**
    تعيد حساب وتصحيح إحصائيات الإحالة (الحقيقية والوهمية) لجميع المستخدمين في قاعدة البيانات.
    """
    owner_id = context.job.chat_id
    await context.bot.send_message(owner_id, "⏳ *بدء المزامنة الشاملة للإحصائيات...*\nسيتم تحديث أعمدة `real_referrals` و `fake_referrals` في قاعدة البيانات لتعكس الواقع.", parse_mode=ParseMode.MARKDOWN)
    
    accurate_counts = await get_accurate_referral_counts(context)
    if not accurate_counts:
        await context.bot.send_message(owner_id, "✅ لا يوجد مستخدمون في قاعدة البيانات للمزامنة.")
        return
        
    users_to_update = []
    for user_id, counts in accurate_counts.items():
        user_info = counts['user_info']
        calculated_real = counts['organic_real']
        calculated_fake = counts['fake']
        
        # Check if the stored values in DB are different from calculated ones
        if (int(user_info.get('real_referrals') or 0) != calculated_real or 
            int(user_info.get('fake_referrals') or 0) != calculated_fake):
            users_to_update.append({
                'user_id': user_id,
                'real_referrals': calculated_real,
                'fake_referrals': calculated_fake
            })
    
    if users_to_update:
        await upsert_users_batch(users_to_update)
        
    await context.bot.send_message(owner_id, f"✅ *اكتملت المزامنة الشاملة.*\nتم تحديث بيانات *{len(users_to_update)}* مستخدم في قاعدة البيانات.", parse_mode=ParseMode.MARKDOWN)

async def recheck_leavers_and_notify_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    await context.bot.send_message(context.job.chat_id, "⏳ جاري بدء فحص شامل (للمغادرين وغيرهم)...", parse_mode=ParseMode.MARKDOWN)
    await reconcile_all_referrals_job(context)

async def handle_confirm_join(query: CallbackQuery, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    معالج تأكيد الانضمام
    """
    user = query.from_user
    await query.edit_message_text("⏳ جاري التحقق من انضمامك...")
    
    if await is_user_in_channel_and_group(user.id, context):
        db_user = await get_user_from_db(user.id)

        if not db_user or not db_user.get('is_verified'):
            await upsert_user_in_db({'user_id': user.id, 'is_verified': True, 'full_name': user.full_name, 'username': user.username})
            
            # Find referrer and notify them
            referrer_id = await get_referrer(user.id)
            if referrer_id:
                try:
                    # Run a quick reconciliation for the referrer to get the new correct count
                    await reconcile_single_user(referrer_id, context)
                    referrer_db = await get_user_from_db(referrer_id)
                    if referrer_db:
                        accurate_counts = await get_accurate_referral_counts(context)
                        new_real_count = accurate_counts.get(referrer_id, {}).get('total_real', 0)
                        
                        mention = await get_user_mention(user.id, context)
                        await context.bot.send_message(
                            chat_id=referrer_id,
                            text=f"🎉 تهانينا! لقد انضم مستخدم جديد ({mention}) عن طريق رابطك.\n\n"
                                 f"رصيدك المحدث هو: *{new_real_count}* إحالة حقيقية.",
                            parse_mode=ParseMode.MARKDOWN
                        )
                except TelegramError as e:
                    logger.warning(f"Could not send notification to referrer {referrer_id}: {e}")
                except Exception as e:
                    logger.error(f"Failed to process referral notification for referrer {referrer_id}: {e}")

        await query.edit_message_text(Messages.JOIN_SUCCESS)
        await query.message.reply_text(Messages.VERIFIED_WELCOME, reply_markup=get_main_menu_keyboard(user.id))
    
    else:
        await query.answer(text=Messages.JOIN_FAIL, show_alert=True)
        keyboard = [
            [InlineKeyboardButton("1. الانضمام للقناة", url=Config.CHANNEL_URL)],
            [InlineKeyboardButton("2. الانضمام للمجموعة", url=Config.GROUP_URL)],
            [InlineKeyboardButton("✅ لقد انضممت، تحقق الآن", callback_data=Callback.CONFIRM_JOIN.value)]
        ]
        await query.edit_message_text(Messages.JOIN_PROMPT, reply_markup=InlineKeyboardMarkup(keyboard))

async def handle_button_press_my_referrals(query: CallbackQuery, context: ContextTypes.DEFAULT_TYPE) -> None:
    await query.edit_message_text(Messages.LOADING)
    user_id = query.from_user.id
    accurate_counts = await get_accurate_referral_counts(context)
    text = get_referral_stats_text(user_id, accurate_counts)
    await query.edit_message_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=get_main_menu_keyboard(user_id))
    
async def handle_button_press_top5(query: CallbackQuery, context: ContextTypes.DEFAULT_TYPE) -> None:
    await query.edit_message_text(Messages.LOADING)
    user_id = query.from_user.id
    text = await get_top_5_text(user_id, context)
    try:
        await query.edit_message_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=get_main_menu_keyboard(user_id), disable_web_page_preview=True)
    except BadRequest as e:
        if "message is not modified" in str(e).lower():
            await query.answer()
        else: raise e

async def handle_button_press_link(query: CallbackQuery, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = query.from_user.id
    text = get_referral_link_text(user_id, context.bot.username)
    await query.edit_message_text(
        text,
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=get_main_menu_keyboard(user_id),
        disable_web_page_preview=True
    )

# --- معالجات لوحة التحكم (Admin Panel Handlers) ---
async def handle_admin_panel(query: CallbackQuery, context: ContextTypes.DEFAULT_TYPE) -> None:
    await query.edit_message_text(text=Messages.ADMIN_WELCOME, reply_markup=get_admin_panel_keyboard())

async def handle_admin_user_count(query: CallbackQuery, context: ContextTypes.DEFAULT_TYPE) -> None:
    all_users = await get_all_users_from_db()
    total = len(all_users)
    verified = sum(1 for u in all_users if u.get('is_verified'))
    text = f"📈 *إحصائيات مستخدمي البوت:*\n\n▫️ إجمالي المستخدمين: *{total}*\n✅ المستخدمون الموثقون: *{verified}*"
    await query.edit_message_text(text=text, parse_mode=ParseMode.MARKDOWN, reply_markup=get_admin_panel_keyboard())

async def handle_pick_winner(query: CallbackQuery, context: ContextTypes.DEFAULT_TYPE) -> None:
    context.user_data['state'] = State.AWAITING_WINNER_THRESHOLD
    await query.edit_message_text(text="الرجاء إرسال الحد الأدنى لعدد الإحالات الحقيقية لدخول السحب (مثال: أرسل الرقم 5).")

async def handle_admin_broadcast(query: CallbackQuery, context: ContextTypes.DEFAULT_TYPE) -> None:
    context.user_data['state'] = State.AWAITING_BROADCAST_MESSAGE
    await query.edit_message_text(text="الآن، أرسل الرسالة التي تريد إذاعتها لجميع المستخدمين الموثقين.")

async def handle_admin_reset_all(query: CallbackQuery, context: ContextTypes.DEFAULT_TYPE) -> None:
    await query.edit_message_text(
        text="⚠️ *تأكيد الإجراء* ⚠️\n\nهل أنت متأكد من أنك تريد تصفير *جميع* الإحالات؟ هذا سيحذف سجلات الإحالات و يصفر العدادات اليدوية.",
        parse_mode=ParseMode.MARKDOWN, 
        reply_markup=get_reset_confirmation_keyboard()
    )

async def handle_admin_reset_confirm(query: CallbackQuery, context: ContextTypes.DEFAULT_TYPE) -> None:
    await query.edit_message_text(text="⏳ جاري تصفير جميع الإحالات...")
    await reset_all_referrals_in_db()
    await query.edit_message_text(text="✅ تم تصفير جميع إحصائيات الإحالات بنجاح.", reply_markup=get_admin_panel_keyboard())

async def handle_admin_checker(query: CallbackQuery, context: ContextTypes.DEFAULT_TYPE) -> None:
    text = (
        "🔫 *المدقق*\n\n"
        "- *مزامنة إحصائيات الجميع*: يقوم بمراجعة *كل* الإحالات وتصحيح الأرقام المخزنة في قاعدة البيانات.\n"
        "- *فحص مستخدم محدد*: يقوم بنفس عملية الفحص ولكن لمستخدم واحد فقط."
    )
    await query.edit_message_text(text=text, parse_mode=ParseMode.MARKDOWN, reply_markup=get_checker_keyboard())

async def handle_admin_check_all(query: CallbackQuery, context: ContextTypes.DEFAULT_TYPE) -> None:
    context.job_queue.run_once(reconcile_all_referrals_job, 1, chat_id=query.from_user.id, name=f"reconcile_all_{query.from_user.id}")
    await query.edit_message_text(text="تم جدولة المزامنة الشاملة. ستبدأ العملية في الخلفية وستصلك رسالة عند الانتهاء.", reply_markup=get_admin_panel_keyboard())

async def handle_admin_check_one(query: CallbackQuery, context: ContextTypes.DEFAULT_TYPE) -> None:
    context.user_data['state'] = State.AWAITING_CHECK_USER_ID
    await query.edit_message_text(text="الرجاء إرسال الـ ID الرقمي للمستخدم الذي تريد فحص إحالاته.")

async def handle_get_real_referrals_request(query: CallbackQuery, context: ContextTypes.DEFAULT_TYPE) -> None:
    context.user_data['state'] = State.AWAITING_REAL_REFERRAL_LIST_USER_ID
    await query.edit_message_text(text="الرجاء إرسال الـ ID الرقمي للمستخدم الذي تريد عرض قائمة إحالاته *الحقيقية*.")

async def handle_get_fake_referrals_request(query: CallbackQuery, context: ContextTypes.DEFAULT_TYPE) -> None:
    context.user_data['state'] = State.AWAITING_FAKE_REFERRAL_LIST_USER_ID
    await query.edit_message_text(text="الرجاء إرسال الـ ID الرقمي للمستخدم الذي تريد عرض قائمة إحالاته *الوهمية*.")

async def handle_booo_menu(query: CallbackQuery, context: ContextTypes.DEFAULT_TYPE) -> None:
    await query.edit_message_text(text="👾 *Booo*\n\nاختر الأداة التي تريد استخدامها:", parse_mode=ParseMode.MARKDOWN, reply_markup=get_booo_menu_keyboard())

async def handle_recheck_leavers(query: CallbackQuery, context: ContextTypes.DEFAULT_TYPE) -> None:
    context.job_queue.run_once(recheck_leavers_and_notify_job, 1, chat_id=query.from_user.id, name=f"recheck_leavers_{query.from_user.id}")
    await query.edit_message_text(text="تم جدولة فحص المغادرين. ستبدأ العملية في الخلفية وستصلك رسالة عند الانتهاء.", reply_markup=get_admin_panel_keyboard())

async def handle_user_edit_menu(query: CallbackQuery, context: ContextTypes.DEFAULT_TYPE) -> None:
    await query.edit_message_text(text="� *تعديل المستخدم*\n\nاختر الإجراء المطلوب:", parse_mode=ParseMode.MARKDOWN, reply_markup=get_user_edit_keyboard())

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

# --- معالج الأزرار الرئيسي (Main Button Handler) ---
async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query or not query.data: return
    
    try:
        await query.answer()
    except BadRequest as e:
        if "Query is too old" in str(e):
            logger.warning(f"Could not answer callback query {query.id}: {e}")
        else:
            raise
    
    if query.data == Callback.MAIN_MENU.value: await query.edit_message_text(text=Messages.VERIFIED_WELCOME, reply_markup=get_main_menu_keyboard(query.from_user.id))
    elif query.data == Callback.MY_REFERRALS.value: await handle_button_press_my_referrals(query, context)
    elif query.data == Callback.MY_LINK.value: await handle_button_press_link(query, context)
    elif query.data == Callback.TOP_5.value: await handle_button_press_top5(query, context)
    elif query.data == Callback.CONFIRM_JOIN.value: await handle_confirm_join(query, context)
    elif query.data == Callback.ADMIN_PANEL.value: await handle_admin_panel(query, context)
    elif query.data == Callback.ADMIN_USER_COUNT.value: await handle_admin_user_count(query, context)
    elif query.data == Callback.PICK_WINNER.value: await handle_pick_winner(query, context)
    elif query.data == Callback.ADMIN_BROADCAST.value: await handle_admin_broadcast(query, context)
    elif query.data == Callback.ADMIN_RESET_ALL.value: await handle_admin_reset_all(query, context)
    elif query.data == Callback.ADMIN_RESET_CONFIRM.value: await handle_admin_reset_confirm(query, context)
    elif query.data == Callback.ADMIN_CHECKER.value: await handle_admin_checker(query, context)
    elif query.data == Callback.ADMIN_CHECK_ALL.value: await handle_admin_check_all(query, context)
    elif query.data == Callback.ADMIN_CHECK_ONE.value: await handle_admin_check_one(query, context)
    elif query.data == Callback.ADMIN_BOOO_MENU.value: await handle_booo_menu(query, context)
    elif query.data == Callback.ADMIN_RECHECK_LEAVERS.value: await handle_recheck_leavers(query, context)
    elif query.data == Callback.ADMIN_USER_EDIT_MENU.value: await handle_user_edit_menu(query, context)
    elif query.data == Callback.ADMIN_GET_REAL_REFERRALS_LIST.value: await handle_get_real_referrals_request(query, context)
    elif query.data == Callback.ADMIN_GET_FAKE_REFERRALS_LIST.value: await handle_get_fake_referrals_request(query, context)
    elif query.data in [c.value for c in [Callback.USER_ADD_MANUAL, Callback.USER_REMOVE_MANUAL]]: await handle_user_edit_action(query, context)
    elif query.data.startswith(Callback.REPORT_PAGE.value): await handle_report_pagination(query, context)

# --- معالج رسائل المالك (Admin Message Handler) ---
async def handle_admin_messages(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    state = context.user_data.pop('state', None)
    if not state or not update.message or not update.message.text: return
    text = update.message.text

    if state == State.AWAITING_REAL_REFERRAL_LIST_USER_ID or state == State.AWAITING_FAKE_REFERRAL_LIST_USER_ID:
        try:
            target_user_id = int(text)
            target_user = await get_user_from_db(target_user_id)
            if not target_user:
                await update.message.reply_text("لم يتم العثور على مستخدم بهذا الـ ID.", reply_markup=get_admin_panel_keyboard())
                return
            
            list_type = "الحقيقية" if state == State.AWAITING_REAL_REFERRAL_LIST_USER_ID else "الوهمية"
            mention = await get_user_mention(target_user_id, context)
            await update.message.reply_text(f"⏳ جارٍ جلب قائمة الإحالات *{list_type}* للمستخدم {mention}...", parse_mode=ParseMode.MARKDOWN)

            all_users = await get_all_users_from_db()
            verified_user_ids = {u['user_id'] for u in all_users if u.get('is_verified')}
            user_referrals = await get_referrals_for_user(target_user_id)

            if state == State.AWAITING_REAL_REFERRAL_LIST_USER_ID:
                referral_ids = [ref['referred_user_id'] for ref in user_referrals if ref['referred_user_id'] in verified_user_ids]
            else: 
                referral_ids = [ref['referred_user_id'] for ref in user_referrals if ref['referred_user_id'] not in verified_user_ids]

            if not referral_ids:
                await update.message.reply_text(f"المستخدم {mention} ليس لديه أي إحالات {list_type}.", parse_mode=ParseMode.MARKDOWN, reply_markup=get_admin_panel_keyboard())
                return
            
            response_text = f"✅ *قائمة الإحالات الـ{list_type} للمستخدم {mention} ({len(referral_ids)}):*\n\n"
            
            mentions = await asyncio.gather(*[get_user_mention(ref_id, context) for ref_id in referral_ids])
            for user_mention in mentions:
                response_text += f"• {user_mention}\n"
            
            # Split message if too long
            for i in range(0, len(response_text), 4096):
                await update.message.reply_text(response_text[i:i + 4096], parse_mode=ParseMode.MARKDOWN)
            
            await update.message.reply_text("اكتمل عرض القائمة.", reply_markup=get_admin_panel_keyboard())

        except (ValueError, TypeError):
            await update.message.reply_text(Messages.INVALID_INPUT, reply_markup=get_admin_panel_keyboard())

    elif state == State.AWAITING_EDIT_USER_ID:
        try:
            target_user_id = int(text)
            user_to_fix = await get_user_from_db(target_user_id)
            if not user_to_fix:
                await update.message.reply_text("لم يتم العثور على مستخدم بهذا الـ ID.", reply_markup=get_admin_panel_keyboard())
                return

            context.user_data['state'] = State.AWAITING_EDIT_AMOUNT
            context.user_data['target_id'] = target_user_id
            
            action_map = {
                Callback.USER_ADD_MANUAL.value: "زيادة إحالات (يدوي)",
                Callback.USER_REMOVE_MANUAL.value: "خصم إحالات (يدوي)",
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
        try:
            amount = int(text)
            if amount <= 0:
                context.user_data['state'] = State.AWAITING_EDIT_AMOUNT
                await update.message.reply_text("الرجاء إرسال عدد صحيح أكبر من صفر.")
                return

            target_user_id = context.user_data.pop('target_id', None)
            action_type = context.user_data.pop('action_type', None)
            if not target_user_id or not action_type: return

            user_to_fix = await get_user_from_db(target_user_id)
            if not user_to_fix: return
            
            update_data = {}
            current_manual = int(user_to_fix.get('manual_referrals', 0) or 0)
            
            if action_type == Callback.USER_ADD_MANUAL.value:
                update_data = {'manual_referrals': current_manual + amount}
            elif action_type == Callback.USER_REMOVE_MANUAL.value:
                update_data = {'manual_referrals': max(0, current_manual - amount)}

            if update_data:
                await upsert_user_in_db({'user_id': target_user_id, **update_data})
                
                mention = await get_user_mention(target_user_id, context)
                # Get the new accurate counts
                accurate_counts = await get_accurate_referral_counts(context)
                new_user_data = accurate_counts.get(target_user_id)

                if new_user_data:
                    final_text = (f"✅ تم التعديل بنجاح.\n\n"
                                  f"المستخدم: {mention}\n"
                                  f"الرصيد الجديد الدقيق:\n"
                                  f"- *{new_user_data.get('total_real', 0)}* إحالة حقيقية\n"
                                  f"- *{new_user_data.get('fake', 0)}* إحالة وهمية")
                    await update.message.reply_text(final_text, parse_mode=ParseMode.MARKDOWN, reply_markup=get_admin_panel_keyboard())
        except (ValueError, TypeError):
            await update.message.reply_text(Messages.INVALID_INPUT, reply_markup=get_admin_panel_keyboard())

    elif state == State.AWAITING_WINNER_THRESHOLD:
        try:
            threshold = int(text)
            accurate_counts = await get_accurate_referral_counts(context)
            
            eligible = [u for u in accurate_counts.values() if u['total_real'] >= threshold and u['user_info'].get('is_verified')]
            
            if not eligible:
                await update.message.reply_text(f"لا يوجد مستخدمون موثقون لديهم {threshold} إحالة حقيقية أو أكثر.", reply_markup=get_admin_panel_keyboard())
            else:
                winner_data = random.choice(eligible)
                winner_id = winner_data['user_info']['user_id']
                mention = await get_user_mention(winner_id, context)
                await update.message.reply_text(
                    f"🎉 *الفائز هو*!!!\n\n"
                    f"*المستخدم:* {mention}\n"
                    f"*عدد الإحالات:* {winner_data['total_real']}\n\nتهانينا!",
                    parse_mode=ParseMode.MARKDOWN, reply_markup=get_admin_panel_keyboard()
                )
        except (ValueError, TypeError):
            await update.message.reply_text(Messages.INVALID_INPUT, reply_markup=get_admin_panel_keyboard())

    elif state == State.AWAITING_BROADCAST_MESSAGE:
        await update.message.reply_text("⏳ جاري بدء الإذاعة...")
        all_users = await get_all_users_from_db()
        verified_users_ids = [u['user_id'] for u in all_users if u.get('is_verified')]
        sent, failed = 0, 0
        
        for user_id in verified_users_ids:
            try:
                await context.bot.send_message(chat_id=user_id, text=text, parse_mode=ParseMode.MARKDOWN)
                sent += 1
            except TelegramError: failed += 1
            await asyncio.sleep(0.04) # 25 messages per second
            
        await update.message.reply_text(f"✅ اكتملت الإذاعة.\n- تم الإرسال إلى: {sent}\n- فشل الإرسال إلى: {failed}", reply_markup=get_admin_panel_keyboard())

    elif state == State.AWAITING_CHECK_USER_ID:
        try:
            target_user_id = int(text)
            await update.message.reply_text(f"⏳ جاري فحص ومزامنة المستخدم `{target_user_id}`...")
            changes = await reconcile_single_user(target_user_id, context)
            
            accurate_counts = await get_accurate_referral_counts(context)
            new_user_data = accurate_counts.get(target_user_id)
            if new_user_data:
                await update.message.reply_text(
                    f"✅ اكتمل الفحص. تم إجراء *{changes}* تعديل في قاعدة البيانات.\n"
                    f"البيانات الدقيقة للمستخدم: *{new_user_data.get('total_real',0)}* حقيقي, *{new_user_data.get('fake',0)}* وهمي.",
                    parse_mode=ParseMode.MARKDOWN, reply_markup=get_admin_panel_keyboard()
                )
            else:
                await update.message.reply_text("لم يتم العثور على المستخدم.", reply_markup=get_admin_panel_keyboard())

        except (ValueError, TypeError):
            await update.message.reply_text(Messages.INVALID_INPUT, reply_markup=get_admin_panel_keyboard())

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
            # Reconcile the referrer to update their counts in the database
            changes = await reconcile_single_user(referrer_id, context)
            
            if changes > 0:
                try:
                    # Notify the referrer with their new accurate count
                    accurate_counts = await get_accurate_referral_counts(context)
                    new_referrer_data = accurate_counts.get(referrer_id)
                    new_real_count = new_referrer_data.get('total_real', 'N/A') if new_referrer_data else 'N/A'

                    mention = await get_user_mention(user.id, context)
                    await context.bot.send_message(
                        chat_id=referrer_id,
                        text=f"⚠️ تنبيه! أحد المستخدمين الذين دعوتهم ({mention}) غادر.\n\n"
                             f"تم تحديث رصيدك. رصيدك الحالي الدقيق هو: *{new_real_count}* إحالة حقيقية.",
                        parse_mode=ParseMode.MARKDOWN
                    )
                except TelegramError as e:
                    logger.warning(f"Could not send leave notification to referrer {referrer_id}: {e}")

# --- الدالة الرئيسية (Main Function) ---
def main() -> None:
    if "YOUR_BOT_TOKEN" in Config.BOT_TOKEN or "YOUR_SUPABASE_URL" in Config.SUPABASE_URL:
        logger.critical("FATAL: Bot token or Supabase credentials are not configured.")
        return

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
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND & private_chat_filter, handle_verification_text), group=2)
    
    logger.info("Bot is starting...")
    application.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()
�