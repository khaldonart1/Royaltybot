import os
import random
import json
import datetime
import math
import asyncio
from supabase import create_client, Client
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, KeyboardButton, ReplyKeyboardMarkup, ReplyKeyboardRemove, ChatMemberUpdated, Chat
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes, CallbackQueryHandler, ChatMemberHandler, JobQueue
from telegram.constants import ParseMode
from telegram.error import TelegramError

# --- Configuration ---
BOT_TOKEN = "7950170561:AAECeQpxb1G4zrnFhrol_uBgPNoxZN-Qkz0"
SUPABASE_URL = "https://jofxsqsgarvzolgphqjg.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImpvZnhzcXNnYXJ2em9sZ3BocWpnIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc0OTU5NTI4NiwiZXhwIjoyMDY1MTcxMjg2fQ.egB9qticc7ABgo6vmpsrPi3cOHooQmL5uQOKI4Jytqg"

# --- Static IDs ---
# IMPORTANT: Replace these with your actual IDs
CHANNEL_ID = -1002686156311  # Your public channel ID
GROUP_ID = -1002472491601    # Your group ID
# Your Telegram User IDs for owner access
BOT_OWNER_IDS = [596472053, 7164133014, 1971453570]

# List of allowed country codes for the phone verification step
ALLOWED_COUNTRY_CODES = ["213", "973", "269", "253", "20", "964", "962", "965", "961", "218", "222", "212", "968", "970", "974", "966", "252", "249", "963", "216", "971", "967"]
USERS_PER_PAGE = 15 # For paginated reports in the admin panel

# --- Initialize Supabase Client ---
if not all([BOT_TOKEN, SUPABASE_URL, SUPABASE_KEY]):
    print("FATAL: Missing required environment variables. Please check your .env file on PythonAnywhere.")
    exit()
try:
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    print("Successfully connected to Supabase.")
except Exception as e:
    print(f"FATAL: Failed to connect to Supabase. Error: {e}")
    exit()

# --- Bot Messages ---
VERIFIED_WELCOME_MESSAGE = "أهلاً بك مجدداً! ✅\n\nاستخدم الأزرار أو الأوامر (/) للتفاعل مع البوت."
WELCOME_MESSAGE = "أهلاً بك في البوت! 👋\n\nيجب عليك إتمام خطوات بسيطة للتحقق أولاً."
MATH_QUESTION_MESSAGE = "الرجاء حل هذه المسألة الرياضية البسيطة للمتابعة:"
PHONE_REQUEST_MESSAGE = "رائع! الآن، من فضلك شارك رقم هاتفك لإكمال عملية التحقق."
JOIN_PROMPT_MESSAGE = "ممتاز! الخطوة الأخيرة هي الانضمام إلى قناتنا ومجموعتنا. انضم ثم اضغط على الزر أدناه."
JOIN_SUCCESS_MESSAGE = "تهانينا! لقد تم التحقق منك بنجاح."
JOIN_FAIL_MESSAGE = "❌ لم تنضم بعد. الرجاء الانضمام إلى القناة والمجموعة ثم حاول مرة أخرى."
INVALID_COUNTRY_CODE_MESSAGE = "عذراً، هذا البوت مخصص فقط للمستخدمين من الدول العربية. رقمك غير مدعوم."

# --- Supabase Async Helper Functions ---
# These functions handle all database interactions asynchronously to prevent blocking.
async def run_sync_db(func, *args, **kwargs):
    """Runs a synchronous Supabase call in a thread pool."""
    return await asyncio.to_thread(func, *args, **kwargs)

async def get_user_from_db_async(user_id):
    try:
        res = await run_sync_db(supabase.table('users').select("*").eq('user_id', user_id).single().execute)
        return res.data
    except Exception: return None

async def upsert_user_in_db_async(user_data):
    try:
        await run_sync_db(supabase.table('users').upsert, user_data, on_conflict='user_id')
    except Exception as e: print(f"DB_ERROR: Upserting user {user_data.get('user_id')}: {e}")

async def update_user_in_db_async(user_id, data_to_update):
    try:
        await run_sync_db(supabase.table('users').update, data_to_update, eq=('user_id', user_id))
    except Exception as e: print(f"DB_ERROR: Updating user {user_id}: {e}")

async def get_all_users_sorted_by_async(column="real_referrals"):
    try:
        res = await run_sync_db(supabase.table('users').select("user_id, full_name, real_referrals, fake_referrals, is_verified").order(column, desc=True).execute)
        return res.data or []
    except Exception: return []

async def get_users_with_fake_referrals_async():
    """Fetches only users with fake referrals > 0, sorted. More efficient."""
    try:
        res = await run_sync_db(
            supabase.table('users')
            .select("user_id, full_name, real_referrals, fake_referrals, is_verified")
            .gt('fake_referrals', 0)
            .order("fake_referrals", desc=True)
            .execute
        )
        return res.data or []
    except Exception: return []


async def get_all_referral_mappings_async():
    try:
        res = await run_sync_db(supabase.table('referrals').select("*").execute)
        return res.data or []
    except Exception: return []

async def get_user_counts_async():
    try:
        # Running these sequentially to avoid overwhelming the DB connection pool
        total_res = await run_sync_db(supabase.table('users').select('user_id', count='exact').execute)
        verified_res = await run_sync_db(supabase.table('users').select('user_id', count='exact').eq('is_verified', True).execute)
        return getattr(total_res, 'count', 0), getattr(verified_res, 'count', 0)
    except Exception as e:
        print(f"DB_ERROR: Getting user counts: {e}")
        return 0, 0

async def add_referral_mapping_async(referred_id, referrer_id):
    try:
        await run_sync_db(supabase.table('referrals').upsert, {'referred_user_id': referred_id, 'referrer_user_id': referrer_id}, on_conflict='referred_user_id')
    except Exception as e: print(f"DB_ERROR: Adding referral map for {referred_id}: {e}")

async def get_referrer_async(referred_id):
    try:
        res = await run_sync_db(supabase.table('referrals').select('referrer_user_id').eq('referred_user_id', referred_id).single().execute)
        return res.data.get('referrer_user_id') if res.data else None
    except Exception: return None

async def delete_referral_mapping_async(referred_id):
    try:
        await run_sync_db(supabase.table('referrals').delete, eq=('referred_user_id', referred_id))
    except Exception as e: print(f"DB_ERROR: Deleting map for {referred_id}: {e}")

async def reset_all_referrals_in_db_async():
    try:
        await run_sync_db(supabase.table('users').update, {"real_referrals": 0, "fake_referrals": 0}, gt=('user_id', 0))
        await run_sync_db(supabase.table('referrals').delete, gt=('referred_user_id', 0))
        print("All referrals have been reset.")
    except Exception as e: print(f"DB_ERROR: Resetting all referrals: {e}")

# --- Text Generation Functions ---
def get_referral_stats_text(user_info):
    if not user_info: return "لا توجد لديك بيانات بعد."
    real_count = user_info.get("real_referrals", 0)
    fake_count = user_info.get("fake_referrals", 0)
    return (f"📊 **إحصائيات إحالاتك:**\n\n"
            f"✅ الإحالات الحقيقية: **{real_count}**\n"
            f"⏳ الإحالات الوهمية: **{fake_count}**")

def get_referral_link_text(user_id, bot_username):
    return f"🔗 رابط الإحالة الخاص بك:\n`https://t.me/{bot_username}?start={user_id}`"

async def get_top_5_text_async(user_id):
    sorted_users = await get_all_users_sorted_by_async("real_referrals")
    text = "🏆 **أفضل 5 متسابقين لدينا:**\n\n"
    users_with_referrals = [u for u in sorted_users if u.get("real_referrals", 0) > 0]

    if not users_with_referrals:
        text += "لم يصل أحد إلى القائمة بعد. كن أنت الأول!\n"
    else:
        for i, uinfo in enumerate(users_with_referrals[:5]):
            full_name = uinfo.get("full_name", f"User_{uinfo.get('user_id')}")
            count = uinfo.get("real_referrals", 0)
            text += f"{i+1}. {full_name} - **{count}** إحالة\n"

    text += "\n---\n**ترتيبك الشخصي:**\n"
    try:
        user_rank_str = "غير مصنف"
        user_index = next((i for i, u in enumerate(sorted_users) if u.get('user_id') == user_id), -1)
        if user_index != -1:
            user_rank_str = f"#{user_index + 1}"

        current_user_info = sorted_users[user_index] if user_index != -1 else await get_user_from_db_async(user_id)
        current_user_real_refs = current_user_info.get("real_referrals", 0) if current_user_info else 0

        text += f"🎖️ ترتيبك: **{user_rank_str}**\n✅ رصيدك: **{current_user_real_refs}** إحالة حقيقية."
    except (StopIteration, IndexError):
        text += "لا يمكنك رؤية ترتيبك حتى تقوم بدعوة شخص واحد على الأقل."

    return text

def get_paginated_report(all_users, page, report_type):
    if not all_users: return "لا يوجد أي مستخدمين في هذا التقرير حالياً.", None
    start_index = (page - 1) * USERS_PER_PAGE
    end_index = start_index + USERS_PER_PAGE
    page_users = all_users[start_index:end_index]
    total_pages = math.ceil(len(all_users) / USERS_PER_PAGE)

    title = "📊 **تقرير الإحالات الحقيقية**" if report_type == 'real' else "⏳ **تقرير الإحالات الوهمية**"
    report = f"{title} (صفحة {page} من {total_pages}):\n\n"
    column = 'real_referrals' if report_type == 'real' else 'fake_referrals'

    for uinfo in page_users:
        full_name = uinfo.get('full_name', f"User_{uinfo.get('user_id')}")
        user_id = uinfo.get('user_id')
        count = uinfo.get(column, 0)
        report += f"• {full_name} (`{user_id}`) - **{count}**\n"

    nav_buttons = []
    if page > 1: nav_buttons.append(InlineKeyboardButton("⬅️ السابق", callback_data=f"report_{report_type}_page_{page-1}"))
    if page < total_pages: nav_buttons.append(InlineKeyboardButton("التالي ➡️", callback_data=f"report_{report_type}_page_{page+1}"))

    keyboard = [nav_buttons, [InlineKeyboardButton("🔙 العودة للوحة التحكم", callback_data="admin_panel")]]
    return report, InlineKeyboardMarkup(keyboard)

# --- Keyboards ---
def get_main_menu_keyboard(user_id):
    is_owner = user_id in BOT_OWNER_IDS
    keyboard = [[InlineKeyboardButton("إحصائياتي 📊", callback_data="my_referrals")],
                [InlineKeyboardButton("رابطي 🔗", callback_data="my_link")],
                [InlineKeyboardButton("🏆 أفضل 5 متسابقين", callback_data="top_5")]]
    if is_owner:
        keyboard.append([InlineKeyboardButton("👑 لوحة تحكم المالك 👑", callback_data="admin_panel")])
    return InlineKeyboardMarkup(keyboard)

def get_admin_panel_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📊 تقرير الإحالات الحقيقية", callback_data="report_real_page_1")],
        [InlineKeyboardButton("⏳ تقرير الإحالات الوهمية", callback_data="report_fake_page_1")],
        [InlineKeyboardButton("👥 عدد مستخدمي البوت", callback_data="admin_user_count")],
        [InlineKeyboardButton("🏆 اختيار فائز عشوائي", callback_data="pick_winner")],
        [InlineKeyboardButton("Checker 🔫", callback_data="admin_checker")],
        [InlineKeyboardButton("Booo 👾", callback_data="admin_booo_menu")],
        [InlineKeyboardButton("📢 إرسال رسالة للجميع", callback_data="admin_broadcast")],
        [InlineKeyboardButton("⚠️ تصفير كل الإحالات ⚠️", callback_data="admin_reset_all")],
        [InlineKeyboardButton("⬅️ العودة للقائمة الرئيسية", callback_data="main_menu")]
    ])

def get_checker_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔍 فحص شامل للكل", callback_data="admin_check_all")],
        [InlineKeyboardButton("👤 فحص مستخدم محدد", callback_data="admin_check_one")],
        [InlineKeyboardButton("🔙 العودة للوحة التحكم", callback_data="admin_panel")]
    ])

def get_booo_menu_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔄 إعادة فحص المغادرين", callback_data="admin_recheck_leavers")],
        [InlineKeyboardButton("User 👤", callback_data="admin_user_edit_menu")],
        [InlineKeyboardButton("🔙 العودة للوحة التحكم", callback_data="admin_panel")]
    ])

def get_user_edit_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("زيادة إحالة حقيقة 🐥", callback_data="user_add_real")],
        [InlineKeyboardButton("خصم إحالة حقيقية 🐣", callback_data="user_remove_real")],
        [InlineKeyboardButton("زيادة إحالة وهمية 🐥", callback_data="user_add_fake")],
        [InlineKeyboardButton("خصم إحالة وهمية 🐣", callback_data="user_remove_fake")],
        [InlineKeyboardButton("🔙 العودة لقائمة Booo", callback_data="admin_booo_menu")]
    ])

def get_reset_confirmation_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ نعم، قم بالتصفير", callback_data="admin_reset_confirm")],
        [InlineKeyboardButton("❌ لا، الغِ الأمر", callback_data="admin_panel")]
    ])

# --- Helper Functions ---
async def is_user_in_channel_and_group(user_id, context: ContextTypes.DEFAULT_TYPE) -> bool:
    try:
        ch_mem = await context.bot.get_chat_member(chat_id=CHANNEL_ID, user_id=user_id)
        if ch_mem.status not in ['member', 'administrator', 'creator']:
            return False
        gr_mem = await context.bot.get_chat_member(chat_id=GROUP_ID, user_id=user_id)
        if gr_mem.status not in ['member', 'administrator', 'creator']:
            return False
        return True
    except TelegramError as e:
        print(f"Error checking membership for {user_id}: {e}")
        return False
    except Exception as e:
        print(f"An unexpected error occurred while checking membership for {user_id}: {e}")
        return False

def generate_math_question():
    num1, num2 = random.randint(1, 10), random.randint(1, 10)
    question = f"{num1} + {num2}"
    answer = num1 + num2
    return question, answer

# --- Core Command Handlers ---
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_chat.type != Chat.PRIVATE: return
    user = update.effective_user
    db_user = await get_user_from_db_async(user.id)

    if not db_user:
        await upsert_user_in_db_async({'user_id': user.id, 'full_name': user.full_name, 'username': user.username, 'is_verified': False, 'real_referrals': 0, 'fake_referrals': 0})
        db_user = await get_user_from_db_async(user.id)
    elif db_user.get('full_name') != user.full_name or db_user.get('username') != user.username:
        await update_user_in_db_async(user.id, {'full_name': user.full_name, 'username': user.username})

    if db_user and db_user.get("is_verified"):
        await update.message.reply_text(VERIFIED_WELCOME_MESSAGE, reply_markup=get_main_menu_keyboard(user.id))
        return

    args = context.args
    if args:
        try:
            referrer_id = int(args[0])
            if referrer_id != user.id and not await get_referrer_async(user.id):
                context.user_data['referrer_id'] = referrer_id
                referrer_db = await get_user_from_db_async(referrer_id)
                if not referrer_db:
                    await upsert_user_in_db_async({'user_id': referrer_id, 'full_name': f"User_{referrer_id}", 'is_verified': False, 'real_referrals': 0, 'fake_referrals': 0})
                    referrer_db = await get_user_from_db_async(referrer_id)
                new_fake_count = referrer_db.get('fake_referrals', 0) + 1
                await update_user_in_db_async(referrer_id, {'fake_referrals': new_fake_count})
        except (ValueError, IndexError):
            pass

    await update.message.reply_text(WELCOME_MESSAGE)
    await ask_math_question(update, context)

async def invites_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_chat.type != Chat.PRIVATE: return
    user_info = await get_user_from_db_async(update.effective_user.id)
    await update.message.reply_text(get_referral_stats_text(user_info), parse_mode=ParseMode.MARKDOWN, reply_markup=get_main_menu_keyboard(update.effective_user.id))

async def link_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_chat.type != Chat.PRIVATE: return
    await update.message.reply_text(get_referral_link_text(update.effective_user.id, context.bot.username), parse_mode=ParseMode.MARKDOWN, reply_markup=get_main_menu_keyboard(update.effective_user.id))

async def top_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_chat.type != Chat.PRIVATE: return
    text = await get_top_5_text_async(update.effective_user.id)
    await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=get_main_menu_keyboard(update.effective_user.id))

# --- Core Message & Callback Handlers ---
async def ask_math_question(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    question, answer = generate_math_question()
    context.user_data['math_answer'] = answer
    await update.message.reply_text(f"{MATH_QUESTION_MESSAGE}\n\nما هو ناتج {question}؟")

async def handle_verification_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    if update.effective_chat.type != Chat.PRIVATE: return

    if user_id in BOT_OWNER_IDS and context.user_data.get('state'):
        await handle_admin_messages(update, context)
        return

    db_user = await get_user_from_db_async(user_id)
    if db_user and db_user.get('is_verified'):
        await update.message.reply_text(VERIFIED_WELCOME_MESSAGE, reply_markup=get_main_menu_keyboard(user_id))
        return

    if 'math_answer' in context.user_data:
        try:
            if int(update.message.text) == context.user_data['math_answer']:
                del context.user_data['math_answer']
                phone_button = [[KeyboardButton("اضغط هنا لمشاركة رقم هاتفك", request_contact=True)]]
                await update.message.reply_text(PHONE_REQUEST_MESSAGE, reply_markup=ReplyKeyboardMarkup(phone_button, resize_keyboard=True, one_time_keyboard=True))
            else:
                await update.message.reply_text("إجابة خاطئة. حاول مرة اخرى.")
                await ask_math_question(update, context)
        except (ValueError, TypeError):
            await update.message.reply_text("من فضلك أدخل رقماً صحيحاً كإجابة.")

async def handle_contact(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_chat.type != Chat.PRIVATE: return
    contact = update.effective_message.contact
    if contact and contact.user_id == update.effective_user.id:
        phone_number = contact.phone_number.lstrip('+')
        if any(phone_number.startswith(code) for code in ALLOWED_COUNTRY_CODES):
            channel_username = "Ry_Hub" # Replace with your channel username
            group_invite_link = "Rrx4fWReNLxlYWNk" # Replace with your group invite link hash
            keyboard = [[InlineKeyboardButton("1. الانضمام للقناة", url=f"https://t.me/{channel_username}")],
                        [InlineKeyboardButton("2. الانضمام للمجموعة", url=f"https://t.me/joinchat/{group_invite_link}")],
                        [InlineKeyboardButton("✅ لقد انضممت، تحقق الآن", callback_data="confirm_join")]]
            await update.message.reply_text(JOIN_PROMPT_MESSAGE, reply_markup=InlineKeyboardMarkup(keyboard))
            await update.message.reply_text("تم استلام الرقم بنجاح.", reply_markup=ReplyKeyboardRemove())
        else:
            await update.message.reply_text(INVALID_COUNTRY_CODE_MESSAGE, reply_markup=ReplyKeyboardRemove())
            await ask_math_question(update, context)

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    user = query.from_user
    user_id = user.id
    is_owner = user_id in BOT_OWNER_IDS
    data = query.data

    # --- User Menu Buttons ---
    if data == "main_menu":
        await query.edit_message_text(text=VERIFIED_WELCOME_MESSAGE, reply_markup=get_main_menu_keyboard(user_id))
    elif data == "my_referrals":
        user_info = await get_user_from_db_async(user_id)
        await query.edit_message_text(get_referral_stats_text(user_info), parse_mode=ParseMode.MARKDOWN, reply_markup=get_main_menu_keyboard(user_id))
    elif data == "my_link":
        await query.edit_message_text(get_referral_link_text(user_id, context.bot.username), parse_mode=ParseMode.MARKDOWN, reply_markup=get_main_menu_keyboard(user_id))
    elif data == "top_5":
        text = await get_top_5_text_async(user_id)
        await query.edit_message_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=get_main_menu_keyboard(user_id))
    # --- Verification Flow ---
    elif data == "confirm_join":
        await query.edit_message_text("⏳ جاري التحقق من انضمامك...")
        if await is_user_in_channel_and_group(user.id, context):
            db_user = await get_user_from_db_async(user.id)
            if not db_user or not db_user.get('is_verified'):
                await update_user_in_db_async(user.id, {'is_verified': True, 'full_name': user.full_name, 'username': user.username})
                if 'referrer_id' in context.user_data:
                    referrer_id = context.user_data['referrer_id']
                    referrer_db = await get_user_from_db_async(referrer_id)
                    if referrer_db:
                        new_real = referrer_db.get('real_referrals', 0) + 1
                        new_fake = max(0, referrer_db.get('fake_referrals', 0) - 1)
                        await update_user_in_db_async(referrer_id, {'real_referrals': new_real, 'fake_referrals': new_fake})
                        await add_referral_mapping_async(user.id, referrer_id)
                        del context.user_data['referrer_id']
                        try:
                            await context.bot.send_message(
                                chat_id=referrer_id,
                                text=f"🎉 تهانينا! لقد انضم مستخدم جديد (**{user.full_name}**) عن طريق رابطك.\n\n"
                                     f"رصيدك الجديد هو: **{new_real}** إحالة حقيقية.",
                                parse_mode=ParseMode.MARKDOWN
                            )
                        except TelegramError as e:
                            print(f"Could not send notification to referrer {referrer_id}: {e}")
            await query.edit_message_text(JOIN_SUCCESS_MESSAGE)
            await query.message.reply_text(VERIFIED_WELCOME_MESSAGE, reply_markup=get_main_menu_keyboard(user_id))
        else:
            await query.answer(text=JOIN_FAIL_MESSAGE, show_alert=True)
            channel_username = "Ry_Hub"
            group_invite_link = "Rrx4fWReNLxlYWNk"
            keyboard = [[InlineKeyboardButton("1. الانضمام للقناة", url=f"https://t.me/{channel_username}")],
                        [InlineKeyboardButton("2. الانضمام للمجموعة", url=f"https://t.me/joinchat/{group_invite_link}")],
                        [InlineKeyboardButton("✅ لقد انضممت، تحقق الآن", callback_data="confirm_join")]]
            await query.edit_message_text(JOIN_PROMPT_MESSAGE, reply_markup=InlineKeyboardMarkup(keyboard))

    # --- Admin Panel Buttons (Owner Only) ---
    if not is_owner: return

    if data == "admin_panel":
        await query.edit_message_text(text="👑 أهلاً بك في لوحة تحكم المالك.", reply_markup=get_admin_panel_keyboard())
    elif data == "admin_user_count":
        total_users, verified_users = await get_user_counts_async()
        text = (f"📈 **إحصائيات مستخدمي البوت:**\n\n"
                f"▫️ إجمالي المستخدمين المسجلين: **{total_users}**\n"
                f"✅ المستخدمون الموثقون: **{verified_users}**")
        await query.edit_message_text(text=text, parse_mode=ParseMode.MARKDOWN, reply_markup=get_admin_panel_keyboard())
    elif data.startswith("report_real_page_"):
        page = int(data.split('_')[-1])
        all_users = await get_all_users_sorted_by_async("real_referrals")
        text, keyboard = get_paginated_report(all_users, page, 'real')
        await query.edit_message_text(text=text, parse_mode=ParseMode.MARKDOWN, reply_markup=keyboard)
    elif data.startswith("report_fake_page_"):
        page = int(data.split('_')[-1])
        all_users = await get_users_with_fake_referrals_async() # Using the optimized function
        text, keyboard = get_paginated_report(all_users, page, 'fake')
        await query.edit_message_text(text=text, parse_mode=ParseMode.MARKDOWN, reply_markup=keyboard)
    elif data == "pick_winner":
        context.user_data['state'] = 'awaiting_winner_threshold'
        await query.edit_message_text(text="الرجاء إرسال الحد الأدنى لعدد الإحالات الحقيقية لدخول السحب (مثال: أرسل الرقم 5).")
    elif data == "admin_broadcast":
        context.user_data['state'] = 'awaiting_broadcast_message'
        await query.edit_message_text(text="الآن، أرسل الرسالة التي تريد إذاعتها لجميع المستخدمين الموثقين. يمكنك استخدام تنسيق Markdown.")
    elif data == "admin_reset_all":
        await query.edit_message_text(text="⚠️ **تأكيد الإجراء** ⚠️\n\nهل أنت متأكد من أنك تريد تصفير **جميع** الإحالات؟ هذا الإجراء لا يمكن التراجع عنه.", parse_mode=ParseMode.MARKDOWN, reply_markup=get_reset_confirmation_keyboard())
    elif data == "admin_reset_confirm":
        await reset_all_referrals_in_db_async()
        await query.edit_message_text(text="✅ تم تصفير جميع إحصائيات الإحالات بنجاح.", reply_markup=get_admin_panel_keyboard())
    # --- New Booo & Checker Menus ---
    elif data == "admin_checker":
        await query.edit_message_text(text="🔫 **المدقق**\n\n- **فحص شامل للكل**: يقوم بمراجعة **كل** الإحالات المسجلة ومقارنتها بالحالة الحالية للمستخدمين (هل ما زالوا مشتركين؟) وتصحيح الأرقام. **قد تكون عملية بطيئة جداً**.\n- **فحص مستخدم محدد**: يقوم بنفس عملية الفحص ولكن لمستخدم واحد فقط.", parse_mode=ParseMode.MARKDOWN, reply_markup=get_checker_keyboard())
    elif data == "admin_check_all":
        context.job_queue.run_once(reconcile_all_referrals_job, 1, chat_id=user_id, name=f"reconcile_all_{user_id}")
        await query.edit_message_text(text="تم جدولة الفحص الشامل. ستبدأ العملية في الخلفية وستصلك رسالة عند الانتهاء. قد يستغرق هذا وقتاً طويلاً.", reply_markup=get_admin_panel_keyboard())
    elif data == "admin_check_one":
        context.user_data['state'] = 'awaiting_check_user_id'
        await query.edit_message_text(text="الرجاء إرسال الـ ID الرقمي للمستخدم الذي تريد فحص إحالاته.")
    elif data == "admin_booo_menu":
        await query.edit_message_text(text="👾 **Booo**\n\nاختر الأداة التي تريد استخدامها:", reply_markup=get_booo_menu_keyboard())
    elif data == "admin_recheck_leavers":
        context.job_queue.run_once(recheck_leavers_and_notify_job, 1, chat_id=user_id, name=f"recheck_{user_id}")
        await query.edit_message_text(text="تم جدولة فحص المغادرين. ستبدأ العملية في الخلفية وستصلك رسالة عند الانتهاء.", reply_markup=get_admin_panel_keyboard())
    elif data == "admin_user_edit_menu":
        await query.edit_message_text(text="👤 **تعديل المستخدم**\n\nاختر الإجراء المطلوب:", reply_markup=get_user_edit_keyboard())
    elif data in ["user_add_real", "user_remove_real", "user_add_fake", "user_remove_fake"]:
        context.user_data['state'] = f'awaiting_id_for_{data}'
        await query.edit_message_text(text=f"الرجاء إرسال الـ ID الرقمي للمستخدم لتنفيذ الإجراء: `{data}`")


async def handle_admin_messages(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id not in BOT_OWNER_IDS or not context.user_data.get('state'):
        return

    state = context.user_data.pop('state')
    text = update.message.text

    # --- User Edit Handlers (Booo Menu) ---
    if state.startswith('awaiting_id_for_'):
        action = state.replace('awaiting_id_for_', '')
        try:
            target_user_id = int(text)
            user_to_fix = await get_user_from_db_async(target_user_id)
            if not user_to_fix:
                await update.message.reply_text("لم يتم العثور على مستخدم بهذا الـ ID.", reply_markup=get_admin_panel_keyboard())
                return

            real_refs = user_to_fix.get('real_referrals', 0)
            fake_refs = user_to_fix.get('fake_referrals', 0)
            update_data = {}
            response_text = ""

            if action == 'user_add_real':
                update_data = {'real_referrals': real_refs + 1}
                response_text = f"تمت زيادة إحالة حقيقية. الرصيد الجديد: {real_refs + 1} حقيقي."
            elif action == 'user_remove_real':
                update_data = {'real_referrals': max(0, real_refs - 1)}
                response_text = f"تم خصم إحالة حقيقية. الرصيد الجديد: {max(0, real_refs - 1)} حقيقي."
            elif action == 'user_add_fake':
                update_data = {'fake_referrals': fake_refs + 1}
                response_text = f"تمت زيادة إحالة وهمية. الرصيد الجديد: {fake_refs + 1} وهمي."
            elif action == 'user_remove_fake':
                update_data = {'fake_referrals': max(0, fake_refs - 1)}
                response_text = f"تم خصم إحالة وهمية. الرصيد الجديد: {max(0, fake_refs - 1)} وهمي."

            if update_data:
                await update_user_in_db_async(target_user_id, update_data)
                await update.message.reply_text(f"✅ تم بنجاح تعديل المستخدم {user_to_fix.get('full_name')}.\n\n{response_text}", reply_markup=get_admin_panel_keyboard())
            else:
                await update.message.reply_text("حدث خطأ غير معروف.", reply_markup=get_admin_panel_keyboard())

        except (ValueError, TypeError):
            await update.message.reply_text("الرجاء إرسال ID رقمي صحيح. أعد المحاولة من لوحة التحكم.", reply_markup=get_admin_panel_keyboard())
            context.user_data['state'] = state # Restore state

    # --- Other Admin Handlers ---
    elif state == 'awaiting_broadcast_message':
        await update.message.reply_text("⏳ جاري بدء الإذاعة... ستصلك رسالة عند الانتهاء.")
        all_users = await get_all_users_sorted_by_async()
        all_verified_users = [u for u in all_users if u.get('is_verified')]
        sent_count, failed_count = 0, 0
        for user in all_verified_users:
            try:
                await context.bot.send_message(chat_id=user['user_id'], text=text, parse_mode=ParseMode.MARKDOWN)
                sent_count += 1
            except TelegramError as e:
                print(f"Failed to send broadcast to {user['user_id']}: {e}")
                failed_count += 1
            await asyncio.sleep(0.1)
        await update.message.reply_text(f"✅ اكتملت الإذاعة.\n\n- تم الإرسال بنجاح إلى: {sent_count} مستخدم\n- فشل الإرسال إلى: {failed_count} مستخدم", reply_markup=get_admin_panel_keyboard())
    elif state == 'awaiting_winner_threshold':
        try:
            threshold = int(text)
            all_users = await get_all_users_sorted_by_async()
            eligible_users = [u for u in all_users if u.get('real_referrals', 0) >= threshold]
            if not eligible_users:
                await update.message.reply_text(f"لا يوجد مستخدمون لديهم {threshold} إحالة حقيقية أو أكثر.", reply_markup=get_admin_panel_keyboard())
                return
            winner = random.choice(eligible_users)
            await update.message.reply_text(f"🎉 الفائز هو...!\n\n"
                                          f"**الاسم:** {winner.get('full_name', 'غير معروف')}\n"
                                          f"**ID:** `{winner.get('user_id')}`\n"
                                          f"**عدد الإحالات:** {winner.get('real_referrals')}\n\n"
                                          f"تهانينا!", parse_mode=ParseMode.MARKDOWN, reply_markup=get_admin_panel_keyboard())
        except (ValueError, TypeError):
            await update.message.reply_text("الرجاء إرسال رقم صحيح. أعد المحاولة من لوحة التحكم.")
            context.user_data['state'] = 'awaiting_winner_threshold'
    elif state == 'awaiting_check_user_id':
        try:
            target_user_id = int(text)
            await update.message.reply_text("⏳ جاري فحص المستخدم المحدد...")
            fixed_count = await reconcile_single_user(target_user_id)
            await update.message.reply_text(f"✅ اكتمل الفحص. تم العثور على {fixed_count} تغييرات وتصحيحها.", reply_markup=get_admin_panel_keyboard())
        except (ValueError, TypeError):
             await update.message.reply_text("الرجاء إرسال ID رقمي صحيح. أعد المحاولة من لوحة التحكم.")
             context.user_data['state'] = 'awaiting_check_user_id'

# --- Automated & Background Handlers ---
async def reconcile_single_user(user_id: int) -> int:
    """Checks and fixes the referral counts for a single user."""
    user_data = await get_user_from_db_async(user_id)
    if not user_data: return 0

    all_mappings = await get_all_referral_mappings_async()
    user_mappings = [m for m in all_mappings if m.get('referrer_user_id') == user_id]

    # This is a simplified check. A full check would also involve checking group/channel membership of referred users.
    # This logic assumes the 'is_verified' flag on the referred user is the source of truth.
    verified_users_db = await get_all_users_sorted_by_async()
    verified_ids = {u['user_id'] for u in verified_users_db if u.get('is_verified')}

    calculated_real = 0
    calculated_fake = 0
    for mapping in user_mappings:
        if mapping['referred_user_id'] in verified_ids:
            calculated_real +=1
        else:
            calculated_fake += 1 # A pending or left user is considered 'fake' for this check

    db_real = user_data.get('real_referrals', 0)
    db_fake = user_data.get('fake_referrals', 0)

    changes_made = 0
    if calculated_real != db_real or calculated_fake != db_fake:
        await update_user_in_db_async(user_id, {"real_referrals": calculated_real, "fake_referrals": calculated_fake})
        changes_made = abs(calculated_real - db_real) + abs(calculated_fake - db_fake)
        print(f"Reconciled user {user_id}. DB: {db_real}R/{db_fake}F. Calculated: {calculated_real}R/{calculated_fake}F.")

    return changes_made


async def reconcile_all_referrals_job(context: ContextTypes.DEFAULT_TYPE):
    """(Checker) A manually triggered job to check ALL users and reconcile counts."""
    owner_id = context.job.chat_id
    await context.bot.send_message(owner_id, "⏳ جاري بدء الفحص الشامل... هذه العملية قد تستغرق وقتاً طويلاً جداً.")
    
    all_users = await get_all_users_sorted_by_async()
    all_mappings = await get_all_referral_mappings_async()
    if not all_mappings:
        await context.bot.send_message(owner_id, "✅ لا توجد إحالات مسجلة لفحصها.")
        return
    
    verified_ids = {u['user_id'] for u in all_users if u.get('is_verified')}
    
    # Create a map of calculated counts
    calculated_counts = {u['user_id']: {'real': 0, 'fake': 0} for u in all_users}
    
    for mapping in all_mappings:
        referrer_id = mapping.get('referrer_user_id')
        referred_id = mapping.get('referred_user_id')
        if referrer_id in calculated_counts:
            if referred_id in verified_ids:
                calculated_counts[referrer_id]['real'] += 1
            else:
                # Any user in the mapping that isn't currently verified is counted as 'fake'
                # This logic differs from the main flow where 'fake' is only for unverified new joins.
                # This is a reconciliation logic.
                calculated_counts[referrer_id]['fake'] += 1

    total_changes = 0
    for user in all_users:
        user_id = user['user_id']
        db_real = user.get('real_referrals', 0)
        db_fake = user.get('fake_referrals', 0)
        
        calc_real = calculated_counts[user_id]['real']
        calc_fake = calculated_counts[user_id]['fake']

        if db_real != calc_real or db_fake != calc_fake:
            total_changes += 1
            await update_user_in_db_async(user_id, {'real_referrals': calc_real, 'fake_referrals': calc_fake})
            print(f"Reconciled user {user_id}. DB: {db_real}R/{db_fake}F -> New: {calc_real}R/{calc_fake}F")
            await asyncio.sleep(0.1) # Be gentle

    await context.bot.send_message(owner_id, f"✅ اكتمل الفحص الشامل. تم فحص وتصحيح **{total_changes}** ملف مستخدم.")

async def recheck_leavers_and_notify_job(context: ContextTypes.DEFAULT_TYPE):
    owner_id = context.job.chat_id
    await context.bot.send_message(owner_id, "⏳ جاري بدء فحص المغادرين...")
    all_mappings = await get_all_referral_mappings_async()
    if not all_mappings:
        await context.bot.send_message(owner_id, "✅ لا توجد إحالات مسجلة لفحصها.")
        return
    fixed_count = 0
    for mapping in all_mappings:
        referred_id = mapping.get('referred_user_id')
        referrer_id = mapping.get('referrer_user_id')
        try:
            if not await is_user_in_channel_and_group(referred_id, context):
                referrer_db = await get_user_from_db_async(referrer_id)
                if referrer_db and referrer_db.get('real_referrals', 0) > 0:
                    new_real = referrer_db.get('real_referrals', 0) - 1
                    await update_user_in_db_async(referrer_id, {'real_referrals': new_real})
                    await delete_referral_mapping_async(referred_id)
                    await update_user_in_db_async(referred_id, {'is_verified': False})
                    fixed_count += 1
                    print(f"Corrected: User {referred_id} left, decremented score for referrer {referrer_id}.")
        except Exception as e:
            print(f"Error during recheck for referred user {referred_id}: {e}")
        await asyncio.sleep(0.2)
    await context.bot.send_message(owner_id, f"✅ اكتمل فحص المغادرين. تم تصحيح **{fixed_count}** حالة.")

async def handle_chat_member_updates(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    result = update.chat_member
    if not result: return

    user = result.new_chat_member.user
    user_id = user.id
    was_member = result.old_chat_member.status in ['member', 'administrator', 'creator']
    is_no_longer_member = result.new_chat_member.status in ['left', 'kicked']

    if was_member and is_no_longer_member:
        print(f"User {user.full_name} ({user_id}) left/was kicked from chat {result.chat.title}.")
        referrer_id = await get_referrer_async(user_id)
        if referrer_id:
            print(f"User {user_id} was referred by {referrer_id}. Adjusting score.")
            referrer_db = await get_user_from_db_async(referrer_id)
            if referrer_db and referrer_db.get('real_referrals', 0) > 0:
                new_real = referrer_db.get('real_referrals', 0) - 1
                new_fake = referrer_db.get('fake_referrals', 0) + 1
                await update_user_in_db_async(referrer_id, {'real_referrals': new_real, 'fake_referrals': new_fake})
                await delete_referral_mapping_async(user_id)
                await update_user_in_db_async(user_id, {'is_verified': False})
                try:
                    await context.bot.send_message(
                        chat_id=referrer_id,
                        text=f"⚠️ تنبيه! أحد المستخدمين الذين دعوتهم (**{user.full_name}**) غادر القناة/المجموعة.\n\n"
                             f"تم خصم إحالته من رصيدك. رصيدك الجديد هو: **{new_real}** إحالة حقيقية.",
                        parse_mode=ParseMode.MARKDOWN
                    )
                except TelegramError as e:
                    print(f"Could not send leave notification to referrer {referrer_id}: {e}")

def main() -> None:
    application = Application.builder().token(BOT_TOKEN).job_queue(JobQueue()).build()

    application.add_handler(ChatMemberHandler(handle_chat_member_updates, ChatMemberHandler.CHAT_MEMBER), group=0)
    application.add_handler(CommandHandler("start", start), group=1)
    application.add_handler(CommandHandler("invites", invites_command), group=1)
    application.add_handler(CommandHandler("link", link_command), group=1)
    application.add_handler(CommandHandler("top", top_command), group=1)
    application.add_handler(CallbackQueryHandler(button_handler), group=1)
    application.add_handler(MessageHandler(filters.CONTACT, handle_contact), group=2)
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_verification_text), group=2)

    print("Bot is running...")
    application.run_polling()

if __name__ == "__main__":
    main()
