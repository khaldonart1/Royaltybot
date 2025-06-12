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
# The bot will read these from the .env file on PythonAnywhere
# Make sure to set these environment variables on your server
BOT_TOKEN = "7950170561:AAECeQpxb1G4zrnFhrol_uBgPNoxZN-Qkz0"
SUPABASE_URL = "https://jofxsqsgarvzolgphqjg.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImpvZnhzcXNnYXJ2em9sZ3BocWpnIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc0OTU5NTI4NiwiZXhwIjoyMDY1MTcxMjg2fQ.egB9qticc7ABgo6vmpsrPi3cOHooQmL5uQOKI4Jytqg"

# --- Static IDs ---
# IMPORTANT: Replace these with your actual IDs
CHANNEL_ID = -1002686156311 # Your public channel ID
GROUP_ID = -1002472491601   # Your group ID
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

# --- Supabase Helper Functions ---
# These functions handle all database interactions.
def get_user_from_db(user_id):
    """Fetches a single user's data from the 'users' table."""
    try:
        res = supabase.table('users').select("*").eq('user_id', user_id).single().execute()
        return res.data
    except Exception: return None

def upsert_user_in_db(user_data):
    """Inserts a new user or updates an existing one."""
    try:
        supabase.table('users').upsert(user_data, on_conflict='user_id').execute()
    except Exception as e: print(f"DB_ERROR: Upserting user {user_data.get('user_id')}: {e}")

def update_user_in_db(user_id, data_to_update):
    """Updates specific fields for a user."""
    try:
        supabase.table('users').update(data_to_update).eq('user_id', user_id).execute()
    except Exception as e: print(f"DB_ERROR: Updating user {user_id}: {e}")

def get_all_users_sorted_by(column="real_referrals"):
    """Fetches all users, sorted by a specific column."""
    try:
        res = supabase.table('users').select("user_id, full_name, real_referrals, fake_referrals, is_verified").order(column, desc=True).execute()
        return res.data or []
    except Exception: return []

def get_all_referral_mappings():
    """Fetches all entries from the 'referrals' table."""
    try:
        res = supabase.table('referrals').select("*").execute()
        return res.data or []
    except Exception: return []

def get_user_counts():
    """Gets the total and verified user counts."""
    try:
        # Note: The Python client might not return 'count' directly in this format.
        # A more robust way might be to fetch all IDs and count them.
        total_res = supabase.table('users').select('user_id', count='exact').execute()
        verified_res = supabase.table('users').select('user_id', count='exact').eq('is_verified', True).execute()
        return total_res.count or 0, verified_res.count or 0
    except Exception as e: 
        print(f"DB_ERROR: Getting user counts: {e}")
        return 0, 0
    
def add_referral_mapping(referred_id, referrer_id):
    """Records a new referral relationship."""
    try:
        supabase.table('referrals').upsert({'referred_user_id': referred_id, 'referrer_user_id': referrer_id}, on_conflict='referred_user_id').execute()
    except Exception as e: print(f"DB_ERROR: Adding referral map for {referred_id}: {e}")

def get_referrer(referred_id):
    """Finds who referred a given user."""
    try:
        res = supabase.table('referrals').select('referrer_user_id').eq('referred_user_id', referred_id).single().execute()
        return res.data.get('referrer_user_id') if res.data else None
    except Exception: return None

def delete_referral_mapping(referred_id):
    """Deletes a referral relationship, e.g., when a user leaves."""
    try:
        supabase.table('referrals').delete().eq('referred_user_id', referred_id).execute()
    except Exception as e: print(f"DB_ERROR: Deleting map for {referred_id}: {e}")
    
def reset_all_referrals_in_db():
    """Admin function to reset all referral stats to zero."""
    try:
        supabase.table('users').update({"real_referrals": 0, "fake_referrals": 0}).gt('user_id', 0).execute()
        supabase.table('referrals').delete().gt('referred_user_id', 0).execute()
        print("All referrals have been reset.")
    except Exception as e: print(f"DB_ERROR: Resetting all referrals: {e}")

# --- Text Generation Functions ---
# These functions create the formatted text messages sent to users.
def get_referral_stats_text(user_info):
    """Generates the text for 'My Stats'."""
    if not user_info: return "لا توجد لديك بيانات بعد."
    real_count = user_info.get("real_referrals", 0)
    fake_count = user_info.get("fake_referrals", 0)
    return (f"📊 **إحصائيات إحالاتك:**\n\n"
            f"✅ الإحالات الحقيقية: **{real_count}**\n"
            f"⏳ الإحالات الوهمية: **{fake_count}**")

def get_referral_link_text(user_id, bot_username):
    """Generates the user's unique referral link."""
    return f"🔗 رابط الإحالة الخاص بك:\n`https://t.me/{bot_username}?start={user_id}`"

def get_top_5_text(user_id):
    """Generates the leaderboard text."""
    sorted_users = get_all_users_sorted_by("real_referrals")
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
        # Find the rank of the current user
        user_rank_str = "غير مصنف"
        user_index = next((i for i, u in enumerate(sorted_users) if u.get('user_id') == user_id), -1)
        if user_index != -1:
            user_rank_str = f"#{user_index + 1}"
        
        current_user_info = sorted_users[user_index] if user_index != -1 else get_user_from_db(user_id)
        current_user_real_refs = current_user_info.get("real_referrals", 0) if current_user_info else 0
        
        text += f"🎖️ ترتيبك: **{user_rank_str}**\n✅ رصيدك: **{current_user_real_refs}** إحالة حقيقية."
    except (StopIteration, IndexError):
         text += "لا يمكنك رؤية ترتيبك حتى تقوم بدعوة شخص واحد على الأقل."

    return text

def get_paginated_report(all_users, page, report_type):
    """Generates a paginated report for the admin panel."""
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
        count = uinfo.get(column, 0)
        report += f"• {full_name} - **{count}**\n"
        
    nav_buttons = []
    if page > 1: nav_buttons.append(InlineKeyboardButton("⬅️ السابق", callback_data=f"report_{report_type}_page_{page-1}"))
    if page < total_pages: nav_buttons.append(InlineKeyboardButton("التالي ➡️", callback_data=f"report_{report_type}_page_{page+1}"))
    
    keyboard = [nav_buttons, [InlineKeyboardButton("🔙 العودة للوحة التحكم", callback_data="admin_panel")]]
    return report, InlineKeyboardMarkup(keyboard)

# --- Keyboards ---
# These functions generate the various inline and reply keyboards.
def get_main_menu_keyboard(user_id):
    """Generates the main menu keyboard, showing admin button for owners."""
    is_owner = user_id in BOT_OWNER_IDS
    keyboard = [[InlineKeyboardButton("إحصائياتي 📊", callback_data="my_referrals")],
                [InlineKeyboardButton("رابطي 🔗", callback_data="my_link")],
                [InlineKeyboardButton("🏆 أفضل 5 متسابقين", callback_data="top_5")]]
    if is_owner:
        keyboard.append([InlineKeyboardButton("👑 لوحة تحكم المالك 👑", callback_data="admin_panel")])
    return InlineKeyboardMarkup(keyboard)

def get_admin_panel_keyboard():
    """Generates the admin panel keyboard."""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📊 تقرير الإحالات الحقيقية", callback_data="report_real_page_1")],
        [InlineKeyboardButton("⏳ تقرير الإحالات الوهمية", callback_data="report_fake_page_1")],
        [InlineKeyboardButton("👥 عدد مستخدمي البوت", callback_data="admin_user_count")],
        [InlineKeyboardButton("🏆 اختيار فائز عشوائي", callback_data="pick_winner")],
        [InlineKeyboardButton("📢 إرسال رسالة للجميع", callback_data="admin_broadcast")],
        [InlineKeyboardButton("🔧 أدوات التصحيح", callback_data="admin_correction_tools")],
        [InlineKeyboardButton("⚠️ تصفير كل الإحالات ⚠️", callback_data="admin_reset_all")],
        [InlineKeyboardButton("⬅️ العودة للقائمة الرئيسية", callback_data="main_menu")]
    ])

def get_correction_tools_keyboard():
    """Generates the correction tools keyboard for admins."""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔄 إعادة فحص المغادرين", callback_data="admin_recheck_leavers")],
        [InlineKeyboardButton("✍️ تعديل مستخدم فردي", callback_data="admin_fix_individual")],
        [InlineKeyboardButton("🔙 العودة للوحة التحكم", callback_data="admin_panel")]
    ])

def get_reset_confirmation_keyboard():
    """Generates the confirmation keyboard for resetting stats."""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ نعم، قم بالتصفير", callback_data="admin_reset_confirm")],
        [InlineKeyboardButton("❌ لا، الغِ الأمر", callback_data="admin_panel")]
    ])

# --- Helper Functions ---
async def is_user_in_channel_and_group(user_id, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """Checks if a user is a member of both the required channel and group."""
    try:
        # Check channel membership
        ch_mem = await context.bot.get_chat_member(chat_id=CHANNEL_ID, user_id=user_id)
        if ch_mem.status not in ['member', 'administrator', 'creator']:
            return False
        # Check group membership
        gr_mem = await context.bot.get_chat_member(chat_id=GROUP_ID, user_id=user_id)
        if gr_mem.status not in ['member', 'administrator', 'creator']:
            return False
        return True
    except TelegramError as e:
        # Handle cases where the user has blocked the bot or other permission issues
        print(f"Error checking membership for {user_id}: {e}")
        return False
    except Exception as e:
        print(f"An unexpected error occurred while checking membership for {user_id}: {e}")
        return False

def generate_math_question():
    """Generates a simple addition question for verification."""
    num1, num2 = random.randint(1, 10), random.randint(1, 10)
    question = f"{num1} + {num2}"
    answer = num1 + num2
    return question, answer

# --- Core Command Handlers ---
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handles the /start command. Onboards new users and processes referrals."""
    if update.effective_chat.type != Chat.PRIVATE: return
    
    user = update.effective_user
    db_user = get_user_from_db(user.id)

    # If user is new or has a different name, update DB
    if not db_user:
        upsert_user_in_db({'user_id': user.id, 'full_name': user.full_name, 'username': user.username, 'is_verified': False, 'real_referrals': 0, 'fake_referrals': 0})
        db_user = get_user_from_db(user.id) # Re-fetch after creation
    elif db_user.get('full_name') != user.full_name or db_user.get('username') != user.username:
        update_user_in_db(user.id, {'full_name': user.full_name, 'username': user.username})

    # If user is already verified, show main menu and stop
    if db_user and db_user.get("is_verified"):
        await update.message.reply_text(VERIFIED_WELCOME_MESSAGE, reply_markup=get_main_menu_keyboard(user.id))
        return

    # Process referral link (if present)
    args = context.args
    if args:
        try:
            referrer_id = int(args[0])
            # A user cannot refer themselves. Only process if the new user isn't already linked.
            if referrer_id != user.id and not get_referrer(user.id):
                context.user_data['referrer_id'] = referrer_id
                
                # Ensure referrer exists in the DB
                referrer_db = get_user_from_db(referrer_id)
                if not referrer_db:
                    upsert_user_in_db({'user_id': referrer_id, 'full_name': f"User_{referrer_id}", 'is_verified': False, 'real_referrals': 0, 'fake_referrals': 0})
                    referrer_db = get_user_from_db(referrer_id)

                # Increment fake referrals for now. This will be converted to a real one upon verification.
                new_fake_count = referrer_db.get('fake_referrals', 0) + 1
                update_user_in_db(referrer_id, {'fake_referrals': new_fake_count})
        except (ValueError, IndexError):
            pass # Invalid start parameter

    await update.message.reply_text(WELCOME_MESSAGE)
    await ask_math_question(update, context)

async def invites_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handles the /invites command, showing referral stats."""
    if update.effective_chat.type != Chat.PRIVATE: return
    user_info = get_user_from_db(update.effective_user.id)
    await update.message.reply_text(get_referral_stats_text(user_info), parse_mode=ParseMode.MARKDOWN, reply_markup=get_main_menu_keyboard(update.effective_user.id))
    
async def link_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handles the /link command, showing the referral link."""
    if update.effective_chat.type != Chat.PRIVATE: return
    await update.message.reply_text(get_referral_link_text(update.effective_user.id, context.bot.username), parse_mode=ParseMode.MARKDOWN, reply_markup=get_main_menu_keyboard(update.effective_user.id))

async def top_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handles the /top command, showing the leaderboard."""
    if update.effective_chat.type != Chat.PRIVATE: return
    await update.message.reply_text(get_top_5_text(update.effective_user.id), parse_mode=ParseMode.MARKDOWN, reply_markup=get_main_menu_keyboard(update.effective_user.id))

# --- Core Message & Callback Handlers ---

async def ask_math_question(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Sends the math question to the user."""
    question, answer = generate_math_question()
    context.user_data['math_answer'] = answer
    # Use reply to maintain context for the user
    await update.message.reply_text(f"{MATH_QUESTION_MESSAGE}\n\nما هو ناتج {question}؟")

async def handle_verification_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handles text messages during the verification process."""
    user_id = update.effective_user.id
    if update.effective_chat.type != Chat.PRIVATE: return

    # If the message is from an admin with a pending action, route to admin handler
    if user_id in BOT_OWNER_IDS and context.user_data.get('state'):
        await handle_admin_messages(update, context)
        return

    db_user = get_user_from_db(user_id)
    if db_user and db_user.get('is_verified'):
        # If a verified user types something, just show them the menu
        await update.message.reply_text(VERIFIED_WELCOME_MESSAGE, reply_markup=get_main_menu_keyboard(user_id))
        return

    # Check for math answer
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
    """Handles the shared contact for phone verification."""
    if update.effective_chat.type != Chat.PRIVATE: return
    
    contact = update.effective_message.contact
    if contact and contact.user_id == update.effective_user.id:
        phone_number = contact.phone_number.lstrip('+')
        # Check if the phone number's country code is in the allowed list
        if any(phone_number.startswith(code) for code in ALLOWED_COUNTRY_CODES):
            keyboard = [[InlineKeyboardButton("1. الانضمام للقناة", url=f"https://t.me/{os.environ.get('CHANNEL_USERNAME', 'Ry_Hub')}")],
                        [InlineKeyboardButton("2. الانضمام للمجموعة", url=f"https://t.me/joinchat/{os.environ.get('GROUP_INVITE_LINK', 'Rrx4fWReNLxlYWNk')}")],
                        [InlineKeyboardButton("✅ لقد انضممت، تحقق الآن", callback_data="confirm_join")]]
            await update.message.reply_text(JOIN_PROMPT_MESSAGE, reply_markup=InlineKeyboardMarkup(keyboard))
            await update.message.reply_text("تم استلام الرقم بنجاح.", reply_markup=ReplyKeyboardRemove())
        else:
            await update.message.reply_text(INVALID_COUNTRY_CODE_MESSAGE, reply_markup=ReplyKeyboardRemove())
            # Optionally, restart the process for them
            await ask_math_question(update, context)

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handles all inline keyboard button presses."""
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
        user_info = get_user_from_db(user_id)
        await query.edit_message_text(get_referral_stats_text(user_info), parse_mode=ParseMode.MARKDOWN, reply_markup=get_main_menu_keyboard(user_id))
    
    elif data == "my_link":
        await query.edit_message_text(get_referral_link_text(user_id, context.bot.username), parse_mode=ParseMode.MARKDOWN, reply_markup=get_main_menu_keyboard(user_id))
    
    elif data == "top_5":
        await query.edit_message_text(get_top_5_text(user_id), parse_mode=ParseMode.MARKDOWN, reply_markup=get_main_menu_keyboard(user_id))
    
    # --- Verification Flow ---
    elif data == "confirm_join":
        await query.edit_message_text("⏳ جاري التحقق من انضمامك...")
        if await is_user_in_channel_and_group(user.id, context):
            db_user = get_user_from_db(user.id)
            if not db_user or not db_user.get('is_verified'):
                # Mark user as verified
                update_user_in_db(user.id, {'is_verified': True, 'full_name': user.full_name, 'username': user.username})
                
                # If they were referred, update the referrer's stats
                if 'referrer_id' in context.user_data:
                    referrer_id = context.user_data['referrer_id']
                    referrer_db = get_user_from_db(referrer_id)
                    if referrer_db:
                        new_real = referrer_db.get('real_referrals', 0) + 1
                        new_fake = max(0, referrer_db.get('fake_referrals', 0) - 1)
                        update_user_in_db(referrer_id, {'real_referrals': new_real, 'fake_referrals': new_fake})
                        add_referral_mapping(user.id, referrer_id)
                        del context.user_data['referrer_id'] # Clear after use
                        
                        # Notify the referrer
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
            # Re-edit message to show the buttons again
            keyboard = [[InlineKeyboardButton("1. الانضمام للقناة", url=f"https://t.me/{os.environ.get('CHANNEL_USERNAME', 'Ry_Hub')}")],
                        [InlineKeyboardButton("2. الانضمام للمجموعة", url=f"https://t.me/joinchat/{os.environ.get('GROUP_INVITE_LINK', 'Rrx4fWReNLxlYWNk')}")],
                        [InlineKeyboardButton("✅ لقد انضممت، تحقق الآن", callback_data="confirm_join")]]
            await query.edit_message_text(JOIN_PROMPT_MESSAGE, reply_markup=InlineKeyboardMarkup(keyboard))
    
    # --- Admin Panel Buttons (Owner Only) ---
    if not is_owner: return # All following actions are owner-only

    if data == "admin_panel":
        await query.edit_message_text(text="👑 أهلاً بك في لوحة تحكم المالك.", reply_markup=get_admin_panel_keyboard())
    
    elif data == "admin_user_count":
        total_users, verified_users = get_user_counts()
        text = (f"📈 **إحصائيات مستخدمي البوت:**\n\n"
                f"▫️ إجمالي المستخدمين المسجلين: **{total_users}**\n"
                f"✅ المستخدمون الموثقون: **{verified_users}**")
        await query.edit_message_text(text=text, parse_mode=ParseMode.MARKDOWN, reply_markup=get_admin_panel_keyboard())
    
    elif data == "pick_winner":
        context.user_data['state'] = 'awaiting_winner_threshold'
        await query.edit_message_text(text="الرجاء إرسال الحد الأدنى لعدد الإحالات الحقيقية لدخول السحب (مثال: أرسل الرقم 5).")
    
    elif data == "admin_broadcast":
        context.user_data['state'] = 'awaiting_broadcast_message'
        await query.edit_message_text(text="الآن، أرسل الرسالة التي تريد إذاعتها لجميع المستخدمين الموثقين. يمكنك استخدام تنسيق Markdown.")
    
    elif data == "admin_correction_tools":
        await query.edit_message_text(text="🔧 **أدوات التصحيح**\n\nاختر الأداة التي تريد استخدامها:", reply_markup=get_correction_tools_keyboard())
    
    elif data == "admin_recheck_leavers":
        context.job_queue.run_once(recheck_leavers_and_notify, 1, chat_id=user_id, name=f"recheck_{user_id}")
        await query.edit_message_text(text="تم جدولة فحص المغادرين. ستبدأ العملية في الخلفية وستصلك رسالة عند الانتهاء.", reply_markup=get_admin_panel_keyboard())
    
    elif data == "admin_fix_individual":
        context.user_data['state'] = 'awaiting_fix_user_id'
        await query.edit_message_text(text="الرجاء إرسال الـ ID الرقمي للمستخدم الذي تريد تحويل إحدى إحالاته الوهمية إلى حقيقية.")
    
    elif data == "admin_reset_all":
        await query.edit_message_text(text="⚠️ **تأكيد الإجراء** ⚠️\n\nهل أنت متأكد من أنك تريد تصفير **جميع** الإحالات الحقيقية والوهمية **لجميع** المستخدمين؟ هذا الإجراء لا يمكن التراجع عنه.", parse_mode=ParseMode.MARKDOWN, reply_markup=get_reset_confirmation_keyboard())
    
    elif data == "admin_reset_confirm":
        reset_all_referrals_in_db()
        await query.edit_message_text(text="✅ تم تصفير جميع إحصائيات الإحالات بنجاح.", reply_markup=get_admin_panel_keyboard())
    
    elif data.startswith("report_real_page_"):
        page = int(data.split('_')[-1])
        all_users = get_all_users_sorted_by("real_referrals")
        text, keyboard = get_paginated_report(all_users, page, 'real')
        await query.edit_message_text(text=text, parse_mode=ParseMode.MARKDOWN, reply_markup=keyboard)
    
    elif data.startswith("report_fake_page_"):
        page = int(data.split('_')[-1])
        all_users = [u for u in get_all_users_sorted_by("fake_referrals") if u.get('fake_referrals', 0) > 0]
        text, keyboard = get_paginated_report(all_users, page, 'fake')
        await query.edit_message_text(text=text, parse_mode=ParseMode.MARKDOWN, reply_markup=keyboard)


async def handle_admin_messages(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles text messages from owners when a specific 'state' is set."""
    user_id = update.effective_user.id
    if user_id not in BOT_OWNER_IDS or not context.user_data.get('state'):
        # This function should only be triggered for admins in a specific state.
        # The routing in handle_verification_text ensures this.
        return

    state = context.user_data.pop('state') # Get and clear state in one step
    text = update.message.text

    if state == 'awaiting_broadcast_message':
        await update.message.reply_text("⏳ جاري بدء الإذاعة... ستصلك رسالة عند الانتهاء.")
        all_verified_users = [u for u in get_all_users_sorted_by() if u.get('is_verified')]
        sent_count = 0
        failed_count = 0
        for user in all_verified_users:
            try:
                await context.bot.send_message(chat_id=user['user_id'], text=text, parse_mode=ParseMode.MARKDOWN)
                sent_count += 1
            except TelegramError as e:
                print(f"Failed to send broadcast to {user['user_id']}: {e}")
                failed_count += 1
            await asyncio.sleep(0.1) # Avoid rate limits
        await update.message.reply_text(f"✅ اكتملت الإذاعة.\n\n- تم الإرسال بنجاح إلى: {sent_count} مستخدم\n- فشل الإرسال إلى: {failed_count} مستخدم", reply_markup=get_admin_panel_keyboard())
    
    elif state == 'awaiting_winner_threshold':
        try:
            threshold = int(text)
            eligible_users = [u for u in get_all_users_sorted_by() if u.get('real_referrals', 0) >= threshold]
            if not eligible_users:
                await update.message.reply_text(f"لا يوجد مستخدمون لديهم {threshold} إحالة حقيقية أو أكثر.", reply_markup=get_admin_panel_keyboard())
                return
            
            winner = random.choice(eligible_users)
            winner_name = winner.get('full_name', 'غير معروف')
            winner_id = winner.get('user_id')
            winner_referrals = winner.get('real_referrals')
            
            await update.message.reply_text(f"🎉 الفائز هو...!\n\n"
                                          f"**الاسم:** {winner_name}\n"
                                          f"**ID:** `{winner_id}`\n"
                                          f"**عدد الإحالات:** {winner_referrals}\n\n"
                                          f"تهانينا!", parse_mode=ParseMode.MARKDOWN, reply_markup=get_admin_panel_keyboard())
        except (ValueError, TypeError):
            await update.message.reply_text("الرجاء إرسال رقم صحيح. أعد المحاولة من لوحة التحكم.")
            context.user_data['state'] = 'awaiting_winner_threshold' # Restore state

    elif state == 'awaiting_fix_user_id':
        try:
            target_user_id = int(text)
            user_to_fix = get_user_from_db(target_user_id)
            if not user_to_fix:
                await update.message.reply_text("لم يتم العثور على مستخدم بهذا الـ ID.", reply_markup=get_admin_panel_keyboard())
                return
            
            if user_to_fix.get('fake_referrals', 0) > 0:
                new_real = user_to_fix.get('real_referrals', 0) + 1
                new_fake = user_to_fix.get('fake_referrals', 0) - 1
                update_user_in_db(target_user_id, {'real_referrals': new_real, 'fake_referrals': new_fake})
                await update.message.reply_text(f"✅ تم بنجاح تعديل المستخدم {user_to_fix.get('full_name')}.\n\n"
                                              f"الرصيد الجديد: {new_real} حقيقي, {new_fake} وهمي.", reply_markup=get_admin_panel_keyboard())
            else:
                await update.message.reply_text("هذا المستخدم ليس لديه أي إحالات وهمية لتصحيحها.", reply_markup=get_admin_panel_keyboard())
        except (ValueError, TypeError):
            await update.message.reply_text("الرجاء إرسال ID رقمي صحيح. أعد المحاولة من لوحة التحكم.")
            context.user_data['state'] = 'awaiting_fix_user_id' # Restore state


# --- Automated & Background Handlers ---
async def recheck_leavers_and_notify(context: ContextTypes.DEFAULT_TYPE):
    """A manually triggered job to check all referred users and fix stats if they left."""
    owner_id = context.job.chat_id
    await context.bot.send_message(owner_id, "⏳ جاري بدء فحص المغادرين... هذه العملية قد تستغرق وقتاً.")
    
    all_mappings = get_all_referral_mappings()
    if not all_mappings:
        await context.bot.send_message(owner_id, "✅ لا توجد إحالات مسجلة لفحصها.")
        return
        
    fixed_count = 0
    for mapping in all_mappings:
        referred_id = mapping.get('referred_user_id')
        referrer_id = mapping.get('referrer_user_id')
        
        try:
            # We only need to check one, but checking both is more robust
            is_still_member = await is_user_in_channel_and_group(referred_id, context)
            if not is_still_member:
                referrer_db = get_user_from_db(referrer_id)
                # Only decrement if they had real referrals
                if referrer_db and referrer_db.get('real_referrals', 0) > 0:
                    new_real = referrer_db.get('real_referrals', 0) - 1
                    # We can optionally add back to fake referrals or just leave it
                    update_user_in_db(referrer_id, {'real_referrals': new_real})
                    delete_referral_mapping(referred_id)
                    update_user_in_db(referred_id, {'is_verified': False}) # Un-verify the leaver
                    fixed_count += 1
                    print(f"Corrected: User {referred_id} left, decremented score for referrer {referrer_id}.")
        except Exception as e:
            print(f"Error during recheck for referred user {referred_id}: {e}")
        await asyncio.sleep(0.2) # Be gentle with the API

    await context.bot.send_message(owner_id, f"✅ اكتمل فحص المغادرين. تم تصحيح **{fixed_count}** حالة.")


async def handle_chat_member_updates(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    This is the most important handler for maintaining accurate referral counts.
    It triggers whenever a user's status changes in the channel or group.
    """
    result = update.chat_member
    if not result: return

    user = result.new_chat_member.user
    user_id = user.id
    
    # Check if the status changed to 'left' or 'kicked'
    was_member = result.old_chat_member.status in ['member', 'administrator', 'creator']
    is_no_longer_member = result.new_chat_member.status in ['left', 'kicked']

    if was_member and is_no_longer_member:
        print(f"User {user.full_name} ({user_id}) left/was kicked from chat {result.chat.title}.")
        
        # Now, check if this user was a "real" referral for someone
        referrer_id = get_referrer(user_id)
        
        if referrer_id:
            print(f"User {user_id} was referred by {referrer_id}. Adjusting score.")
            referrer_db = get_user_from_db(referrer_id)
            
            if referrer_db and referrer_db.get('real_referrals', 0) > 0:
                # Decrement real referrals and add to fake referrals
                new_real = referrer_db.get('real_referrals', 0) - 1
                new_fake = referrer_db.get('fake_referrals', 0) + 1
                update_user_in_db(referrer_id, {'real_referrals': new_real, 'fake_referrals': new_fake})
                
                # Remove the referral mapping since it's no longer valid
                delete_referral_mapping(user_id)
                
                # Mark the user who left as unverified
                update_user_in_db(user_id, {'is_verified': False})

                # Notify the original referrer
                try:
                    referrer_name = referrer_db.get('full_name', f"User {referrer_id}")
                    await context.bot.send_message(
                        chat_id=referrer_id,
                        text=f"⚠️ تنبيه! أحد المستخدمين الذين دعوتهم (**{user.full_name}**) غادر القناة/المجموعة.\n\n"
                             f"تم خصم إحالته من رصيدك. رصيدك الجديد هو: **{new_real}** إحالة حقيقية.",
                        parse_mode=ParseMode.MARKDOWN
                    )
                except TelegramError as e:
                    print(f"Could not send leave notification to referrer {referrer_id}: {e}")


def main() -> None:
    """Starts the bot."""
    application = Application.builder().token(BOT_TOKEN).job_queue(JobQueue()).build()

    # Priority 0: The ChatMemberHandler is crucial and should be checked first.
    application.add_handler(ChatMemberHandler(handle_chat_member_updates, ChatMemberHandler.CHAT_MEMBER), group=0)

    # Priority 1: Core commands and callbacks for all users.
    application.add_handler(CommandHandler("start", start), group=1)
    application.add_handler(CommandHandler("invites", invites_command), group=1)
    application.add_handler(CommandHandler("link", link_command), group=1)
    application.add_handler(CommandHandler("top", top_command), group=1)
    application.add_handler(CallbackQueryHandler(button_handler), group=1)
    
    # Priority 2: Handlers for the verification flow.
    application.add_handler(MessageHandler(filters.CONTACT, handle_contact), group=2)
    # This handler must come after contact handler. It handles math answers and routes admin messages.
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_verification_text), group=2)

    print("Bot is running...")
    application.run_polling()

if __name__ == "__main__":
    main()
