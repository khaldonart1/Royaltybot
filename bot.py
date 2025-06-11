import os
import random
import json
import datetime
import asyncio
from supabase import create_client, Client
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, KeyboardButton, ReplyKeyboardMarkup, ReplyKeyboardRemove, ChatMemberUpdated
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes, CallbackQueryHandler, ChatMemberHandler

# --- Configuration ---
# !!! استبدل هذه القيم بالقيم الحقيقية والسرية الخاصة بك !!!
BOT_TOKEN = "7950170561:AAER-L3TyzKll--bl4n7FyPVxLxsFju6wSs"
SUPABASE_URL = "https://jofxsqsgarvzolgphqjg.supabase.co" 
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImpvZnhzcXNnYXJ2em9sZ3BocWpnIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc0OTU5NTI4NiwiZXhwIjoyMDY1MTcxMjg2fQ.egB9qticc7ABgo6vmpsrPi3cOHooQmL5uQOKI4Jytqg"

# --- Static IDs ---
CHANNEL_ID = -1002686156311
GROUP_ID = -1002472491601
BOT_OWNER_IDS = [596472053, 7164133014, 1971453570]
ALLOWED_COUNTRY_CODES = ["213", "973", "269", "253", "20", "964", "962", "965", "961", "218", "222", "212", "968", "970", "974", "966", "252", "249", "963", "216", "971", "967"]

# --- Initialize Supabase Client ---
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
def get_user_from_db(user_id):
    try:
        res = supabase.table('users').select("*").eq('user_id', user_id).single().execute()
        return res.data
    except Exception: return None

def upsert_user_in_db(user_data):
    try:
        supabase.table('users').upsert(user_data).execute()
    except Exception as e: print(f"DB_ERROR: Upserting user {user_data.get('user_id')}: {e}")

def update_user_in_db(user_id, data_to_update):
    try:
        supabase.table('users').update(data_to_update).eq('user_id', user_id).execute()
    except Exception as e: print(f"DB_ERROR: Updating user {user_id}: {e}")

def get_all_users_sorted_by(column="real_referrals"):
    try:
        res = supabase.table('users').select("user_id, full_name, real_referrals, fake_referrals, is_verified").order(column, desc=True).execute()
        return res.data or []
    except Exception: return []

def add_referral_mapping(referred_id, referrer_id):
    try:
        supabase.table('referrals').upsert({'referred_user_id': referred_id, 'referrer_user_id': referrer_id}).execute()
    except Exception as e: print(f"DB_ERROR: Adding referral map for {referred_id}: {e}")

def get_referrer(referred_id):
    try:
        res = supabase.table('referrals').select('referrer_user_id').eq('referred_user_id', referred_id).single().execute()
        return res.data.get('referrer_user_id') if res.data else None
    except Exception: return None

def delete_referral_mapping(referred_id):
    try:
        supabase.table('referrals').delete().eq('referred_user_id', referred_id).execute()
    except Exception as e: print(f"DB_ERROR: Deleting map for {referred_id}: {e}")
    
def reset_all_referrals_in_db():
    try:
        supabase.table('users').update({"real_referrals": 0, "fake_referrals": 0}).gt('user_id', 0).execute()
        supabase.table('referrals').delete().gt('referred_user_id', 0).execute()
        print("All referrals have been reset.")
    except Exception as e: print(f"DB_ERROR: Resetting all referrals: {e}")

# --- Text Generation Functions ---
def get_referral_stats_text(user_info):
    if not user_info: return "لا توجد لديك بيانات بعد."
    real_count = user_info.get("real_referrals", 0)
    fake_count = user_info.get("fake_referrals", 0)
    return (f"📊 **إحصائيات إحالاتك:**\n\n✅ الإحالات الحقيقية: **{real_count}**\n⏳ الإحالات الوهمية: **{fake_count}**")

def get_referral_link_text(user_id, bot_username):
    return f"🔗 رابط الإحالة الخاص بك:\n`https://t.me/{bot_username}?start={user_id}`"

def get_top_5_text(user_id):
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
    current_user_info = get_user_from_db(user_id)
    current_user_real_refs = current_user_info.get("real_referrals", 0) if current_user_info else 0
    user_rank_str = "غير مصنف"
    if current_user_real_refs > 0:
        for i, uinfo in enumerate(sorted_users):
            if uinfo.get('user_id') == user_id:
                user_rank_str = f"#{i + 1}"; break
    text += f"🎖️ ترتيبك: **{user_rank_str}**\n✅ رصيدك: **{current_user_real_refs}** إحالة حقيقية."
    return text

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
    return InlineKeyboardMarkup([[InlineKeyboardButton("تقرير الإحالات الحقيقية", callback_data="admin_report_real")],
                                 [InlineKeyboardButton("تقرير الإحالات الوهمية", callback_data="admin_report_fake")],
                                 [InlineKeyboardButton("🏆 اختيار فائز عشوائي", callback_data="pick_winner")],
                                 [InlineKeyboardButton("📢 إرسال رسالة للجميع", callback_data="admin_broadcast")],
                                 [InlineKeyboardButton("⚠️ تصفير كل الإحالات ⚠️", callback_data="admin_reset_all")],
                                 [InlineKeyboardButton("➡️ العودة للقائمة الرئيسية", callback_data="main_menu")]])
def get_reset_confirmation_keyboard():
    return InlineKeyboardMarkup([[InlineKeyboardButton("✅ نعم، قم بالتصفير", callback_data="admin_reset_confirm")],
                                 [InlineKeyboardButton("❌ لا، الغِ الأمر", callback_data="admin_panel")]])

# --- Helper Functions ---
async def is_user_in_channel_and_group(user_id, context):
    try:
        ch_mem = await context.bot.get_chat_member(chat_id=CHANNEL_ID, user_id=user_id)
        if ch_mem.status not in ['member', 'administrator', 'creator']: return False
        gr_mem = await context.bot.get_chat_member(chat_id=GROUP_ID, user_id=user_id)
        if gr_mem.status not in ['member', 'administrator', 'creator']: return False
        return True
    except Exception: return False
def generate_math_question():
    num1, num2 = random.randint(1, 10), random.randint(1, 10)
    return f"{num1} + {num2}", num1 + num2

# --- Core Handlers ---
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_chat.type != 'private': return
    user = update.effective_user
    db_user = get_user_from_db(user.id)
    if not db_user:
        upsert_user_in_db({'user_id': user.id, 'full_name': user.full_name, 'is_verified': False, 'real_referrals': 0, 'fake_referrals': 0})
        db_user = get_user_from_db(user.id)
    elif db_user.get('full_name') != user.full_name:
        update_user_in_db(user.id, {'full_name': user.full_name})
    if db_user and db_user.get("is_verified"):
        await update.message.reply_text(VERIFIED_WELCOME_MESSAGE, reply_markup=get_main_menu_keyboard(user.id))
        return
    args = context.args
    if args:
        try:
            referrer_id = int(args[0])
            if referrer_id != user.id and not await is_user_in_channel_and_group(user.id, context):
                context.user_data['referrer_id'] = referrer_id
                if not get_user_from_db(referrer_id):
                    upsert_user_in_db({'user_id': referrer_id, 'full_name': f"User_{referrer_id}", 'is_verified': False})
                referrer_db = get_user_from_db(referrer_id)
                new_fake_count = referrer_db.get('fake_referrals', 0) + 1
                update_user_in_db(referrer_id, {'fake_referrals': new_fake_count})
        except (ValueError, IndexError): pass
    await update.message.reply_text(WELCOME_MESSAGE)
    await ask_math_question(update, context)

async def ask_math_question(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    question, answer = generate_math_question()
    context.user_data['math_answer'] = answer
    await update.message.reply_text(f"{MATH_QUESTION_MESSAGE}\n\nما هو ناتج {question}؟")

async def handle_verification_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    if update.effective_chat.type != 'private': return
    if user_id in BOT_OWNER_IDS and context.user_data.get('state'):
        await handle_admin_messages(update, context); return
    db_user = get_user_from_db(user_id)
    if db_user and db_user.get('is_verified'):
        await update.message.reply_text(VERIFIED_WELCOME_MESSAGE, reply_markup=get_main_menu_keyboard(user_id)); return
    if 'math_answer' in context.user_data:
        try:
            if int(update.message.text) == context.user_data['math_answer']:
                del context.user_data['math_answer']
                phone_button = [[KeyboardButton("شارك رقم هاتفي", request_contact=True)]]
                await update.message.reply_text(PHONE_REQUEST_MESSAGE, reply_markup=ReplyKeyboardMarkup(phone_button, resize_keyboard=True, one_time_keyboard=True))
            else:
                await update.message.reply_text("إجابة خاطئة. حاول مرة اخرى."); await ask_math_question(update, context)
        except (ValueError, TypeError): await update.message.reply_text("من فضلك أدخل رقماً صحيحاً.")

async def handle_contact(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_chat.type != 'private': return
    contact = update.effective_message.contact
    if contact and contact.user_id == update.effective_user.id:
        phone_number = contact.phone_number
        if any(phone_number.lstrip('+').startswith(code) for code in ALLOWED_COUNTRY_CODES):
            keyboard = [
                [InlineKeyboardButton("1. الانضمام للقناة", url="https://t.me/Ry_Hub")],
                [InlineKeyboardButton("2. الانضمام للمجموعة", url="https://t.me/+Rrx4fWReNLxlYWNk")],
                [InlineKeyboardButton("✅ لقد انضممت الآن", callback_data="confirm_join")]
            ]
            await update.message.reply_text(JOIN_PROMPT_MESSAGE, reply_markup=InlineKeyboardMarkup(keyboard))
            await update.message.reply_text("تم استلام الرقم.", reply_markup=ReplyKeyboardRemove())
        else:
            await update.message.reply_text(INVALID_COUNTRY_CODE_MESSAGE, reply_markup=ReplyKeyboardRemove()); return

async def handle_chat_member_updates(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    result = ChatMemberUpdated.extract_status_change(update.chat_member)
    if not result or update.chat.id != GROUP_ID: return
    was_member, is_now_member = result
    user = update.chat_member.new_chat_member.user
    if was_member and not is_now_member:
        referrer_id = get_referrer(user.id)
        if referrer_id:
            update_user_in_db(user.id, {'is_verified': False}) 
            delete_referral_mapping(user.id)
            referrer_db = get_user_from_db(referrer_id)
            if referrer_db:
                new_real = max(0, referrer_db.get('real_referrals', 0) - 1)
                update_user_in_db(referrer_id, {'real_referrals': new_real})

async def handle_admin_messages(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_user.id not in BOT_OWNER_IDS or update.effective_chat.type != 'private': return
    state = context.user_data.get('state')
    
    if state == 'awaiting_winner_threshold':
        try:
            threshold = int(update.message.text)
            del context.user_data['state']
            eligible_users = [u for u in get_all_users_sorted_by() if u.get("real_referrals", 0) >= threshold]
            if not eligible_users:
                await update.message.reply_text(f"لا يوجد أي متسابقين لديهم {threshold} إحالة حقيقية أو أكثر."); return
            winner_info = random.choice(eligible_users)
            winner_name = winner_info.get('full_name', f"User_{winner_info.get('user_id')}")
            winner_refs = winner_info.get('real_referrals', 0)
            announcement = (f"🎉 **تم اختيار الفائز عشوائياً!** 🎉\n\nمن بين **{len(eligible_users)}** متسابق مؤهل، الفائز هو:\n\n🏆 **{winner_name}** 🏆\nبرصيد **{winner_refs}** إحالة حقيقية.")
            await update.message.reply_text(announcement, parse_mode="Markdown")
        except (ValueError): await update.message.reply_text("الرجاء إرسال رقم صحيح فقط.")
        except Exception as e: await update.message.reply_text(f"حدث خطأ: {e}"); context.user_data.pop('state', None)

    elif state == 'awaiting_broadcast_message':
        del context.user_data['state']
        all_users = get_all_users_sorted_by()
        verified_users = [u for u in all_users if u.get('is_verified')]
        if not verified_users:
            await update.message.reply_text("لا يوجد مستخدمين موثقين لإرسال الرسالة إليهم."); return
        
        await update.message.reply_text(f"⏳ جاري بدء إرسال الرسالة إلى {len(verified_users)} مستخدم. قد تستغرق هذه العملية بعض الوقت...")
        success_count, fail_count = 0, 0
        
        for user_data in verified_users:
            user_id = user_data.get('user_id')
            try:
                await context.bot.forward_message(chat_id=user_id, from_chat_id=update.message.chat_id, message_id=update.message.message_id)
                success_count += 1
                await asyncio.sleep(0.1) # To avoid hitting rate limits
            except Exception as e:
                fail_count += 1; print(f"Failed to send broadcast to {user_id}: {e}")
        
        await update.message.reply_text(f"✅ اكتملت الإذاعة.\n\n- تم الإرسال بنجاح إلى: {success_count} مستخدم.\n- فشل الإرسال إلى: {fail_count} مستخدم (غالباً قاموا بحظر البوت).")

# --- Command and Button Handlers ---
async def invites_command(update, context):
    if update.effective_chat.type != 'private': return
    user_info = get_user_from_db(update.effective_user.id)
    await update.message.reply_text(get_referral_stats_text(user_info), parse_mode="Markdown")
async def link_command(update, context):
    if update.effective_chat.type != 'private': return
    await update.message.reply_text(get_referral_link_text(update.effective_user.id, context.bot.username), parse_mode="Markdown")
async def top_command(update, context):
    if update.effective_chat.type != 'private': return
    await update.message.reply_text(get_top_5_text(update.effective_user.id), parse_mode="Markdown")

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query; await query.answer(); user = query.from_user; user_id = user.id
    is_owner = user_id in BOT_OWNER_IDS
    if query.data == "main_menu": await query.edit_message_text(text=VERIFIED_WELCOME_MESSAGE, reply_markup=get_main_menu_keyboard(user_id))
    elif query.data == "my_referrals":
        user_info = get_user_from_db(user_id)
        await query.edit_message_text(get_referral_stats_text(user_info), parse_mode="Markdown", reply_markup=get_main_menu_keyboard(user_id))
    elif query.data == "my_link":
        await query.edit_message_text(get_referral_link_text(user_id, context.bot.username), parse_mode="Markdown", reply_markup=get_main_menu_keyboard(user_id))
    elif query.data == "top_5":
        await query.edit_message_text(get_top_5_text(user_id), parse_mode="Markdown", reply_markup=get_main_menu_keyboard(user_id))
    elif query.data == "confirm_join":
        if await is_user_in_channel_and_group(user.id, context):
            db_user = get_user_from_db(user.id)
            if not db_user or not db_user.get('is_verified'):
                update_user_in_db(user.id, {'is_verified': True, 'full_name': user.full_name})
                if 'referrer_id' in context.user_data:
                    referrer_id = context.user_data['referrer_id']
                    referrer_db = get_user_from_db(referrer_id)
                    if referrer_db:
                        new_real = referrer_db.get('real_referrals', 0) + 1
                        new_fake = max(0, referrer_db.get('fake_referrals', 0) - 1)
                        update_user_in_db(referrer_id, {'real_referrals': new_real, 'fake_referrals': new_fake})
                        add_referral_mapping(user.id, referrer_id)
                        try:
                            await context.bot.send_message(chat_id=referrer_id, text=f"🎉 تهانينا! لقد انضم مستخدم جديد عن طريق رابطك.\n\nرصيدك الجديد هو: **{new_real}** إحالة حقيقية.", parse_mode='Markdown')
                        except Exception as e: print(f"Could not send notification to referrer {referrer_id}: {e}")
            await query.message.edit_text(JOIN_SUCCESS_MESSAGE)
            await query.message.reply_text(VERIFIED_WELCOME_MESSAGE, reply_markup=get_main_menu_keyboard(user.id))
        else:
            await query.answer(text=JOIN_FAIL_MESSAGE, show_alert=True)
            
    # Admin Panel Logic
    elif query.data == "admin_panel" and is_owner:
        await query.edit_message_text(text="👑 أهلاً بك في لوحة تحكم المالك.", reply_markup=get_admin_panel_keyboard())
    elif query.data == "pick_winner" and is_owner:
        context.user_data['state'] = 'awaiting_winner_threshold'; await query.edit_message_text(text="الرجاء إرسال الحد الأدنى لعدد الإحالات الحقيقية لدخول السحب (مثال: أرسل الرقم 5).")
    elif query.data == "admin_broadcast" and is_owner:
        context.user_data['state'] = 'awaiting_broadcast_message'
        await query.edit_message_text(text="الآن، أرسل الرسالة التي تريد إذاعتها لجميع المستخدمين الموثقين.")
    elif query.data == "admin_reset_all" and is_owner:
        await query.edit_message_text(text="⚠️ **تأكيد الإجراء** ⚠️\n\nهل أنت متأكد؟ هذا الإجراء لا يمكن التراجع عنه.", parse_mode="Markdown", reply_markup=get_reset_confirmation_keyboard())
    elif query.data == "admin_reset_confirm" and is_owner:
        reset_all_referrals_in_db()
        await query.edit_message_text(text="✅ تم تصفير جميع إحصائيات الإحالات بنجاح.", reply_markup=get_admin_panel_keyboard())
    elif query.data == "admin_report_real" and is_owner:
        sorted_users = get_all_users_sorted_by("real_referrals")
        report = "📊 **تقرير جميع المستخدمين (حسب الإحالات الحقيقية):**\n\n"
        for uinfo in sorted_users:
            full_name = uinfo.get('full_name', f"User_{uinfo.get('user_id')}"); report += f"• {full_name} - **{uinfo.get('real_referrals', 0)}** إحالة\n"
        await query.edit_message_text(text=report, parse_mode="Markdown", reply_markup=get_admin_panel_keyboard())
    elif query.data == "admin_report_fake" and is_owner:
        sorted_users = get_all_users_sorted_by("fake_referrals")
        report = "⏳ **تقرير جميع المستخدمين (حسب الإحالات الوهمية):**\n\n"
        users_with_fakes = [u for u in sorted_users if u.get("fake_referrals", 0) > 0]
        if not users_with_fakes: report = "لا توجد إحالات وهمية حالياً."
        else:
            for uinfo in users_with_fakes:
                full_name = uinfo.get('full_name', f"User_{uinfo.get('user_id')}"); report += f"• {full_name} - **{uinfo.get('fake_referrals', 0)}** إحالة وهمية\n"
        await query.edit_message_text(text=report, parse_mode="Markdown", reply_markup=get_admin_panel_keyboard())

def main() -> None:
    application = Application.builder().token(BOT_TOKEN).build()
    application.add_handler(ChatMemberHandler(handle_chat_member_updates, ChatMemberHandler.CHAT_MEMBER))
    # State handlers must have priority (lower group number)
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_admin_messages), group=0)
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_verification_text), group=1)
    # Command and other handlers
    application.add_handler(CommandHandler("start", start), group=2)
    application.add_handler(CommandHandler("Invites", invites_command), group=2)
    application.add_handler(CommandHandler("link", link_command), group=2)
    application.add_handler(CommandHandler("Top", top_command), group=2)
    application.add_handler(CallbackQueryHandler(button_handler), group=2)
    application.add_handler(MessageHandler(filters.CONTACT, handle_contact), group=2)
    print("Bot is running with Supabase integration and all features...")
    application.run_polling()

if __name__ == "__main__":
    main()
