"""
KPI модули – менеджерларнинг ойлик кўрсаткичларини ҳисоблаш ва бонус аниқлаш.
Маълумотлар SQLite базасида сақланади.
"""

import sqlite3
import re
import io
from datetime import datetime
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from aiogram import F, Router
from aiogram.filters import Command
from aiogram.types import Message, CallbackQuery, BufferedInputFile, InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardMarkup, KeyboardButton
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup

# ------------------- РОУТЕР -------------------
router = Router()

# ------------------- МАЪЛУМОТЛАР БАЗАСИ -------------------
def init_db():
    """SQLite базасини яратиш (агар мавжуд бўлмаса)"""
    conn = sqlite3.connect('kpi.db')
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS users
                 (user_id INTEGER PRIMARY KEY,
                  username TEXT,
                  first_name TEXT,
                  registered_at TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS kpi_records
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                  user_id INTEGER,
                  month TEXT,
                  quality_score REAL,
                  detail_score REAL,
                  delivery_score REAL,
                  total_kpi REAL,
                  bonus REAL,
                  created_at TEXT,
                  FOREIGN KEY(user_id) REFERENCES users(user_id))''')
    conn.commit()
    conn.close()

def add_user(user_id, username, first_name):
    conn = sqlite3.connect('kpi.db')
    c = conn.cursor()
    now = datetime.now().isoformat()
    try:
        c.execute("INSERT INTO users (user_id, username, first_name, registered_at) VALUES (?, ?, ?, ?)",
                  (user_id, username, first_name, now))
        conn.commit()
    except sqlite3.IntegrityError:
        pass
    finally:
        conn.close()

def save_kpi(user_id, month, quality, detail, delivery, total_kpi, bonus):
    conn = sqlite3.connect('kpi.db')
    c = conn.cursor()
    now = datetime.now().isoformat()
    c.execute('''INSERT INTO kpi_records 
                 (user_id, month, quality_score, detail_score, delivery_score, total_kpi, bonus, created_at)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
              (user_id, month, quality, detail, delivery, total_kpi, bonus, now))
    conn.commit()
    conn.close()

def get_last_kpi(user_id):
    conn = sqlite3.connect('kpi.db')
    c = conn.cursor()
    c.execute('''SELECT month, quality_score, detail_score, delivery_score, total_kpi, bonus, created_at 
                 FROM kpi_records 
                 WHERE user_id=? 
                 ORDER BY created_at DESC LIMIT 1''', (user_id,))
    row = c.fetchone()
    conn.close()
    return row

def get_all_kpi(user_id):
    conn = sqlite3.connect('kpi.db')
    c = conn.cursor()
    c.execute('''SELECT month, total_kpi, created_at 
                 FROM kpi_records 
                 WHERE user_id=? 
                 ORDER BY created_at DESC''', (user_id,))
    rows = c.fetchall()
    conn.close()
    return rows

# ------------------- ЁРДАМЧИ ФУНКЦИЯЛАР -------------------
def calculate_kpi(quality, detail, delivery):
    """KPI ҳисоблаш: 0.2*сифат + 0.2*детал + 0.6*муддат"""
    return 0.2 * quality + 0.2 * detail + 0.6 * delivery

def calculate_bonus(kpi, salary=7_500_000):
    """Bonus hisoblash: KPI >= 90% → 30%, 80% ≤ KPI < 90% → 15%, KPI < 80% → 0%"""
    if kpi >= 0.9:
        return salary * 0.3
    elif kpi >= 0.8:
        return salary * 0.15
    else:
        return 0.0

def create_kpi_chart(kpi_data):
    """
    kpi_data: рўйхат [(month, total_kpi, created_at), ...]
    График яратиб, bytes қайтаради
    """
    if not kpi_data:
        return None

    kpi_data = kpi_data[:12][::-1]  # охирги 12 та, эскилари аввал
    months = [row[0] for row in kpi_data]
    values = [row[1] * 100 for row in kpi_data]

    plt.figure(figsize=(8, 5))
    plt.plot(months, values, marker='o', linestyle='-', color='b', linewidth=2)
    plt.axhline(y=90, color='g', linestyle='--', label='90% (юқори бонус)')
    plt.axhline(y=80, color='orange', linestyle='--', label='80% (паст бонус)')
    plt.xlabel('Ой')
    plt.ylabel('KPI %')
    plt.title('KPI динамикаси')
    plt.legend()
    plt.grid(True, linestyle=':', alpha=0.6)
    plt.xticks(rotation=45)
    plt.tight_layout()

    buf = io.BytesIO()
    plt.savefig(buf, format='png')
    buf.seek(0)
    plt.close()
    return buf

# ------------------- КЛАВИАТУРАЛАР -------------------
def kpi_main_menu():
    """Асосий KPI менюси (кириллица)"""
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📥 Янги KPI киритиш", callback_data="kpi_input")],
        [InlineKeyboardButton(text="📊 Охирги ҳисобот", callback_data="kpi_report")],
        [InlineKeyboardButton(text="📈 Тарих (рўйхат)", callback_data="kpi_history")],
        [InlineKeyboardButton(text="📉 График", callback_data="kpi_chart")],
        [InlineKeyboardButton(text="❓ Ёрдам", callback_data="kpi_help")]
    ])
    return keyboard

def cancel_keyboard():
    """Бекор қилиш тугмаси"""
    keyboard = ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="❌ Бекор қилиш")]], resize_keyboard=True)
    return keyboard

# ------------------- FSM ҲОЛАТЛАРИ -------------------
class KPIInput(StatesGroup):
    month = State()
    quality = State()
    detail = State()
    delivery = State()

# ------------------- ХЕНДЛЕРЛАР -------------------
@router.message(Command("kpi"))
async def cmd_kpi(message: Message):
    """KPI бўлимига кириш"""
    user = message.from_user
    add_user(user.id, user.username, user.first_name)
    await message.answer(
        f"📊 KPI ҳисоблаш тизимига хуш келибсиз, {user.first_name}!",
        reply_markup=kpi_main_menu()
    )

@router.callback_query(F.data == "kpi_help")
async def cb_kpi_help(callback: CallbackQuery):
    await callback.answer()
    text = (
        "🤖 KPI бўйича ёрдам:\n\n"
        "📥 Янги KPI киритиш – ой, сифат, детал ва муддат кўрсаткичларини киритасиз.\n"
        "📊 Охирги ҳисобот – сўнгги сақланган KPI натижаси.\n"
        "📈 Тарих – барча KPI ёзувлари рўйхати.\n"
        "📉 График – KPI ўзгариши динамикаси.\n\n"
        "KPI формуласи: 0.2*сифат + 0.2*детал + 0.6*муддат\n"
        "Бонус:\n"
        "• KPI ≥ 90% → 30% (2 250 000 сўм)\n"
        "• 80% ≤ KPI < 90% → 15% (1 125 000 сўм)\n"
        "• KPI < 80% → 0%"
    )
    await callback.message.answer(text)

@router.callback_query(F.data == "kpi_input")
async def cb_kpi_input(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    await state.set_state(KPIInput.month)
    await callback.message.answer(
        "📅 Илтимос, ой номини киритинг (масалан: 2025-01):",
        reply_markup=cancel_keyboard()
    )

@router.message(KPIInput.month)
async def input_month(message: Message, state: FSMContext):
    if message.text == "❌ Бекор қилиш":
        await state.clear()
        await message.answer("Бекор қилинди.", reply_markup=kpi_main_menu())
        return

    month_pattern = r'^\d{4}-\d{2}$'
    if not re.match(month_pattern, message.text.strip()):
        await message.answer("Нотўғри формат. Илтимос, YYYY-MM шаклида киритинг (масалан: 2025-01):")
        return

    await state.update_data(month=message.text.strip())
    await state.set_state(KPIInput.quality)
    await message.answer("📊 Асосий полотно сифатини киритинг (0-100 оралиғида):")

@router.message(KPIInput.quality)
async def input_quality(message: Message, state: FSMContext):
    if message.text == "❌ Бекор қилиш":
        await state.clear()
        await message.answer("Бекор қилинди.", reply_markup=kpi_main_menu())
        return

    try:
        quality = float(message.text)
        if quality < 0 or quality > 100:
            raise ValueError
        await state.update_data(quality=quality / 100.0)
        await state.set_state(KPIInput.detail)
        await message.answer("📊 Деталлар сифатини киритинг (0-100 оралиғида):")
    except ValueError:
        await message.answer("Нотўғри қиймат. Илтимос, 0 дан 100 гача сон киритинг:")

@router.message(KPIInput.detail)
async def input_detail(message: Message, state: FSMContext):
    if message.text == "❌ Бекор қилиш":
        await state.clear()
        await message.answer("Бекор қилинди.", reply_markup=kpi_main_menu())
        return

    try:
        detail = float(message.text)
        if detail < 0 or detail > 100:
            raise ValueError
        await state.update_data(detail=detail / 100.0)
        await state.set_state(KPIInput.delivery)
        await message.answer("📊 Етказиб бериш муддатини киритинг (0-100 оралиғида):")
    except ValueError:
        await message.answer("Нотўғри қиймат. Илтимос, 0 дан 100 гача сон киритинг:")

@router.message(KPIInput.delivery)
async def input_delivery(message: Message, state: FSMContext):
    if message.text == "❌ Бекор қилиш":
        await state.clear()
        await message.answer("Бекор қилинди.", reply_markup=kpi_main_menu())
        return

    try:
        delivery = float(message.text)
        if delivery < 0 or delivery > 100:
            raise ValueError
        delivery = delivery / 100.0

        data = await state.get_data()
        month = data['month']
        quality = data['quality']
        detail = data['detail']

        total_kpi = calculate_kpi(quality, detail, delivery)
        bonus = calculate_bonus(total_kpi)

        # Базага сақлаш
        save_kpi(message.from_user.id, month, quality, detail, delivery, total_kpi, bonus)

        await state.clear()

        result_text = (
            f"✅ KPI муваффақиятли сақланди!\n\n"
            f"📅 Ой: {month}\n"
            f"📊 Асосий полотно: {quality*100:.2f}%\n"
            f"📊 Деталлар: {detail*100:.2f}%\n"
            f"📊 Етказиб бериш: {delivery*100:.2f}%\n"
            f"🔢 Умумий KPI: {total_kpi*100:.2f}%\n"
            f"💰 Бонус: {bonus:,.0f} сўм\n"
            f"💵 Жами даромад: {7_500_000 + bonus:,.0f} сўм"
        )
        await message.answer(result_text, reply_markup=kpi_main_menu())

    except ValueError:
        await message.answer("Нотўғри қиймат. Илтимос, 0 дан 100 гача сон киритинг:")

@router.callback_query(F.data == "kpi_report")
async def cb_kpi_report(callback: CallbackQuery):
    await callback.answer()
    last = get_last_kpi(callback.from_user.id)

    if not last:
        text = "❌ Ҳали ҳеч қандай KPI маълумоти киритилмаган."
    else:
        month, quality, detail, delivery, total, bonus, created = last
        text = (
            f"📅 **Охирги ҳисобот**\n\n"
            f"Ой: {month}\n"
            f"📊 Полотно: {quality*100:.2f}%\n"
            f"📊 Деталлар: {detail*100:.2f}%\n"
            f"📊 Етказиб бериш: {delivery*100:.2f}%\n"
            f"🔢 KPI: {total*100:.2f}%\n"
            f"💰 Бонус: {bonus:,.0f} сўм\n"
            f"💵 Жами: {7_500_000 + bonus:,.0f} сўм\n"
            f"🕐 Сана: {created[:10]}"
        )

    await callback.message.answer(text, parse_mode="Markdown")

@router.callback_query(F.data == "kpi_history")
async def cb_kpi_history(callback: CallbackQuery):
    await callback.answer()
    records = get_all_kpi(callback.from_user.id)

    if not records:
        text = "❌ Ҳали ҳеч қандай KPI маълумоти киритилмаган."
    else:
        text = "**KPI тарихи (охирги 10 та):**\n\n"
        for i, (month, total, created) in enumerate(records[:10], 1):
            text += f"{i}. {month}: {total*100:.2f}% (🕐 {created[:10]})\n"

    await callback.message.answer(text, parse_mode="Markdown")

@router.callback_query(F.data == "kpi_chart")
async def cb_kpi_chart(callback: CallbackQuery):
    await callback.answer()
    records = get_all_kpi(callback.from_user.id)

    if not records:
        await callback.message.answer("❌ График яратиш учун маълумот йўқ.")
        return

    chart_buf = create_kpi_chart(records)
    if not chart_buf:
        await callback.message.answer("❌ График яратишда хатолик.")
        return

    file = BufferedInputFile(chart_buf.getvalue(), filename="kpi_chart.png")
    await callback.message.answer_photo(photo=file, caption="📈 KPI динамикаси")

# Базани ишга тушириш
init_db()