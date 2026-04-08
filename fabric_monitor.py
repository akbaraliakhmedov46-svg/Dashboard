# fabric_monitor.py
# -*- coding: utf-8 -*-
"""
Модуль учёта поступления тканей, заявок и тестирования.
Все надписи на узбекском языке (кириллица).
"""

import logging
import os
from datetime import datetime
from io import BytesIO
from typing import Dict, List, Optional

import gspread
import matplotlib.pyplot as plt
from aiogram import Dispatcher, F, Router
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    BufferedInputFile,
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)
from aiogram.utils.keyboard import InlineKeyboardBuilder

logger = logging.getLogger(__name__)

# Импорты из основного бота
from bot import (
    PRODUCTION_TOPIC_ID,
    TZ,
    is_admin,
    safe_sheets_call,
    send_to_group,
    sheets_rate_limiter,
    doc,
)

FABRIC_CONTROL_TOPIC_ID = os.getenv("FABRIC_CONTROL_TOPIC_ID")
if FABRIC_CONTROL_TOPIC_ID:
    try:
        FABRIC_CONTROL_TOPIC_ID = int(FABRIC_CONTROL_TOPIC_ID)
    except ValueError:
        logger.error("FABRIC_CONTROL_TOPIC_ID not a valid integer, falling back to PRODUCTION_TOPIC_ID")
        FABRIC_CONTROL_TOPIC_ID = PRODUCTION_TOPIC_ID
else:
    FABRIC_CONTROL_TOPIC_ID = PRODUCTION_TOPIC_ID

# ---------- Конфигурация ----------
FABRIC_USERS = list(map(int, os.getenv("FABRIC_USERS", "0").split(",")))
if not FABRIC_USERS or FABRIC_USERS == [0]:
    FABRIC_USERS = []  # если не задано – только админ

# ---------- Router ----------
fabric_router = Router()

# ---------- FSM для поступления тканей (13 шагов) ----------
class FabricStates(StatesGroup):
    waiting_for_app_number = State()      # 1. Заявка №
    waiting_for_postavshik = State()      # 2. Етказиб берувчи
    waiting_for_buyurtmachi = State()     # 3. Буюртмачи
    waiting_for_name = State()             # 4. Мато номи
    waiting_for_reja_weight = State()      # 5. Режа вазни
    waiting_for_fakt_weight = State()      # 6. Факт вазни
    waiting_for_partiya = State()          # 7. Партия рақами
    waiting_for_rulons = State()           # 8. Рулонлар сони
    waiting_for_shirina = State()          # 9. Эни
    waiting_for_plotnost = State()         # 10. Зичлиги
    waiting_for_color = State()            # 11. Ранги
    waiting_for_ton = State()               # 12. Тонг мослиги
    waiting_for_date = State()              # 13. Келган сана

# ---------- FSM для заявок (10 шагов) ----------
class FabricOrderStates(StatesGroup):
    waiting_for_app_number = State()      # 1. Заявка №
    waiting_for_postavshik = State()      # 2. Етказиб берувчи
    waiting_for_buyurtmachi = State()     # 3. Буюртмачи
    waiting_for_name = State()             # 4. Мато номи
    waiting_for_plotnost_shirina = State() # 5. Зичлик ва эни
    waiting_for_reja_weight = State()      # 6. Режа вазни
    waiting_for_fakt_weight = State()      # 7. Факт вазни
    waiting_for_color = State()            # 8. Ранги
    waiting_for_date = State()             # 9. Тузулган сана (DD-MM-YYYY)
    waiting_for_comment = State()          # 10. Изоҳ

class FabricTestStates(StatesGroup):
    waiting_for_partiya = State()
    waiting_for_length = State()
    waiting_for_width = State()
    waiting_for_skew = State()
    waiting_for_test_date = State()

# ---------- Проверка прав ----------
def is_fabric_user(user_id: int) -> bool:
    if not FABRIC_USERS:
        return is_admin(user_id)
    return user_id in FABRIC_USERS

# ---------- Работа с Google Sheets (поступление тканей) ----------
def ensure_fabric_worksheets():
    try:
        sheet_fabric = doc.worksheet("MatoKelishi")
    except gspread.exceptions.WorksheetNotFound:
        sheet_fabric = doc.add_worksheet("MatoKelishi", rows=1000, cols=20)
        sheet_fabric.append_row([
            "ID", "Заявка №", "Етказиб берувчи", "Буюртмачи", "Мато номи",
            "Режа вазни (кг)", "Факт вазни (кг)", "Партия рақами",
            "Рулонлар сони", "Эни (м)", "Зичлиги (гр/м²)",
            "Ранги (код)", "Тонг мослиги (%)", "Келган сана", "Изоҳ"
        ])
    try:
        sheet_test = doc.worksheet("MatoTest")
    except gspread.exceptions.WorksheetNotFound:
        sheet_test = doc.add_worksheet("MatoTest", rows=1000, cols=10)
        sheet_test.append_row([
            "Партия рақами", "Узунлик кискариши (%)",
            "Эн кискариши (%)", "Қийшайиш (%)", "Тест санаси"
        ])
    return sheet_fabric, sheet_test

# ---------- Работа с Google Sheets (заявки) ----------
def ensure_fabric_orders_worksheet():
    try:
        sheet_orders = doc.worksheet("Buyurtmalar")
    except gspread.exceptions.WorksheetNotFound:
        sheet_orders = doc.add_worksheet("Buyurtmalar", rows=1000, cols=20)
        sheet_orders.append_row([
            "ID", "Заявка №", "Етказиб берувчи", "Буюртмачи", "Мато номи",
            "Зичлиги (гр/м²)", "Эни (м)", "Режа вазни (кг)", "Факт вазни (кг)",
            "Ранги", "Тузулган сана", "Изоҳ", "Яратилган сана"
        ])
    return sheet_orders

@sheets_rate_limiter
def add_fabric_entry(data: Dict) -> Optional[int]:
    sheet_fabric, _ = ensure_fabric_worksheets()
    if not sheet_fabric:
        return None
    try:
        all_rows = safe_sheets_call(sheet_fabric.get_all_values)
        next_id = len(all_rows)
        row = [
            next_id,
            data.get("app_number", ""),
            data.get("postavshik", ""),
            data.get("buyurtmachi", ""),
            data.get("name", ""),
            data.get("reja_weight", ""),
            data.get("fakt_weight", ""),
            data.get("partiya", ""),
            data.get("rulons", ""),
            data.get("shirina", ""),
            data.get("plotnost", ""),
            data.get("color", ""),
            data.get("ton", ""),
            data.get("date", ""),
            data.get("comment", "")
        ]
        safe_sheets_call(sheet_fabric.append_row, row)
        return next_id
    except Exception as e:
        logger.error(f"Ошибка записи в MatoKelishi: {e}")
        return None

@sheets_rate_limiter
def add_fabric_order(data: Dict) -> Optional[int]:
    sheet_orders = ensure_fabric_orders_worksheet()
    if not sheet_orders:
        return None
    try:
        all_rows = safe_sheets_call(sheet_orders.get_all_values)
        next_id = len(all_rows)
        row = [
            next_id,
            data.get("app_number", ""),
            data.get("postavshik", ""),
            data.get("buyurtmachi", ""),
            data.get("name", ""),
            data.get("plotnost", ""),
            data.get("shirina", ""),
            data.get("reja_weight", ""),
            data.get("fakt_weight", ""),
            data.get("color", ""),
            data.get("date", ""),
            data.get("comment", ""),
            datetime.now(TZ).strftime("%d.%m.%Y %H:%M")
        ]
        safe_sheets_call(sheet_orders.append_row, row)
        return next_id
    except Exception as e:
        logger.error(f"Ошибка записи в Buyurtmalar: {e}")
        return None

@sheets_rate_limiter
def add_test_entry(partiya: str, length: float, width: float, skew: float, test_date: str) -> bool:
    _, sheet_test = ensure_fabric_worksheets()
    if not sheet_test:
        return False
    try:
        row = [partiya, length, width, skew, test_date]
        safe_sheets_call(sheet_test.append_row, row)
        return True
    except Exception as e:
        logger.error(f"Ошибка записи в MatoTest: {e}")
        return False

@sheets_rate_limiter
def get_all_fabric_entries(limit: int = 10) -> List[Dict]:
    sheet_fabric, _ = ensure_fabric_worksheets()
    if not sheet_fabric:
        return []
    try:
        all_rows = safe_sheets_call(sheet_fabric.get_all_values)
        if len(all_rows) <= 1:
            return []
        data_rows = all_rows[1:]
        last_rows = data_rows[-limit:]
        result = []
        for row in last_rows:
            if len(row) < 15:
                continue
            result.append({
                "id": row[0],
                "app_number": row[1],
                "postavshik": row[2],
                "buyurtmachi": row[3],
                "name": row[4],
                "reja_weight": row[5],
                "fakt_weight": row[6],
                "partiya": row[7],
                "rulons": row[8],
                "shirina": row[9],
                "plotnost": row[10],
                "color": row[11],
                "ton": row[12],
                "date": row[13],
                "comment": row[14] if len(row) > 14 else ""
            })
        return result
    except Exception as e:
        logger.error(f"Ошибка чтения MatoKelishi: {e}")
        return []

@sheets_rate_limiter
def get_all_fabric_orders(limit: int = 10) -> List[Dict]:
    sheet_orders = ensure_fabric_orders_worksheet()
    if not sheet_orders:
        return []
    try:
        all_rows = safe_sheets_call(sheet_orders.get_all_values)
        if len(all_rows) <= 1:
            return []
        data_rows = all_rows[1:]
        last_rows = data_rows[-limit:]
        result = []
        for row in last_rows:
            if len(row) < 12:
                continue
            result.append({
                "id": row[0],
                "app_number": row[1],
                "postavshik": row[2],
                "buyurtmachi": row[3],
                "name": row[4],
                "plotnost": row[5],
                "shirina": row[6],
                "reja_weight": row[7],
                "fakt_weight": row[8],
                "color": row[9],
                "date": row[10],
                "comment": row[11] if len(row) > 11 else "",
                "created_at": row[12] if len(row) > 12 else ""
            })
        return result
    except Exception as e:
        logger.error(f"Ошибка чтения Buyurtmalar: {e}")
        return []

@sheets_rate_limiter
def get_test_by_partiya(partiya: str) -> Optional[Dict]:
    _, sheet_test = ensure_fabric_worksheets()
    if not sheet_test:
        return None
    try:
        all_rows = safe_sheets_call(sheet_test.get_all_values)
        if len(all_rows) <= 1:
            return None
        data_rows = all_rows[1:]
        matches = [r for r in data_rows if len(r) >= 5 and r[0] == partiya]
        if not matches:
            return None
        last = matches[-1]
        return {
            "partiya": last[0],
            "length": last[1],
            "width": last[2],
            "skew": last[3],
            "test_date": last[4]
        }
    except Exception as e:
        logger.error(f"Ошибка чтения MatoTest: {e}")
        return None

# ---------- Генерация изображений (для поступлений) ----------
def create_fabric_card(entry: Dict, test: Optional[Dict] = None) -> BytesIO:
    try:
        fig, ax = plt.subplots(figsize=(10, 8))
        ax.axis('off')
        ax.set_title(f"📦 Мато келиши (ID: {entry['id']})", fontsize=16, fontweight='bold', pad=20)

        data = [
            ["Заявка №", entry.get("app_number", "")],
            ["Етказиб берувчи", entry.get("postavshik", "")],
            ["Буюртмачи", entry.get("buyurtmachi", "")],
            ["Мато номи", entry.get("name", "")],
            ["Режа вазни (кг)", entry.get("reja_weight", "")],
            ["Факт вазни (кг)", entry.get("fakt_weight", "")],
            ["Партия рақами", entry.get("partiya", "")],
            ["Рулонлар сони", entry.get("rulons", "")],
            ["Эни (м)", entry.get("shirina", "")],
            ["Зичлиги (гр/м²)", entry.get("plotnost", "")],
            ["Ранги (код)", entry.get("color", "")],
            ["Тонг мослиги (%)", entry.get("ton", "")],
            ["Келган сана", entry.get("date", "")],
            ["Изоҳ", entry.get("comment", "")]
        ]

        table = ax.table(cellText=data, loc='center', cellLoc='left', colWidths=[0.3, 0.5])
        table.auto_set_font_size(False)
        table.set_fontsize(10)
        table.scale(1, 1.5)

        for i in range(len(data)):
            table[(i, 0)].set_facecolor("#2E7D32")
            table[(i, 0)].set_text_props(color='white', weight='bold')
            table[(i, 1)].set_facecolor("#F5F5F5")

        if test:
            ax2 = plt.axes([0.1, 0.25, 0.8, 0.2])
            ax2.axis('off')
            test_data = [
                ["Тест натижалари", ""],
                ["Узунлик кискариши (%)", test.get("length", "")],
                ["Эн кискариши (%)", test.get("width", "")],
                ["Қийшайиш (%)", test.get("skew", "")],
                ["Тест санаси", test.get("test_date", "")]
            ]
            table2 = ax2.table(cellText=test_data, loc='center', cellLoc='left', colWidths=[0.4, 0.4])
            table2.auto_set_font_size(False)
            table2.set_fontsize(10)
            table2.scale(1, 1.5)
            for i in range(len(test_data)):
                table2[(i, 0)].set_facecolor("#FF8C00")
                table2[(i, 0)].set_text_props(color='white', weight='bold')
                table2[(i, 1)].set_facecolor("#FFF3E0")

        plt.tight_layout()
        buf = BytesIO()
        plt.savefig(buf, format='png', dpi=100, bbox_inches='tight')
        buf.seek(0)
        plt.close()
        return buf
    except Exception as e:
        logger.error(f"Ошибка создания изображения: {e}")
        return BytesIO()

def create_fabric_orders_list_table(entries: List[Dict]) -> BytesIO:
    """Создаёт таблицу PNG для списка заявок"""
    try:
        fig, ax = plt.subplots(figsize=(14, 6 + len(entries)*0.5))
        ax.axis('off')
        ax.set_title("📋 Охирги заявкалар", fontsize=16, fontweight='bold', pad=20)

        headers = ["ID", "Заявка №", "Етказиб берувчи", "Мато номи", "Режа (кг)", "Факт (кг)", "Сана"]
        cell_data = [headers]
        for e in entries:
            cell_data.append([
                e.get("id", ""),
                e.get("app_number", "")[:10],
                e.get("postavshik", "")[:15],
                e.get("name", "")[:15],
                e.get("reja_weight", ""),
                e.get("fakt_weight", ""),
                e.get("date", "")[:10]
            ])

        table = ax.table(cellText=cell_data, loc='center', cellLoc='center')
        table.auto_set_font_size(False)
        table.set_fontsize(9)
        table.scale(1, 1.5)

        for i in range(len(headers)):
            table[(0, i)].set_facecolor("#2E7D32")
            table[(0, i)].set_text_props(color='white', weight='bold')

        for i in range(1, len(cell_data)):
            for j in range(len(headers)):
                table[(i, j)].set_facecolor("#F5F5F5" if i % 2 == 1 else "#E0E0E0")

        plt.tight_layout()
        buf = BytesIO()
        plt.savefig(buf, format='png', dpi=100, bbox_inches='tight')
        buf.seek(0)
        plt.close()
        return buf
    except Exception as e:
        logger.error(f"Ошибка создания списка заявок: {e}")
        return BytesIO()

# ---------- Инлайн-клавиатуры ----------
def fabric_main_menu() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(text="📦 Киритиш (келиш)", callback_data="fabric_add"),
        InlineKeyboardButton(text="📋 Рўйхат (келишлар)", callback_data="fabric_list")
    )
    builder.row(
        InlineKeyboardButton(text="📝 Заявка киритиш", callback_data="fabric_order_add"),
        InlineKeyboardButton(text="📝 Тест киритиш", callback_data="fabric_test")
    )
    builder.row(
        InlineKeyboardButton(text="🔍 Қидириш", callback_data="fabric_search"),
        InlineKeyboardButton(text="⬅️ Бош меню", callback_data="back_main")
    )
    return builder.as_markup()

def cancel_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="❌ Бекор қилиш", callback_data="fabric_cancel")
    return builder.as_markup()

# ---------- Обработчики ----------
@fabric_router.message(Command("fabric"))
async def cmd_fabric_menu(message: Message):
    if not is_fabric_user(message.from_user.id):
        await message.answer("❌ Рухсат йўқ.")
        return
    await message.answer("📋 Мато бўлими:", reply_markup=fabric_main_menu())

@fabric_router.callback_query(F.data == "fabric_add")
async def cb_fabric_add(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    await callback.message.edit_text(
        "📦 Янги мато келишини киритиш.\n"
        "1. Заявка № ни киритинг:",
        reply_markup=cancel_keyboard()
    )
    await state.set_state(FabricStates.waiting_for_app_number)

@fabric_router.callback_query(F.data == "fabric_order_add")
async def cb_fabric_order_add(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    await callback.message.edit_text(
        "📝 Янги заявка киритиш.\n"
        "1. Заявка № ни киритинг:",
        reply_markup=cancel_keyboard()
    )
    await state.set_state(FabricOrderStates.waiting_for_app_number)

@fabric_router.callback_query(F.data == "fabric_list")
async def cb_fabric_list(callback: CallbackQuery):
    await callback.answer()
    entries = get_all_fabric_orders(limit=10)
    if not entries:
        await callback.message.edit_text("Ҳали заявка мавжуд эмас.", reply_markup=fabric_main_menu())
        return
    text = "📋 Охирги 10 та заявка:\n\n"
    for e in entries:
        text += f"ID: {e['id']} | Заявка: {e['app_number']} | {e['postavshik']} | {e['name']} | {e['reja_weight']} кг | {e['date']}\n"
    img = create_fabric_orders_list_table(entries)
    if img.getbuffer().nbytes > 100:
        await callback.message.answer_photo(
            BufferedInputFile(img.getvalue(), filename="fabric_orders_list.png"),
            caption=text[:200]
        )
    else:
        await callback.message.answer(text, reply_markup=fabric_main_menu())

@fabric_router.callback_query(F.data == "fabric_search")
async def cb_fabric_search(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    await callback.message.edit_text(
        "🔍 Партия рақами ёки ID киритинг:",
        reply_markup=cancel_keyboard()
    )
    await state.set_state("waiting_for_fabric_search")

@fabric_router.message(F.data == "waiting_for_fabric_search")
async def process_fabric_search(message: Message, state: FSMContext):
    query = message.text.strip()
    entries = get_all_fabric_entries(limit=100)
    entry = None
    for e in entries:
        if e["id"] == query or e["partiya"] == query:
            entry = e
            break
    if not entry:
        await message.answer("❌ Бундай маълумот топилмади.", reply_markup=fabric_main_menu())
    else:
        test = get_test_by_partiya(entry["partiya"])
        img = create_fabric_card(entry, test)
        if img.getbuffer().nbytes > 100:
            await message.answer_photo(
                BufferedInputFile(img.getvalue(), filename="fabric_card.png"),
                caption=f"📌 Партия: {entry['partiya']}"
            )
        else:
            text = f"📦 Мато келиши (ID: {entry['id']})\n"
            text += f"Заявка №: {entry['app_number']}\n"
            text += f"Етказиб берувчи: {entry['postavshik']}\n"
            text += f"Буюртмачи: {entry['buyurtmachi']}\n"
            text += f"Мато номи: {entry['name']}\n"
            text += f"Режа вазни: {entry['reja_weight']} кг\n"
            text += f"Факт вазни: {entry['fakt_weight']} кг\n"
            text += f"Партия: {entry['partiya']}\n"
            text += f"Рулонлар: {entry['rulons']}\n"
            text += f"Эни: {entry['shirina']} м\n"
            text += f"Зичлиги: {entry['plotnost']} гр/м²\n"
            text += f"Ранги: {entry['color']}\n"
            text += f"Тонг мослиги: {entry['ton']}%\n"
            text += f"Келган сана: {entry['date']}\n"
            if test:
                text += "\n📊 Тест натижалари:\n"
                text += f"Узунлик кискариши: {test['length']}%\n"
                text += f"Эн кискариши: {test['width']}%\n"
                text += f"Қийшайиш: {test['skew']}%\n"
                text += f"Тест санаси: {test['test_date']}"
            await message.answer(text)
    await state.clear()

@fabric_router.callback_query(F.data == "fabric_test")
async def cb_fabric_test(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    await callback.message.edit_text(
        "📝 Партия рақами киритинг (тест натижалари учун):",
        reply_markup=cancel_keyboard()
    )
    await state.set_state(FabricTestStates.waiting_for_partiya)

@fabric_router.callback_query(F.data == "fabric_cancel")
async def cb_fabric_cancel(callback: CallbackQuery, state: FSMContext):
    await callback.answer("Бекор қилинди")
    await state.clear()
    await callback.message.edit_text("📋 Мато бўлими:", reply_markup=fabric_main_menu())

# ---------- Обработчики ввода для поступления тканей (FabricStates) ----------
@fabric_router.message(FabricStates.waiting_for_app_number)
async def process_app_number(message: Message, state: FSMContext):
    await state.update_data(app_number=message.text.strip())
    await message.answer("2. Етказиб берувчи номини киритинг:", reply_markup=cancel_keyboard())
    await state.set_state(FabricStates.waiting_for_postavshik)

@fabric_router.message(FabricStates.waiting_for_postavshik)
async def process_postavshik(message: Message, state: FSMContext):
    await state.update_data(postavshik=message.text.strip())
    await message.answer("3. Буюртмачи (мижоз) номини киритинг:", reply_markup=cancel_keyboard())
    await state.set_state(FabricStates.waiting_for_buyurtmachi)

@fabric_router.message(FabricStates.waiting_for_buyurtmachi)
async def process_buyurtmachi(message: Message, state: FSMContext):
    await state.update_data(buyurtmachi=message.text.strip())
    await message.answer("4. Мато номини киритинг:", reply_markup=cancel_keyboard())
    await state.set_state(FabricStates.waiting_for_name)

@fabric_router.message(FabricStates.waiting_for_name)
async def process_name(message: Message, state: FSMContext):
    await state.update_data(name=message.text.strip())
    await message.answer("5. Режа бўйича вазн (кг) киритинг:", reply_markup=cancel_keyboard())
    await state.set_state(FabricStates.waiting_for_reja_weight)

@fabric_router.message(FabricStates.waiting_for_reja_weight)
async def process_reja_weight(message: Message, state: FSMContext):
    try:
        weight = float(message.text.replace(',', '.'))
        await state.update_data(reja_weight=weight)
        await message.answer("6. Факт (келган) вазн (кг) киритинг:", reply_markup=cancel_keyboard())
        await state.set_state(FabricStates.waiting_for_fakt_weight)
    except ValueError:
        await message.answer("❌ Илтимос, сон киритинг (масалан: 125.5):", reply_markup=cancel_keyboard())

@fabric_router.message(FabricStates.waiting_for_fakt_weight)
async def process_fakt_weight(message: Message, state: FSMContext):
    try:
        weight = float(message.text.replace(',', '.'))
        await state.update_data(fakt_weight=weight)
        await message.answer("7. Партия рақами (ёки идентификатор) киритинг:", reply_markup=cancel_keyboard())
        await state.set_state(FabricStates.waiting_for_partiya)
    except ValueError:
        await message.answer("❌ Илтимос, сон киритинг:", reply_markup=cancel_keyboard())

@fabric_router.message(FabricStates.waiting_for_partiya)
async def process_partiya(message: Message, state: FSMContext):
    await state.update_data(partiya=message.text.strip())
    await message.answer("8. Рулонлар сони (дона) киритинг:", reply_markup=cancel_keyboard())
    await state.set_state(FabricStates.waiting_for_rulons)

@fabric_router.message(FabricStates.waiting_for_rulons)
async def process_rulons(message: Message, state: FSMContext):
    try:
        rulons = int(message.text)
        await state.update_data(rulons=rulons)
        await message.answer("9. Эни (метр) киритинг (масалан: 1.8):", reply_markup=cancel_keyboard())
        await state.set_state(FabricStates.waiting_for_shirina)
    except ValueError:
        await message.answer("❌ Бутун сон киритинг:", reply_markup=cancel_keyboard())

@fabric_router.message(FabricStates.waiting_for_shirina)
async def process_shirina(message: Message, state: FSMContext):
    try:
        shirina = float(message.text.replace(',', '.'))
        await state.update_data(shirina=shirina)
        await message.answer("10. Зичлиги (гр/м²) киритинг (масалан: 180):", reply_markup=cancel_keyboard())
        await state.set_state(FabricStates.waiting_for_plotnost)
    except ValueError:
        await message.answer("❌ Илтимос, сон киритинг:", reply_markup=cancel_keyboard())

@fabric_router.message(FabricStates.waiting_for_plotnost)
async def process_plotnost(message: Message, state: FSMContext):
    try:
        plotnost = float(message.text.replace(',', '.'))
        await state.update_data(plotnost=plotnost)
        await message.answer("11. Ранги (пантон коди ёки ранг номи) киритинг:", reply_markup=cancel_keyboard())
        await state.set_state(FabricStates.waiting_for_color)
    except ValueError:
        await message.answer("❌ Илтимос, сон киритинг:", reply_markup=cancel_keyboard())

@fabric_router.message(FabricStates.waiting_for_color)
async def process_color(message: Message, state: FSMContext):
    await state.update_data(color=message.text.strip())
    await message.answer("12. Тонг мослиги (% фоиз) киритинг (0-100):", reply_markup=cancel_keyboard())
    await state.set_state(FabricStates.waiting_for_ton)

@fabric_router.message(FabricStates.waiting_for_ton)
async def process_ton(message: Message, state: FSMContext):
    try:
        ton = float(message.text.replace(',', '.'))
        if ton < 0 or ton > 100:
            await message.answer("❌ 0-100 оралиғида киритинг:", reply_markup=cancel_keyboard())
            return
        await state.update_data(ton=ton)
        await message.answer("13. Келган сана (кун.ой.йил, масалан: 25.02.2026) киритинг:", reply_markup=cancel_keyboard())
        await state.set_state(FabricStates.waiting_for_date)
    except ValueError:
        await message.answer("❌ Сон киритинг:", reply_markup=cancel_keyboard())

@fabric_router.message(FabricStates.waiting_for_date)
async def process_date(message: Message, state: FSMContext):
    date_str = message.text.strip()
    try:
        datetime.strptime(date_str, "%d.%m.%Y")
    except ValueError:
        await message.answer("❌ Нотўғри формат. Қайта киритинг (кун.ой.йил):", reply_markup=cancel_keyboard())
        return

    data = await state.get_data()
    data["date"] = date_str
    data["comment"] = ""

    entry_id = add_fabric_entry(data)
    if entry_id is None:
        await message.answer("❌ Маълумотларни сақлашда хатолик.", reply_markup=fabric_main_menu())
        await state.clear()
        return

    await send_to_group(
        f"📦 Янги мато келиши (ID: {entry_id})\n"
        f"Заявка №: {data['app_number']}\n"
        f"Етказиб берувчи: {data['postavshik']}\n"
        f"Буюртмачи: {data['buyurtmachi']}\n"
        f"Мато: {data['name']}\n"
        f"Факт вазни: {data['fakt_weight']} кг\n"
        f"Партия: {data['partiya']}",
        FABRIC_CONTROL_TOPIC_ID
    )

    await message.answer(
        f"✅ Маълумотлар сақланди! ID: {entry_id}",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📝 Тест киритиш", callback_data=f"test_{data['partiya']}")],
            [InlineKeyboardButton(text="📋 Мато менюси", callback_data="fabric_main")]
        ])
    )
    await state.clear()

# ---------- Обработчики ввода для заявок (FabricOrderStates) ----------
@fabric_router.message(FabricOrderStates.waiting_for_app_number)
async def order_process_app_number(message: Message, state: FSMContext):
    await state.update_data(app_number=message.text.strip())
    await message.answer("2. Етказиб берувчи номини киритинг:", reply_markup=cancel_keyboard())
    await state.set_state(FabricOrderStates.waiting_for_postavshik)

@fabric_router.message(FabricOrderStates.waiting_for_postavshik)
async def order_process_postavshik(message: Message, state: FSMContext):
    await state.update_data(postavshik=message.text.strip())
    await message.answer("3. Буюртмачи (мижоз) номини киритинг:", reply_markup=cancel_keyboard())
    await state.set_state(FabricOrderStates.waiting_for_buyurtmachi)

@fabric_router.message(FabricOrderStates.waiting_for_buyurtmachi)
async def order_process_buyurtmachi(message: Message, state: FSMContext):
    await state.update_data(buyurtmachi=message.text.strip())
    await message.answer("4. Мато номини киритинг:", reply_markup=cancel_keyboard())
    await state.set_state(FabricOrderStates.waiting_for_name)

@fabric_router.message(FabricOrderStates.waiting_for_name)
async def order_process_name(message: Message, state: FSMContext):
    await state.update_data(name=message.text.strip())
    await message.answer(
        "5. Зичлиги (гр/м²) ва эни (м) ни киритинг (масалан: 180 1.5):",
        reply_markup=cancel_keyboard()
    )
    await state.set_state(FabricOrderStates.waiting_for_plotnost_shirina)

@fabric_router.message(FabricOrderStates.waiting_for_plotnost_shirina)
async def order_process_plotnost_shirina(message: Message, state: FSMContext):
    text = message.text.strip()
    parts = text.replace(',', ' ').split()
    if len(parts) != 2:
        await message.answer(
            "❌ Иккита сон киритинг (зичлик ва эни). Мисол: 180 1.5",
            reply_markup=cancel_keyboard()
        )
        return
    try:
        plotnost = float(parts[0])
        shirina = float(parts[1])
        await state.update_data(plotnost=plotnost, shirina=shirina)
        await message.answer("6. Режа вазни (кг) киритинг:", reply_markup=cancel_keyboard())
        await state.set_state(FabricOrderStates.waiting_for_reja_weight)
    except ValueError:
        await message.answer("❌ Сонларни тўғри киритинг. Мисол: 180 1.5", reply_markup=cancel_keyboard())

@fabric_router.message(FabricOrderStates.waiting_for_reja_weight)
async def order_process_reja_weight(message: Message, state: FSMContext):
    try:
        weight = float(message.text.replace(',', '.'))
        await state.update_data(reja_weight=weight)
        await message.answer("7. Факт вазни (кг) киритинг:", reply_markup=cancel_keyboard())
        await state.set_state(FabricOrderStates.waiting_for_fakt_weight)
    except ValueError:
        await message.answer("❌ Илтимос, сон киритинг (масалан: 125.5):", reply_markup=cancel_keyboard())

@fabric_router.message(FabricOrderStates.waiting_for_fakt_weight)
async def order_process_fakt_weight(message: Message, state: FSMContext):
    try:
        weight = float(message.text.replace(',', '.'))
        await state.update_data(fakt_weight=weight)
        await message.answer("8. Ранги (пантон коди ёки ранг номи) киритинг:", reply_markup=cancel_keyboard())
        await state.set_state(FabricOrderStates.waiting_for_color)
    except ValueError:
        await message.answer("❌ Илтимос, сон киритинг:", reply_markup=cancel_keyboard())

@fabric_router.message(FabricOrderStates.waiting_for_color)
async def order_process_color(message: Message, state: FSMContext):
    await state.update_data(color=message.text.strip())
    await message.answer(
        "9. Тузулган сана (DD-MM-YYYY форматида, масалан: 25-02-2026):",
        reply_markup=cancel_keyboard()
    )
    await state.set_state(FabricOrderStates.waiting_for_date)

@fabric_router.message(FabricOrderStates.waiting_for_date)
async def order_process_date(message: Message, state: FSMContext):
    date_str = message.text.strip()
    try:
        datetime.strptime(date_str, "%d-%m-%Y")
    except ValueError:
        await message.answer(
            "❌ Нотўғри формат. Илтимос, DD-MM-YYYY форматида киритинг (масалан: 25-02-2026):",
            reply_markup=cancel_keyboard()
        )
        return
    await state.update_data(date=date_str)
    await message.answer("10. Изоҳ (ихтиёрий, киритмасангиз 'йўқ' деб ёзинг):", reply_markup=cancel_keyboard())
    await state.set_state(FabricOrderStates.waiting_for_comment)

@fabric_router.message(FabricOrderStates.waiting_for_comment)
async def order_process_comment(message: Message, state: FSMContext):
    comment = message.text.strip()
    if comment.lower() in ['йўқ', 'нет', '']:
        comment = ""
    await state.update_data(comment=comment)

    data = await state.get_data()
    order_id = add_fabric_order(data)

    if order_id is None:
        await message.answer("❌ Маълумотларни сақлашда хатолик.", reply_markup=fabric_main_menu())
        await state.clear()
        return

    await send_to_group(
        f"📝 Янги заявка (ID: {order_id})\n"
        f"Заявка №: {data['app_number']}\n"
        f"Етказиб берувчи: {data['postavshik']}\n"
        f"Буюртмачи: {data['buyurtmachi']}\n"
        f"Мато: {data['name']}\n"
        f"Режа вазни: {data['reja_weight']} кг\n"
        f"Факт вазни: {data['fakt_weight']} кг\n"
        f"Ранги: {data['color']}\n"
        f"Тузулган сана: {data['date']}",
        FABRIC_CONTROL_TOPIC_ID
    )

    await message.answer(
        f"✅ Заявка сақланди! ID: {order_id}",
        reply_markup=fabric_main_menu()
    )
    await state.clear()

# ---------- Тест ----------
@fabric_router.callback_query(F.data.startswith("test_"))
async def cb_test_from_entry(callback: CallbackQuery, state: FSMContext):
    partiya = callback.data.split("_", 1)[1]
    await callback.answer()
    await state.update_data(partiya=partiya)
    await callback.message.answer(
        f"Партия {partiya} учун тест натижаларини киритинг.\n1. Узунлик бўйича кискариш (%):",
        reply_markup=cancel_keyboard()
    )
    await state.set_state(FabricTestStates.waiting_for_length)

@fabric_router.message(FabricTestStates.waiting_for_length)
async def process_test_length(message: Message, state: FSMContext):
    try:
        val = float(message.text.replace(',', '.'))
        await state.update_data(length=val)
        await message.answer("2. Эн бўйича кискариш (%):", reply_markup=cancel_keyboard())
        await state.set_state(FabricTestStates.waiting_for_width)
    except ValueError:
        await message.answer("❌ Сон киритинг:", reply_markup=cancel_keyboard())

@fabric_router.message(FabricTestStates.waiting_for_width)
async def process_test_width(message: Message, state: FSMContext):
    try:
        val = float(message.text.replace(',', '.'))
        await state.update_data(width=val)
        await message.answer("3. Қийшайиш (%):", reply_markup=cancel_keyboard())
        await state.set_state(FabricTestStates.waiting_for_skew)
    except ValueError:
        await message.answer("❌ Сон киритинг:", reply_markup=cancel_keyboard())

@fabric_router.message(FabricTestStates.waiting_for_skew)
async def process_test_skew(message: Message, state: FSMContext):
    try:
        val = float(message.text.replace(',', '.'))
        await state.update_data(skew=val)
        await message.answer("4. Тест санаси (кун.ой.йил):", reply_markup=cancel_keyboard())
        await state.set_state(FabricTestStates.waiting_for_test_date)
    except ValueError:
        await message.answer("❌ Сон киритинг:", reply_markup=cancel_keyboard())

@fabric_router.message(FabricTestStates.waiting_for_test_date)
async def process_test_date(message: Message, state: FSMContext):
    date_str = message.text.strip()
    try:
        datetime.strptime(date_str, "%d.%m.%Y")
    except ValueError:
        await message.answer("❌ Нотўғри формат. Қайта киритинг:", reply_markup=cancel_keyboard())
        return

    data = await state.get_data()
    partiya = data["partiya"]
    length = data["length"]
    width = data["width"]
    skew = data["skew"]

    success = add_test_entry(partiya, length, width, skew, date_str)
    if success:
        await message.answer(
            f"✅ Тест натижалари сақланди! Партия: {partiya}",
            reply_markup=fabric_main_menu()
        )
        await send_to_group(
            f"📊 Тест натижалари: партия {partiya}\n"
            f"Узунлик кискариши: {length}%\n"
            f"Эн кискариши: {width}%\n"
            f"Қийшайиш: {skew}%",
            FABRIC_CONTROL_TOPIC_ID
        )
    else:
        await message.answer("❌ Сақлашда хатолик.", reply_markup=fabric_main_menu())
    await state.clear()

# ---------- Команды для прямого доступа ----------
@fabric_router.message(Command("mato"))
async def cmd_mato(message: Message, state: FSMContext):
    if not is_fabric_user(message.from_user.id):
        await message.answer("❌ Рухсат йўқ.")
        return
    await message.answer(
        "📦 Янги мато келишини киритиш.\n1. Заявка № ни киритинг:",
        reply_markup=cancel_keyboard()
    )
    await state.set_state(FabricStates.waiting_for_app_number)

@fabric_router.message(Command("matolar"))
async def cmd_matolar(message: Message):
    if not is_fabric_user(message.from_user.id):
        await message.answer("❌ Рухсат йўқ.")
        return
    entries = get_all_fabric_entries(limit=10)
    if not entries:
        await message.answer("Ҳали мато келиши мавжуд эмас.")
        return
    text = "📋 Охирги 10 та мато келиши:\n\n"
    for e in entries:
        text += f"ID: {e['id']} | Заявка: {e['app_number']} | {e['postavshik']} | {e['name']} | {e['fakt_weight']} кг | {e['date']}\n"
    # используем старую таблицу для поступлений (можно создать отдельную, но пока так)
    img = create_fabric_orders_list_table(entries)  # временно, лучше сделать отдельную
    if img.getbuffer().nbytes > 100:
        await message.answer_photo(
            BufferedInputFile(img.getvalue(), filename="fabric_list.png"),
            caption=text[:200]
        )
    else:
        await message.answer(text, reply_markup=fabric_main_menu())

@fabric_router.message(Command("mato_info"))
async def cmd_mato_info(message: Message):
    if not is_fabric_user(message.from_user.id):
        await message.answer("❌ Рухсат йўқ.")
        return
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        await message.answer("📌 Партия рақами ёки ID киритинг. Мисол: /mato_info PRT-123")
        return
    query = args[1].strip()
    entries = get_all_fabric_entries(limit=100)
    entry = None
    for e in entries:
        if e["id"] == query or e["partiya"] == query:
            entry = e
            break
    if not entry:
        await message.answer("❌ Бундай маълумот топилмади.")
        return
    test = get_test_by_partiya(entry["partiya"])
    img = create_fabric_card(entry, test)
    if img.getbuffer().nbytes > 100:
        await message.answer_photo(
            BufferedInputFile(img.getvalue(), filename="fabric_card.png"),
            caption=f"📌 Партия: {entry['partiya']}"
        )
    else:
        text = f"📦 Мато келиши (ID: {entry['id']})\n"
        text += f"Заявка №: {entry['app_number']}\n"
        text += f"Етказиб берувчи: {entry['postavshik']}\n"
        text += f"Буюртмачи: {entry['buyurtmachi']}\n"
        text += f"Мато номи: {entry['name']}\n"
        text += f"Режа вазни: {entry['reja_weight']} кг\n"
        text += f"Факт вазни: {entry['fakt_weight']} кг\n"
        text += f"Партия: {entry['partiya']}\n"
        text += f"Рулонлар: {entry['rulons']}\n"
        text += f"Эни: {entry['shirina']} м\n"
        text += f"Зичлиги: {entry['plotnost']} гр/м²\n"
        text += f"Ранги: {entry['color']}\n"
        text += f"Тонг мослиги: {entry['ton']}%\n"
        text += f"Келган сана: {entry['date']}\n"
        if test:
            text += "\n📊 Тест натижалари:\n"
            text += f"Узунлик кискариши: {test['length']}%\n"
            text += f"Эн кискариши: {test['width']}%\n"
            text += f"Қийшайиш: {test['skew']}%\n"
            text += f"Тест санаси: {test['test_date']}"
        await message.answer(text)

@fabric_router.callback_query(F.data == "fabric_main")
async def cb_fabric_main(callback: CallbackQuery):
    await callback.answer()
    await callback.message.edit_text("📋 Мато бўлими:", reply_markup=fabric_main_menu())

# ---------- Регистрация ----------
def register_fabric_handlers(dp: Dispatcher):
    dp.include_router(fabric_router)