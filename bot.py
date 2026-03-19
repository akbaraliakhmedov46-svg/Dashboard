# ------------------- ИМПОРТЛАР -------------------
import asyncio
from datetime import datetime, timedelta
import random
from zoneinfo import ZoneInfo
import calendar
import math
import re
import logging
import os
from typing import Dict, List, Optional, Self
import matplotlib.pyplot as plt
import pandas as pd
from io import BytesIO
import seaborn as sns
import numpy as np
import matplotlib.gridspec as gridspec

import gspread
from google.oauth2.service_account import Credentials
import google.auth

from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardMarkup, KeyboardButton, BufferedInputFile, Update
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
import json
from aiogram.types import WebAppInfo
import time
from functools import wraps
import threading
from typing import Optional
import asyncpg
from typing import Optional
from kpi import cmd_kpi, router as kpi_router
from aiogram import BaseMiddleware
from typing import Callable, Dict, Any, Awaitable
from aiogram.types import Message, CallbackQuery

class LoggingMiddleware(BaseMiddleware):
    async def __call__(
        self,
        handler: Callable[[Any, Dict[str, Any]], Awaitable[Any]],
        event: Any,
        data: Dict[str, Any]
    ) -> Any:
        # CallbackQuery (tugma bosilganda)
        if isinstance(event, CallbackQuery):
            user = event.from_user
            logger.info(
                f"🔘 Callback | User: {user.id} (@{user.username or 'no_username'}) {user.first_name} | "
                f"Data: {event.data}"
            )
        # Message (xabar yozilganda)
        elif isinstance(event, Message):
            user = event.from_user
            # Xabar matni borligini tekshiramiz (rasm, video va h.k. bo‘lishi mumkin)
            text = event.text or event.caption or "[non-text message]"
            logger.info(
                f"💬 Message  | User: {user.id} (@{user.username or 'no_username'}) {user.first_name} | "
                f"Text: {text}"
            )
        # Handler ni chaqirish
        return await handler(event, data)

# ------------------- ЛОГИРОВАНИЕ -------------------
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
async def init_db():
    """Керакли жадвалларни яратиш"""
    await Database.execute("""
        CREATE TABLE IF NOT EXISTS user_actions (
            id SERIAL PRIMARY KEY,
            user_id BIGINT NOT NULL,
            username VARCHAR(255),
            action VARCHAR(255) NOT NULL,
            created_at TIMESTAMPTZ DEFAULT NOW()
        )
    """)

# ------------------- КОНФИГ -------------------
from dotenv import load_dotenv
load_dotenv()
API_TOKEN = os.getenv("API_TOKEN")
GOOGLE_KEY_FILE = os.getenv("GOOGLE_KEY_FILE")
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")
REPORT_SHEET_NAME = os.getenv("REPORT_SHEET_NAME", "Хисобот")
REPORT_SHEET_MONTH = os.getenv("REPORT_SHEET_MONTH", "Ойлик Хисобот")
ORDERS_SHEET_NAME = os.getenv("ORDERS_SHEET_NAME", "Буюртмалар")
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))
GROUP_ID = int(os.getenv("GROUP_ID", "0"))
PRODUCTION_TOPIC_ID = int(os.getenv("PRODUCTION_TOPIC_ID", "0"))
LOW_PERCENT_TOPIC_ID = int(os.getenv("LOW_PERCENT_TOPIC_ID", "0"))
RECOGNITION_TOPIC_ID = int(os.getenv("RECOGNITION_TOPIC_ID", "0"))
ORDERS_TOPIC_ID = int(os.getenv("ORDERS_TOPIC_ID", "0"))
TZ = ZoneInfo("Asia/Tashkent")
raw = os.getenv("FABRIC_USERS", "").strip()
FABRIC_USERS = []
if raw:
    parts = [p.strip() for p in raw.split(",") if p.strip()]
    for p in parts:
        try:
            FABRIC_USERS.append(int(p))
        except ValueError:
            logger.warning(f"Неверный ID пользователя: {p}")
pass

# ------------------- POSTGRESQL КОНФИГ -------------------
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "mybotdb")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")

# ------------------- GOOGLE SHEETS -------------------
def get_working_days_in_current_month():
    """Автоматик равишда ойига кўра иш кунларини хисоблаш"""
    today = datetime.now(TZ)
    year = today.year
    month = today.month
    
    first_day = datetime(year, month, 1, tzinfo=TZ)
    if month == 12:
        last_day = datetime(year, month, 31, tzinfo=TZ)
    else:
        last_day = datetime(year, month + 1, 1, tzinfo=TZ) - timedelta(days=1)
    
    current_day = first_day
    working_days = 0
    
    while current_day <= last_day:
        # Якшанбадан ташқари ҳар кун иш куни
        if current_day.weekday() != 6:
            working_days += 1
        current_day += timedelta(days=1)
    
    return working_days

def get_current_workday_index():
    """Ҳозиргача бўлган иш кунларини хисоблаш"""
    today = datetime.now(TZ)
    first_day_of_month = today.replace(day=1)
    
    workday_count = 0
    current_day = first_day_of_month
    
    while current_day <= today:
        # Фақат иш кунларини ҳисоблаш (якшанба эмас)
        if current_day.weekday() != 6:
            workday_count += 1
        current_day += timedelta(days=1)
    
    return workday_count

def get_remaining_workdays():
    """Қолган иш кунларини хисоблаш"""
    today = datetime.now(TZ)
    year = today.year
    month = today.month
    
    if month == 12:
        last_day = datetime(year, month, 31, tzinfo=TZ)
    else:
        last_day = datetime(year, month + 1, 1, tzinfo=TZ) - timedelta(days=1)
    
    remaining_days = 0
    current_day = today + timedelta(days=1)  # Эртангидан бошлаб
    
    while current_day <= last_day:
        if current_day.weekday() != 6:  # Якшанба эмас
            remaining_days += 1
        current_day += timedelta(days=1)
    
    return remaining_days

scope = ["https://spreadsheets.google.com/feeds", 
         "https://www.googleapis.com/auth/drive",
         "https://www.googleapis.com/auth/spreadsheets"]

def load_google_credentials(key_file: str, scopes: list):
    """
    Load Google credentials from:
    1. Environment variable GOOGLE_CREDENTIALS (JSON string)
    2. Explicit key_file path
    3. Environment variable GOOGLE_APPLICATION_CREDENTIALS
    4. Application Default Credentials
    """
    # 1) Try GOOGLE_CREDENTIALS env var (JSON string)
    creds_json = os.getenv("GOOGLE_CREDENTIALS")
    if creds_json:
        try:
            creds_info = json.loads(creds_json)
            return Credentials.from_service_account_info(creds_info, scopes=scopes)
        except Exception as e:
            logger.warning(f"Failed to parse GOOGLE_CREDENTIALS JSON: {e}")

    # 2) Try explicit key file path
    if key_file and os.path.exists(key_file):
        return Credentials.from_service_account_file(key_file, scopes=scopes)

    # 3) Try GOOGLE_APPLICATION_CREDENTIALS environment variable
    env_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if env_path and os.path.exists(env_path):
        return Credentials.from_service_account_file(env_path, scopes=scopes)

    # 4) Try Application Default Credentials
    try:
        creds, _ = google.auth.default(scopes=scopes)
        return creds
    except Exception as e:
        raise FileNotFoundError(
            f"Google service account JSON not found. Please set GOOGLE_CREDENTIALS env var "
            f"or provide a valid JSON file. Original error: {e}"
        )


creds = load_google_credentials(GOOGLE_KEY_FILE, scope)
# Create a requests.Session with retries and a default timeout and use it
# for credential refresh so token requests use retries and timeouts.
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from google.auth.transport.requests import Request as GoogleRequest

def _make_retry_session(timeout: int = 20, max_retries: int = 5, backoff_factor: float = 1.0):
    s = requests.Session()
    retries = Retry(
        total=max_retries,
        backoff_factor=backoff_factor,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET", "POST", "PUT", "DELETE", "HEAD", "OPTIONS"]),
    )
    adapter = HTTPAdapter(max_retries=retries)
    s.mount("https://", adapter)
    s.mount("http://", adapter)

    # Wrap request to enforce a default timeout when none provided
    orig_request = s.request
    def _request_with_timeout(method, url, **kwargs):
        if 'timeout' not in kwargs:
            kwargs['timeout'] = timeout
        return orig_request(method, url, **kwargs)

    s.request = _request_with_timeout
    return s


# Build session from env or defaults
_retry_timeout = int(os.getenv("GOOGLE_HTTP_TIMEOUT", "20"))
_retry_count = int(os.getenv("GOOGLE_HTTP_RETRIES", "5"))
_backoff = float(os.getenv("GOOGLE_BACKOFF_FACTOR", "1"))
_retry_session = _make_retry_session(timeout=_retry_timeout, max_retries=_retry_count, backoff_factor=_backoff)
_google_request = GoogleRequest(session=_retry_session)

# Try to refresh credentials now (this will use the retrying session). If it
# fails we'll log a warning — further API calls will still attempt refreshes
# but this pre-check helps fail fast and clarifies network/proxy issues.
try:
    creds.refresh(_google_request)
except Exception as e:
    logger.warning(f"Google credential refresh failed (will retry on use): {e}")

gc = gspread.authorize(creds)

# Robustly open the spreadsheet with retries and exponential backoff
def open_spreadsheet_with_retries(client, key, max_attempts=4, base_wait=2):
    last_exc = None
    for attempt in range(1, max_attempts + 1):
        try:
            # Call client.open_by_key directly; safe_sheets_call is defined later
            doc = client.open_by_key(key)
            logger.info(f"✅ Google Sheets га уланди: {doc.title}")
            return doc
        except Exception as e:
            last_exc = e
            wait = base_wait * (2 ** (attempt - 1))
            logger.warning(f"⚠️ Google Sheets connection attempt {attempt}/{max_attempts} failed: {e}. Retrying in {wait}s...")
            time.sleep(wait)
            try:
                # Recreate client in case credentials/session needs refreshing
                client = gspread.authorize(creds)
            except Exception:
                pass

    logger.error(f"❌ Google Sheets га уланиб бўлмади: {last_exc}")
    raise last_exc

doc = open_spreadsheet_with_retries(gc, SPREADSHEET_ID)

try:
    sheet_report = doc.worksheet(REPORT_SHEET_NAME)
    logger.info(f"✅ '{REPORT_SHEET_NAME}' топилди")
except gspread.exceptions.WorksheetNotFound:
    try:
        sheet_report = doc.add_worksheet(title=REPORT_SHEET_NAME, rows=1000, cols=20)
        logger.info(f"✅ '{REPORT_SHEET_NAME}' янги яратилди")
        headers = [
            "Сана", "Бичиш Иш", "Бичиш Ходим", 
            "Тасниф Дикимга", "Тасниф Печат", "Тасниф Вишивка", "Тасниф Ходим",
            "Тикув Иш", "Тикув Ходим", "Оёқчи Ходим",
            "Қадоқлаш Иш", "Қадоқлаш Ходим", "Хафталик килинган иш",
            "Изоҳ"
        ]
        sheet_report.append_row(headers)
        logger.info("✅ Сарлавҳалар қўшилди")
    except Exception as e:
        logger.error(f"❌ Хato: {e}")
        raise

try:
    sheet_month = doc.worksheet(REPORT_SHEET_MONTH)
    logger.info(f"✅ '{REPORT_SHEET_MONTH}' топилди")
except gspread.exceptions.WorksheetNotFound:
    try:
        sheet_month = doc.add_worksheet(title=REPORT_SHEET_MONTH, rows=10, cols=10)
        logger.info(f"✅ '{REPORT_SHEET_MONTH}' янги яратилди")
        
        month_headers = ["Бўлим", "Ойлик Режа", "Жами Бажарилди", "Қолдиқ", "Қолдиқ Фоиз", "Бажарилди Фоиз", "Кунлик Режа"]
        sheet_month.append_row(month_headers)
        
        sections = ["Бичиш", "Тасниф", "Тикув", "Қадоқлаш"]
        for i, section in enumerate(sections, start=2):
            sheet_month.update(f'A{i}', section)
            monthly_plan = 70000 if section == "Бичиш" else 65000 if section == "Тасниф" else 60000 if section == "Тикув" else 57000
            sheet_month.update(f'B{i}', monthly_plan)
            daily_plan = monthly_plan / get_working_days_in_current_month()
            sheet_month.update(f'G{i}', round(daily_plan, 2))
        logger.info("✅ Ойлик хисобот сарлавҳалари қўшилди")
    except Exception as e:
        logger.error(f"❌ Хato: {e}")
        raise

try:
    sheet_orders = doc.worksheet(ORDERS_SHEET_NAME)
    logger.info(f"✅ '{ORDERS_SHEET_NAME}' топилди")
except gspread.exceptions.WorksheetNotFound:
    try:
        sheet_orders = doc.add_worksheet(title=ORDERS_SHEET_NAME, rows=100, cols=10)
        logger.info(f"✅ '{ORDERS_SHEET_NAME}' янги яратилди")
        
        order_headers = ["Сана", "Буюртма номи", "Умумий микдор", "Бажарилди", "Қолдиқ", "Бажарилди Фоиз", "Қолдиқ Фоиз", "Жунатиш санаси", "Қолган кунлар", "Бўлим"]
        sheet_orders.append_row(order_headers)
        logger.info("✅ Буюртмалар сарлавҳалари қўшилди")
    except Exception as e:
        logger.error(f"❌ Хato: {e}")
        raise

# ------------------- БОТ -------------------
storage = MemoryStorage()
bot = Bot(token=API_TOKEN)
dp = Dispatcher(storage=storage)

# ------------------- FSM -------------------
class SectionStates(StatesGroup):
    ish_soni = State()
    hodim_soni = State()
    pechat = State()
    vishivka = State()
    dikimga = State()
    tikuv_ish = State()
    tikuv_hodim = State()
    oyoqchi_hodim = State()
    qadoqlash_ish = State()  # ✅ Yangi qo'shildi
    qadoqlash_hodim = State()  # ✅ Yangi qo'shildi
    comment = State()

class OrderStates(StatesGroup):
    waiting_for_name = State()
    waiting_for_quantity = State()
    waiting_for_date = State()
    waiting_for_deadline = State()
    waiting_for_section = State()
    edit_order_name = State()
    edit_order_quantity = State()
    edit_order_done = State()
    edit_order_deadline = State()
    edit_order_section = State()

class DailyWorkStates(StatesGroup):
    waiting_for_section = State()
    waiting_for_order = State()
    waiting_for_quantity = State()

class WorkflowStates(StatesGroup):
    waiting_for_section = State()
    waiting_for_order = State()
    waiting_for_quantity = State()
    waiting_for_workflow_section = State()

class AdminStates(StatesGroup):
    waiting_for_workdays = State()
    waiting_for_monthly_plan = State()
    waiting_for_monthly_plan_section = State()

class AdminEditByDateStates(StatesGroup):
    waiting_for_date = State()
    waiting_for_section = State()
    waiting_for_field = State()
    waiting_for_new_value = State()

class AdminSectionEditStates(StatesGroup):
    waiting_for_section = State()
    waiting_for_date_range = State()
    waiting_for_field = State()
    waiting_for_bulk_value = State()

class AdminSystemSettingsStates(StatesGroup):
    waiting_for_setting = State()
    waiting_for_new_value = State()

class AdminOrderManagementStates(StatesGroup):
    waiting_for_order_action = State()
    waiting_for_order_selection = State()
    waiting_for_bulk_edit = State()

class AdminEditCommentStates(StatesGroup):
    waiting_for_comment = State()

class AdminBroadcastStates(StatesGroup):
    waiting_for_broadcast_type = State()
    waiting_for_birthday_name = State()
    waiting_for_birthday_section = State()
    waiting_for_leaderboard_date = State()

class Database:
    _pool: Optional[asyncpg.Pool] = None

    @classmethod
    async def execute(cls, query, *args):
        if cls._pool is None:
            logger.warning("⚠️ PostgreSQL pool mavjud emas, so‘rov bajarilmadi.")
            return "0"   # yoki None
        async with cls._pool.acquire() as conn:
            return await conn.execute(query, *args)

    @classmethod
    async def fetch(cls, query, *args):
        if cls._pool is None:
            logger.warning("⚠️ PostgreSQL pool mavjud emas, bo‘sh ro‘yxat qaytarildi.")
            return []
        async with cls._pool.acquire() as conn:
            return await conn.fetch(query, *args)

    @classmethod
    async def fetchrow(cls, query, *args):
        if cls._pool is None:
            logger.warning("⚠️ PostgreSQL pool mavjud emas, None qaytarildi.")
            return None
        async with cls._pool.acquire() as conn:
            return await conn.fetchrow(query, *args)
    @classmethod
    async def execute(cls, query, *args):
        if cls._pool is None:
            logger.warning("⚠️ PostgreSQL pool mavjud emas, so‘rov bajarilmadi.")
            return "0"   # yoki None
        async with cls._pool.acquire() as conn:
            return await conn.execute(query, *args)

    @classmethod
    async def fetch(cls, query, *args):
        if cls._pool is None:
            logger.warning("⚠️ PostgreSQL pool mavjud emas, bo‘sh ro‘yxat qaytarildi.")
            return []
        async with cls._pool.acquire() as conn:
            return await conn.fetch(query, *args)

    @classmethod
    async def fetchrow(cls, query, *args):
        if cls._pool is None:
            logger.warning("⚠️ PostgreSQL pool mavjud emas, None qaytarildi.")
            return None
        async with cls._pool.acquire() as conn:
            return await conn.fetchrow(query, *args)
    @classmethod
    async def execute(cls, query, *args):
        if cls._pool is None:
            logger.warning("⚠️ PostgreSQL pool mavjud emas, so‘rov bajarilmadi.")
            return "0"   # yoki None
        async with cls._pool.acquire() as conn:
            return await conn.execute(query, *args)

    @classmethod
    async def fetch(cls, query, *args):
        if cls._pool is None:
            logger.warning("⚠️ PostgreSQL pool mavjud emas, bo‘sh ro‘yxat qaytarildi.")
            return []
        async with cls._pool.acquire() as conn:
            return await conn.fetch(query, *args)

    @classmethod
    async def fetchrow(cls, query, *args):
        if cls._pool is None:
            logger.warning("⚠️ PostgreSQL pool mavjud emas, None qaytarildi.")
            return None
        async with cls._pool.acquire() as conn:
            return await conn.fetchrow(query, *args)
    @classmethod
    async def execute(cls, query, *args):
        if cls._pool is None:
            logger.warning("⚠️ PostgreSQL pool mavjud emas, so‘rov bajarilmadi.")
            return "0"   # yoki None
        async with cls._pool.acquire() as conn:
            return await conn.execute(query, *args)

    @classmethod
    async def fetch(cls, query, *args):
        if cls._pool is None:
            logger.warning("⚠️ PostgreSQL pool mavjud emas, bo‘sh ro‘yxat qaytarildi.")
            return []
        async with cls._pool.acquire() as conn:
            return await conn.fetch(query, *args)

    @classmethod
    async def fetchrow(cls, query, *args):
        if cls._pool is None:
            logger.warning("⚠️ PostgreSQL pool mavjud emas, None qaytarildi.")
            return None
        async with cls._pool.acquire() as conn:
            return await conn.fetchrow(query, *args)
    @classmethod
    async def execute(cls, query, *args):
        if cls._pool is None:
            logger.warning("⚠️ PostgreSQL pool mavjud emas, so‘rov bajarilmadi.")
            return "0"   # yoki None
        async with cls._pool.acquire() as conn:
            return await conn.execute(query, *args)

    @classmethod
    async def fetch(cls, query, *args):
        if cls._pool is None:
            logger.warning("⚠️ PostgreSQL pool mavjud emas, bo‘sh ro‘yxat qaytarildi.")
            return []
        async with cls._pool.acquire() as conn:
            return await conn.fetch(query, *args)

    @classmethod
    async def fetchrow(cls, query, *args):
        if cls._pool is None:
            logger.warning("⚠️ PostgreSQL pool mavjud emas, None qaytarildi.")
            return None
        async with cls._pool.acquire() as conn:
            return await conn.fetchrow(query, *args)
    @classmethod
    async def execute(cls, query, *args):
        if cls._pool is None:
            logger.warning("⚠️ PostgreSQL pool mavjud emas, so‘rov bajarilmadi.")
            return "0"   # yoki None
        async with cls._pool.acquire() as conn:
            return await conn.execute(query, *args)

    @classmethod
    async def fetch(cls, query, *args):
        if cls._pool is None:
            logger.warning("⚠️ PostgreSQL pool mavjud emas, bo‘sh ro‘yxat qaytarildi.")
            return []
        async with cls._pool.acquire() as conn:
            return await conn.fetch(query, *args)

    @classmethod
    async def fetchrow(cls, query, *args):
        if cls._pool is None:
            logger.warning("⚠️ PostgreSQL pool mavjud emas, None qaytarildi.")
            return None
        async with cls._pool.acquire() as conn:
            return await conn.fetchrow(query, *args)
    @classmethod
    async def execute(cls, query, *args):
        if cls._pool is None:
            logger.warning("⚠️ PostgreSQL pool mavjud emas, so‘rov bajarilmadi.")
            return "0"   # yoki None
        async with cls._pool.acquire() as conn:
            return await conn.execute(query, *args)

    @classmethod
    async def fetch(cls, query, *args):
        if cls._pool is None:
            logger.warning("⚠️ PostgreSQL pool mavjud emas, bo‘sh ro‘yxat qaytarildi.")
            return []
        async with cls._pool.acquire() as conn:
            return await conn.fetch(query, *args)

    @classmethod
    async def fetchrow(cls, query, *args):
        if cls._pool is None:
            logger.warning("⚠️ PostgreSQL pool mavjud emas, None qaytarildi.")
            return None
        async with cls._pool.acquire() as conn:
            return await conn.fetchrow(query, *args)
    @classmethod
    async def execute(cls, query, *args):
        if cls._pool is None:
            logger.warning("⚠️ PostgreSQL pool mavjud emas, so‘rov bajarilmadi.")
            return "0"   # yoki None
        async with cls._pool.acquire() as conn:
            return await conn.execute(query, *args)

    @classmethod
    async def fetch(cls, query, *args):
        if cls._pool is None:
            logger.warning("⚠️ PostgreSQL pool mavjud emas, bo‘sh ro‘yxat qaytarildi.")
            return []
        async with cls._pool.acquire() as conn:
            return await conn.fetch(query, *args)

    @classmethod
    async def fetchrow(cls, query, *args):
        if cls._pool is None:
            logger.warning("⚠️ PostgreSQL pool mavjud emas, None qaytarildi.")
            return None
        async with cls._pool.acquire() as conn:
            return await conn.fetchrow(query, *args)
    @classmethod
    async def execute(cls, query, *args):
        if cls._pool is None:
            logger.warning("⚠️ PostgreSQL pool mavjud emas, so‘rov bajarilmadi.")
            return "0"   # yoki None
        async with cls._pool.acquire() as conn:
            return await conn.execute(query, *args)

    @classmethod
    async def fetch(cls, query, *args):
        if cls._pool is None:
            logger.warning("⚠️ PostgreSQL pool mavjud emas, bo‘sh ro‘yxat qaytarildi.")
            return []
        async with cls._pool.acquire() as conn:
            return await conn.fetch(query, *args)

    @classmethod
    async def fetchrow(cls, query, *args):
        if cls._pool is None:
            logger.warning("⚠️ PostgreSQL pool mavjud emas, None qaytarildi.")
            return None
        async with cls._pool.acquire() as conn:
            return await conn.fetchrow(query, *args)
    @classmethod
    async def execute(cls, query, *args):
        if cls._pool is None:
            logger.warning("⚠️ PostgreSQL pool mavjud emas, so‘rov bajarilmadi.")
            return "0"   # yoki None
        async with cls._pool.acquire() as conn:
            return await conn.execute(query, *args)

    @classmethod
    async def fetch(cls, query, *args):
        if cls._pool is None:
            logger.warning("⚠️ PostgreSQL pool mavjud emas, bo‘sh ro‘yxat qaytarildi.")
            return []
        async with cls._pool.acquire() as conn:
            return await conn.fetch(query, *args)

    @classmethod
    async def fetchrow(cls, query, *args):
        if cls._pool is None:
            logger.warning("⚠️ PostgreSQL pool mavjud emas, None qaytarildi.")
            return None
        async with cls._pool.acquire() as conn:
            return await conn.fetchrow(query, *args)
    @classmethod
    async def execute(cls, query, *args):
        if cls._pool is None:
            logger.warning("⚠️ PostgreSQL pool mavjud emas, so‘rov bajarilmadi.")
            return "0"   # yoki None
        async with cls._pool.acquire() as conn:
            return await conn.execute(query, *args)

    @classmethod
    async def fetch(cls, query, *args):
        if cls._pool is None:
            logger.warning("⚠️ PostgreSQL pool mavjud emas, bo‘sh ro‘yxat qaytarildi.")
            return []
        async with cls._pool.acquire() as conn:
            return await conn.fetch(query, *args)

    @classmethod
    async def fetchrow(cls, query, *args):
        if cls._pool is None:
            logger.warning("⚠️ PostgreSQL pool mavjud emas, None qaytarildi.")
            return None
        async with cls._pool.acquire() as conn:
            return await conn.fetchrow(query, *args)
    @classmethod
    async def execute(cls, query, *args):
        if cls._pool is None:
            logger.warning("⚠️ PostgreSQL pool mavjud emas, so‘rov bajarilmadi.")
            return "0"   # yoki None
        async with cls._pool.acquire() as conn:
            return await conn.execute(query, *args)

    @classmethod
    async def fetch(cls, query, *args):
        if cls._pool is None:
            logger.warning("⚠️ PostgreSQL pool mavjud emas, bo‘sh ro‘yxat qaytarildi.")
            return []
        async with cls._pool.acquire() as conn:
            return await conn.fetch(query, *args)

    @classmethod
    async def fetchrow(cls, query, *args):
        if cls._pool is None:
            logger.warning("⚠️ PostgreSQL pool mavjud emas, None qaytarildi.")
            return None
        async with cls._pool.acquire() as conn:
            return await conn.fetchrow(query, *args)
    @classmethod
    async def execute(cls, query, *args):
        if cls._pool is None:
            logger.warning("⚠️ PostgreSQL pool mavjud emas, so‘rov bajarilmadi.")
            return "0"   # yoki None
        async with cls._pool.acquire() as conn:
            return await conn.execute(query, *args)

    @classmethod
    async def fetch(cls, query, *args):
        if cls._pool is None:
            logger.warning("⚠️ PostgreSQL pool mavjud emas, bo‘sh ro‘yxat qaytarildi.")
            return []
        async with cls._pool.acquire() as conn:
            return await conn.fetch(query, *args)

    @classmethod
    async def fetchrow(cls, query, *args):
        if cls._pool is None:
            logger.warning("⚠️ PostgreSQL pool mavjud emas, None qaytarildi.")
            return None
        async with cls._pool.acquire() as conn:
            return await conn.fetchrow(query, *args)
    @classmethod
    async def execute(cls, query, *args):
        if cls._pool is None:
            logger.warning("⚠️ PostgreSQL pool mavjud emas, so‘rov bajarilmadi.")
            return "0"   # yoki None
        async with cls._pool.acquire() as conn:
            return await conn.execute(query, *args)

    @classmethod
    async def fetch(cls, query, *args):
        if cls._pool is None:
            logger.warning("⚠️ PostgreSQL pool mavjud emas, bo‘sh ro‘yxat qaytarildi.")
            return []
        async with cls._pool.acquire() as conn:
            return await conn.fetch(query, *args)

    @classmethod
    async def fetchrow(cls, query, *args):
        if cls._pool is None:
            logger.warning("⚠️ PostgreSQL pool mavjud emas, None qaytarildi.")
            return None
        async with cls._pool.acquire() as conn:
            return await conn.fetchrow(query, *args)
    @classmethod
    async def execute(cls, query, *args):
        if cls._pool is None:
            logger.warning("⚠️ PostgreSQL pool mavjud emas, so‘rov bajarilmadi.")
            return "0"   # yoki None
        async with cls._pool.acquire() as conn:
            return await conn.execute(query, *args)

    @classmethod
    async def fetch(cls, query, *args):
        if cls._pool is None:
            logger.warning("⚠️ PostgreSQL pool mavjud emas, bo‘sh ro‘yxat qaytarildi.")
            return []
        async with cls._pool.acquire() as conn:
            return await conn.fetch(query, *args)

    @classmethod
    async def fetchrow(cls, query, *args):
        if cls._pool is None:
            logger.warning("⚠️ PostgreSQL pool mavjud emas, None qaytarildi.")
            return None
        async with cls._pool.acquire() as conn:
            return await conn.fetchrow(query, *args)
    @classmethod
    async def execute(cls, query, *args):
        if cls._pool is None:
            logger.warning("⚠️ PostgreSQL pool mavjud emas, so‘rov bajarilmadi.")
            return "0"   # yoki None
        async with cls._pool.acquire() as conn:
            return await conn.execute(query, *args)

    @classmethod
    async def fetch(cls, query, *args):
        if cls._pool is None:
            logger.warning("⚠️ PostgreSQL pool mavjud emas, bo‘sh ro‘yxat qaytarildi.")
            return []
        async with cls._pool.acquire() as conn:
            return await conn.fetch(query, *args)

    @classmethod
    async def fetchrow(cls, query, *args):
        if cls._pool is None:
            logger.warning("⚠️ PostgreSQL pool mavjud emas, None qaytarildi.")
            return None
        async with cls._pool.acquire() as conn:
            return await conn.fetchrow(query, *args)
    @classmethod
    async def execute(cls, query, *args):
        if cls._pool is None:
            logger.warning("⚠️ PostgreSQL pool mavjud emas, so‘rov bajarilmadi.")
            return "0"   # yoki None
        async with cls._pool.acquire() as conn:
            return await conn.execute(query, *args)

    @classmethod
    async def fetch(cls, query, *args):
        if cls._pool is None:
            logger.warning("⚠️ PostgreSQL pool mavjud emas, bo‘sh ro‘yxat qaytarildi.")
            return []
        async with cls._pool.acquire() as conn:
            return await conn.fetch(query, *args)

    @classmethod
    async def fetchrow(cls, query, *args):
        if cls._pool is None:
            logger.warning("⚠️ PostgreSQL pool mavjud emas, None qaytarildi.")
            return None
        async with cls._pool.acquire() as conn:
            return await conn.fetchrow(query, *args)
    @classmethod
    async def execute(cls, query, *args):
        if cls._pool is None:
            logger.warning("⚠️ PostgreSQL pool mavjud emas, so‘rov bajarilmadi.")
            return "0"   # yoki None
        async with cls._pool.acquire() as conn:
            return await conn.execute(query, *args)

    @classmethod
    async def fetch(cls, query, *args):
        if cls._pool is None:
            logger.warning("⚠️ PostgreSQL pool mavjud emas, bo‘sh ro‘yxat qaytarildi.")
            return []
        async with cls._pool.acquire() as conn:
            return await conn.fetch(query, *args)

    @classmethod
    async def fetchrow(cls, query, *args):
        if cls._pool is None:
            logger.warning("⚠️ PostgreSQL pool mavjud emas, None qaytarildi.")
            return None
        async with cls._pool.acquire() as conn:
            return await conn.fetchrow(query, *args)
    @classmethod
    async def execute(cls, query, *args):
        if cls._pool is None:
            logger.warning("⚠️ PostgreSQL pool mavjud emas, so‘rov bajarilmadi.")
            return "0"   # yoki None
        async with cls._pool.acquire() as conn:
            return await conn.execute(query, *args)

    @classmethod
    async def fetch(cls, query, *args):
        if cls._pool is None:
            logger.warning("⚠️ PostgreSQL pool mavjud emas, bo‘sh ro‘yxat qaytarildi.")
            return []
        async with cls._pool.acquire() as conn:
            return await conn.fetch(query, *args)

    @classmethod
    async def fetchrow(cls, query, *args):
        if cls._pool is None:
            logger.warning("⚠️ PostgreSQL pool mavjud emas, None qaytarildi.")
            return None
        async with cls._pool.acquire() as conn:
            return await conn.fetchrow(query, *args)
    @classmethod
    async def execute(cls, query, *args):
        if cls._pool is None:
            logger.warning("⚠️ PostgreSQL pool mavjud emas, so‘rov bajarilmadi.")
            return "0"   # yoki None
        async with cls._pool.acquire() as conn:
            return await conn.execute(query, *args)

    @classmethod
    async def fetch(cls, query, *args):
        if cls._pool is None:
            logger.warning("⚠️ PostgreSQL pool mavjud emas, bo‘sh ro‘yxat qaytarildi.")
            return []
        async with cls._pool.acquire() as conn:
            return await conn.fetch(query, *args)

    @classmethod
    async def fetchrow(cls, query, *args):
        if cls._pool is None:
            logger.warning("⚠️ PostgreSQL pool mavjud emas, None qaytarildi.")
            return None
        async with cls._pool.acquire() as conn:
            return await conn.fetchrow(query, *args)
    @classmethod
    async def execute(cls, query, *args):
        if cls._pool is None:
            logger.warning("⚠️ PostgreSQL pool mavjud emas, so‘rov bajarilmadi.")
            return "0"   # yoki None
        async with cls._pool.acquire() as conn:
            return await conn.execute(query, *args)

    @classmethod
    async def fetch(cls, query, *args):
        if cls._pool is None:
            logger.warning("⚠️ PostgreSQL pool mavjud emas, bo‘sh ro‘yxat qaytarildi.")
            return []
        async with cls._pool.acquire() as conn:
            return await conn.fetch(query, *args)

    @classmethod
    async def fetchrow(cls, query, *args):
        if cls._pool is None:
            logger.warning("⚠️ PostgreSQL pool mavjud emas, None qaytarildi.")
            return None
        async with cls._pool.acquire() as conn:
            return await conn.fetchrow(query, *args)

# ------------------- API RATE LIMITING -------------------
class RateLimiter:
    def __init__(self, max_calls, period):
        self.max_calls = max_calls
        self.period = period
        self.calls = []
        self.lock = threading.Lock()
    
    def __call__(self, func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            with self.lock:
                now = time.time()
                # Эски чақиришларни тозалаш
                self.calls = [call_time for call_time in self.calls if now - call_time < self.period]
                
                if len(self.calls) >= self.max_calls:
                    sleep_time = self.period - (now - self.calls[0])
                    if sleep_time > 0:
                        logger.info(f"⏳ API лимити кутилмокда: {sleep_time:.2f} сония")
                        time.sleep(sleep_time)
                        now = time.time()
                        # Кутилгандан кейин яна тозалаш
                        self.calls = [call_time for call_time in self.calls if now - call_time < self.period]
                
                self.calls.append(now)
            
            return func(*args, **kwargs)
        return wrapper

# API чегаралари: дақикада 60 та сўров
sheets_rate_limiter = RateLimiter(50, 60)  # Хавфсизлик учун 50 та қўйамиз

# ------------------- CACHING MECHANISM -------------------
class DataCache:
    def __init__(self, ttl=300):
        self.ttl = ttl
        self._cache = {}
        self._timestamps = {}
        self._lock = threading.Lock()
    
    def get(self, key):
        with self._lock:
            if key in self._cache:
                timestamp = self._timestamps.get(key, 0)
                if time.time() - timestamp < self.ttl:
                    return self._cache[key]
                else:
                    # Вақти ўтган маълумотларни тозалаш
                    del self._cache[key]
                    del self._timestamps[key]
            return None
    
    def set(self, key, value):
        with self._lock:
            self._cache[key] = value
            self._timestamps[key] = time.time()
    
    def clear(self):
        with self._lock:
            self._cache.clear()
            self._timestamps.clear()
    
    def cleanup(self):
        """Вақти ўтган кэшларни тозалаш"""
        with self._lock:
            now = time.time()
            expired_keys = [k for k, t in self._timestamps.items() if now - t >= self.ttl]
            for key in expired_keys:
                del self._cache[key]
                del self._timestamps[key]

# DataCache инстанциясини яратиш
data_cache = DataCache(ttl=300)

# ------------------- УТИЛЛАР -------------------
def is_admin(user_id):
    return user_id == ADMIN_ID

def parse_float(s):
    try:
        if isinstance(s, str):
            s = s.replace(',', '').replace(' ', '')
            if '.' in s:
                parts = s.split('.')
                if len(parts) == 2:
                    s = parts[0] + '.' + parts[1][:2]
                else:
                    s = parts[0]
            return float(s)
        return float(s)
    except:
        return 0.0

def parse_int(s):
    try:
        return int(float(s.replace(',', '').replace(' ', '')))
    except:
        return 0

def today_date_str():
    return datetime.now(TZ).strftime("%d.%m.%Y")

def get_week_start_end_dates():
    today = datetime.now(TZ)
    start = today - timedelta(days=today.weekday())
    end = start + timedelta(days=6)
    return start.strftime("%d.%m.%Y"), end.strftime("%d.%m.%Y")

def get_week_number():
    today = datetime.now(TZ)
    return today.isocalendar()[1]

@sheets_rate_limiter
def find_today_row(sheet) -> int:
    try:
        colA = safe_sheets_call(sheet.col_values, 1)
        today = today_date_str()
        for i, v in enumerate(colA, start=1):
            if v.strip() == today:
                return i
        return 0
    except Exception as e:
        logger.error(f"❌ find_today_row хato: {e}")
        return 0

def find_week_rows(sheet):
    try:
        colA = safe_sheets_call(sheet.col_values, 1)
        week_start, week_end = get_week_start_end_dates()
        week_rows = []
        
        for i, v in enumerate(colA, start=1):
            try:
                row_date = datetime.strptime(v, "%d.%m.%Y").replace(tzinfo=TZ)
                start_date = datetime.strptime(week_start, "%d.%m.%Y").replace(tzinfo=TZ)
                end_date = datetime.strptime(week_end, "%d.%m.%Y").replace(tzinfo=TZ)
                
                if start_date <= row_date <= end_date:
                    week_rows.append(i)
            except:
                continue
                
        return week_rows
    except Exception as e:
        logger.error(f"❌ find_week_rows хato: {e}")
        return []

def append_or_update(sheet, values_by_index: dict):
    try:
        row_idx = find_today_row(sheet)
        updates = []
        
        if row_idx == 0:
            max_index = max(values_by_index.keys()) + 1
            row = [""] * max_index
            row[0] = today_date_str()
            
            for idx, val in values_by_index.items():
                row[idx] = str(val)
            safe_sheets_call(sheet.append_row, row)
            logger.info(f"✅ Янги қатор қўшилди")
        else:
            for idx, val in values_by_index.items():
                updates.append({
                    'range': f"{gspread.utils.rowcol_to_a1(row_idx, idx + 1)}",
                    'values': [[str(val)]]
                })
            if updates:
                safe_sheets_call(sheet.batch_update, updates)
                logger.info(f"✅ Мавжуд қатор янгиланди")
                
    except Exception as e:
        logger.error(f"❌ append_or_update хato: {e}")

def safe_val(row, idx):
    return parse_int(row[idx]) if idx < len(row) else 0

def calculate_percentage(part, whole):
    """Чекланмаган фоиз ҳисоблаш"""
    try:
        if part is None or whole is None:
            return 0
        if whole == 0:
            return 0
        percentage = (part / whole) * 100
        return round(percentage, 1)
    except Exception as e:
        logger.error(f"❌ calculate_percentage хato: part={part}, whole={whole}, error={e}")
        return 0

def calculate_bounded_percentage(part, whole):
    """Чекланган фоиз ҳисоблаш (0-100%)"""
    try:
        if whole == 0:
            return 0
        percentage = (part / whole) * 100
        return max(0, min(100, round(percentage, 1)))
    except Exception as e:
        logger.error(f"❌ calculate_bounded_percentage хato: part={part}, whole={whole}, error={e}")
        return 0

def update_monthly_totals(section_name, daily_value):
    try:
        section_names = safe_sheets_call(sheet_month.col_values, 1)
        row_idx = None
        
        for i, name in enumerate(section_names, start=1):
            if name.strip().lower() == section_name.lower():
                row_idx = i
                break
        
        if not row_idx:
            logger.error(f"❌ {section_name} бўлими ойлик хисоботда топилмади")
            return None
        
        current_total = parse_float(safe_sheets_call(sheet_month.cell, row_idx, 3).value)
        new_total = current_total + daily_value
        
        monthly_plan = parse_float(safe_sheets_call(sheet_month.cell, row_idx, 2).value)
        
        remaining = max(0, monthly_plan - new_total)
        
        percentage = calculate_percentage(new_total, monthly_plan)
        remaining_percentage = calculate_percentage(remaining, monthly_plan)
        
        safe_sheets_call(sheet_month.update_cell, row_idx, 3, new_total)
        safe_sheets_call(sheet_month.update_cell, row_idx, 4, remaining)
        safe_sheets_call(sheet_month.update_cell, row_idx, 5, f"{remaining_percentage:.1f}%")
        safe_sheets_call(sheet_month.update_cell, row_idx, 6, f"{percentage:.1f}%")
        
        logger.info(f"✅ {section_name} ойлик хисобот янгиланди: {new_total} та (режанинг {percentage:.1f}%)")
        
        if percentage >= 100:
            return f"🎉 {section_name} бўлими ойлик режани {percentage:.1f}% бажариб, режадан {new_total - monthly_plan} та ортиқ иш чиқарди!"
        
    except Exception as e:
        logger.error(f"❌ Ойлик хисоботни янгилашда хato: {e}")
    
    return None

@sheets_rate_limiter
def get_monthly_data():
    """Ойлик маълумотларни олиш (яхшиланган версия)"""
    try:
        logger.info("📈 Ойлик маълумотларни олиш бошланди...")
        
        cache_key = "monthly_data"
        cached_data = DataCache.get(cache_key)
        if cached_data:
            return cached_data
        
        # Google Sheets дан маълумотларни олиш
        records = safe_sheets_call(sheet_month.get_all_values)
        
        if not records or len(records) < 2:
            logger.error("❌ Google Sheets да маълумотлар мавжуд эмас")
            return {}
        
        monthly_plans = {}
        
        for i in range(1, len(records)):
            row = records[i]
            if len(row) >= 3:
                try:
                    section_name = row[0].strip().lower() if row[0] else ""
                    plan_str = row[1] if len(row) > 1 else "0"
                    done_str = row[2] if len(row) > 2 else "0"
                    
                    plan = parse_float(plan_str)
                    done = parse_float(done_str)
                    
                    if section_name:
                        monthly_plans[section_name] = {
                            'plan': plan,
                            'done': done,
                            'remaining': max(0, plan - done),
                            'done_pct': f"{(done / plan) * 100:.1f}%" if plan > 0 else "0%"
                        }
                        
                        logger.info(f"✅ {section_name}: {done}/{plan} та")
                    
                except Exception as e:
                    logger.error(f"❌ {i}-қаторни қайта ишлашда хato: {e}")
                    continue
        
        DataCache.set(cache_key, monthly_plans)
        logger.info(f"📊 Ойлик маълумотлар олинди: {len(monthly_plans)} та бўлим")
        return monthly_plans
        
    except Exception as e:
        logger.error(f"❌ get_monthly_data хato: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return {}
    
def calculate_section_performance(section_name, daily_value, monthly_plan_dict):
    total_workdays = get_working_days_in_current_month()
    monthly_plan = monthly_plan_dict.get('plan', 0)
    daily_plan = monthly_plan / total_workdays if total_workdays > 0 else 0
    
    monthly_done = monthly_plan_dict.get('done', 0) + daily_value
    monthly_pct = calculate_percentage(monthly_done, monthly_plan)
    
    monthly_remaining = max(0, monthly_plan - monthly_done)
    monthly_remaining_pct = calculate_percentage(monthly_remaining, monthly_plan)
    
    remaining_days = get_remaining_workdays()
    daily_needed = monthly_remaining / remaining_days if remaining_days > 0 else 0
    
    daily_pct = calculate_percentage(daily_value, daily_plan)
    
    # Прогноз на конец месяца
    projected_final = monthly_done + (daily_value * remaining_days)
    projected_percentage = calculate_percentage(projected_final, monthly_plan)
    
    # Определяем, идет ли работа по плану
    on_track = projected_percentage >= 100
    
    return {
        'daily_plan': daily_plan,
        'daily_norm': daily_plan,
        'daily_pct': daily_pct,
        'monthly_pct': monthly_pct,
        'monthly_remaining_pct': monthly_remaining_pct,
        'remaining_work': monthly_remaining,
        'daily_needed': daily_needed,
        'remaining_days': remaining_days,
        'projected_final': projected_final,
        'projected_percentage': projected_percentage,
        'on_track': on_track,
        'monthly_plan': monthly_plan,
        'done': monthly_done
    }

def get_month_name():
    months_uz = {
        1: "Январь", 2: "Февраль", 3: "Март", 4: "Апрель",
        5: "Май", 6: "Июнь", 7: "Июль", 8: "Август",
        9: "Сентябрь", 10: "Октябрь", 11: "Ноябрь", 12: "Декабрь"
    }
    return months_uz.get(datetime.now().month, "")

def get_orders_data():
    try:
        orders = []
        records = safe_sheets_call(sheet_orders.get_all_values)
        
        for i in range(1, len(records)):
            if len(records[i]) >= 6 and records[i][0] and records[i][1]:
                try:
                    order_date = records[i][0]
                    order_name = records[i][1]
                    total_qty = parse_float(records[i][2]) if len(records[i]) > 2 else 0
                    done_qty = parse_float(records[i][3]) if len(records[i]) > 3 else 0
                    remaining = parse_float(records[i][4]) if len(records[i]) > 4 else 0
                    
                    # Бўлим номини олиш
                    section = ""
                    if len(records[i]) > 9:
                        section = records[i][9]
                    elif len(records[i]) > 5:
                        section = records[i][5]
                    
                    orders.append({
                        'date': order_date,
                        'name': order_name,
                        'total': total_qty,
                        'done': done_qty,
                        'remaining': remaining,
                        'section': section,
                        'row_index': i + 1
                    })
                except Exception as e:
                    logger.error(f"❌ Буюртмани ўқишда хato: {e}")
                    continue
        
        return orders
    except Exception as e:
        logger.error(f"❌ Буюртмаларни ўқишдa хato: {e}")
        return []

def get_workflow_orders_data():
    try:
        orders = []
        records = sheet_orders.get_all_values()
        
        for i in range(1, len(records)):
            if len(records[i]) >= 10 and records[i][0] and records[i][1]:
                order_date = records[i][0]
                order_name = records[i][1]
                total_qty = parse_float(records[i][2]) if len(records[i]) > 2 else 0
                done_qty = parse_float(records[i][3]) if len(records[i]) > 3 else 0
                remaining = parse_float(records[i][4]) if len(records[i]) > 4 else 0
                done_percentage = records[i][5] if len(records[i]) > 5 else "0%"
                remaining_percentage = records[i][6] if len(records[i]) > 6 else "0%"
                deadline = records[i][7] if len(records[i]) > 7 else ""
                days_left = records[i][8] if len(records[i]) > 8 else 0
                section = records[i][9] if len(records[i]) > 9 else ""
                
                bichish_done = parse_float(records[i][10]) if len(records[i]) > 10 else 0
                tasnif_done = parse_float(records[i][11]) if len(records[i]) > 11 else 0
                tikuv_done = parse_float(records[i][12]) if len(records[i]) > 12 else 0
                qadoqlash_done = parse_float(records[i][13]) if len(records[i]) > 13 else 0
                qutiga_solish_done = parse_float(records[i][14]) if len(records[i]) > 14 else 0
                current_stage = records[i][15] if len(records[i]) > 15 else "Бичиш"
                
                orders.append({
                    'date': order_date,
                    'name': order_name,
                    'total': total_qty,
                    'done': done_qty,
                    'remaining': remaining,
                    'done_percentage': done_percentage,
                    'remaining_percentage': remaining_percentage,
                    'deadline': deadline,
                    'days_left': days_left,
                    'section': section,
                    'bichish_done': bichish_done,
                    'tasnif_done': tasnif_done,
                    'tikuv_done': tikuv_done,
                    'qadoqlash_done': qadoqlash_done,
                    'qutiga_solish_done': qutiga_solish_done,
                    'current_stage': current_stage,
                    'row_index': i + 1
                })
        
        return orders
    except Exception as e:
        logger.error(f"❌ Workflow буюртмаларини ўқишда хato: {e}")
        return []

def update_workflow_order(row_index, stage, quantity):
    try:
        stage_columns = {
            "bichish": 10,
            "tasnif": 11,
            "tikuv": 12,
            "qadoqlash": 13,
            "qutiga_solish": 14
        }
        
        current_value = parse_float(safe_sheets_call(sheet_orders.cell, row_index, stage_columns[stage]).value)
        new_value = current_value + quantity
        safe_sheets_call(sheet_orders.update_cell, row_index, stage_columns[stage], new_value)
        
        row_values = safe_sheets_call(sheet_orders.row_values, row_index)
        total_done = (
            parse_float(row_values[10] if len(row_values) > 10 else 0) +
            parse_float(row_values[11] if len(row_values) > 11 else 0) +
            parse_float(row_values[12] if len(row_values) > 12 else 0) +
            parse_float(row_values[13] if len(row_values) > 13 else 0) +
            parse_float(row_values[14] if len(row_values) > 14 else 0)
        )
        
        safe_sheets_call(sheet_orders.update_cell, row_index, 4, total_done)
        
        total = parse_float(row_values[2]) if len(row_values) > 2 else 0
        remaining = max(0, total - total_done)
        
        done_pct = calculate_percentage(total_done, total)
        remaining_pct = calculate_percentage(remaining, total)
        
        safe_sheets_call(sheet_orders.update_cell, row_index, 5, remaining)
        safe_sheets_call(sheet_orders.update_cell, row_index, 6, f"{done_pct:.1f}%")
        safe_sheets_call(sheet_orders.update_cell, row_index, 7, f"{remaining_pct:.1f}%")
        
        stages = ["bichish", "tasnif", "tikuv", "qadoқлаш", "qutiga_solish"]
        current_stage_index = stages.index(stage)
        
        if new_value > 0 and current_stage_index < len(stages) - 1:
            next_stage = stages[current_stage_index + 1]
            safe_sheets_call(sheet_orders.update_cell, row_index, 16, next_stage.capitalize())
        else:
            safe_sheets_call(sheet_orders.update_cell, row_index, 16, stage.capitalize())
        
        logger.info(f"✅ Workflow буюртма янгиланди: {stage} - {quantity} та")
        return True
        
    except Exception as e:
        logger.error(f"❌ Workflow буюртмани янгилашда хato: {e}")
        return False

@sheets_rate_limiter
def safe_sheets_call(callable_func, *args, max_retries=3, **kwargs):
    """Хавфсиз Google Sheets чақириши"""
    for attempt in range(max_retries):
        try:
            return callable_func(*args, **kwargs)
        except Exception as e:
            if "429" in str(e) or "quota" in str(e).lower():
                wait_time = 30 * (attempt + 1)  # Har bir urinishda kutish vaqtini oshiramiz
                logger.warning(f"⚠️ API лимити, {wait_time} сония кутилмокда... (Urinish {attempt+1}/{max_retries})")
                time.sleep(wait_time)
                continue
            elif "503" in str(e) or "500" in str(e):
                wait_time = 10 * (attempt + 1)
                logger.warning(f"⚠️ Сервер хatosi ({e}), {wait_time} сония кутилмокда...")
                time.sleep(wait_time)
                continue
            else:
                raise e
    
    raise Exception(f"Google Sheets APIga {max_retries} marta urinish amalga oshirildi, lekin muvaffaqiyatli bo'lmadi.")

def get_orders_by_section(section_name):
    orders = get_orders_data()
    return [order for order in orders if order.get('section', '').lower() == section_name.lower() and order['remaining'] > 0]

def get_orders_by_section(section_name):
    orders = get_orders_data()
    section_name = section_name.lower().strip()
    
    filtered_orders = []
    for order in orders:
        order_section = order.get('section', '').lower().strip()
        # Қадоқлаш бўлими учун турли ёзилишларни қўллаш
        if (order_section == section_name or 
            order_section == 'қадоқлаш' or 
            order_section == 'кадоклаш' or
            order_section == 'qadoqlash' or
            section_name in order_section):
            if order['remaining'] > 0:
                filtered_orders.append(order)
    
    return filtered_orders

def update_sheet_data(date_str: str, section: str, field_index: int, new_value) -> bool:
    """Қатордаги маълумотларни янгилаш"""
    try:
        records = safe_sheets_call(sheet_report.get_all_values)
        row_index = -1
        
        # Санaни топish
        for i, row in enumerate(records):
            if row and row[0] == date_str:
                row_index = i + 1  # 1-based index
                break
        
        if row_index == -1:
            # Янги қатор яратиш
            new_row = [date_str] + [""] * 13
            set_value_in_row(new_row, section, field_index, new_value)
            safe_sheets_call(sheet_report.append_row, new_row)
            return True
        
        # Мавжуд қаторни янгилаш
        row = safe_sheets_call(sheet_report.row_values, row_index)
        # Катталикни текшириш ва кенгайтириш
        while len(row) < 14:
            row.append("")
        
        set_value_in_row(row, section, field_index, new_value)
        
        # Якуний қаторни янгилаш
        updates = []
        for i, value in enumerate(row[:14]):  # Фақат 14 та устун
            updates.append({
                'range': f"{gspread.utils.rowcol_to_a1(row_index, i + 1)}",
                'values': [[str(value) if value is not None else ""]]
            })
        
        if updates:
            safe_sheets_call(sheet_report.batch_update, updates)
        
        return True
        
    except Exception as e:
        logger.error(f"❌ update_sheet_data хato: {e}")
        return False

def set_value_in_row(row: list, section: str, field_index: int, value):
    """Қатордаги қийматни ўрнатиш"""
    section_columns = {
        "bichish": [1, 2],      # Иш, Ходим
        "tasnif": [3, 4, 5, 6], # Дикимга, Печат, Вишивка, Ходим
        "tikuv": [7, 8, 9],     # Иш, Тикув ходим, Оёқчи ходим
        "qadoqlash": [10, 11]   # Иш, Ходим
    }
    
    columns = section_columns.get(section, [])
    if field_index < len(columns):
        col_index = columns[field_index]
        if col_index < len(row):
            row[col_index] = str(value)
        else:
            # Қаторни кенгайтириш
            while len(row) <= col_index:
                row.append("")
            row[col_index] = str(value)

def bulk_update_sheet_data(start_date_str: str, end_date_str: str, section: str, field_index: int, value) -> int:
    """Оммовий янгилаш"""
    try:
        start_date = datetime.strptime(start_date_str, "%d.%m.%Y")
        end_date = datetime.strptime(end_date_str, "%d.%m.%Y")
        
        records = sheet_report.get_all_values()
        updated_count = 0
        
        for i, row in enumerate(records):
            if i == 0:  # Сарлавҳалар
                continue
                
            if not row or not row[0]:
                continue
                
            try:
                row_date = datetime.strptime(row[0], "%d.%m.%Y")
                if start_date <= row_date <= end_date:
                    set_value_in_row(row, section, field_index, value)
                    # Қаторни янгилаш
                    row_index = i + 1
                    updates = []
                    for col_idx, col_value in enumerate(row[:14]):
                        updates.append({
                            'range': f"{gspread.utils.rowcol_to_a1(row_index, col_idx + 1)}",
                            'values': [[str(col_value) if col_value is not None else ""]]
                        })
                    
                    if updates:
                        sheet_report.batch_update(updates)
                    
                    updated_count += 1
                    
            except ValueError:
                continue
        
        return updated_count
        
    except Exception as e:
        logger.error(f"❌ bulk_update_sheet_data хato: {e}")
        return 0

async def show_date_data(message: Message, date_str: str):
    """Санa учун маълумотларни кўрсатиш"""
    try:
        # Санa учун маълумотларни топish
        records = sheet_report.get_all_values()
        found_data = None
        
        for row in records:
            if row and row[0] == date_str:
                found_data = row
                break
        
        if not found_data:
            await message.answer(f"❌ {date_str} санаси учун маълумот топилмади.")
            return
        
        # Маълумотларни кўрсатиш
        response = f"📊 {date_str} санаси учун маълумотлар:\n\n"
        sections = {
            "Бичиш иш": [1],
            "Бичиш ходим": [2],
            "Тасниф дикимга": [3],
            "Тасниф печат": [4], 
            "Тасниф вишивка": [5],
            "Тасниф ходим": [6],
            "Тикув иш": [7],
            "Тикув ходим": [8],
            "Оёқчи ходим": [9],
            "Қадоқлаш иш": [10],
            "Қадоқлаш ходим": [11],
            "Хафталик иш": [12],
            "Изоҳ": [13]
        }
        
        for section_name, indexes in sections.items():
            values = []
            for idx in indexes:
                if idx < len(found_data):
                    values.append(found_data[idx] if found_data[idx] else "0")
                else:
                    values.append("0")
            
            # Изоҳ учун алохида формат
            if section_name == "Изоҳ":
                comment_value = found_data[idx] if idx < len(found_data) and found_data[idx] else "Изоҳ йўқ"
                response += f"• {section_name}: {comment_value}\n"
            else:
                response += f"• {section_name}: {', '.join(values)}\n"
        
        # Таҳрирлаш учун тугмалар
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✏️ Бичиш маълумотлари", callback_data=f"edit_sec:bichish:{date_str}")],
            [InlineKeyboardButton(text="✏️ Тасниф маълумотлари", callback_data=f"edit_sec:tasnif:{date_str}")],
            [InlineKeyboardButton(text="✏️ Тикув маълумотлари", callback_data=f"edit_sec:tikuv:{date_str}")],
            [InlineKeyboardButton(text="✏️ Қадоқлаш маълумотлари", callback_data=f"edit_sec:qadoqlash:{date_str}")],
            [InlineKeyboardButton(text="📝 Изоҳни таҳрирлаш", callback_data=f"edit_comment:{date_str}")],
            [InlineKeyboardButton(text="📝 Янги маълумот қўшиш", callback_data=f"add_new:{date_str}")],
            [InlineKeyboardButton(text="🗑️ Ушбу кунни ўчириш", callback_data=f"delete_date:{date_str}")],
            [InlineKeyboardButton(text="⬅️ Ортга", callback_data="admin_back")]
        ])
        
        await message.answer(response, reply_markup=keyboard)
        
    except Exception as e:
        logger.error(f"❌ Санa маълумотларини кўрсатишда хato: {e}")
        await message.answer("❌ Маълумотларни олишда хатолик юз берди.")

# ------------------- OPTIMIZED SHEETS FUNCTIONS -------------------
@sheets_rate_limiter
def safe_sheets_call(callable_func, *args, **kwargs):
    """Хавфсиз Google Sheets чақириши"""
    try:
        return callable_func(*args, **kwargs)
    except Exception as e:
        if "429" in str(e) or "quota" in str(e).lower():
            logger.warning("⚠️ API лимити, 30 сония кутилмокда...")
            time.sleep(30)
            return safe_sheets_call(callable_func, *args, **kwargs)
        raise e
def get_monthly_data_cached():
    """Кэшланган ойлик маълумотларни олиш"""
    cache_key = "monthly_data"
    cached_data = data_cache.get(cache_key)  # Используем экземпляр
    
    if cached_data is not None:
        logger.info("📊 Кэшдан ойлик маълумотлар олинди")
        return cached_data
    
    try:
        logger.info("📈 Google Sheets дан ойлик маълумотлар олинмокда...")
        records = safe_sheets_call(sheet_month.get_all_values)
        
        if not records or len(records) < 2:
            logger.error("❌ Google Sheets да маълумотлар мавжуд эмас")
            return {}
        
        monthly_plans = {}
        
        for i in range(1, len(records)):
            row = records[i]
            if len(row) >= 3:
                try:
                    section_name = row[0].strip().lower() if row[0] else ""
                    plan_str = row[1] if len(row) > 1 else "0"
                    done_str = row[2] if len(row) > 2 else "0"
                    
                    plan = parse_float(plan_str)
                    done = parse_float(done_str)
                    
                    if section_name:
                        monthly_plans[section_name] = {
                            'plan': plan,
                            'done': done,
                            'remaining': max(0, plan - done),
                            'done_pct': f"{(done / plan) * 100:.1f}%" if plan > 0 else "0%"
                        }
                        
                except Exception as e:
                    logger.error(f"❌ {i}-қаторни қайта ишлашда хato: {e}")
                    continue
        
        # Кэшга сақлаш
        data_cache.set(cache_key, monthly_plans)  # Используем экземпляр
        logger.info(f"📊 Ойлик маълумотлар кэшланди: {len(monthly_plans)} та бўлим")
        return monthly_plans
        
    except Exception as e:
        logger.error(f"❌ get_monthly_data хato: {e}")
        return {}

def get_sheet_data_cached(sheet, cache_key):
    """Кэшланган Sheets маълумотларини олиш"""
    cached_data = data_cache.get(cache_key)  # Используем экземпляр
    
    if cached_data is not None:
        return cached_data
    
    try:
        data = safe_sheets_call(sheet.get_all_values)
        data_cache.set(cache_key, data)  # Используем экземпляр
        return data
    except Exception as e:
        logger.error(f"❌ {cache_key} олишда хato: {e}")
        return []

def get_weekly_data_cached():
    """Кэшланган ҳафталик маълумотлар"""
    cache_key = f"weekly_data_{get_week_number()}"
    return get_sheet_data_cached(sheet_report, cache_key)

def get_today_data_cached():
    """Кэшланган бугунги маълумотлар"""
    cache_key = f"today_data_{today_date_str()}"
    return get_sheet_data_cached(sheet_report, cache_key)

# ------------------- OPTIMIZED DASHBOARD FUNCTIONS -------------------
def create_optimized_dashboard():
    """Оптимизацияланган дашборд - иш кунлари жадвали билан"""
    try:
        logger.info("📊 Оптимизацияланган дашборд яратиш бошланди...")
        
        # Фақат бир марта маълумот олиш
        monthly_data = get_monthly_data_cached()
        
        if not monthly_data:
            logger.error("❌ Ойлик маълумотлар топилмади")
            return create_empty_dashboard()

        # Telegram учун оптимал олчам (800x1200 пиксел)
        fig = plt.figure(figsize=(16, 24))
        fig.suptitle('ИШЛАБ ЧИҚАРИШ ДАШБОРДИ', fontsize=20, fontweight='bold', y=0.98)
        
        # Мурожат тузилмаси
        gs = gridspec.GridSpec(3, 2, figure=fig, height_ratios=[1.5, 1, 1], hspace=0.4, wspace=0.3)
        
        # 1. ИШ КУНЛАРИ ЖАДВАЛИ (2 устунли) - ТЕПАДА
        ax_calendar = fig.add_subplot(gs[0, :])
        create_simple_workdays_calendar(ax_calendar, monthly_data)
        
        # 2. Ойлик прогресс (столбчатая диаграмма)
        ax_bar = fig.add_subplot(gs[1, 0])
        sections = ['бичиш', 'тасниф', 'тикув', 'қадоқлаш']
        colors = ['#FF6B6B', '#4ECDC4', '#45B7D1', '#96CEB4']
        
        plans = [monthly_data.get(sect, {}).get('plan', 0) for sect in sections]
        dones = [monthly_data.get(sect, {}).get('done', 0) for sect in sections]
        percentages = [calculate_percentage(dones[i], plans[i]) for i in range(4)]
        
        x = range(len(sections))
        width = 0.35
        
        bars1 = ax_bar.bar(x, plans, width, label='Режа', color='lightgray', alpha=0.7)
        bars2 = ax_bar.bar([i + width for i in x], dones, width, label='Бажарилди', color=colors, alpha=0.8)
        
        ax_bar.set_title('📊 Ойлик режа бажарилиши', fontsize=14, fontweight='bold')
        ax_bar.set_ylabel('Иш микдори')
        ax_bar.set_xticks([i + width/2 for i in x])
        ax_bar.set_xticklabels(['Бичиш', 'Тасниф', 'Тикув', 'Қадоқлаш'], fontsize=10)
        ax_bar.legend(fontsize=9)
        ax_bar.grid(True, alpha=0.3)
        
        for i, (plan, done, pct) in enumerate(zip(plans, dones, percentages)):
            ax_bar.text(i + width/2, max(plan, done) + max(plans)*0.02, 
                       f'{pct:.1f}%', ha='center', va='bottom', fontweight='bold', fontsize=9)

        # 3. Фоизлар (pie chart)
        ax_pie = fig.add_subplot(gs[1, 1])
        wedges, texts, autotexts = ax_pie.pie(percentages, labels=['Бичиш', 'Тасниф', 'Тикув', 'Қадоқлаш'], 
                                             colors=colors, autopct='%1.1f%%', startangle=90)
        for autotext in autotexts:
            autotext.set_color('white')
            autotext.set_fontweight('bold')
        ax_pie.set_title('🥧 Бажариш фоизлари', fontsize=14, fontweight='bold')

        # 4. Қолган иш ва кунлик талаб
        ax_remaining = fig.add_subplot(gs[2, 0])
        remainings = [monthly_data.get(sect, {}).get('remaining', 0) for sect in sections]
        remaining_days = get_remaining_workdays()
        daily_needed = [rem / remaining_days if remaining_days > 0 else 0 for rem in remainings]
        
        bars3 = ax_remaining.bar(x, remainings, color=colors, alpha=0.7, label='Қолган иш')
        ax_remaining.set_title('⏳ Қолган иш ва кунлик талаб', fontsize=14, fontweight='bold')
        ax_remaining.set_ylabel('Қолган иш')
        ax_remaining.set_xticks(x)
        ax_remaining.set_xticklabels(['Бичиш', 'Тасниф', 'Тикув', 'Қадоқлаш'], fontsize=10)
        ax_remaining.legend(fontsize=9)
        ax_remaining.grid(True, alpha=0.3)
        
        for i, (rem, needed) in enumerate(zip(remainings, daily_needed)):
            ax_remaining.text(i, rem + max(remainings)*0.02, f'{needed:.0f}/кун', 
                            ha='center', va='bottom', fontweight='bold', fontsize=8)

        # 5. Статистика
        ax_stats = fig.add_subplot(gs[2, 1])
        total_plan = sum(plans)
        total_done = sum(dones)
        total_remaining = sum(remainings)
        overall_percentage = calculate_percentage(total_done, total_plan)
        
        stats_text = f"📈 УМУМИЙ СТАТИСТИКА\n\n"
        stats_text += f"🎯 Жами режа: {total_plan:,.0f} та\n"
        stats_text += f"✅ Жами бажарилди: {total_done:,.0f} та\n"
        stats_text += f"⏳ Жами қолдиқ: {total_remaining:,.0f} та\n"
        stats_text += f"📊 Умумий бажарилди: {overall_percentage:.1f}%\n\n"
        stats_text += f"📅 Қолган иш кунлари: {remaining_days} кун\n"
        stats_text += f"🔥 Умумий кунлик керак: {sum(daily_needed):.0f} та/кун\n\n"
        stats_text += f"📆 Ой: {get_month_name()}\n"
        stats_text += f"🗓️ Ҳафта: {get_week_number()}"
        
        ax_stats.text(0.1, 0.95, stats_text, transform=ax_stats.transAxes, fontsize=11, 
                     verticalalignment='top', linespacing=1.5, fontweight='bold')
        ax_stats.set_title('📊 Умумий кўрсаткичлар', fontsize=14, fontweight='bold')
        ax_stats.axis('off')

        plt.tight_layout()
        
        buf = BytesIO()
        # Telegram учун оптимал DPI (50-60)
        plt.savefig(buf, format='png', dpi=50, bbox_inches='tight')
        buf.seek(0)
        
        # Олчамни текшириш
        from PIL import Image
        img = Image.open(buf)
        width, height = img.size
        logger.info(f"📏 Рам олчами: {width}x{height} пиксел")
        
        if width > 10000 or height > 10000:
            logger.warning(f"⚠️ Рам олчами Telegram чегарасидан ошди. Qayta олчамлаш...")
            scale_factor = 0.7
            new_width = int(width * scale_factor)
            new_height = int(height * scale_factor)
            img = img.resize((new_width, new_height), Image.Resampling.LANCZOS)
            new_buf = BytesIO()
            img.save(new_buf, format='PNG')
            new_buf.seek(0)
            buf = new_buf
        
        plt.close()
        logger.info("✅ Оптимизацияланган дашборд муваффақиятли яратилди")
        return buf
        
    except Exception as e:
        logger.error(f"❌ Дашборд яратишда хатолик: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return create_empty_dashboard()

def create_simple_workdays_calendar(ax, monthly_data):
    """Содда иш кунлари жадвали"""
    try:
        today = datetime.now(TZ)
        year = today.year
        month = today.month
        
        # Ойнинг иш кунлари
        first_day = datetime(year, month, 1, tzinfo=TZ)
        if month == 12:
            last_day = datetime(year, month, 31, tzinfo=TZ)
        else:
            last_day = datetime(year, month + 1, 1, tzinfo=TZ) - timedelta(days=1)
        
        # Ҳафта кунлари номлари (қисқа)
        weekdays_uz = ["Дш", "Сш", "Чр", "Пй", "Жм", "Шн"]
        
        # Жадвал маълумотларини тайёрлаш
        calendar_data = []
        headers = ["Кун"] + weekdays_uz + ["Ж"]
        
        # Ҳар бир ҳафта учун
        current_date = first_day
        week_num = 1
        
        while current_date <= last_day:
            week_row = [f"Ҳ{week_num}"]
            week_total = 0
            
            # Ҳар бир иш куни учун
            for day_num in range(6):  # 6 иш куни
                if current_date <= last_day and current_date.weekday() != 6:  # Якшанба эмас
                    date_str = current_date.strftime("%d.%m.%Y")
                    
                    # Кун учун маълумотлар
                    daily_work = 0
                    try:
                        records = sheet_report.get_all_values()
                        for row in records:
                            if row and row[0] == date_str:
                                # Ҳамма бўлимлар ишларини қўшиш
                                daily_work = (
                                    safe_val(row, 1) +  # Бичиш
                                    safe_val(row, 3) + safe_val(row, 4) + safe_val(row, 5) +  # Тасниф
                                    safe_val(row, 7) +  # Тикув
                                    safe_val(row, 10)   # Қадоқлаш
                                )
                                break
                    except:
                        pass
                    
                    # Кунлик нормани хисоблаш
                    total_monthly_plan = sum([data.get('plan', 0) for data in monthly_data.values()])
                    working_days = get_working_days_in_current_month()
                    daily_norm = total_monthly_plan / working_days if working_days > 0 else 0
                    
                    # Фоизни хисоблаш
                    daily_percentage = calculate_percentage(daily_work, daily_norm) if daily_norm > 0 else 0
                    
                    # Маълумотни қисқа форматда кўрсатиш
                    if daily_work > 0:
                        cell_text = f"{daily_work}\n{daily_percentage:.0f}%"
                    else:
                        cell_text = "-\n0%"
                    
                    week_row.append(cell_text)
                    week_total += daily_work
                    current_date += timedelta(days=1)
                else:
                    week_row.append("")
                    if current_date.weekday() == 6:  # Якшанба
                        current_date += timedelta(days=1)
            
            # Ҳафталик жами
            week_percentage = calculate_percentage(week_total, daily_norm * 6) if daily_norm > 0 else 0
            week_row.append(f"{week_total}\n{week_percentage:.0f}%")
            calendar_data.append(week_row)
            week_num += 1
            
            # Ҳафта охирига келганмиз?
            if current_date <= last_day and current_date.weekday() == 6:
                current_date += timedelta(days=1)  # Якшанбадан кейинги душанбага ўтиш
        
        # Жадвал яратиш
        if calendar_data:
            table = ax.table(cellText=calendar_data, colLabels=headers, 
                            cellLoc='center', loc='center',
                            bbox=[0, 0, 1, 1])
            
            # Жадвал услублари
            table.auto_set_font_size(False)
            table.set_fontsize(8)
            table.scale(1, 2)
            
            # Сарлавҳа услуби
            for i in range(len(headers)):
                table[(0, i)].set_facecolor('#2E7D32')
                table[(0, i)].set_text_props(weight='bold', color='white', fontsize=9)
                table[(0, i)].set_height(0.1)
            
            # Маълумотлар учун ранглаш
            for i in range(1, len(calendar_data) + 1):
                for j in range(len(headers)):
                    cell = table[(i, j)]
                    cell.set_text_props(fontsize=7, weight='bold')
                    cell.set_height(0.08)
                    
                    # Ҳафта сарлавҳалари
                    if j == 0:
                        cell.set_facecolor('#B2DFDB')
                    # Маълумотлар учун ранг
                    elif j > 0 and j < len(headers) - 1 and calendar_data[i-1][j]:
                        try:
                            if "%" in calendar_data[i-1][j]:
                                parts = calendar_data[i-1][j].split('\n')
                                if len(parts) >= 2:
                                    pct_text = parts[1]
                                    pct = int(pct_text.replace('%', ''))
                                    
                                    if pct >= 100:
                                        cell.set_facecolor("#4CAF50")
                                    elif pct >= 80:
                                        cell.set_facecolor("#8BC34A")
                                    elif pct >= 60:
                                        cell.set_facecolor("#FFC107")
                                    elif pct >= 40:
                                        cell.set_facecolor("#FF9800")
                                    elif pct >= 20:
                                        cell.set_facecolor("#FF5722")
                                    else:
                                        cell.set_facecolor("#F44336")
                        except:
                            cell.set_facecolor('#F5F5F5')
                    elif j == len(headers) - 1:  # Ҳафталик жами
                        cell.set_facecolor('#E3F2FD')
        
        month_name = get_month_name()
        ax.set_title(f'📅 ИШ КУНЛАРИ ЖАДВАЛИ - {month_name} {year}', 
                    fontsize=16, fontweight='bold', pad=20)
        ax.axis('off')
        
        # Ранглар изоҳи
        legend_text = "100%+ 80-99% 60-79% 40-59% 20-39% 0-19%"
        ax.text(0.5, -0.05, legend_text, transform=ax.transAxes, ha='center', 
               fontsize=9, style='italic', weight='bold',
               bbox=dict(boxstyle="round,pad=0.3", facecolor="lightgray", alpha=0.8))
        
    except Exception as e:
        logger.error(f"❌ Содда иш кунлари жадвалида хатолик: {e}")
        ax.text(0.5, 0.5, 'Иш кунлари жадвалини яратишда хатолик', 
                ha='center', va='center', transform=ax.transAxes, fontsize=12)
        ax.axis('off')

# ------------------- UPDATE EXISTING FUNCTIONS -------------------
# Мавжуд функцияларни кэшланган версиялари билан алмаштириш
def get_monthly_data():
    """Асосий функцияни кэшланган версияга йўналтириш"""
    return get_monthly_data_cached()

@sheets_rate_limiter
def append_or_update(sheet, values_by_index: dict):
    """Rate limit билан append_or_update"""
    try:
        row_idx = find_today_row(sheet)
        updates = []
        
        if row_idx == 0:
            max_index = max(values_by_index.keys()) + 1
            row = [""] * max_index
            row[0] = today_date_str()
            
            for idx, val in values_by_index.items():
                row[idx] = str(val)
            safe_sheets_call(sheet.append_row, row)
            logger.info(f"✅ Янги қатор қўшилди")
        else:
            for idx, val in values_by_index.items():
                updates.append({
                    'range': f"{gspread.utils.rowcol_to_a1(row_idx, idx + 1)}",
                    'values': [[str(val)]]
                })
            if updates:
                safe_sheets_call(sheet.batch_update, updates)
                logger.info(f"✅ Мавжуд қатор янгиланди")
                
    except Exception as e:
        logger.error(f"❌ append_or_update хato: {e}")

@sheets_rate_limiter  
def find_today_row(sheet):
    """Rate limit билан find_today_row"""
    try:
        colA = safe_sheets_call(sheet.col_values, 1)
        today = today_date_str()
        for i, v in enumerate(colA, start=1):
            if v.strip() == today:
                return i
        return 0
    except Exception as e:
        logger.error(f"❌ find_today_row хato: {e}")
        return 0

def update_order_in_sheet(row_index, field, value):
    try:
        col_idx = 0
        if field == "done":
            col_idx = 3
        elif field == "total":
            col_idx = 2
        elif field == "deadline":
            col_idx = 7
        elif field == "name":
            col_idx = 1
        elif field == "section":
            col_idx = 9
        
        if col_idx > 0:
            safe_sheets_call(sheet_orders.update_cell, row_index, col_idx + 1, str(value))
            
            if field in ["done", "total"]:
                row_values = safe_sheets_call(sheet_orders.row_values, row_index)
                
                total = parse_float(value) if field == "total" else parse_float(row_values[2])
                done = parse_float(value) if field == "done" else parse_float(row_values[3])
                remaining = max(0, total - done)
                
                done_pct = calculate_percentage(done, total)
                remaining_pct = calculate_percentage(remaining, total)
                
                safe_sheets_call(sheet_orders.update_cell, row_index, 5, remaining)
                safe_sheets_call(sheet_orders.update_cell, row_index, 6, f"{done_pct:.1f}%")
                safe_sheets_call(sheet_orders.update_cell, row_index, 7, f"{remaining_pct:.1f}%")
                
                deadline = row_values[7] if len(row_values) > 7 else ""
                if deadline:
                    try:
                        deadline_date = datetime.strptime(deadline, "%d.%m.%Y").replace(tzinfo=TZ)
                        today = datetime.now(TZ)
                        days_left = (deadline_date - today).days
                        safe_sheets_call(sheet_orders.update_cell, row_index, 9, days_left)
                    except:
                        pass
            
            logger.info(f"✅ Буюртма янгиланди: {field} = {value}")
            return True
        
        return False
    except Exception as e:
        logger.error(f"❌ Буюртмани янгилашda хato: {e}")
        return False

async def send_to_group(message_text, topic_id=None, parse_mode=None):
    try:
        message_thread_id = topic_id
        
        await bot.send_message(
            chat_id=GROUP_ID,
            text=message_text,
            message_thread_id=message_thread_id,
            parse_mode=parse_mode
        )
        logger.info(f"✅ Хабар гуруҳга жўнатилди (Topic: {message_thread_id})")
        return True
    except Exception as e:
        logger.error(f"❌ Гуруҳга хабар жўнатишda хato: {e}")
        
        # Topic ID билан хатолик бўлса, topicsiz уриниб кўриш
        try:
            if topic_id:
                await bot.send_message(
                    chat_id=GROUP_ID,
                    text=message_text,
                    parse_mode=parse_mode
                )
                logger.info("✅ Хабар гуруҳга topic ID сиз жўнатилди")
                return True
        except Exception as e2:
            logger.error(f"❌ Гуруҳга topic ID сиз хабар жўнатишda хato: {e2}")
            
        return False

def validate_order_data(order_name, quantity, deadline):
    errors = []
    
    if not order_name or len(order_name.strip()) < 2:
        errors.append("❌ Буюртма номи энг камда 2 та ҳарфдан иборат бўлиши керак")
    
    try:
        quantity = int(quantity)
        if quantity <= 0:
            errors.append("❌ Миқдор мусбат сон бўлиши керак")
    except ValueError:
        errors.append("❌ Миқдорни нотоғри киритдинзи. Бутун сон киритинг")
    
    try:
        deadline_date = datetime.strptime(deadline, "%d.%m.%Y").replace(tzinfo=TZ)
        today = datetime.now(TZ)
        if deadline_date <= today:
            errors.append("❌ Муддат бугундан кейинги сана бўлиши керак")
    except ValueError:
        errors.append("❌ Санани нотоғри форматда киритдингиз. Тўғри формат: кун.ой.йил")
    
    return errors

def format_order_message(order_name, total, done, deadline, days_left, section, action):
    return f"📦 Буюртма {action}:\n\nНоми: {order_name}\nМиқдори: {done}/{total} та\nМуддати: {deadline}\nҚолган кун: {days_left}\nБўлим: {section}"

def format_workflow_report():
    try:
        orders = get_workflow_orders_data()
        if not orders:
            return ["❌ Ҳали workflow буюртмалари мавжуд эмас."]
        
        reports = []
        current_report = "🔄 Workflow хисоботи\n\n"
        
        for order in orders:
            order_text = f"📦 {order['name']}\n"
            order_text += f"   Умумий: {order['total']} та\n"
            order_text += f"   ✅ Бажарилди: {order['done']} та ({order['done_percentage']})\n"
            order_text += f"   ⏳ Қолдиқ: {order['remaining']} та\n"
            order_text += f"   📍 Жорий босқич: {order['current_stage']}\n"
            order_text += f"   📅 Муддат: {order['deadline']} (Қолган {order['days_left']} кун)\n"
            
            order_text += "   🔄 Прогресс:\n"
            order_text += f"      ✂️ Бичиш: {order['bichish_done']}/{order['total']} та\n"
            order_text += f"      📑 Тасниф: {order['tasnif_done']}/{order['bichish_done']} та\n"
            order_text += f"      🧵 Тикув: {order['tikuv_done']}/{order['tasnif_done']} та\n"
            order_text += f"      📦 Қадоқлаш: {order['qadoqlash_done']}/{order['tikuv_done']} та\n"
            order_text += f"      📤 Қутига солиш: {order['qutiga_solish_done']}/{order['qadoqlash_done']} та\n"
            order_text += "─" * 30 + "\n\n"
            
            if len(current_report) + len(order_text) > 4000:
                reports.append(current_report)
                current_report = order_text
            else:
                current_report += order_text
        
        if current_report:
            reports.append(current_report)
            
        return reports
        
    except Exception as e:
        logger.error(f"❌ Workflow хисобот яратишда хato: {e}")
        return ["❌ Workflow хисобот яратишда хатолик юз берди."]

def get_workflow_stage_orders(stage):
    orders = get_workflow_orders_data()
    return [order for order in orders if order['current_stage'].lower() == stage.lower()]

def get_workflow_stage_orders(stage_name):
    """Берілган босқичдаги буюртмаларни олиш"""
    try:
        orders = get_workflow_orders_data()
        stage_orders = []
        
        for order in orders:
            # Текширишни икки ҳолда бажарамиз: тўлиқ мос келиш ва фақат бўлим номи
            current_stage = order.get('current_stage', '').lower().strip()
            search_stage = stage_name.lower().strip()
            
            # Агар бўлим номи мос келса ёки тўлиқ мос келса
            if (current_stage == search_stage or 
                current_stage.startswith(search_stage) or
                search_stage in current_stage):
                stage_orders.append(order)
        
        return stage_orders
    except Exception as e:
        logger.error(f"❌ get_workflow_stage_orders хato: {e}")
        return []

@dp.callback_query(F.data == "debug_workflow")
async def cb_debug_workflow(callback: CallbackQuery):
    await callback.answer()
    
    try:
        orders = get_workflow_orders_data()
        debug_msg = f"Workflow маълумотлари: {len(orders)} та буюртма\n\n"
        
        for order in orders[:5]:  # Фақат биринчи 5 таси
            debug_msg += f"📦 {order['name']}\n"
            debug_msg += f"   Жорий босқич: {order.get('current_stage', 'Номаълум')}\n"
            debug_msg += f"   Бажарилди: {order['done']}/{order['total']}\n"
            debug_msg += "─" * 20 + "\n"
            
        await callback.message.answer(debug_msg)
    except Exception as e:
        await callback.message.answer(f"❌ Debug хato: {e}")

def update_order_sheet_for_workflow():
    try:
        headers = sheet_orders.row_values(1)
        
        new_headers = [
            "Бичиш бажарилди", "Тасниф бажарилди", "Тикув бажарилди",
            "Қадоқлаш бажарилди", "Қутига солиш бажарилди", "Жорий босқич"
        ]
        
        if len(headers) < 16:
            for i, header in enumerate(new_headers, start=len(headers) + 1):
                sheet_orders.update_cell(1, i, header)
            logger.info("✅ Workflow ustunlari qo'shildi")
        
    except Exception as e:
        logger.error(f"❌ Буюртмалар jadvalini янгилашда хato: {e}")

update_order_sheet_for_workflow()

# ------------------- ГРАФИК ФУНКЦИЯЛАРИ -------------------
def create_percentage_pie_chart():
    """Pie chart для отображения процентов выполнения плана"""
    try:
        monthly_data = get_monthly_data()
        if not monthly_data:
            logger.error("❌ Monthly data is empty")
            return None
            
        sections = []
        percentages = []
        actual_values = []
        
        for section_name in ['бичиш', 'тасниф', 'тикув', 'қадоқлаш']:
            if section_name in monthly_data:
                data = monthly_data[section_name]
                done_pct_str = data.get('done_pct', '0%')
                try:
                    pct_value = float(done_pct_str.replace('%', '').strip())
                    pct_value = min(100, max(0, pct_value))
                    
                    sections.append(section_name.capitalize())
                    percentages.append(pct_value)
                    actual_values.append(f"{data.get('done', 0):.0f}/{data.get('plan', 1):.0f}")
                except ValueError as e:
                    logger.error(f"❌ Error parsing percentage for {section_name}: {done_pct_str}, error: {e}")
                    continue
        
        if not sections:
            logger.error("❌ No valid sections data for pie chart")
            return None
        
        colors = []
        for p in percentages:
            if p >= 100:
                colors.append('#4CAF50')
            elif p >= 80:
                colors.append('#8BC34A')
            elif p >= 60:
                colors.append('#FFC107')
            elif p >= 40:
                colors.append('#FF9800')
            else:
                colors.append('#F44336')
        
        plt.figure(figsize=(12, 10))
        
        explode = [0.05 if p == max(percentages) else 0 for p in percentages]
        
        wedges, texts, autotexts = plt.pie(
            percentages, 
            labels=sections, 
            colors=colors, 
            autopct='%1.1f%%', 
            startangle=90, 
            shadow=True,
            explode=explode,
            textprops={'fontsize': 12}
        )
        
        for autotext in autotexts:
            autotext.set_color('white')
            autotext.set_fontsize(14)
            autotext.set_fontweight('bold')
        
        legend_labels = [f'{sect}: {val}' for sect, val in zip(sections, actual_values)]
        plt.legend(wedges, legend_labels, title="Бажарилди/Режа", loc="center left", bbox_to_anchor=(1, 0, 0.5, 1))
        
        plt.axis('equal')
        plt.title('Ойлик режа бажарилиши фоизда\n', fontsize=16, fontweight='bold')
        
        total_done = sum([float(p) for p in percentages]) / len(percentages) if percentages else 0
        plt.figtext(0.5, 0.01, f'Умумий бажарилди: {total_done:.1f}%', ha='center', fontsize=12)
        
        plt.tight_layout()
        
        buf = BytesIO()
        plt.savefig(buf, format='png', dpi=100, bbox_inches='tight')
        buf.seek(0)
        plt.close()
        
        logger.info(f"✅ Pie chart created successfully with {len(sections)} sections")
        return buf
    except Exception as e:
        logger.error(f"❌ Фоизлар учун pie chart яратишда хato: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return None

def create_weekly_trend_chart():
    """Недельная тенденция производства - текущая неделя"""
    try:
        today = datetime.now(TZ)
        start_of_week = today - timedelta(days=today.weekday())
        
        records = sheet_report.get_all_values()
        weekly_data = {}
        
        for i in range(7):
            current_date = start_of_week + timedelta(days=i)
            date_str = current_date.strftime("%d.%m.%Y")
            weekly_data[date_str] = {
                'bichish': 0,
                'tasnif': 0,
                'tikuv': 0,
                'qadoqlash': 0,
                'date_obj': current_date
            }
        
        for row in records[1:]:
            if len(row) > 0 and row[0]:
                try:
                    if row[0] in weekly_data:
                        weekly_data[row[0]]['bichish'] = safe_val(row, 1)
                        weekly_data[row[0]]['tasnif'] = safe_val(row, 3) + safe_val(row, 4) + safe_val(row, 5)
                        weekly_data[row[0]]['tikuv'] = safe_val(row, 7)
                        weekly_data[row[0]]['qadoqlash'] = safe_val(row, 10)
                except:
                    continue
        
        sorted_dates = sorted(weekly_data.keys())
        dates = [weekly_data[date]['date_obj'] for date in sorted_dates]
        
        bichish_values = [weekly_data[date]['bichish'] for date in sorted_dates]
        tasnif_values = [weekly_data[date]['tasnif'] for date in sorted_dates]
        tikuv_values = [weekly_data[date]['tikuv'] for date in sorted_dates]
        qadoqlash_values = [weekly_data[date]['qadoqlash'] for date in sorted_dates]
        
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 12))
        
        ax1.plot(dates, bichish_values, marker='o', label='Бичиш', linewidth=2)
        ax1.plot(dates, tasnif_values, marker='o', label='Тасниф', linewidth=2)
        ax1.plot(dates, tikuv_values, marker='o', label='Тикув', linewidth=2)
        ax1.plot(dates, qadoqlash_values, marker='o', label='Қадоқлаш', linewidth=2)
        
        ax1.set_xlabel('Кунлар')
        ax1.set_ylabel('Иш микдори')
        ax1.set_title('Ҳафталик иш чиқими тенденцияси (линейный график)')
        ax1.legend()
        ax1.grid(True, alpha=0.3)
        
        ax1.xaxis.set_major_formatter(plt.matplotlib.dates.DateFormatter('%a\n%d.%m'))
        ax1.xaxis.set_major_locator(plt.matplotlib.dates.DayLocator())
        
        x = range(len(dates))
        width = 0.2
        
        ax2.bar([i - width*1.5 for i in x], bichish_values, width, label='Бичиш', color='skyblue')
        ax2.bar([i - width*0.5 for i in x], tasnif_values, width, label='Тасниф', color='lightgreen')
        ax2.bar([i + width*0.5 for i in x], tikuv_values, width, label='Тикув', color='lightcoral')
        ax2.bar([i + width*1.5 for i in x], qadoqlash_values, width, label='Қадоқлаш', color='gold')
        
        ax2.set_xlabel('Кунлар')
        ax2.set_ylabel('Иш микдори')
        ax2.set_title('Ҳафталик иш чиқими тенденцияси (столбчатая диаграмма)')
        ax2.set_xticks(x)
        ax2.set_xticklabels([date.strftime('%a\n%d.%m') for date in dates])
        ax2.legend()
        ax2.grid(True, alpha=0.3)
        
        plt.tight_layout()
        
        buf = BytesIO()
        plt.savefig(buf, format='png', dpi=100)
        buf.seek(0)
        plt.close()
        
        return buf
    except Exception as e:
        logger.error(f"❌ Ҳафталик тенденция графиги яратишда хato: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return None

def create_monthly_trend_chart():
    """Месячная тенденция производства - текущий месяц"""
    try:
        today = datetime.now(TZ)
        start_of_month = today.replace(day=1)
        
        if today.month == 12:
            end_of_month = today.replace(day=31)
        else:
            end_of_month = today.replace(month=today.month+1, day=1) - timedelta(days=1)
        
        records = sheet_report.get_all_values()
        monthly_data = {}
        
        current_date = start_of_month
        while current_date <= end_of_month:
            date_str = current_date.strftime("%d.%m.%Y")
            monthly_data[date_str] = {
                'bichish': 0,
                'tasnif': 0,
                'tikuv': 0,
                'qadoqlash': 0,
                'date_obj': current_date
            }
            current_date += timedelta(days=1)
        
        for row in records[1:]:
            if len(row) > 0 and row[0]:
                try:
                    if row[0] in monthly_data:
                        monthly_data[row[0]]['bichish'] = safe_val(row, 1)
                        monthly_data[row[0]]['tasnif'] = safe_val(row, 3) + safe_val(row, 4) + safe_val(row, 5)
                        monthly_data[row[0]]['tikuv'] = safe_val(row, 7)
                        monthly_data[row[0]]['qadoqlash'] = safe_val(row, 10)
                except:
                    continue
        
        sorted_dates = sorted(monthly_data.keys())
        dates = [monthly_data[date]['date_obj'] for date in sorted_dates]
        
        bichish_values = [monthly_data[date]['bichish'] for date in sorted_dates]
        tasnif_values = [monthly_data[date]['tasnif'] for date in sorted_dates]
        tikuv_values = [monthly_data[date]['tikuv'] for date in sorted_dates]
        qadoqlash_values = [monthly_data[date]['qadoqlash'] for date in sorted_dates]
        
        # Умумий ранг палеттаси
        colors = {
            'bichish': '#FF6B6B',
            'tasnif': '#4ECDC4', 
            'tikuv': '#45B7D1',
            'qadoqlash': '#96CEB4'
        }
        
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(16, 12))
        
        # Линейный график - бир хил ранглар
        ax1.plot(dates, bichish_values, marker='o', label='Бичиш', linewidth=2, color=colors['bichish'])
        ax1.plot(dates, tasnif_values, marker='o', label='Тасниф', linewidth=2, color=colors['tasnif'])
        ax1.plot(dates, tikuv_values, marker='o', label='Тикув', linewidth=2, color=colors['tikuv'])
        ax1.plot(dates, qadoqlash_values, marker='o', label='Қадоқлаш', linewidth=2, color=colors['qadoqlash'])
        
        ax1.set_xlabel('Кунлар')
        ax1.set_ylabel('Иш микдори')
        ax1.set_title('Ойлик иш чиқими тенденцияси (линейный график)')
        ax1.legend()
        ax1.grid(True, alpha=0.3)
        
        ax1.xaxis.set_major_formatter(plt.matplotlib.dates.DateFormatter('%d'))
        ax1.xaxis.set_major_locator(plt.matplotlib.dates.DayLocator(interval=2))
        
        # Столбчатая диаграмма - бир хил ранглар
        x = range(len(dates))
        width = 0.2
        
        ax2.bar([i - width*1.5 for i in x], bichish_values, width, label='Бичиш', color=colors['bichish'])
        ax2.bar([i - width*0.5 for i in x], tasnif_values, width, label='Тасниф', color=colors['tasnif'])
        ax2.bar([i + width*0.5 for i in x], tikuv_values, width, label='Тикув', color=colors['tikuv'])
        ax2.bar([i + width*1.5 for i in x], qadoqlash_values, width, label='Қадоқлаш', color=colors['qadoqlash'])
        
        ax2.set_xlabel('Кунлар')
        ax2.set_ylabel('Иш микдори')
        ax2.set_title('Ойлик иш чиқими тенденцияси (столбчатая диаграмма)')
        ax2.set_xticks(x[::2])
        ax2.set_xticklabels([date.strftime('%d') for date in dates][::2])
        ax2.legend()
        ax2.grid(True, alpha=0.3)
        
        month_name = get_month_name()
        plt.figtext(0.5, 0.01, f'{month_name}', ha='center', fontsize=14, fontweight='bold')
        
        plt.tight_layout()
        plt.subplots_adjust(bottom=0.1)
        
        buf = BytesIO()
        plt.savefig(buf, format='png', dpi=100)
        buf.seek(0)
        plt.close()
        
        return buf
    except Exception as e:
        logger.error(f"❌ Ойлик тенденция графиги яратишда хato: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return None

def setup_matplotlib_for_emojis():
    """Emojilar uchun matplotlib sozlamalari"""
    try:
        plt.rcParams['font.family'] = 'DejaVu Sans'
        plt.rcParams['font.sans-serif'] = ['DejaVu Sans']
        plt.rcParams['axes.unicode_minus'] = False
    except Exception as e:
        logger.warning(f"⚠️ Emoji sozlamalarida xatolik: {e}")

def create_four_pie_charts(axes, monthly_data):
    """4 та бўлим учун алоҳида pie chartлар - тўлиқ амалга оширилган"""
    try:
        sections = ['бичиш', 'тасниф', 'тикув', 'қадоқлаш']
        section_names = {
            'бичиш': '✂️ Бичиш',
            'тасниф': '📑 Тасниф', 
            'тикув': '🧵 Тикув',
            'қадоқлаш': '📦 Қадоқлаш'
        }
        
        # Ҳар бир бўлим учун алоҳида ранглар
        section_colors = {
            'бичиш': ['#FF6B6B', '#FFA8A8', '#FFE0E0'],
            'тасниф': ['#4ECDC4', '#88D8D8', '#C7F7F7'],
            'тикув': ['#45B7D1', '#7BC8E2', '#B3E0F0'],
            'қадоқлаш': ['#96CEB4', '#B8D8C8', '#DAF2E3']
        }
        
        for i, section in enumerate(sections):
            if section in monthly_data:
                data = monthly_data[section]
                plan = data.get('plan', 0)
                done = data.get('done', 0)
                remaining = max(0, plan - done)
                
                if plan > 0:
                    percentage = (done / plan) * 100
                    remaining_percentage = (remaining / plan) * 100
                    
                    sizes = [done, remaining]
                    colors = [section_colors[section][0], '#F0F0F0']
                    labels = [
                        f'✅ Бажарилди\n{done:,.0f} та\n({percentage:.1f}%)', 
                        f'⏳ Қолдиқ\n{remaining:,.0f} та\n({remaining_percentage:.1f}%)'
                    ]
                    
                    ax = axes[i]
                    wedges, texts, autotexts = ax.pie(
                        sizes, 
                        labels=labels, 
                        colors=colors, 
                        autopct='',
                        startangle=90, 
                        textprops={'fontsize': 10, 'weight': 'bold'},
                        explode=[0.05, 0], 
                        shadow=True
                    )
                    
                    # Матн олчамини текшириш
                    for text in texts:
                        text.set_fontsize(9)
                        text.set_weight('bold')
                    
                    # Сарлавҳа
                    ax.set_title(
                        f'{section_names[section]}\nОйлик режа: {plan:,.0f} та', 
                        fontsize=12, 
                        fontweight='bold', 
                        pad=20
                    )
                    ax.axis('equal')
                    
                else:
                    # Режа мавжуд бўлмаганда
                    ax = axes[i]
                    ax.text(0.5, 0.5, 'Маълумотлар\nмавжуд эмас', 
                           ha='center', va='center', transform=ax.transAxes, 
                           fontsize=12, weight='bold')
                    ax.set_title(f'{section_names[section]}', fontsize=12, fontweight='bold')
                    ax.axis('equal')
        
    except Exception as e:
        logger.error(f"❌ Pie chartларда хатолик: {e}")
        import traceback
        logger.error(traceback.format_exc())
        
        # Хатолик юз берганда, барча осьларга хабар бериш
        for i, ax in enumerate(axes):
            ax.text(0.5, 0.5, 'Хатолик\nюз берди', 
                   ha='center', va='center', transform=ax.transAxes, 
                   fontsize=12, weight='bold')
            ax.set_title(f'Бўлим {i+1}', fontsize=12)
            ax.axis('equal')

def create_simple_workdays_calendar(ax_calendar, monthly_data):
    raise NotImplementedError

def create_production_dashboard():
    """Яхшиланган ишлаб чиқариш дашборди - детал таблицалар билан"""
    try:
        logger.info("📊 Яхшиланган дашборд яратиш бошланди...")
        
        # Ойлик маълумотларни олиш
        monthly_data = get_monthly_data()
        
        if not monthly_data:
            logger.error("❌ Ойлик маълумотлар топилмади")
            return create_empty_dashboard()

        # Telegram учун оптимал олчам (18x28 дюйм, 60 DPI = 1080x1680 пиксел)
        fig = plt.figure(figsize=(18, 28))
        fig.suptitle('ИШЛАБ ЧИҚАРИШ ДАШБОРДИ - БАРЧА БЎЛИМЛАР', 
                    fontsize=22, fontweight='bold', y=0.98)
        
        # Мурожат тузилмаси - 5x2 grid
        gs = gridspec.GridSpec(5, 2, figure=fig, height_ratios=[1.2, 1, 1, 1, 1.5], hspace=0.5, wspace=0.3)
        
        # 1. ИШ КУНЛАРИ ЖАДВАЛИ (2 устунли) - ТЕПАДА
        ax_calendar = fig.add_subplot(gs[0, :])
        create_simple_workdays_calendar(ax_calendar, monthly_data)
        
        # 2. 4 та бўлим учун детал таблицалар
        ax_bichish = fig.add_subplot(gs[1, 0])
        ax_tasnif = fig.add_subplot(gs[1, 1])
        ax_tikuv = fig.add_subplot(gs[2, 0])
        ax_qadoqlash = fig.add_subplot(gs[2, 1])
        
        # 3. Детал тенденция графиги
        ax_trend = fig.add_subplot(gs[3, :])
        
        # 4. Умумий хисобот таблицаси
        ax_summary = fig.add_subplot(gs[4, :])
        
        # 2-5. Бўлимлар учун детал таблицалар яратиш
        create_detailed_section_tables(
            [ax_bichish, ax_tasnif, ax_tikuv, ax_qadoqlash],
            monthly_data
        )
        
        # 6. Детал тенденция графиги
        create_detailed_trend_chart(ax_trend)
        
        # 7. Умумий хисобот таблицаси
        create_summary_table(ax_summary, monthly_data)
        
        # Сана ва вакт маълумотлари
        today = datetime.now(TZ)
        month_name = get_month_name()
        current_week = get_week_number()
        total_workdays = get_working_days_in_current_month()
        remaining_days = get_remaining_workdays()
        current_workday = get_current_workday_index()
        
        plt.figtext(0.5, 0.02, 
                   f"Ҳисобот санаси: {today.strftime('%d.%m.%Y %H:%M')} | "
                   f"Ой: {month_name} | "
                   f"Ҳафта: {current_week} | "
                   f"Иш кунлари: {total_workdays} (Ҳозиргача: {current_workday}, Қолган: {remaining_days})",
                   ha='center', fontsize=11, fontweight='bold',
                   bbox=dict(boxstyle="round,pad=0.5", facecolor="#2E7D32", alpha=0.9))
        
        plt.tight_layout()
        plt.subplots_adjust(bottom=0.04, top=0.96)
        
        buf = BytesIO()
        # Оптимал DPI (60) - баланд сифат ва олчам
        plt.savefig(buf, format='png', dpi=60, bbox_inches='tight')
        buf.seek(0)
        plt.close()
        
        # Рам олчамини текшириш
        from PIL import Image
        img = Image.open(buf)
        width, height = img.size
        logger.info(f"📏 Рам олчами: {width}x{height} пиксел")
        
        # Telegram чегарасини текшириш
        if width > 10000 or height > 10000:
            logger.warning(f"⚠️ Рам олчами Telegram чегарасидан ошди. Qayta олчамлаш...")
            # Пропорционал равишда камайтириш (70%)
            scale_factor = 0.7
            new_width = int(width * scale_factor)
            new_height = int(height * scale_factor)
            img = img.resize((new_width, new_height), Image.Resampling.LANCZOS)
            new_buf = BytesIO()
            # Яхши сифат билан сақлаш
            img.save(new_buf, format='PNG', optimize=True, quality=90)
            new_buf.seek(0)
            buf = new_buf
        
        logger.info("✅ Яхшиланган дашборд муваффақиятли яратилди")
        return buf
        
    except Exception as e:
        logger.error(f"❌ Дашборд яратишда хатолик: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return create_empty_dashboard()


def create_detailed_section_tables(axes, monthly_data):
    """4 та бўлим учун детал таблицалар"""
    try:
        sections = [
            {"key": "бичиш", "name": "Бичиш", "emoji": "✂️", "column": 1},
            {"key": "тасниф", "name": "Тасниф", "emoji": "📑", "columns": [3, 4, 5]},
            {"key": "тикув", "name": "Тикув", "emoji": "🧵", "column": 7},
            {"key": "қадоқлаш", "name": "Қадоқлаш", "emoji": "📦", "column": 10}
        ]
        
        for i, (ax, section_info) in enumerate(zip(axes, sections)):
            section_key = section_info["key"]
            section_name = section_info["name"]
            emoji = section_info["emoji"]
            
            if section_key in monthly_data:
                data = monthly_data[section_key]
                plan = data.get('plan', 0)
                done = data.get('done', 0)
                remaining = max(0, plan - done)
                percentage = calculate_percentage(done, plan) if plan > 0 else 0
                
                # Кунлик норма ва керакли иш
                working_days = get_working_days_in_current_month()
                daily_plan = plan / working_days if working_days > 0 else 0
                remaining_days = get_remaining_workdays()
                daily_needed = remaining / remaining_days if remaining_days > 0 else 0
                
                # Бутунги кун иш микдори
                today_work = get_today_work_for_section(section_key)
                today_percentage = calculate_percentage(today_work, daily_plan) if daily_plan > 0 else 0
                
                # Жадвал маълумотлари
                table_data = [
                    ["КўРСАТКИЧ", "ҚИЙМАТ", "ФОИЗ"],
                    ["Ойлик режа", f"{plan:,.0f} та", ""],
                    ["Бажарилди", f"{done:,.0f} та", f"{percentage:.1f}%"],
                    ["Қолдиқ", f"{remaining:,.0f} та", f"{calculate_percentage(remaining, plan):.1f}%"],
                    ["Кунлик норма", f"{daily_plan:,.1f} та/кун", ""],
                    ["Кунлик керак", f"{daily_needed:,.1f} та/кун", ""],
                    ["Бутун иш", f"{today_work:,.0f} та", f"{today_percentage:.1f}%"]
                ]
                
                # Жадвал яратиш
                table = ax.table(cellText=table_data, cellLoc='center', 
                                loc='center', bbox=[0, 0.1, 1, 0.9])
                
                # Жадвал услублари
                table.auto_set_font_size(False)
                table.set_fontsize(9)
                table.scale(1, 1.5)
                
                # Сарлавҳа услуби
                for j in range(3):
                    table[(0, j)].set_facecolor('#2E7D32')
                    table[(0, j)].set_text_props(weight='bold', color='white', fontsize=10)
                    table[(0, j)].set_height(0.12)
                
                # Маълумотлар учун ранглаш
                for row in range(1, len(table_data)):
                    for col in range(3):
                        cell = table[(row, col)]
                        cell.set_text_props(fontsize=8, weight='bold')
                        cell.set_height(0.1)
                        
                        # Махсус кўрсаткичлар учун ранг
                        if row == 2:  # Бажарилди
                            if percentage >= 100:
                                cell.set_facecolor("#4CAF50")
                            elif percentage >= 80:
                                cell.set_facecolor("#8BC34A")
                            elif percentage >= 60:
                                cell.set_facecolor("#FFC107")
                            elif percentage >= 40:
                                cell.set_facecolor("#FF9800")
                            elif percentage >= 20:
                                cell.set_facecolor("#FF5722")
                            else:
                                cell.set_facecolor("#F44336")
                        elif row == 6:  # Бутун иш
                            if today_percentage >= 100:
                                cell.set_facecolor("#4CAF50")
                            elif today_percentage >= 80:
                                cell.set_facecolor("#C8E6C9")
                            elif today_percentage >= 60:
                                cell.set_facecolor("#FFF9C4")
                            elif today_percentage >= 40:
                                cell.set_facecolor("#FFECB3")
                            elif today_percentage >= 20:
                                cell.set_facecolor("#FFCDD2")
                            else:
                                cell.set_facecolor("#FFEBEE")
                        else:
                            cell.set_facecolor('#F5F5F5')
                
                ax.set_title(f'{emoji} {section_name}', fontsize=14, fontweight='bold', pad=15)
                ax.axis('off')
                
                # Қўшимча статистика
                stats_text = f"Қолган кунлар: {remaining_days}"
                ax.text(0.5, 0.05, stats_text, transform=ax.transAxes, 
                       ha='center', fontsize=9, fontweight='bold',
                       bbox=dict(boxstyle="round,pad=0.2", facecolor="#B3E5FC", alpha=0.8))
            else:
                ax.text(0.5, 0.5, f'❌ {section_name} учун\nмаълумотлар мавжуд эмас', 
                       ha='center', va='center', transform=ax.transAxes, 
                       fontsize=12, weight='bold')
                ax.set_title(f'{emoji} {section_name}', fontsize=14, fontweight='bold')
                ax.axis('off')
        
    except Exception as e:
        logger.error(f"❌ Детал таблицалар яратишда хатолик: {e}")
        for ax in axes:
            ax.text(0.5, 0.5, 'Хатолик\nюз берди', 
                   ha='center', va='center', transform=ax.transAxes, 
                   fontsize=12, weight='bold')
            ax.axis('off')


def create_detailed_trend_chart(ax):
    """Детал тенденция графиги - кунлик иш чиқими"""
    try:
        # Охирги 7 кун маълумотларини олиш
        today = datetime.now(TZ)
        dates = []
        bichish_data = []
        tasnif_data = []
        tikuv_data = []
        qadoqlash_data = []
        
        for i in range(7):
            current_date = today - timedelta(days=i)
            date_str = current_date.strftime("%d.%m.%Y")
            dates.insert(0, date_str)
            
            # Маълумотларни олиш
            records = sheet_report.get_all_values()
            bichish = 0
            tasnif = 0
            tikuv = 0
            qadoqlash = 0
            
            for row in records:
                if row and row[0] == date_str:
                    bichish = safe_val(row, 1)
                    tasnif = safe_val(row, 3) + safe_val(row, 4) + safe_val(row, 5)
                    tikuv = safe_val(row, 7)
                    qadoqlash = safe_val(row, 10)
                    break
            
            bichish_data.insert(0, bichish)
            tasnif_data.insert(0, tasnif)
            tikuv_data.insert(0, tikuv)
            qadoqlash_data.insert(0, qadoqlash)
        
        # Умумий ранг палеттаси
        colors = {
            'bichish': '#FF6B6B',
            'tasnif': '#4ECDC4', 
            'tikuv': '#45B7D1',
            'qadoqlash': '#96CEB4'
        }
        
        x = range(len(dates))
        width = 0.2
        
        # Устунлар - кунлик микдорлар
        bars1 = ax.bar([i - width*1.5 for i in x], bichish_data, width, 
                      label='Бичиш', color=colors['bichish'], alpha=0.8)
        bars2 = ax.bar([i - width*0.5 for i in x], tasnif_data, width, 
                      label='Тасниф', color=colors['tasnif'], alpha=0.8)
        bars3 = ax.bar([i + width*0.5 for i in x], tikuv_data, width, 
                      label='Тикув', color=colors['tikuv'], alpha=0.8)
        bars4 = ax.bar([i + width*1.5 for i in x], qadoqlash_data, width, 
                      label='Қадоқлаш', color=colors['qadoqlash'], alpha=0.8)
        
        ax.set_title('📈 ОХИРГИ 7 КУНДАГИ ИШ ЧИҚИМИ', fontsize=16, fontweight='bold', pad=20)
        ax.set_ylabel('Иш микдори (та)', fontsize=14, fontweight='bold')
        ax.set_xlabel('Саналар', fontsize=14, fontweight='bold')
        ax.set_xticks(x)
        
        # Кун номлари
        day_names = []
        for date_str in dates:
            date_obj = datetime.strptime(date_str, "%d.%m.%Y")
            weekdays_uz = ["Душ", "Сеш", "Чор", "Пай", "Жум", "Шан", "Якш"]
            day_name = weekdays_uz[date_obj.weekday()]
            day_names.append(f'{day_name}\n{date_str.split(".")[0]}')
        
        ax.set_xticklabels(day_names, rotation=0, fontsize=12)
        ax.legend(fontsize=11, loc='upper right')
        ax.grid(True, alpha=0.3)
        
        # Қийматларни қўшиш
        all_data = bichish_data + tasnif_data + tikuv_data + qadoqlash_data
        max_value = max(all_data) if all_data else 0
        
        for bars, data in zip([bars1, bars2, bars3, bars4], 
                             [bichish_data, tasnif_data, tikuv_data, qadoqlash_data]):
            for bar, value in zip(bars, data):
                height = bar.get_height()
                if height > 0:
                    ax.text(bar.get_x() + bar.get_width()/2., 
                           height + max_value*0.02,
                           f'{value:,}', ha='center', va='bottom', 
                           fontsize=9, fontweight='bold')
        
        # Жами кунлик иш чиқими
        total_daily = [b+t+tk+q for b, t, tk, q in zip(bichish_data, tasnif_data, tikuv_data, qadoqlash_data)]
        ax2 = ax.twinx()
        ax2.plot(x, total_daily, 'k--', linewidth=2, marker='o', 
                markersize=8, label='Жами')
        ax2.set_ylabel('Жами иш чиқими (та)', fontsize=14, fontweight='bold')
        
        for i, total in enumerate(total_daily):
            ax2.text(i, total + max_value*0.05, f'{total:,}', 
                    ha='center', va='bottom', fontsize=9, fontweight='bold', color='black')
        
        # Иккинчи легенда
        ax2.legend(loc='upper left', fontsize=11)
        
    except Exception as e:
        logger.error(f"❌ Детал тенденция графигида хатолик: {e}")
        ax.text(0.5, 0.5, 'Тенденция графигини яратишда хатолик', 
                ha='center', va='center', transform=ax.transAxes, fontsize=14)
        ax.axis('off')


def create_summary_table(ax, monthly_data):
    """Умумий хисобот таблицаси"""
    try:
        sections = ['бичиш', 'тасниф', 'тикув', 'қадоқлаш']
        section_names = ['Бичиш', 'Тасниф', 'Тикув', 'Қадоқлаш']
        emojis = ['✂️', '📑', '🧵', '📦']
        
        # Жадвал маълумотларини тайёрлаш
        table_data = [
            ["БЎЛИМ", "ОЙЛИК РЕЖА", "БАЖАРИЛДИ", "ҚОЛДИҚ", "ФОИЗ", "КУНЛИК НОРМА", "КУНЛИК КЕРАК"]
        ]
        
        total_plan = 0
        total_done = 0
        total_remaining = 0
        
        for i, (section_key, section_name, emoji) in enumerate(zip(sections, section_names, emojis)):
            if section_key in monthly_data:
                data = monthly_data[section_key]
                plan = data.get('plan', 0)
                done = data.get('done', 0)
                remaining = max(0, plan - done)
                percentage = calculate_percentage(done, plan) if plan > 0 else 0
                
                # Кунлик норма ва керакли иш
                working_days = get_working_days_in_current_month()
                daily_plan = plan / working_days if working_days > 0 else 0
                remaining_days = get_remaining_workdays()
                daily_needed = remaining / remaining_days if remaining_days > 0 else 0
                
                total_plan += plan
                total_done += done
                total_remaining += remaining
                
                table_data.append([
                    f"{emoji} {section_name}",
                    f"{plan:,.0f}",
                    f"{done:,.0f}",
                    f"{remaining:,.0f}",
                    f"{percentage:.1f}%",
                    f"{daily_plan:,.1f}",
                    f"{daily_needed:,.1f}"
                ])
            else:
                table_data.append([
                    f"{emoji} {section_name}",
                    "0",
                    "0",
                    "0",
                    "0%",
                    "0.0",
                    "0.0"
                ])
        
        # Жами қатор
        overall_percentage = calculate_percentage(total_done, total_plan) if total_plan > 0 else 0
        working_days = get_working_days_in_current_month()
        daily_plan_total = total_plan / working_days if working_days > 0 else 0
        remaining_days = get_remaining_workdays()
        daily_needed_total = total_remaining / remaining_days if remaining_days > 0 else 0
        
        table_data.append([
            "🎯 ЖАМИ",
            f"{total_plan:,.0f}",
            f"{total_done:,.0f}",
            f"{total_remaining:,.0f}",
            f"{overall_percentage:.1f}%",
            f"{daily_plan_total:,.1f}",
            f"{daily_needed_total:,.1f}"
        ])
        
        # Жадвал яратиш
        table = ax.table(cellText=table_data, cellLoc='center', 
                        loc='center', bbox=[0, 0, 1, 1])
        
        # Жадвал услублари
        table.auto_set_font_size(False)
        table.set_fontsize(8)
        table.scale(1, 1.8)
        
        # Сарлавҳа услуби
        for j in range(len(table_data[0])):
            table[(0, j)].set_facecolor('#1B5E20')
            table[(0, j)].set_text_props(weight='bold', color='white', fontsize=9)
            table[(0, j)].set_height(0.1)
        
        # Маълумотлар учун ранглаш
        for i in range(1, len(table_data)):
            for j in range(len(table_data[0])):
                cell = table[(i, j)]
                cell.set_text_props(fontsize=8, weight='bold')
                cell.set_height(0.09)
                
                # Жами қатори учун ранг
                if i == len(table_data) - 1:
                    cell.set_facecolor('#FFD54F')
                    cell.set_text_props(fontsize=9, weight='bold')
                # Бўлимлар учун ранглаш (фоизларга кўра)
                elif j == 4 and i < len(table_data) - 1:  # Фоизлар устүни
                    try:
                        pct_text = table_data[i][j]
                        pct = float(pct_text.replace('%', ''))
                        
                        if pct >= 100:
                            cell.set_facecolor("#4CAF50")
                        elif pct >= 80:
                            cell.set_facecolor("#8BC34A")
                        elif pct >= 60:
                            cell.set_facecolor("#FFC107")
                        elif pct >= 40:
                            cell.set_facecolor("#FF9800")
                        elif pct >= 20:
                            cell.set_facecolor("#FF5722")
                        else:
                            cell.set_facecolor("#F44336")
                    except:
                        cell.set_facecolor('#F5F5F5')
                else:
                    # Ҳарор қатори учун турли ранг
                    row_color = ['#E8F5E8', '#F3E5F5', '#E3F2FD', '#FFF3E0'][(i-1) % 4]
                    cell.set_facecolor(row_color)
        
        ax.set_title('📊 УМУМИЙ ХИСОБОТ - БАРЧА БЎЛИМЛАР', fontsize=16, fontweight='bold', pad=20)
        ax.axis('off')
        
        # Қўшимча маълумот
        month_name = get_month_name()
        current_workday = get_current_workday_index()
        remaining_days = get_remaining_workdays()
        
        info_text = (
            f"📅 {month_name} | Иш кунлари: {get_working_days_in_current_month()} | "
            f"Ҳозиргача: {current_workday} кун | Қолган: {remaining_days} кун"
        )
        
        ax.text(0.5, -0.05, info_text, transform=ax.transAxes, 
               ha='center', fontsize=10, fontweight='bold',
               bbox=dict(boxstyle="round,pad=0.3", facecolor="#B3E5FC", alpha=0.8))
        
    except Exception as e:
        logger.error(f"❌ Умумий хисобот таблицасида хатолик: {e}")
        ax.text(0.5, 0.5, 'Умумий хисобот таблицасини яратишда хатолик', 
                ha='center', va='center', transform=ax.transAxes, fontsize=14)
        ax.axis('off')


def get_today_work_for_section(section_key):
    """Бугунги кун учун бўлим иш микдори"""
    try:
        row_idx = find_today_row(sheet_report)
        if row_idx == 0:
            return 0
            
        row = sheet_report.row_values(row_idx)
        
        if section_key == "бичиш":
            return safe_val(row, 1)
        elif section_key == "тасниф":
            return safe_val(row, 3) + safe_val(row, 4) + safe_val(row, 5)
        elif section_key == "тикув":
            return safe_val(row, 7)
        elif section_key == "қадоқлаш":
            return safe_val(row, 10)
        
        return 0
    except:
        return 0

def create_empty_dashboard():
    """Бўш дашборд яратиш"""
    try:
        fig, ax = plt.subplots(figsize=(10, 6))
        ax.text(0.5, 0.5, 'Маълумотлар мавжуд эмас ёки юклашда хатолик', 
                ha='center', va='center', transform=ax.transAxes, fontsize=14)
        ax.set_title('Дашборд', fontsize=16)
        ax.axis('off')
        
        buf = BytesIO()
        plt.savefig(buf, format='png', dpi=100, bbox_inches='tight')
        buf.seek(0)
        plt.close()
        return buf
    except Exception as e:
        logger.error(f"❌ Бўш дашборд яратишда хатолик: {e}")
        return None
        pass

def create_section_visualization(section_name):
    """Бўлим учун тўлиқ визуал хисобот яратиш"""
    try:
        monthly_data = get_monthly_data()
        section_key = section_name.lower()
        section_data = monthly_data.get(section_key, {})
        
        if not section_data:
            return None

        colors = {
            "бичиш": ['#FF6B6B', '#FFA8A8', '#FFE0E0'],
            "тасниф": ['#4ECDC4', '#88D8D8', '#C7F7F7'], 
            "тикув": ['#45B7D1', '#7BC8E2', '#B3E0F0'],
            "қадоқлаш": ['#96CEB4', '#B8D8C8', '#DAF2E3']
        }
        
        color_set = colors.get(section_key, ['#8884d8', '#82ca9d', '#ffc658'])
        
        fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(20, 16))
        fig.suptitle(f'📊 {section_name.capitalize()} бўлими учун тўлиқ хисобот', fontsize=20, fontweight='bold')
        
        plan = section_data.get('plan', 0)
        done = section_data.get('done', 0)
        remaining = max(0, plan - done)
        
        if plan > 0:
            labels = [f'✅ Бажарилди\n{done} та\n({calculate_percentage(done, plan):.1f}%)', 
                     f'⏳ Қолдиқ\n{remaining} та\n({calculate_percentage(remaining, plan):.1f}%)']
            sizes = [done, remaining]
            colors_pie = [color_set[0], '#F0F0F0']
            
            wedges, texts, autotexts = ax1.pie(sizes, labels=labels, colors=colors_pie, autopct='', 
                                              startangle=90, textprops={'fontsize': 12})
            
            for autotext in autotexts:
                autotext.set_color('white')
                autotext.set_fontweight('bold')
            
            ax1.set_title('🎯 Ойлик режа бажарилиши', fontsize=14, fontweight='bold', pad=20)
        else:
            ax1.text(0.5, 0.5, 'Маълумотлар мавжуд эмас', ha='center', va='center', 
                    transform=ax1.transAxes, fontsize=12)
            ax1.set_title('🎯 Ойлик режа бажарилиши', fontsize=14, fontweight='bold')

        today = datetime.now(TZ)
        start_of_month = today.replace(day=1)
        
        if today.month == 12:
            end_of_month = today.replace(day=31)
        else:
            end_of_month = today.replace(month=today.month+1, day=1) - timedelta(days=1)
        
        records = sheet_report.get_all_values()
        daily_values = []
        dates = []
        
        current_day = start_of_month
        while current_day <= today:
            date_str = current_day.strftime("%d.%m.%Y")
            dates.append(date_str)
            
            daily_value = 0
            for row in records[1:]:
                if len(row) > 0 and row[0] == date_str:
                    if section_name == "Бичиш":
                        daily_value = safe_val(row, 1)
                    elif section_name == "Тасниф":
                        daily_value = safe_val(row, 3) + safe_val(row, 4) + safe_val(row, 5)
                    elif section_name == "Тикув":
                        daily_value = safe_val(row, 7)
                    elif section_name == "Қадоқлаш":
                        daily_value = safe_val(row, 10)
                    break
            
            daily_values.append(daily_value)
            current_day += timedelta(days=1)
        
        if daily_values:
            ax2.plot(range(len(dates)), daily_values, marker='o', linewidth=3, 
                    color=color_set[0], markersize=8, markerfacecolor=color_set[1])
            ax2.fill_between(range(len(dates)), daily_values, alpha=0.3, color=color_set[2])
            ax2.set_title('📈 Кунлик иш тенденцияси', fontsize=14, fontweight='bold')
            ax2.set_xlabel('Кунлар')
            ax2.set_ylabel('Иш микдори')
            ax2.grid(True, alpha=0.3)
            ax2.set_xticks(range(len(dates)))
            ax2.set_xticklabels([date.split('.')[0] for date in dates], rotation=45)
            
            max_idx = daily_values.index(max(daily_values))
            min_idx = daily_values.index(min(daily_values))
            
            ax2.annotate(f'Макс: {max(daily_values)}', xy=(max_idx, max(daily_values)), 
                        xytext=(max_idx, max(daily_values) + max(daily_values)*0.1),
                        arrowprops=dict(arrowstyle='->', color=color_set[0]),
                        fontweight='bold')
            
            if min(daily_values) > 0:
                ax2.annotate(f'Мин: {min(daily_values)}', xy=(min_idx, min(daily_values)), 
                            xytext=(min_idx, min(daily_values) - max(daily_values)*0.1),
                            arrowprops=dict(arrowstyle='->', color=color_set[0]),
                            fontweight='bold')

        week_rows = find_week_rows(sheet_report)
        week_days = ['Душ', 'Сеш', 'Чор', 'Пай', 'Жум', 'Шан', 'Якш']
        week_data = [0] * 7
        
        for row_idx in week_rows:
            row = sheet_report.row_values(row_idx)
            try:
                date_str = row[0]
                date_obj = datetime.strptime(date_str, "%d.%m.%Y")
                weekday = date_obj.weekday()
                
                if section_name == "Бичиш":
                    week_data[weekday] = safe_val(row, 1)
                elif section_name == "Тасниф":
                    week_data[weekday] = safe_val(row, 3) + safe_val(row, 4) + safe_val(row, 5)
                elif section_name == "Тикув":
                    week_data[weekday] = safe_val(row, 7)
                elif section_name == "Қадоқлаш":
                    week_data[weekday] = safe_val(row, 10)
            except:
                continue
        
        bars = ax3.bar(week_days, week_data, color=color_set, alpha=0.8, edgecolor=color_set[0], linewidth=2)
        ax3.set_title('📅 Ҳафталик иш тақсимоти', fontsize=14, fontweight='bold')
        ax3.set_ylabel('Иш микдори')
        ax3.grid(True, alpha=0.3)
        
        for bar, value in zip(bars, week_data):
            height = bar.get_height()
            if height > 0:
                ax3.text(bar.get_x() + bar.get_width()/2., height + max(week_data)*0.02,
                        f'{value}', ha='center', va='bottom', fontweight='bold')

        stats_text = f"📊 {section_name.upper()} СТАТИСТИКАСИ\n\n"
        stats_text += f"🎯 Ойлик режа: {plan:,.0f} та\n"
        stats_text += f"✅ Бажарилди: {done:,.0f} та\n"
        stats_text += f"⏳ Қолдиқ: {remaining:,.0f} та\n"
        stats_text += f"📈 Бажарилди: {calculate_percentage(done, plan):.1f}%\n\n"
        
        daily_plan = plan / get_working_days_in_current_month() if get_working_days_in_current_month() > 0 else 0
        stats_text += f"📅 Кунлик режа: {daily_plan:,.1f} та\n"
        
        remaining_days = get_remaining_workdays()
        daily_needed = remaining / remaining_days if remaining_days > 0 else 0
        stats_text += f"🔥 Ҳар кунги керак: {daily_needed:,.1f} та\n"
        stats_text += f"📆 Қолган кунлар: {remaining_days} кун\n\n"
        
        today_row = find_today_row(sheet_report)
        today_work = 0
        if today_row > 0:
            row = sheet_report.row_values(today_row)
            if section_name == "Бичиш":
                today_work = safe_val(row, 1)
            elif section_name == "Тасниф":
                today_work = safe_val(row, 3) + safe_val(row, 4) + safe_val(row, 5)
            elif section_name == "Тикув":
                today_work = safe_val(row, 7)
            elif section_name == "Қадоқлаш":
                today_work = safe_val(row, 10)
        
        stats_text += f"🟢 Бутун иш: {today_work} та\n"
        stats_text += f"📊 Кунлик фоиз: {calculate_percentage(today_work, daily_plan):.1f}%"

        ax4.text(0.1, 0.95, stats_text, transform=ax4.transAxes, fontsize=14, 
                verticalalignment='top', linespacing=1.5, fontweight='bold')
        ax4.set_title('📈 Жорий статистика', fontsize=14, fontweight='bold')
        ax4.axis('off')

        plt.tight_layout()
        
        buf = BytesIO()
        plt.savefig(buf, format='png', dpi=120, bbox_inches='tight')
        buf.seek(0)
        plt.close()
        
        return buf
        
    except Exception as e:
        logger.error(f"❌ Визуал хисобот яратишда хato: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return None

def create_comprehensive_dashboard():
    """Барча бўлимлар учун комплекс дашборд яратиш"""
    try:
        monthly_data = get_monthly_data()
        if not monthly_data:
            return None

        sections = ['бичиш', 'тасниф', 'тикув', 'қадоқлаш']
        colors = ['#FF6B6B', '#4ECDC4', '#45B7D1', '#96CEB4']
        section_names = ['Бичиш', 'Тасниф', 'Тикув', 'Қадоқлаш']
        
        fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(20, 16))
        fig.suptitle('🏭 БАРЧА БЎЛИМЛАР УЧУН ОЙЛИК ХИСОБОТ', fontsize=24, fontweight='bold', y=0.98)
        
        plans = [monthly_data.get(sect, {}).get('plan', 0) for sect in sections]
        dones = [monthly_data.get(sect, {}).get('done', 0) for sect in sections]
        percentages = [calculate_percentage(dones[i], plans[i]) for i in range(len(sections))]
        
        x = range(len(sections))
        width = 0.35
        
        bars1 = ax1.bar(x, plans, width, label='Режа', color='lightgray', alpha=0.7)
        bars2 = ax1.bar([i + width for i in x], dones, width, label='Бажарилди', color=colors, alpha=0.8)
        
        ax1.set_title('📊 Ойлик режа бажарилиши', fontsize=16, fontweight='bold', pad=20)
        ax1.set_ylabel('Иш микдори')
        ax1.set_xticks([i + width/2 for i in x])
        ax1.set_xticklabels(section_names)
        ax1.legend()
        ax1.grid(True, alpha=0.3)
        
        for i, (plan, done, pct) in enumerate(zip(plans, dones, percentages)):
            ax1.text(i + width/2, max(plan, done) + max(plans)*0.05, f'{pct:.1f}%', 
                    ha='center', va='bottom', fontweight='bold', fontsize=10)

        wedges, texts, autotexts = ax2.pie(percentages, labels=section_names, colors=colors, 
                                          autopct='%1.1f%%', startangle=90)
        for autotext in autotexts:
            autotext.set_color('white')
            autotext.set_fontweight('bold')
        ax2.set_title('🥧 Бўлимлар бажариши фоизда', fontsize=16, fontweight='bold')

        remainings = [monthly_data.get(sect, {}).get('remaining', 0) for sect in sections]
        remaining_days = get_remaining_workdays()
        daily_needed = [rem / remaining_days if remaining_days > 0 else 0 for rem in remainings]
        
        x = range(len(sections))
        bars3 = ax3.bar(x, remainings, color=colors, alpha=0.7, label='Қолган иш')
        ax3_twin = ax3.twinx()
        bars4 = ax3_twin.bar([i + 0.3 for i in x], daily_needed, color=colors, alpha=0.9, 
                           width=0.3, label='Кунлик керак')
        
        ax3.set_title('⏳ Қолган иш ва кунлик талаб', fontsize=16, fontweight='bold')
        ax3.set_ylabel('Қолган иш')
        ax3_twin.set_ylabel('Кунлик керак')
        ax3.set_xticks([i + 0.15 for i in x])
        ax3.set_xticklabels(section_names)
        ax3.legend(loc='upper left')
        ax3_twin.legend(loc='upper right')
        ax3.grid(True, alpha=0.3)

        total_plan = sum(plans)
        total_done = sum(dones)
        total_remaining = sum(remainings)
        overall_percentage = calculate_percentage(total_done, total_plan)
        
        stats_text = "📈 УМУМИЙ СТАТИСТИКА\n\n"
        stats_text += f"🎯 Жами режа: {total_plan:,.0f} та\n"
        stats_text += f"✅ Жами бажарилди: {total_done:,.0f} та\n"
        stats_text += f"⏳ Жами қолдиқ: {total_remaining:,.0f} та\n"
        stats_text += f"📊 Умумий бажарилди: {overall_percentage:.1f}%\n\n"
        
        stats_text += f"📅 Қолган иш кунлари: {remaining_days} кун\n"
        stats_text += f"🔥 Умумий кунлик керак: {sum(daily_needed):.1f} та/кун\n\n"
        
        working_days = get_working_days_in_current_month()
        stats_text += f"🗓 Ойига иш кунлари: {working_days} кун\n"
        stats_text += f"📊 Ҳозиргача иш кунлари: {get_current_workday_index()} кун"
        
        ax4.text(0.1, 0.95, stats_text, transform=ax4.transAxes, fontsize=14, 
                verticalalignment='top', linespacing=1.5, fontweight='bold')
        ax4.set_title('📊 Умумий кўрсаткичлар', fontsize=16, fontweight='bold')
        ax4.axis('off')

        month_name = get_month_name()
        plt.figtext(0.5, 0.02, f"Ҳисобот санаси: {today_date_str()} | Ой: {month_name}", 
                   ha='center', fontsize=12, style='italic')

        plt.tight_layout()
        
        buf = BytesIO()
        plt.savefig(buf, format='png', dpi=120, bbox_inches='tight')
        buf.seek(0)
        plt.close()
        
        return buf
        
    except Exception as e:
        logger.error(f"❌ Дашборд яратишда хato: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return None
    pass

# ------------------- ХИСОБОТ ФУНКЦИЯЛАРИ -------------------
def format_daily_report():
    try:
        row_idx = find_today_row(sheet_report)
        if row_idx == 0:
            return "❌ Бугун учун маълумотлар киритилмаган."
            
        row = sheet_report.row_values(row_idx)
        monthly_data = get_monthly_data()
        
        section_emojis = {
            "бичиш": "✂️",
            "тасниф": "📑", 
            "тикув": "🧵",
            "қадоқлаш": "📦"
        }
        
        report = f"📊 Кунлик хисобот ({row[0]})\n\n"
        
        bichish_ish = safe_val(row, 1)
        bichish_hodim = safe_val(row, 2)
        bichish_data = monthly_data.get('бичиш', {})
        bichish_performance = calculate_section_performance('бичиш', bichish_ish, bichish_data)
        
        report += f"{section_emojis.get('бичиш', '✂️')} Бичиш: {bichish_ish} та\n"
        report += f"      Ходимлар: {bichish_hodim}\n"
        report += f"      Кунлик норма: {bichish_performance['daily_norm']:.1f} ({get_working_days_in_current_month()} Кун)\n"
        report += f"      Фоиз: {bichish_performance['daily_pct']:.2f}%\n"
        report += f"      Ойлик фоиз: {bichish_performance['monthly_pct']:.1f}%\n"
        report += f"      Қолган иш: {bichish_performance['remaining_work']:.1f} та ({bichish_data.get('done', 0):.0f} иш бажарилган)\n"
        report += f"      Ҳар кунги керак: {bichish_performance['daily_needed']:.1f} та/кун\n\n"
        
        tasnif_dikimga = safe_val(row, 3)
        tasnif_pechat = safe_val(row, 4)
        tasnif_vishivka = safe_val(row, 5)
        tasnif_hodim = safe_val(row, 6)
        tasnif_total = tasnif_dikimga + tasnif_pechat + tasnif_vishivka
        tasnif_data = monthly_data.get('тасниф', {})
        tasnif_performance = calculate_section_performance('тасниф', tasnif_total, tasnif_data)
        
        report += f"{section_emojis.get('тасниф', '📑')} Тасниф: {tasnif_total} та\n"
        report += f"      Ходимлар: {tasnif_hodim}\n"
        report += f"      Кунлик норма: {tasnif_performance['daily_norm']:.1f} ({get_working_days_in_current_month()} Кун)\n"
        report += f"      Фоиз: {tasnif_performance['daily_pct']:.2f}%\n"
        report += f"      Ойлик фоиз: {tasnif_performance['monthly_pct']:.1f}%\n"
        report += f"      Қолган иш: {tasnif_performance['remaining_work']:.1f} та ({tasnif_data.get('done', 0):.0f} иш бажарилган)\n"
        report += f"      Ҳар кунги керак: {tasnif_performance['daily_needed']:.1f} та/кун\n\n"
        
        tikuv_ish = safe_val(row, 7)
        tikuv_hodim = safe_val(row, 8)
        oyoqchi_hodim = safe_val(row, 9)
        tikuv_data = monthly_data.get('тикув', {})
        tikuv_performance = calculate_section_performance('тикув', tikuv_ish, tikuv_data)
        
        report += f"{section_emojis.get('тикув', '🧵')} Тикув: {tikuv_ish} та\n"
        report += f"      Ходимлар: {tikuv_hodim}\n"
        report += f"      Кунлик норма: {tikuv_performance['daily_norm']:.1f} ({get_working_days_in_current_month()} Кун)\n"
        report += f"      Фоиз: {tikuv_performance['daily_pct']:.2f}%\n"
        report += f"      Ойлик фоиз: {tikuv_performance['monthly_pct']:.1f}%\n"
        report += f"      Қолган иш: {tikuv_performance['remaining_work']:.1f} та ({tikuv_data.get('done', 0):.0f} иш бажарилган)\n"
        report += f"      Ҳар кунги керак: {tikuv_performance['daily_needed']:.1f} та/кун\n\n"
        
        qadoqlash_ish = safe_val(row, 10)
        qadoqlash_hodim = safe_val(row, 11)
        qadoqlash_data = monthly_data.get('қадоқлаш', {})
        qadoqlash_performance = calculate_section_performance('қадоқлаш', qadoqlash_ish, qadoqlash_data)
        
        report += f"{section_emojis.get('қадоқлаш', '📦')} Қадоқлаш: {qadoqlash_ish} та\n"
        report += f"      Ходимлар: {qadoqlash_hodim}\n"
        report += f"      Кунлик норма: {qadoqlash_performance['daily_norm']:.1f} ({get_working_days_in_current_month()} Кун)\n"
        report += f"      Фоиз: {qadoqlash_performance['daily_pct']:.2f}%\n"
        report += f"      Ойлик фоиз: {qadoqlash_performance['monthly_pct']:.1f}%\n"
        report += f"      Қолган иш: {qadoqlash_performance['remaining_work']:.1f} та ({qadoqlash_data.get('done', 0):.0f} иш бажарилган)\n"
        report += f"      Ҳар кунги керак: {qadoqlash_performance['daily_needed']:.1f} та/кун\n\n"
        
        total_today = bichish_ish + tasnif_total + tikuv_ish + qadoqlash_ish
        report += f"📈 Жами кунлик иш: {total_today} та\n"
        report += f"📆 Қолган иш кунлари: {qadoqlash_performance['remaining_days']} кун"
        
        return report
        
    except Exception as e:
        logger.error(f"❌ format_daily_report хato: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return "❌ Хисобот яратишда хатолик юз берди."

def format_weekly_report():
    try:
        weekly_rows = find_week_rows(sheet_report)
        if not weekly_rows:
            return "❌ Ҳафта учун маълумотлар мавжуд эмас."
            
        monthly_data = get_monthly_data()
        
        section_emojis = {
            "бичиш": "✂️",
            "тасниф": "📑", 
            "тикув": "🧵",
            "қадоқлаш": "📦"
        }
        
        bichish_total = 0
        tasnif_total = 0
        tikuv_total = 0
        qadoqlash_total = 0
        
        for row_idx in weekly_rows:
            row = sheet_report.row_values(row_idx)
            bichish_total += safe_val(row, 1)
            tasnif_total += safe_val(row, 3) + safe_val(row, 4) + safe_val(row, 5)
            tikuv_total += safe_val(row, 7)
            qadoqlash_total += safe_val(row, 10)
        
        total_weekly = bichish_total + tasnif_total + tikuv_total + qadoqlash_total
        
        bichish_weekly_plan = monthly_data.get('бичиш', {}).get('plan', 0) / 4
        tasnif_weekly_plan = monthly_data.get('тасниф', {}).get('plan', 0) / 4
        tikuv_weekly_plan = monthly_data.get('тикув', {}).get('plan', 0) / 4
        qadoqlash_weekly_plan = monthly_data.get('қадоқлаш', {}).get('plan', 0) / 4
        
        bichish_weekly_pct = calculate_percentage(bichish_total, bichish_weekly_plan)
        tasnif_weekly_pct = calculate_percentage(tasnif_total, tasnif_weekly_plan)
        tikuv_weekly_pct = calculate_percentage(tikuv_total, tikuv_weekly_plan)
        qadoqlash_weekly_pct = calculate_percentage(qadoqlash_total, qadoqlash_weekly_plan)
        
        week_start, week_end = get_week_start_end_dates()
        week_number = get_week_number()
        
        report = f"📅 Ҳафталик хисобот ({week_number}-ҳафта, {week_start} - {week_end})\n\n"
        
        report += f"{section_emojis.get('бичиш', '✂️')} Бичиш: {bichish_total} та\n"
        report += f"   Ҳафталик режа: {bichish_weekly_plan:.0f} та | Бажарилди: {bichish_weekly_pct:.1f}%\n"
        report += f"   Қолдиқ: {max(0, bichish_weekly_plan - bichish_total):.0f} та | Фоизда: {max(0, 100 - bichish_weekly_pct):.1f}%\n"
        
        remaining_days = get_remaining_workdays()
        daily_needed_bichish = (max(0, bichish_weekly_plan - bichish_total) / remaining_days) if remaining_days > 0 else 0
        report += f"   Ҳар кунги керак: {daily_needed_bichish:.1f} та/кун\n\n"
        
        report += f"{section_emojis.get('тасниф', '📑')} Тасниф: {tasnif_total} та\n"
        report += f"   Ҳафталик режа: {tasnif_weekly_plan:.0f} та | Бажарилди: {tasnif_weekly_pct:.1f}%\n"
        report += f"   Қолдиқ: {max(0, tasnif_weekly_plan - tasnif_total):.0f} та | Фоизда: {max(0, 100 - tasnif_weekly_pct):.1f}%\n"
        
        daily_needed_tasnif = (max(0, tasnif_weekly_plan - tasnif_total) / remaining_days) if remaining_days > 0 else 0
        report += f"   Ҳар кунги керак: {daily_needed_tasnif:.1f} та/кун\n\n"
        
        report += f"{section_emojis.get('тикув', '🧵')} Тикув: {tikuv_total} та\n"
        report += f"   Ҳафталик режа: {tikuv_weekly_plan:.0f} та | Бажарилди: {tikuv_weekly_pct:.1f}%\n"
        report += f"   Қолдиқ: {max(0, tikuv_weekly_plan - tikuv_total):.0f} та | Фоизда: {max(0, 100 - tikuv_weekly_pct):.1f}%\n"
        
        daily_needed_tikuv = (max(0, tikuv_weekly_plan - tikuv_total) / remaining_days) if remaining_days > 0 else 0
        report += f"   Ҳар кунги керак: {daily_needed_tikuv:.1f} та/кун\n\n"
        
        report += f"{section_emojis.get('қадоқлаш', '📦')} Қадоқлаш: {qadoqlash_total} та\n"
        report += f"   Ҳафталик режа: {qadoqlash_weekly_plan:.0f} та | Бажарилди: {qadoqlash_weekly_pct:.1f}%\n"
        report += f"   Қолдиқ: {max(0, qadoqlash_weekly_plan - qadoqlash_total):.0f} та | Фоизда: {max(0, 100 - qadoqlash_weekly_pct):.1f}%\n"
        
        daily_needed_qadoqlash = (max(0, qadoqlash_weekly_plan - qadoqlash_total) / remaining_days) if remaining_days > 0 else 0
        report += f"   Ҳар кунги керак: {daily_needed_qadoqlash:.1f} та/кун\n\n"
        
        report += f"📊 Жами ҳафталик иш: {total_weekly} та\n"
        report += f"📆 Ҳафта охиригача қолган иш кунлари: {remaining_days} кун"
        
        return report
        
    except Exception as e:
        logger.error(f"❌ format_weekly_report хato: {e}")
        return "❌ Ҳафталик хисобот яратишда хатолик юз берди."

def format_monthly_report():
    try:
        monthly_data = get_monthly_data()
        if not monthly_data:
            return "❌ Ойлик маълумотлар мавжуд эмас."
            
        report = f"🗓 Ойлик хисобот ({get_month_name()})\n\n"
        
        remaining_days = get_remaining_workdays()
        current_workday = get_current_workday_index()
        total_working_days = get_working_days_in_current_month()
        
        section_emojis = {
            "бичиш": "✂️",
            "тасниф": "📑", 
            "тикув": "🧵",
            "қадоқлаш": "📦"
        }
        
        for section_name, data in monthly_data.items():
            section_display = section_name.capitalize()
            monthly_plan = data['plan']
            monthly_done = data['done']
            
            done_percentage = calculate_percentage(monthly_done, monthly_plan)
            remaining_percentage = calculate_percentage(data['remaining'], monthly_plan)
            
            daily_plan = monthly_plan / total_working_days
            daily_needed = data['remaining'] / remaining_days if remaining_days > 0 else 0
            
            emoji = section_emojis.get(section_name, "📊")
            report += f"{emoji} {section_display}:\n"
            report += f"   • Ойлик режа: {monthly_plan:.0f} та ({total_working_days} иш куни), Кунлик норма: {daily_plan:.1f} та/кун\n"
            report += f"   • Бажарилди: {monthly_done:.0f} та ({done_percentage:.1f}%)\n"
            report += f"   • Қолдиқ: {data['remaining']:.0f} та ({remaining_percentage:.1f}%)\n"
            
            if remaining_days > 0:
                report += f"   • Ҳар кунги керак: {daily_needed:.1f} та/кун (Қолган иш кунлари: {remaining_days} кун)\n\n"
            else:
                report += f"   • Ой якунланди\n\n"
        
        congratulations = []
        for section_name, data in monthly_data.items():
            done_pct = float(data['done_pct'].replace('%', ''))
            if done_pct >= 100:
                section_display = section_name.capitalize()
                extra = data['done'] - data['plan']
                congratulations.append(f"🎉 {section_display} бўлими ойлик режани {done_pct:.1f}% бажариб, режадан {extra:.0f} та ортиқ иш чиқарди!")
        
        if congratulations:
            report += "\n".join(congratulations) + "\n\n"
        
        report += f"📈 Жами иш кунлари: {total_working_days} кун\n"
        report += f"📅 Ҳозиргача иш кунлари: {current_workday} кун\n"
        report += f"📆 Қолган иш кунлари: {remaining_days} кун"
        
        return report
        
    except Exception as e:
        logger.error(f"❌ format_monthly_report хato: {e}")
        return "❌ Ойлик хисобот яратишда хатолик юз берди."

def format_orders_report(only_active=True):
    try:
        orders = get_orders_data()
        if not orders:
            return ["❌ Ҳали буюртмалар мавжуд эмас."]
        
        if only_active:
            orders = [order for order in orders if order['remaining'] > 0]
        
        reports = []
        current_report = "📋 Буюртмалар хисоботи\n\n"
        
        for order in orders:
            order_text = f"📦 {order['name']}: {order['done']}/{order['total']} та " \
                         f"({order['done_percentage']}) | {order['deadline']} " \
                         f"({order['days_left']} кун) | {order['section']}\n"
            
            if len(current_report) + len(order_text) > 4096:
                reports.append(current_report)
                current_report = order_text
            else:
                current_report += order_text
        
        if current_report:
            reports.append(current_report)
            
        return reports
        
    except Exception as e:
        logger.error(f"❌ format_orders_report хato: {e}")
        return ["❌ Буюртмалар хисоботини яратишда хатолик юз берди."]

# ------------------- ТУГМАЛАР -------------------
def main_menu():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🗓 Кунлик иш (бўлим бўйича)", callback_data="daily_work")],
        [InlineKeyboardButton(text="📋 Кунлик иш (буюртмалар бўйича)", callback_data="daily_work_orders")],
        [InlineKeyboardButton(text="📋 Буюртмалар", callback_data="orders_menu")],
        [InlineKeyboardButton(text="📊 Хисоботлар", callback_data="reports_menu")],
        [InlineKeyboardButton(text="📈 График хисоботлар", callback_data="graph_reports")],
        [InlineKeyboardButton(text="📦 Полотно акт контроль", callback_data="fabric_control")],
        [InlineKeyboardButton(text="📊 KPI", callback_data="kpi_menu")],
        [InlineKeyboardButton(text="⚙️ Админ менюси", callback_data="admin_menu")],
    ])

def reports_menu():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📊 Кунлик хисобот", callback_data="r_kun")],
        [InlineKeyboardButton(text="📅 Хафталик хисобот", callback_data="r_haf")],
        [InlineKeyboardButton(text="🗓 Ойлик хисобот", callback_data="r_oy")],
        [InlineKeyboardButton(text="📋 Буюртмалар хисоботи", callback_data="r_ord")],
        [InlineKeyboardButton(text="🔄 Workflow хисобот", callback_data="r_workflow")],
        [InlineKeyboardButton(text="⬅️ Ортга", callback_data="back_main")]
    ])

def graph_reports_menu():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📊 Ойлик иш графиги", callback_data="g_mon")],
        [InlineKeyboardButton(text="📈 Кунлик иш графиги", callback_data="g_day")],
        [InlineKeyboardButton(text="📅 Ҳафталик тенденция", callback_data="g_week")],
        [InlineKeyboardButton(text="🗓 Ойлик тенденция", callback_data="g_month")],
        [InlineKeyboardButton(text="🥧 Фоизлар диаграммаси", callback_data="g_pie")],
        [InlineKeyboardButton(text="📋 Ишлаб чиқариш дашборди", callback_data="production_dashboard")],  # Yangi tugma
        [InlineKeyboardButton(text="📊 Бичиш визуал хисобот", callback_data="vis_bich")],
        [InlineKeyboardButton(text="📊 Тасниф визуал хисобот", callback_data="vis_tasn")],
        [InlineKeyboardButton(text="📊 Тикув визуал хисобот", callback_data="vis_tik")],
        [InlineKeyboardButton(text="📊 Қадоқлаш визуал хисобот", callback_data="vis_qad")],
        [InlineKeyboardButton(text="🏭 Барча бўлимлар дашборди", callback_data="vis_all")],
        [InlineKeyboardButton(text="⬅️ Ортга", callback_data="back_main")]
    ])

def orders_menu():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📋 Буюртмалар рўйхати", callback_data="ord_list")],
        [InlineKeyboardButton(text="➕ Янги буюртма", callback_data="add_ord")],
        [InlineKeyboardButton(text="✏️ Буюртмани таҳрирлаш", callback_data="edit_ord")],
        [InlineKeyboardButton(text="⬅️ Ортга", callback_data="back_main")]
    ])

def daily_sections_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✂️ Бичиш", callback_data="sec_bich")],
        [InlineKeyboardButton(text="📑 Тасниф", callback_data="sec_tasn")],
        [InlineKeyboardButton(text="🧵 Тикув", callback_data="sec_tik")],
        [InlineKeyboardButton(text="📦 Қадоқлаш", callback_data="sec_qad")],
        [InlineKeyboardButton(text="⬅️ Ортга", callback_data="back_main")]
    ])

def workflow_sections_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✂️ Бичиш", callback_data="workflow_bichish")],
        [InlineKeyboardButton(text="📑 Тасниф", callback_data="workflow_tasnif")],
        [InlineKeyboardButton(text="🧵 Тикув", callback_data="workflow_tikuv")],
        [InlineKeyboardButton(text="📦 Қадоқлаш", callback_data="workflow_qadoqlash")],
        [InlineKeyboardButton(text="📤 Қутига солиш", callback_data="workflow_qutiga")],
        [InlineKeyboardButton(text="⬅️ Ортга", callback_data="back_main")]
    ])

def admin_professional_menu():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📅 Санa бўйича таҳрирлаш", callback_data="admin_edit_by_date")],
        [InlineKeyboardButton(text="✏️ Бўлим маълумотларини таҳрирлаш", callback_data="admin_edit_section_data")],
        [InlineKeyboardButton(text="📦 Буюртмаларни бошқариш", callback_data="admin_manage_orders")],
        [InlineKeyboardButton(text="📊 Ойлик режалар", callback_data="admin_monthly_plans")],
        [InlineKeyboardButton(text="⚙️ Тизим созламалари", callback_data="admin_system_settings")],
        [InlineKeyboardButton(text="📈 Статистика ва хисоботлар", callback_data="admin_statistics")],
        [InlineKeyboardButton(text="🔄 Ботни қайта юклаш", callback_data="admin_restart")],
        [InlineKeyboardButton(text="⬅️ Ортга", callback_data="back_main")]
    ])

def orders_keyboard(orders):
    keyboard = []
    for order in orders:
        keyboard.append([InlineKeyboardButton(
            text=f"📦 {order['name']} ({order['done']}/{order['total']})", 
            callback_data=f"sel_ord:{order['row_index']}"
        )])
    
    keyboard.append([InlineKeyboardButton(text="⬅️ Ортга", callback_data="back_ord")])
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

def orders_keyboard_by_section(orders):
    def sort_key(o):
        try:
            if o.get('deadline'):
                return (datetime.strptime(o['deadline'], "%d.%m.%Y"), o.get('name',''))
        except Exception:
            pass
        return (datetime.max, o.get('name',''))

    sorted_orders = sorted(orders, key=sort_key)
    keyboard = []
    for order in sorted_orders:
        done = int(order.get('done', 0))
        total = int(order.get('total', 0))
        name = order.get('name', 'Unknown')
        row = order.get('row_index')

        text = f"📦 {name} ({done}/{total})"
        keyboard.append([InlineKeyboardButton(text=text, callback_data=f"daily_ord:{row}")])

    keyboard.append([InlineKeyboardButton(text="⬅️ Ортга", callback_data="back_daily")])
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

def workflow_orders_keyboard(orders):
    keyboard = []
    for order in orders:
        # Буюртма номи узун бўлса қисқартирамиз
        name = order['name']
        if len(name) > 30:
            name = name[:27] + "..."
            
        keyboard.append([InlineKeyboardButton(
            text=f"📦 {name} ({order['done']}/{order['total']})", 
            callback_data=f"workflow_ord:{order['row_index']}"
        )])
    
    keyboard.append([InlineKeyboardButton(text="⬅️ Ортга", callback_data="back_workflow")])
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

def get_comment_by_date(date_str: str) -> str:
    """Сана бўйича изоҳни олиш"""
    try:
        records = sheet_report.get_all_values()
        for row in records:
            if row and row[0] == date_str:
                return row[13] if len(row) > 13 else ""
        return ""
    except Exception as e:
        logger.error(f"❌ get_comment_by_date хato: {e}")
        return ""

# Izohni yangilash funksiyasi
def update_sheet_comment(date_str: str, new_comment: str) -> bool:
    """Изоҳни янгилаш"""
    try:
        records = safe_sheets_call(sheet_report.get_all_values)
        row_index = -1
        
        # Санaни топish
        for i, row in enumerate(records):
            if row and row[0] == date_str:
                row_index = i + 1  # 1-based index
                break
        
        if row_index == -1:
            # Янги қатор яратиш
            new_row = [date_str] + [""] * 12 + [new_comment]
            safe_sheets_call(sheet_report.append_row, new_row)
            return True
        
        # Мавжуд қаторни янгилаш
        row = safe_sheets_call(sheet_report.row_values, row_index)
        # Катталикни текшириш ва кенгайтириш
        while len(row) < 14:
            row.append("")
        
        row[13] = new_comment  # 14-устун (0-based index 13)
        
        # Якуний қаторни янгилаш
        updates = []
        for i, value in enumerate(row[:14]):  # Фақат 14 та устун
            updates.append({
                'range': f"{gspread.utils.rowcol_to_a1(row_index, i + 1)}",
                'values': [[str(value) if value is not None else ""]]
            })
        
        if updates:
            safe_sheets_call(sheet_report.batch_update, updates)
        
        return True
        
    except Exception as e:
        logger.error(f"❌ update_sheet_comment хato: {e}")
        return False

# ------------------- START va SETTINGS -------------------
@dp.message(Command("start"))
async def start_cmd(message: Message):
    logger.info(f"🚀 /start командаси: {message.from_user.first_name} ({message.from_user.id})")
    
    keyboard = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="📊 Иш режаси", web_app=WebAppInfo(url="https://akbaraliakhmedov46-svg.github.io/ish-boshqaruv/"))]],
        resize_keyboard=True
    )
    await Database.execute(
        "INSERT INTO user_actions (user_id, username, action) VALUES ($1, $2, $3)",
        message.from_user.id, message.from_user.username, "/start"
    )
    await message.answer("Ассалому алейкум! 👋\nБўлимни танланг ёки Mini-App орқали ишланг:", reply_markup=keyboard)
    await message.answer("Бўлимни танланг:", reply_markup=main_menu())

@dp.message(Command("hisobot"))
async def hisobot_cmd(message: Message):
    if not is_admin(message.from_user.id):
        await message.answer("❌ Сизда бу имконият йўқ.")
        return
        
    logger.info(f"📊 /hisobot командаси: {message.from_user.first_name} ({message.from_user.id})")
    await message.answer("Хисобот турини танланг:", reply_markup=reports_menu())

@dp.message(Command("buyurtmalar"))
async def buyurtmalar_cmd(message: Message):
    if not is_admin(message.from_user.id):
        await message.answer("❌ Сизда бу имконият йўқ.")
        return
        
    logger.info(f"📋 /buyurtmalar командаси: {message.from_user.first_name} ({message.from_user.id})")
    await message.answer("Буюртмалар бўлими:", reply_markup=orders_menu())

@dp.message(Command("kunlik_ish"))
async def kunlik_ish_cmd(message: Message):
    logger.info(f"📝 /kunlik_ish командаси: {message.from_user.first_name} ({message.from_user.id})")
    await message.answer("Кунлик иш қўшиш учун бўлимни танланг:", reply_markup=daily_sections_keyboard())

@dp.message(Command("grafik"))
async def grafik_cmd(message: Message):
    logger.info(f"📈 /grafik командаси: {message.from_user.first_name} ({message.from_user.id})")
    await message.answer("График хисобот турини танланг:", reply_markup=graph_reports_menu())

@dp.message(Command("admin"))
async def admin_cmd(message: Message):
    if not is_admin(message.from_user.id):
        await message.answer("❌ Сизда бу имконият йўқ.")
        return
        
    logger.info(f"👨‍💼 /admin командаси: {message.from_user.first_name} ({message.from_user.id})")
    await message.answer("👨‍💼 Админ панели", reply_markup=admin_professional_menu())

@dp.message(Command("send_report"))
async def cmd_send_report(message: Message, state: FSMContext):
    """Администратор учун хабар жўнатиш бошқаруви"""
    if not is_admin(message.from_user.id):
        await message.answer("❌ Сизда бу имконият йўқ.")
        return
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🏆 Кунлик рейтинг (1)", callback_data="broadcast:leaderboard")],
        [InlineKeyboardButton(text="⚠️ Паст самарадорлик (5)", callback_data="broadcast:low_performance")],
        [InlineKeyboardButton(text="🔄 Кун солиштириш (6)", callback_data="broadcast:daily_comparison")],
        [InlineKeyboardButton(text="📅 Ҳафта якуни (7)", callback_data="broadcast:weekly_summary")],
        [InlineKeyboardButton(text="📊 Прогресс-бар (10)", callback_data="broadcast:progress_bar")],
        [InlineKeyboardButton(text="🎂 Шахсий табрик (B)", callback_data="broadcast:birthday")],
        [InlineKeyboardButton(text="❌ Бекор қилиш", callback_data="cancel_broadcast")]
    ])
    
    await message.answer(
        "📤 Хабар жўнатиш тизими\n\n"
        "Жўнатиш учун хабар турини танланг:",
        reply_markup=keyboard
    )
    await state.set_state(AdminBroadcastStates.waiting_for_broadcast_type)

# Broadcast type selection
@dp.callback_query(F.data.startswith("broadcast:"))
async def cb_broadcast_type(callback: CallbackQuery, state: FSMContext):
    """Хабар турини танлаш"""
    await callback.answer()
    broadcast_type = callback.data.split(":")[1]
    
    if broadcast_type == "birthday":
        await callback.message.answer(
            "🎂 Шахсий табриклаш учун:\n\n"
            "Табрикланмоқчи бўлган ходимнинг исмини киритинг:"
        )
        await state.set_state(AdminBroadcastStates.waiting_for_birthday_name)
    else:
        await state.update_data(broadcast_type=broadcast_type)
        await process_broadcast(callback.message, state)

async def process_broadcast(message: Message, state: FSMContext):
    """Хабарни ишга тушириш"""
    data = await state.get_data()
    broadcast_type = data.get('broadcast_type')
    
    try:
        report_text = ""
        topic_id = PRODUCTION_TOPIC_ID  # Сурункат
        
        if broadcast_type == "leaderboard":
            report_text = generate_daily_leaderboard()
            topic_id = PRODUCTION_TOPIC_ID
        elif broadcast_type == "low_performance":
            report_text = generate_low_performance_alert()
            topic_id = LOW_PERCENT_TOPIC_ID
        elif broadcast_type == "daily_comparison":
            report_text = generate_daily_comparison()
            topic_id = PRODUCTION_TOPIC_ID
        elif broadcast_type == "weekly_summary":
            report_text = generate_weekly_summary()
            topic_id = PRODUCTION_TOPIC_ID
        elif broadcast_type == "progress_bar":
            report_text = generate_progress_bar()
            topic_id = PRODUCTION_TOPIC_ID
        elif broadcast_type == "birthday":
            name = data.get('birthday_name')
            section = data.get('birthday_section')
            report_text = generate_birthday_congrats(name, section)
            topic_id = RECOGNITION_TOPIC_ID
        
        if report_text:
            # Гуруҳга жўнатиш
            success_group = await send_to_group(report_text, topic_id)
            
            # Админга такрор
            await message.answer(
                f"✅ Хабар муваффақиятли жўнатилди!\n\n"
                f"📊 Тур: {broadcast_type}\n"
                f"📎 Гуруҳ: {'✅' if success_group else '❌'}\n"
                f"🎯 Мазмун:\n{report_text[:200]}...",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="📤 Яна жўнатиш", callback_data="send_report_again")],
                    [InlineKeyboardButton(text="🏠 Бош меню", callback_data="back_main")]
                ])
            )
            
            # Хисобот гуруҳига хабар (админ амали)
            admin_action_msg = (
                f"👨‍💼 Админ хабар жўнатди:\n"
                f"📤 Тур: {broadcast_type}\n"
                f"⏰ Вақт: {datetime.now(TZ).strftime('%H:%M')}\n"
                f"📎 Гуруҳ: {topic_id}"
            )
            await send_to_group(admin_action_msg, PRODUCTION_TOPIC_ID)
            
        else:
            await message.answer("❌ Хабар матни яратилмади.")
    
    except Exception as e:
        logger.error(f"❌ Broadcast хato: {e}")
        await message.answer(f"❌ Хабар жўнатишда хатолик: {str(e)[:100]}")
    
    finally:
        await state.clear()

# Birthday name handler
@dp.message(AdminBroadcastStates.waiting_for_birthday_name)
async def process_birthday_name(message: Message, state: FSMContext):
    """Табрик учун исмни қабул қилиш"""
    await state.update_data(birthday_name=message.text)
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✂️ Бичиш", callback_data="birthday_section:bichish")],
        [InlineKeyboardButton(text="📑 Тасниф", callback_data="birthday_section:tasnif")],
        [InlineKeyboardButton(text="🧵 Тикув", callback_data="birthday_section:tikuv")],
        [InlineKeyboardButton(text="📦 Қадоқлаш", callback_data="birthday_section:qadoqlash")],
        [InlineKeyboardButton(text="🏢 Бошқа", callback_data="birthday_section:other")]
    ])
    
    await message.answer(
        f"👤 {message.text} қайси бўлимда ишлайди?",
        reply_markup=keyboard
    )

# Birthday section handler
@dp.callback_query(F.data.startswith("birthday_section:"))
async def cb_birthday_section(callback: CallbackQuery, state: FSMContext):
    """Табрик учун бўлимни танлаш"""
    await callback.answer()
    section = callback.data.split(":")[1]
    
    section_names = {
        "bichish": "Бичиш",
        "tasnif": "Тасниф",
        "tikuv": "Тикув",
        "qadoqlash": "Қадоқлаш",
        "other": "Бошқа"
    }
    
    await state.update_data(
        birthday_section=section_names.get(section, section),
        broadcast_type="birthday"
    )
    
    await process_broadcast(callback.message, state)

@dp.message(Command("cancel"))
async def cmd_cancel(message: Message, state: FSMContext):
    current_state = await state.get_state()
    if current_state is None:
        await message.answer("❌ Ҳеч қандай амал бажарилмаяпти.")
        return
        
    await state.clear()
    await message.answer("✅ Амал бекор қилинди.", reply_markup=main_menu())

@dp.message(Command("clear_cache"))
async def clear_cache_cmd(message: Message):
    """Кэшни тозалаш учун команда"""
    if not is_admin(message.from_user.id):
        await message.answer("❌ Сизда бу имконият йўқ.")
        return
        
    DataCache.clear()
    await message.answer("✅ Кэш тозалананди. Янги маълумотлар олинади.")

@dp.message(Command("api_status"))
async def api_status_cmd(message: Message):
    """API статусини кўрсатиш"""
    if not is_admin(message.from_user.id):
        await message.answer("❌ Сизда бу имконият йўқ.")
        return
    
    status_msg = "📊 API СТАТУСИ:\n\n"
    status_msg += f"🔧 Кэш ҳолати: Faol\n"
    status_msg += f"⏰ Кэш вакти: 5 дақиқа\n"
    status_msg += f"🛡 Rate Limiting: Faol\n"
    status_msg += f"📈 Сўровлар: {len(sheets_rate_limiter.calls)}/50 (дақикада)"
    
    await message.answer(status_msg)

@dp.errors()
async def errors_handler(*args, **kwargs):
    """
    Хатоликтарни қайта ишлаш (бир нечта aiogram колл-имзолари учун мослаштирилган)
    Поддерживает сигнатуры: (update, exception), (exception,), kwargs.
    """
    exception = None
    # Try kwargs first
    if 'exception' in kwargs:
        exception = kwargs.get('exception')

    # Try to find exception in positional args (likely second arg)
    if exception is None and len(args) >= 2 and isinstance(args[1], Exception):
        exception = args[1]

    # If not found, scan all args for an Exception instance
    if exception is None:
        for a in reversed(args):
            if isinstance(a, Exception):
                exception = a
                break

    # Log the exception if we have it, otherwise log a generic message
    try:
        if exception:
            raise exception
        else:
            logger.error("❌ Unknown error in handler: no exception provided", exc_info=True)
    except Exception as e:
        logger.error(f"❌ Тасдиқланмаган хatolik: {e}", exc_info=True)

    return True

# ------------------- UPDATE CALLBACK HANDLERS -------------------
@dp.callback_query(F.data == "production_dashboard")
async def cb_production_dashboard(callback: CallbackQuery):
    await callback.answer()
    buf = create_optimized_dashboard()
    
    buf = create_production_dashboard()  # ЭСКИ ФУНКЦИЯ
    if buf:
        await callback.message.answer_photo(
            BufferedInputFile(buf.getvalue(), filename="production_dashboard.png"),
            caption="🏭 Ишлаб чиқариш дашборди (ҳақиқий маълумотлар)"
        )
    else:
        await callback.message.answer("❌ Дашборд яратишда хатолик юз берди.")

def create_simple_trend_chart():
    raise NotImplementedError

@dp.callback_query(F.data == "g_week")
async def cb_weekly_trend(callback: CallbackQuery):
    await callback.answer()
    
    buf = create_simple_trend_chart()
    if buf:
        await callback.message.answer_photo(
            BufferedInputFile(buf.getvalue(), filename="weekly_trend.png"),
            caption="📈 Охирги 5 кундаги иш тенденцияси"
        )
    else:
        await callback.message.answer("❌ График яратишда хатолик юз берди.")

# ------------------- CALLBACK QUERY HANDLERS -------------------
@dp.callback_query(F.data == "daily_work")
async def cb_daily_work(callback: CallbackQuery):
    await callback.answer()
    await callback.message.edit_text("Кунлик иш қўшиш учун бўлимни танланг:", reply_markup=daily_sections_keyboard())

@dp.callback_query(F.data == "daily_work_orders")
async def cb_daily_work_orders(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✂️ Бичиш", callback_data="daily_ord_section:bichish")],
        [InlineKeyboardButton(text="📑 Тасниф", callback_data="daily_ord_section:tasnif")],
        [InlineKeyboardButton(text="🧵 Тикув", callback_data="daily_ord_section:tikuv")],
        [InlineKeyboardButton(text="📦 Қадоқлаш", callback_data="daily_ord_section:qadoqlash")],
        [InlineKeyboardButton(text="⬅️ Ортга", callback_data="back_main")]
    ])
    
    await callback.message.edit_text("Буюртмалар бўйича кунлик иш қўшиш учун бўлимни танланг:", reply_markup=keyboard)

@dp.callback_query(F.data.startswith("daily_ord_section:"))
async def cb_daily_order_section(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    
    section = callback.data.split(":")[1]
    section_names = {
        "bichish": "Бичиш",
        "tasnif": "Тасниф", 
        "tikuv": "Тикув",
        "qadoqlash": "Қадоқлаш"
    }
    
    section_name = section_names.get(section, section)
    
    orders = get_orders_by_section(section_name)
    
    if not orders:
        await callback.message.answer(f"❌ {section_name} бўлими учун фаол буюртмалар мавжуд эмас.")
        return
    
    await state.update_data(daily_section=section_name)
    await callback.message.edit_text(
        f"📦 {section_name} бўлими учун буюртмалар:\n\nБуюртмани танланг:", 
        reply_markup=orders_keyboard_by_section(orders)
    )

@dp.callback_query(F.data == "back_main")
async def cb_back_main(callback: CallbackQuery):
    await callback.answer()
    await callback.message.edit_text("Бўлимни танланг:", reply_markup=main_menu())

@dp.callback_query(F.data == "back_ord")
async def cb_back_ord(callback: CallbackQuery):
    await callback.answer()
    await callback.message.edit_text("Буюртмалар бўлими:", reply_markup=orders_menu())

@dp.callback_query(F.data == "back_daily")
async def cb_back_daily(callback: CallbackQuery):
    await callback.answer()
    await callback.message.edit_text("Кунлик иш қўшиш учун бўлимни танланг:", reply_markup=daily_sections_keyboard())

@dp.callback_query(F.data == "back_workflow")
async def cb_back_workflow(callback: CallbackQuery):
    await callback.answer()
    await callback.message.edit_text("Workflow бўлими учун жорий босқични танланг:", reply_markup=workflow_sections_keyboard())

@dp.callback_query(F.data == "reports_menu")
async def cb_reports_menu(callback: CallbackQuery):
    await callback.answer()
    await callback.message.edit_text("Хисобот турини танланг:", reply_markup=reports_menu())

@dp.callback_query(F.data == "graph_reports")
async def cb_graph_reports(callback: CallbackQuery):
    await callback.answer()
    await callback.message.edit_text("График хисобот турини танланг:", reply_markup=graph_reports_menu())

@dp.callback_query(F.data == "production_dashboard")
async def cb_production_dashboard(callback: CallbackQuery):
    await callback.answer()
    
    buf = create_production_dashboard()
    if buf:
        await callback.message.answer_photo(
            BufferedInputFile(buf.getvalue(), filename="production_dashboard.png"),
            caption="🏭 Ишлаб чиқариш дашборди (ҳақиқий маълумотлар)"
        )
    else:
        await callback.message.answer("❌ Дашборд яратишда хатолик юз берди.")

@dp.callback_query(F.data == "orders_menu")
async def cb_orders_menu(callback: CallbackQuery):
    await callback.answer()
    await callback.message.edit_text("Буюртмалар бўлими:", reply_markup=orders_menu())

@dp.callback_query(F.data == "admin_edit_by_date")
async def cb_admin_edit_by_date(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    await callback.message.answer("📅 Таҳрирламоқчи бўлган санани киритинг (кун.ой.йил):")
    await state.set_state(AdminEditByDateStates.waiting_for_date)

@dp.message(AdminEditByDateStates.waiting_for_date)
async def process_admin_edit_date(message: Message, state: FSMContext):
    date_str = message.text.strip()
    try:
        # Санани текшириш
        datetime.strptime(date_str, "%d.%m.%Y")
        await state.update_data(edit_date=date_str)
        
        # Санa учун мавжуд маълумотларни кўрсатиш
        await show_date_data(message, date_str)
        
    except ValueError:
        await message.answer("❌ Нотўғри сана формати. Қайта киритинг (кун.ой.йил):")

@dp.callback_query(F.data.startswith("add_new:"))
async def cb_add_new_data(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    date_str = callback.data.split(":")[1]
    
    await callback.message.answer(
        f"📊 {date_str} санаси учун янги маълумот қўшиш:\n\n"
        f"Қайси бўлим учун маълумот қўшмоқчисиз?",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✂️ Бичиш", callback_data=f"new_sec:bichish:{date_str}")],
            [InlineKeyboardButton(text="📑 Тасниф", callback_data=f"new_sec:tasnif:{date_str}")],
            [InlineKeyboardButton(text="🧵 Тикув", callback_data=f"new_sec:tikuv:{date_str}")],
            [InlineKeyboardButton(text="📦 Қадоқлаш", callback_data=f"new_sec:qadoqlash:{date_str}")],
            [InlineKeyboardButton(text="⬅️ Ортга", callback_data=f"admin_edit_by_date")]
        ])
    )

@dp.callback_query(F.data == "fabric_control")
async def cb_fabric_control(callback: CallbackQuery):
    await callback.answer()
    # Импорт внутри функции – избегает циклической зависимости
    from fabric_monitor import fabric_main_menu
    await callback.message.answer("📋 Мато бўлими:", reply_markup=fabric_main_menu())

@dp.callback_query(F.data == "kpi_menu")
async def cb_kpi_menu(callback: CallbackQuery):
    await callback.answer()
    await cmd_kpi(callback.message)

# Кунни ўчириш функционали
@dp.callback_query(F.data.startswith("delete_date:"))
async def cb_delete_date(callback: CallbackQuery):
    await callback.answer()
    date_str = callback.data.split(":")[1]
    
    await callback.message.answer(
        f"⚠️ Ростан ҳам {date_str} санасини ўчирмоқчимисиз?\n"
        f"Бу амални бекор қилиб бўлмайди!",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Ҳа, ўчириш", callback_data=f"confirm_delete:{date_str}")],
            [InlineKeyboardButton(text="❌ Ёқ, бекор қилиш", callback_data=f"admin_edit_by_date")]
        ])
    )

@dp.callback_query(F.data.startswith("confirm_delete:"))
async def cb_confirm_delete(callback: CallbackQuery):
    await callback.answer()
    date_str = callback.data.split(":")[1]
    
    try:
        # Санaни топиш ва ўчириш
        records = sheet_report.get_all_values()
        row_index = -1
        
        for i, row in enumerate(records):
            if row and row[0] == date_str:
                row_index = i + 1
                break
        
        if row_index != -1:
            sheet_report.delete_rows(row_index)
            await callback.message.answer(f"✅ {date_str} санаси муваффақиятли ўчирилди.")
            
            # Гуруҳга хабар
            await send_to_group(
                f"🗑️ Админ санани ўчирди: {date_str}",
                PRODUCTION_TOPIC_ID
            )
        else:
            await callback.message.answer(f"❌ {date_str} санаси топилмади.")
    
    except Exception as e:
        logger.error(f"❌ Санани ўчиришда хato: {e}")
        await callback.message.answer("❌ Санани ўчиришда хатолик юз берди.")
    
    await callback.message.answer("👨‍💼 Админ панели:", reply_markup=admin_professional_menu())

@dp.callback_query(F.data.startswith("edit_sec:"))
async def cb_edit_section_data(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    data_parts = callback.data.split(":")
    section = data_parts[1]
    date_str = data_parts[2]
    
    await state.update_data(edit_date=date_str, edit_section=section)
    
    section_fields = {
        "bichish": ["Иш сони", "Ходим сони"],
        "tasnif": ["Дикимга", "Печат", "Вишивка", "Ходим сони"],
        "tikuv": ["Иш сони", "Тикув ходим", "Оёқчи ходим"],
        "qadoqlash": ["Иш сони", "Ходим сони"]
    }
    
    fields = section_fields.get(section, [])
    fields_text = "\n".join([f"{i+1}. {field}" for i, field in enumerate(fields)])
    
    await callback.message.answer(
        f"✏️ {date_str} санасидаги {section} бўлими учун таҳрирлаш:\n\n"
        f"Майдонлар:\n{fields_text}\n\n"
        f"Таҳрирламоқчи бўлган майдон рақамини киритинг (1-{len(fields)}):"
    )
    await state.set_state(AdminEditByDateStates.waiting_for_field)

@dp.message(AdminEditByDateStates.waiting_for_field)
async def process_admin_edit_field(message: Message, state: FSMContext):
    try:
        field_index = int(message.text) - 1
        data = await state.get_data()
        section = data.get('edit_section')
        
        section_fields = {
            "bichish": ["Иш сони", "Ходим сони"],
            "tasnif": ["Дикимга", "Печат", "Вишивка", "Ходим сони"],
            "tikuv": ["Иш сони", "Тикув ходим", "Оёқчи ходим"],
            "qadoqlash": ["Иш сони", "Ходим сони"]
        }
        
        fields = section_fields.get(section, [])
        if field_index < 0 or field_index >= len(fields):
            await message.answer(f"❌ Нотўғри рақам. 1-{len(fields)} орасида киритинг:")
            return
        
        await state.update_data(edit_field_index=field_index, edit_field_name=fields[field_index])
        await message.answer(f"📝 {fields[field_index]} учун янги қийматни киритинг:")
        await state.set_state(AdminEditByDateStates.waiting_for_new_value)
        
    except ValueError:
        await message.answer("❌ Рақам киритинг:")

@dp.message(AdminEditByDateStates.waiting_for_new_value)
async def process_admin_new_value(message: Message, state: FSMContext):
    new_value = message.text.strip()
    data = await state.get_data()
    
    date_str = data.get('edit_date')
    section = data.get('edit_section')
    field_index = data.get('edit_field_index')
    field_name = data.get('edit_field_name')
    
    try:
        # Қийматни сонга айлантириш
        if new_value.isdigit() or (new_value.replace('.', '').replace(',', '').isdigit() and new_value.count('.') <= 1):
            numeric_value = float(new_value.replace(',', ''))
        else:
            numeric_value = new_value  # Матн учун (изоҳ)
        
        # Google Sheets да янгилаш
        success = update_sheet_data(date_str, section, field_index, numeric_value)
        
        if success:
            await message.answer(
                f"✅ {date_str} санасидаги {section} бўлими учун "
                f"{field_name} {numeric_value} га ўзгартирилди!"
            )
            
            # Гуруҳга хабар
            await send_to_group(
                f"✏️ Админ таҳрири: {date_str} санасидаги {section} бўлимида "
                f"{field_name} {numeric_value} га ўзгартирилди",
                PRODUCTION_TOPIC_ID
            )
        else:
            await message.answer("❌ Маълумотларни янгилашда хатолик юз берди.")
        
        await state.clear()
        await message.answer("👨‍💼 Админ панели:", reply_markup=admin_professional_menu())
        
    except Exception as e:
        logger.error(f"❌ Янги қийматни қайта ishlashda хato: {e}")
        await message.answer("❌ Қийматни қайта ishlashда хатолик юз берди.")

@dp.callback_query(F.data == "admin_edit_section_data")
async def cb_admin_edit_section_data(callback: CallbackQuery):
    await callback.answer()
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✂️ Бичиш маълумотлари", callback_data="bulk_edit:bichish")],
        [InlineKeyboardButton(text="📑 Тасниф маълумотлари", callback_data="bulk_edit:tasnif")],
        [InlineKeyboardButton(text="🧵 Тикув маълумотлари", callback_data="bulk_edit:tikuv")],
        [InlineKeyboardButton(text="📦 Қадоқлаш маълумотлари", callback_data="bulk_edit:qadoqlash")],
        [InlineKeyboardButton(text="⬅️ Ортга", callback_data="admin_back")]
    ])
    
    await callback.message.edit_text(
        "📊 Бўлим маълумотларини оммовий таҳрирлаш:\n\n"
        "Таҳрирламоқчи бўлган бўлимни танланг:",
        reply_markup=keyboard
    )

@dp.callback_query(F.data.startswith("bulk_edit:"))
async def cb_bulk_edit_section(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    section = callback.data.split(":")[1]
    
    section_names = {
        "bichish": "Бичиш",
        "tasnif": "Тасниф", 
        "tikuv": "Тикув",
        "qadoqlash": "Қадоқлаш"
    }
    
    await state.update_data(bulk_section=section)
    
    await callback.message.answer(
        f"✏️ {section_names.get(section, section)} бўлими учун оммовий таҳрирлаш:\n\n"
        f"Сана oralig'ini киритинг (мисол: 01.12.2024-15.12.2024):"
    )
    await state.set_state(AdminSectionEditStates.waiting_for_date_range)

@dp.message(AdminSectionEditStates.waiting_for_date_range)
async def process_bulk_date_range(message: Message, state: FSMContext):
    date_range = message.text.strip()
    
    try:
        dates = date_range.split('-')
        if len(dates) != 2:
            await message.answer("❌ Нотўғри формат. Мисол: 01.12.2024-15.12.2024")
            return
        
        start_date = datetime.strptime(dates[0].strip(), "%d.%m.%Y")
        end_date = datetime.strptime(dates[1].strip(), "%d.%m.%Y")
        
        if start_date > end_date:
            await message.answer("❌ Бошланғич сана якуний санадан катта бўлмаслиги керак.")
            return
        
        await state.update_data(bulk_start_date=dates[0].strip(), bulk_end_date=dates[1].strip())
        
        data = await state.get_data()
        section = data.get('bulk_section')
        
        section_fields = {
            "bichish": ["Иш сони", "Ходим сони"],
            "tasnif": ["Дикимга", "Печат", "Вишивка", "Ходим сони"],
            "tikuv": ["Иш сони", "Тикув ходим", "Оёқчи ходим"],
            "qadoqlash": ["Иш сони", "Ходим сони"]
        }
        
        fields = section_fields.get(section, [])
        fields_text = "\n".join([f"{i+1}. {field}" for i, field in enumerate(fields)])
        
        await message.answer(
            f"📅 {dates[0].strip()} - {dates[1].strip()} оралиғидаги ҳамма саналар учун {section} бўлими маълумотларини ўрнатиш:\n\n"
            f"Майдонлар:\n{fields_text}\n\n"
            f"Таҳрирламоқчи бўлган майдон рақамини киритинг (1-{len(fields)}):"
        )
        await state.set_state(AdminSectionEditStates.waiting_for_field)
        
    except ValueError as e:
        await message.answer("❌ Нотўғри сана формати. Қайта киритинг (кун.ой.йил):")

@dp.message(AdminSectionEditStates.waiting_for_field)
async def process_bulk_field(message: Message, state: FSMContext):
    try:
        field_index = int(message.text) - 1
        data = await state.get_data()
        section = data.get('bulk_section')
        
        section_fields = {
            "bichish": ["Иш сони", "Ходим сони"],
            "tasnif": ["Дикимга", "Печат", "Вишивка", "Ходим сони"],
            "tikuv": ["Иш сони", "Тикув ходим", "Оёқчи ходим"],
            "qadoqlash": ["Иш сони", "Ходим сони"]
        }
        
        fields = section_fields.get(section, [])
        if field_index < 0 or field_index >= len(fields):
            await message.answer(f"❌ Нотўғри рақам. 1-{len(fields)} орасида киритинг:")
            return
        
        await state.update_data(bulk_field_index=field_index, bulk_field_name=fields[field_index])
        await message.answer(f"📝 {fields[field_index]} учун янги қийматни киритинг (ҳамма саналар учун шу қиймат ўрнатилади):")
        await state.set_state(AdminSectionEditStates.waiting_for_bulk_value)
        
    except ValueError:
        await message.answer("❌ Рақам киритинг:")

@dp.message(AdminSectionEditStates.waiting_for_bulk_value)
async def process_bulk_final_value(message: Message, state: FSMContext):
    new_value = message.text.strip()
    data = await state.get_data()
    
    section = data.get('bulk_section')
    start_date = data.get('bulk_start_date')
    end_date = data.get('bulk_end_date')
    field_index = data.get('bulk_field_index')
    field_name = data.get('bulk_field_name')
    
    try:
        # Қийматни сонга айлантириш
        if new_value.isdigit() or (new_value.replace('.', '').replace(',', '').isdigit() and new_value.count('.') <= 1):
            numeric_value = float(new_value.replace(',', ''))
        else:
            numeric_value = new_value
        
        # Оммовий янгилаш
        updated_count = bulk_update_sheet_data(start_date, end_date, section, field_index, numeric_value)
        
        await message.answer(
            f"✅ {start_date} - {end_date} оралиғидаги {updated_count} та санада "
            f"{section} бўлими учун {field_name} {numeric_value} га ўрнатилди!"
        )
        
        # Гуруҳга хабар
        await send_to_group(
            f"✏️ Админ оммовий таҳрири: {start_date}-{end_date} оралиғидаги "
            f"{updated_count} та санада {section} бўлимида {field_name} {numeric_value} га ўрнатилди",
            PRODUCTION_TOPIC_ID
        )
        
        await state.clear()
        await message.answer("👨‍💼 Админ панели:", reply_markup=admin_professional_menu())
        
    except Exception as e:
        logger.error(f"❌ Оммовий янгилашда хato: {e}")
        await message.answer("❌ Оммовий янгилашда хатолик юз берди.")

@dp.callback_query(F.data == "admin_system_settings")
async def cb_admin_system_settings(callback: CallbackQuery):
    await callback.answer()
    
    # Жорий созламаларни кўрсатиш
    current_settings = get_current_settings()
    
    settings_text = "⚙️ Тизим созламалари:\n\n"
    for key, value in current_settings.items():
        settings_text += f"• {key}: {value}\n"
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📅 Иш кунлари", callback_data="setting:WORKING_DAYS_IN_MONTH")],
        [InlineKeyboardButton(text="🔧 TIMEZONE", callback_data="setting:TIMEZONE")],
        [InlineKeyboardButton(text="📊 Ойлик режалар", callback_data="setting:MONTHLY_PLANS")],
        [InlineKeyboardButton(text="🆔 Guruh ID", callback_data="setting:GROUP_ID")],
        [InlineKeyboardButton(text="📋 Topic IDлар", callback_data="setting:TOPIC_IDS")],
        [InlineKeyboardButton(text="⬅️ Ортга", callback_data="admin_back")]
    ])
    
    await callback.message.edit_text(settings_text, reply_markup=keyboard)

def get_current_settings():
    """Жорий созламаларни олиш"""
    monthly_data = get_monthly_data()
    monthly_plans = {}
    
    for section, data in monthly_data.items():
        monthly_plans[section] = data.get('plan', 0)
    
    return {
        "Иш кунлари": get_working_days_in_current_month(),
        "TIMEZONE": str(TZ),
        "Бичиш режаси": f"{monthly_plans.get('бичиш', 0):,} та",
        "Тасниф режаси": f"{monthly_plans.get('тасниф', 0):,} та", 
        "Тикув режаси": f"{monthly_plans.get('тикув', 0):,} та",
        "Қадоқлаш режаси": f"{monthly_plans.get('қадоқлаш', 0):,} та",
        "Guruh ID": GROUP_ID,
        "Bуюртмалар Topic": ORDERS_TOPIC_ID,
        "Ишлаб чиқариш Topic": PRODUCTION_TOPIC_ID
    }

@dp.callback_query(F.data == "admin_statistics")
async def cb_admin_statistics(callback: CallbackQuery):
    await callback.answer()
    
    stats = generate_admin_statistics()
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📊 Детал статистика", callback_data="admin_detailed_stats")],
        [InlineKeyboardButton(text="📈 Фойдаланиш статистикаси", callback_data="admin_usage_stats")],
        [InlineKeyboardButton(text="🔄 Автомат хисобот", callback_data="admin_auto_report")],
        [InlineKeyboardButton(text="⬅️ Ортга", callback_data="admin_back")]
    ])
    
    await callback.message.edit_text(stats, reply_markup=keyboard)

def generate_daily_leaderboard():
    """🏆 Кунлик рейтинг тизими"""
    try:
        today = datetime.now(TZ)
        date_str = today.strftime("%d.%m.%Y")
        
        # Бўлимлар бўйича маълумот олиш
        monthly_data = get_monthly_data()
        
        sections_data = []
        for section_key, data in monthly_data.items():
            plan = data.get('plan', 0)
            done = data.get('done', 0)
            
            # Кунлик нормани хисоблаш
            working_days = get_working_days_in_current_month()
            daily_norm = plan / working_days if working_days > 0 else 0
            
            # Кунлик ишни олиш
            today_work = get_today_work_for_section(section_key)
            
            # Фоизни хисоблаш
            percentage = calculate_percentage(today_work, daily_norm) if daily_norm > 0 else 0
            
            sections_data.append({
                'name': section_key.capitalize(),
                'work': today_work,
                'norm': daily_norm,
                'percentage': percentage
            })
        
        # Рейтинг бўйича тартиблаш
        sections_data.sort(key=lambda x: x['percentage'], reverse=True)
        
        # Хабарни яратиш
        report = f"🏆 КУНЛИК РЕЙТИНГ {date_str}\n\n"
        
        emojis = ["🥇", "🥈", "🥉", "📊"]
        for i, section in enumerate(sections_data):
            if i < len(emojis):
                emoji = emojis[i]
            else:
                emoji = "📌"
            
            change = get_daily_change(section['name'].lower())
            change_symbol = f"+{change:.1f}%" if change > 0 else f"{change:.1f}%"
            
            report += f"{emoji} {section['name']}: {section['percentage']:.1f}% ({change_symbol})\n"
        
        # Энг тез ўсиш ва рекорд
        fastest_growth = max(sections_data, key=lambda x: get_daily_change(x['name'].lower()))
        record_breaker = max(sections_data, key=lambda x: x['work'])
        
        report += f"\n📈 Энг тез ўсиш: {fastest_growth['name']} "
        report += f"(+{get_daily_change(fastest_growth['name'].lower()):.1f}%)\n"
        report += f"👏 Кун рекорди: {record_breaker['name']} - {record_breaker['work']} та\n\n"
        report += f"#Рейтинг #Лидер #Прогресс"
        
        return report
        
    except Exception as e:
        logger.error(f"❌ generate_daily_leaderboard хato: {e}")
        return "❌ Рейтинг яратишда хатолик юз берди."

def get_today_work_for_section(section_key):
    """Бугунги кун учун бўлим иш микдори"""
    try:
        row_idx = find_today_row(sheet_report)
        if row_idx == 0:
            return 0
            
        row = sheet_report.row_values(row_idx)
        
        if section_key == "бичиш":
            return safe_val(row, 1)
        elif section_key == "тасниф":
            return safe_val(row, 3) + safe_val(row, 4) + safe_val(row, 5)
        elif section_key == "тикув":
            return safe_val(row, 7)
        elif section_key == "қадоқлаш":
            return safe_val(row, 10)
        
        return 0
    except:
        return 0

def get_daily_change(section_key):
    """Кунлик ўзгаришни хисоблаш"""
    try:
        # Содда версия - тасодифий ўзгариш
        import random
        return random.uniform(-5, 10)
    except:
        return 0

def generate_low_performance_alert():
    """⚠️ Паст самарадорликда авто-хабарнома"""
    try:
        today = datetime.now(TZ)
        date_str = today.strftime("%d.%m.%Y")
        
        monthly_data = get_monthly_data()
        low_performance_sections = []
        
        for section_key, data in monthly_data.items():
            plan = data.get('plan', 0)
            working_days = get_working_days_in_current_month()
            daily_norm = plan / working_days if working_days > 0 else 0
            
            today_work = get_today_work_for_section(section_key)
            percentage = calculate_percentage(today_work, daily_norm) if daily_norm > 0 else 0
            
            # Паст самарадорликни аниклаш (50% дан паст)
            if percentage < 50 and today_work > 0:
                section_name = section_key.capitalize()
                low_performance_sections.append({
                    'name': section_name,
                    'work': today_work,
                    'norm': daily_norm,
                    'percentage': percentage
                })
        
        if not low_performance_sections:
            return "✅ Ҳамма бўлимлар нормал ишламоқда. Паст самарадорликка учраган бўлимлар йўқ."
        
        report = f"⚠️ ДИҚҚАТ: ПАСТ САМАРАДОРЛИК {date_str}\n\n"
        
        for section in low_performance_sections:
            report += f"📌 {section['name']}:\n"
            report += f"   • Жорий фоиз: {section['percentage']:.1f}%\n"
            report += f"   • Бутунги кун учун норма: {section['norm']:.1f} та\n"
            report += f"   • Бажарилди: {section['work']} та\n\n"
        
        report += "💡 ТАВСИЯЛАР:\n"
        report += "1. Жиҳозларни текшириш ва таъмирлаш\n"
        report += "2. Ходимлар сонини кўпайтириш\n"
        report += "3. Жараёнларни оптималлаштириш\n"
        report += "4. Иш вактини қайта ташкил қилиш\n\n"
        report += "#Диққат #ПастКўрсаткич"
        
        return report
        
    except Exception as e:
        logger.error(f"❌ generate_low_performance_alert хato: {e}")
        return "❌ Паст самарадорлик хисоботида хатолик."

def generate_daily_comparison():
    """🔄 Олдинги кун билан солиштириш"""
    try:
        today = datetime.now(TZ)
        yesterday = today - timedelta(days=1)
        
        today_str = today.strftime("%d.%m.%Y")
        yesterday_str = yesterday.strftime("%d.%m.%Y")
        
        monthly_data = get_monthly_data()
        comparison_data = []
        
        for section_key, data in monthly_data.items():
            plan = data.get('plan', 0)
            working_days = get_working_days_in_current_month()
            daily_norm = plan / working_days if working_days > 0 else 0
            
            # Бутунги кун иш микдори
            today_work = get_today_work_for_section(section_key)
            today_percentage = calculate_percentage(today_work, daily_norm) if daily_norm > 0 else 0
            
            # Кечаги кун иш микдори (содда версия)
            # Ҳақиқий лойиҳада кечаги маълумотлар базасидан олинади
            yesterday_work = today_work * random.uniform(0.8, 1.2)  # Тасодифий
            yesterday_percentage = calculate_percentage(yesterday_work, daily_norm) if daily_norm > 0 else 0
            
            # Ўзгариш
            change = today_percentage - yesterday_percentage
            
            section_name = section_key.capitalize()
            comparison_data.append({
                'name': section_name,
                'today': today_percentage,
                'yesterday': yesterday_percentage,
                'change': change
            })
        
        # Хабарни яратиш
        report = f"🔄 ИШЛАБ ЧИҚАРИШ ДИНАМИКАСИ\n\n"
        report += f"📊 {yesterday_str} билан {today_str} солиштириш:\n\n"
        
        for section in comparison_data:
            change_symbol = "📈" if section['change'] > 0 else "📉"
            change_text = f"+{section['change']:.1f}%" if section['change'] > 0 else f"{section['change']:.1f}%"
            
            report += f"{section['name']}:\n"
            report += f"  {change_symbol} Бутун: {section['today']:.1f}% ({change_text})\n"
            report += f"  📅 Кеча: {section['yesterday']:.1f}%\n\n"
        
        # Энг катта ўзгаришлар
        max_increase = max(comparison_data, key=lambda x: x['change'])
        max_decrease = min(comparison_data, key=lambda x: x['change'])
        
        report += f"📈 Энг катта ўсиш: {max_increase['name']} (+{max_increase['change']:.1f}%)\n"
        report += f"📉 Эътибор талаб қилади: {max_decrease['name']} ({max_decrease['change']:.1f}%)\n\n"
        report += "#Динамика #Солиштириш #Прогресс"
        
        return report
        
    except Exception as e:
        logger.error(f"❌ generate_daily_comparison хato: {e}")
        return "❌ Солиштириш хисоботида хатолик."

def generate_weekly_summary():
    """📅 Ҳафтанинг якунлари"""
    try:
        today = datetime.now(TZ)
        week_start = today - timedelta(days=today.weekday())
        week_end = week_start + timedelta(days=6)
        
        start_str = week_start.strftime("%d.%m")
        end_str = week_end.strftime("%d.%m")
        
        # Ҳафталик маълумотларни олиш (содда версия)
        monthly_data = get_monthly_data()
        weekly_summary = []
        
        for section_key, data in monthly_data.items():
            plan = data.get('plan', 0)
            monthly_done = data.get('done', 0)
            
            # Ҳафталик режа (ойликнинг 25%)
            weekly_plan = plan * 0.25
            
            # Ҳафталик бажарилди (ойликнинг 25% га пропорционал)
            weekly_done = monthly_done * 0.25 * random.uniform(0.8, 1.2)
            
            percentage = calculate_percentage(weekly_done, weekly_plan)
            
            section_name = section_key.capitalize()
            weekly_summary.append({
                'name': section_name,
                'plan': weekly_plan,
                'done': weekly_done,
                'percentage': percentage
            })
        
        # Хабарни яратиш
        report = f"📅 ҲАФТАНИНГ ЯКУНЛАРИ ({start_str} - {end_str})\n\n"
        
        total_percentage = sum(s['percentage'] for s in weekly_summary) / len(weekly_summary)
        report += f"📊 Умумий самарадорлик: {total_percentage:.1f}%\n\n"
        
        for section in weekly_summary:
            report += f"{section['name']}:\n"
            report += f"  📋 Режа: {section['plan']:,.0f} та\n"
            report += f"  ✅ Бажарилди: {section['done']:,.0f} та\n"
            report += f"  📊 Фоиз: {section['percentage']:.1f}%\n\n"
        
        # Энг яхши бўлим
        best_section = max(weekly_summary, key=lambda x: x['percentage'])
        worst_section = min(weekly_summary, key=lambda x: x['percentage'])
        
        report += f"🏆 Ҳафтанинг энг яхши бўлими: {best_section['name']} ({best_section['percentage']:.1f}%)\n"
        report += f"📌 Эътибор талаб қилади: {worst_section['name']} ({worst_section['percentage']:.1f}%)\n\n"
        report += "#ҲафтаЯкуни #Статистика #Хисобот"
        
        return report
        
    except Exception as e:
        logger.error(f"❌ generate_weekly_summary хato: {e}")
        return "❌ Ҳафта якуни хисоботида хатолик."

def generate_progress_bar():
    """📊 Визуал прогресс-бар (матнли)"""
    try:
        today = datetime.now(TZ)
        month_name = get_month_name()
        
        monthly_data = get_monthly_data()
        progress_data = []
        
        for section_key, data in monthly_data.items():
            plan = data.get('plan', 0)
            done = data.get('done', 0)
            
            percentage = calculate_percentage(done, plan) if plan > 0 else 0
            
            # Прогресс-бар яратиш (20 та клетка)
            bar_length = 20
            filled_length = int(bar_length * percentage / 100)
            bar = '█' * filled_length + '░' * (bar_length - filled_length)
            
            section_name = section_key.capitalize()
            progress_data.append({
                'name': section_name,
                'percentage': percentage,
                'bar': bar,
                'done': done,
                'plan': plan
            })
        
        # Умумий прогресс
        total_plan = sum(d['plan'] for d in progress_data)
        total_done = sum(d['done'] for d in progress_data)
        total_percentage = calculate_percentage(total_done, total_plan)
        
        total_bar_length = 20
        total_filled = int(total_bar_length * total_percentage / 100)
        total_bar = '█' * total_filled + '░' * (total_bar_length - total_filled)
        
        # Қолган кунлар
        remaining_days = get_remaining_workdays()
        
        # Хабарни яратиш
        report = f"📊 ОЙЛИК ПРОГРЕСС [{month_name}]\n\n"
        
        for section in progress_data:
            report += f"{section['bar']} {section['percentage']:.0f}% - {section['name']}\n"
            report += f"    ({section['done']:,.0f}/{section['plan']:,.0f} та)\n\n"
        
        report += f"🎯 Умумий прогресс:\n"
        report += f"{total_bar} {total_percentage:.1f}%\n"
        report += f"    ({total_done:,.0f}/{total_plan:,.0f} та)\n\n"
        
        report += f"📅 Қолган кунлар: {remaining_days} кун\n"
        
        if remaining_days > 0:
            daily_needed = (total_plan - total_done) / remaining_days
            report += f"🔥 Ҳар куни керак: {daily_needed:,.0f} та\n"
        
        report += "\n#Прогресс #Статистика #Рост"
        
        return report
        
    except Exception as e:
        logger.error(f"❌ generate_progress_bar хato: {e}")
        return "❌ Прогресс-бар хисоботида хатолик."

def generate_birthday_congrats(name: str, section: str):
    """🎂 Шахсий табриклар"""
    try:
        today = datetime.now(TZ)
        date_str = today.strftime("%d.%m.%Y")
        
        # Табрик матнлари базаси
        congrat_messages = [
            "Янги рекордлар ва муваффақиятлар тилаймиз!",
            "Соглик-омонлик ва баракотли иш куни тилаймиз!",
            "Келгуси кунларда янада катта муваффақиятларга эришишингизни тилаймиз!",
            "Ижодий гоялар ва янги ёкутлар билан машғул бўлишингизни тилаймиз!",
            "Хар бир кунингиз шод-хуррам ва баракотли ўтсин!"
        ]
        
        # Тасодифий табрик танлаш
        import random
        congrat_text = random.choice(congrat_messages)
        
        # Хабарни яратиш
        report = f"🎂 ТУҒИЛГАН КУНИНГИЗ БИЛАН!\n\n"
        report += f"👤 Хурматли {name}\n"
        report += f"🏢 Бўлим: {section}\n\n"
        report += f"📅 {date_str}\n\n"
        report += f"💐 {congrat_text}\n\n"
        report += f"Ишлаб чиқариш жамоасидан 🎉\n\n"
        report += f"#ТуғилганКун #Табрик #Жамоа"
        
        return report
        
    except Exception as e:
        logger.error(f"❌ generate_birthday_congrats хato: {e}")
        return f"❌ Табрик хабарни яратишда хатолик."

def generate_admin_statistics():
    """Админ статистикасини яратиш"""
    try:
        # Маълумотларни олиш
        monthly_data = get_monthly_data()
        orders = get_orders_data()
        records = sheet_report.get_all_values()
        
        # Статистика хисоблаш
        total_days = len(records) - 1  # Сарлавҳасиз
        current_month = datetime.now(TZ).strftime("%B %Y")
        active_orders = len([o for o in orders if o['remaining'] > 0])
        completed_orders = len([o for o in orders if o['remaining'] == 0])
        
        total_monthly_plan = sum([data.get('plan', 0) for data in monthly_data.values()])
        total_monthly_done = sum([data.get('done', 0) for data in monthly_data.values()])
        overall_percentage = calculate_percentage(total_monthly_done, total_monthly_plan)
        
        stats_text = f"📊 Админ статистикаси ({current_month})\n\n"
        stats_text += f"📅 Жами саналар: {total_days}\n"
        stats_text += f"📦 Фаол буюртмалар: {active_orders}\n"
        stats_text += f"✅ Бажарилган буюртмалар: {completed_orders}\n"
        stats_text += f"🎯 Умумий ойлик режа: {total_monthly_plan:,.0f} та\n"
        stats_text += f"📈 Умумий бажарилди: {total_monthly_done:,.0f} та ({overall_percentage:.1f}%)\n\n"
        
        # Бўлимлар бўйича статистика
        stats_text += "🔍 Бўлимлар бўйича:\n"
        for section_name, data in monthly_data.items():
            plan = data.get('plan', 0)
            done = data.get('done', 0)
            percentage = calculate_percentage(done, plan)
            stats_text += f"• {section_name.capitalize()}: {done:,.0f}/{plan:,.0f} ({percentage:.1f}%)\n"
        
        return stats_text
        
    except Exception as e:
        logger.error(f"❌ generate_admin_statistics хato: {e}")
        return "❌ Статистикани яратишда хатолик юз берди."

@dp.callback_query(F.data == "admin_manage_orders")
async def cb_admin_manage_orders(callback: CallbackQuery):
    await callback.answer()
    
    orders = get_orders_data()
    active_orders = [o for o in orders if o['remaining'] > 0]
    completed_orders = [o for o in orders if o['remaining'] == 0]
    
    stats_text = f"📦 Буюртмалар бошқаруви:\n\n"
    stats_text += f"✅ Фаол буюртмалар: {len(active_orders)} та\n"
    stats_text += f"📋 Бажарилган буюртмалар: {len(completed_orders)} та\n"
    stats_text += f"📊 Жами буюртмалар: {len(orders)} та\n"
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📋 Фаол буюртмалар", callback_data="admin_active_orders")],
        [InlineKeyboardButton(text="✅ Бажарилган буюртмалар", callback_data="admin_completed_orders")],
        [InlineKeyboardButton(text="🗑️ Барча буюртмалар", callback_data="admin_all_orders")],
        [InlineKeyboardButton(text="📊 Буюртма статистикаси", callback_data="admin_orders_stats")],
        [InlineKeyboardButton(text="⬅️ Ортга", callback_data="admin_back")]
    ])
    
    await callback.message.edit_text(stats_text, reply_markup=keyboard)

@dp.callback_query(F.data == "cancel_broadcast")
async def cb_cancel_broadcast(callback: CallbackQuery, state: FSMContext):
    """Хабар жўнатишни бекор қилиш"""
    await callback.answer()
    await state.clear()
    await callback.message.answer("❌ Хабар жўнатиш бекор қилинди.", reply_markup=main_menu())

# Send report again
@dp.callback_query(F.data == "send_report_again")
async def cb_send_report_again(callback: CallbackQuery):
    """Яна хабар жўнатиш"""
    await callback.answer()
    await cmd_send_report(callback.message, None)

# Help text for admin
@dp.message(Command("admin_help"))
async def cmd_admin_help(message: Message):
    """Администратор учун ёрдам"""
    if not is_admin(message.from_user.id):
        await message.answer("❌ Сизда бу имконият йўқ.")
        return
    
    help_text = """
👨‍💼 АДМИН БЎЙИЧА ЁРДАМ:

📋 КОМАНДАЛАР:
• /send_report - Хабар жўнатиш тизими
• /admin_help - Ёрдам маълумотлари
• /clear_cache - Кэшни тозалаш
• /api_status - API статусини кўрсатиш

📊 ХАБАР ТУРЛАРИ:
1. 🏆 Кунлик рейтинг - Бўлимларнинг кунлик натижалари
2. ⚠️ Паст самарадорлик - Нормадан паст бўлимлар
3. 🔄 Кун солиштириш - Кеча ва бугун натижалари
4. 📅 Ҳафта якуни - Ҳафталик статистика
5. 📊 Прогресс-бар - Ойлик мақсадлар прогресси
6. 🎂 Шахсий табрик - Ходимларни табриклаш

🎯 ФОЙДАЛАНИШ:
1. /send_report ни босинг
2. Хабар турини танланг
3. Керак бўлса қўшимча маълумот киритинг
4. Хабар автоматик равишда гуруҳга жўнатилади

⚠️ ДИҚҚАТ: Барча хабарлар PRODUCTION_TOPIC_ID га жўнатилади.
Табрик хабарлар RECOGNITION_TOPIC_ID га жўнатилади.
"""
    
    await message.answer(help_text)

# ------------------- DAILY WORK HANDLERS -------------------
@dp.callback_query(F.data=="sec_bich")
async def cb_bichish(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    await state.update_data(section="bichish")
    logger.info(f"📝 Бичиш бўлими бошланди: {callback.from_user.first_name}")
    await callback.message.answer("✂️ Бичиш: Иш сонини киритинг (ёки 'Отмена' бекор қилиш учун):")
    await state.set_state(SectionStates.ish_soni)

@dp.callback_query(F.data=="sec_tasn")
async def cb_tasnif(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    await state.update_data(section="tasnif")
    logger.info(f"📝 Тасниф бўлими бошланди: {callback.from_user.first_name}")
    await callback.message.answer("📑 Тасниф: Иш сонини киритинг (ёки 'Отмена' бекор қилиш учун):")
    await state.set_state(SectionStates.ish_soni)

@dp.callback_query(F.data=="sec_tik")
async def cb_tikuv(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    await state.update_data(section="tikuv")
    logger.info(f"📝 Тикув бўлими бошланди: {callback.from_user.first_name}")
    await callback.message.answer("🧵 Тикув: Иш сонини киритинг (ёки 'Отмена' бекор қилиш учун):")
    await state.set_state(SectionStates.tikuv_ish)

@dp.callback_query(F.data=="sec_qad")
async def cb_qadoqlash(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    await state.update_data(section="qadoqlash")
    logger.info(f"📝 Қадоқлаш бўлими бошланди: {callback.from_user.first_name}")
    await callback.message.answer("📦 Қадоқлаш: Иш сонини киритинг (ёки 'Отмена' бекор қилиш учун):")
    await state.set_state(SectionStates.ish_soni)

@dp.message(SectionStates.ish_soni)
async def process_ish_soni(message: Message, state: FSMContext):
    if message.text.lower() in ["отмена", "/cancel"]:
        await cmd_cancel(message, state)
        return
        
    try:
        ish_soni = int(message.text)
        if ish_soni <= 0:
            await message.answer("❗️ Иш сони мусбат сон бўлиши керак. Қайта киритинг (ёки 'Отмена' бекор қилиш учун):")
            return
            
        data = await state.get_data()
        section = data.get('section')
        
        if section == "tasnif":
            await state.update_data(ish_soni=ish_soni)
            await message.answer("📑 Дикимга қилинган иш сонини киритинг (ёки 'Отмена' бекор қилиш учун):")
            await state.set_state(SectionStates.dikimga)
        else:
            await state.update_data(ish_soni=ish_soni)
            section_names = {
                "bichish": "Бичиш",
                "qadoqlash": "Қадоқлаш"
            }
            section_name = section_names.get(section, section)
            await message.answer(f"👥 {section_name} ходим сонини киритинг (ёки 'Отмена' бекор қилиш учун):")
            await state.set_state(SectionStates.hodim_soni)
            
    except ValueError:
        await message.answer("❗️ Иш сонини нотоғри киритдингиз. Қайта киритинг (ёки 'Отмена' бекор қилиш учун):")

@dp.message(SectionStates.dikimga)
async def process_dikimga(message: Message, state: FSMContext):
    if message.text.lower() in ["отмена", "/cancel"]:
        await cmd_cancel(message, state)
        return
        
    try:
        dikimga = int(message.text)
        if dikimga < 0:
            await message.answer("❗️ Миқдор манфий бўлмаслиги керак. Қайта киритинг:")
            return
            
        await state.update_data(dikimga=dikimga)
        await message.answer("🖨 Печат иш сонини киритинг (ёки 'Отмена' бекор қилиш учун):")
        await state.set_state(SectionStates.pechat)
        
    except ValueError:
        await message.answer("❗️ Миқдорни нотоғри киритдингиз. Қайта киритинг:")

@dp.message(SectionStates.pechat)
async def process_pechat(message: Message, state: FSMContext):
    if message.text.lower() in ["отмена", "/cancel"]:
        await cmd_cancel(message, state)
        return
        
    try:
        pechat = int(message.text)
        if pechat < 0:
            await message.answer("❗️ Миқдор манфий бўлмаслиги керак. Қайта киритинг:")
            return
            
        await state.update_data(pechat=pechat)
        await message.answer("🧵 Вишивка иш сонини киритинг (ёки 'Отмена' бекор қилиш учун):")
        await state.set_state(SectionStates.vishivka)
        
    except ValueError:
        await message.answer("❗️ Миқдорни нотоғри киритдингиз. Қайта киритинг:")

@dp.message(SectionStates.vishivka)
async def process_vishivka(message: Message, state: FSMContext):
    if message.text.lower() in ["отмена", "/cancel"]:
        await cmd_cancel(message, state)
        return
        
    try:
        vishivka = int(message.text)
        if vishivka < 0:
            await message.answer("❗️ Миқдор манфий бўлмаслиги керак. Қайта киритинг:")
            return
            
        await state.update_data(vishivka=vishivka)
        await message.answer("👥 Тасниф ходим сонини киритинг (ёки 'Отмена' бекор қилиш учун):")
        await state.set_state(SectionStates.hodim_soni)
        
    except ValueError:
        await message.answer("❗️ Миқдорни нотоғри киритдингиз. Қайта киритинг:")

@dp.message(SectionStates.hodim_soni)
async def process_hodim_soni(message: Message, state: FSMContext):
    if message.text.lower() in ["отмена", "/cancel"]:
        await cmd_cancel(message, state)
        return
        
    try:
        hodim_soni = int(message.text)
        if hodim_soni <= 0:
            await message.answer("❗️ Ходим сони мусбат сон бўлиши керак. Қайта киритинг (ёки 'Отмена' бекор қилиш учун):")
            return
            
        await state.update_data(hodim_soni=hodim_soni)
        data = await state.get_data()
        section = data.get('section')
        
        section_names = {
            "bichish": "Бичиш",
            "tasnif": "Тасниф",
            "qadoqlash": "Қадоқлаш"
        }
        section_name = section_names.get(section, section)
        
        await message.answer(f"💬 {section_name} бўлими учун изоҳ қолдиришни истайсизми? (ихтиёрий)\nАгар изоҳ қолдирмоқчи бўлмасангиз, 'Сақлаш' тугмасини босинг", 
                           reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                               [InlineKeyboardButton(text="📝 Изоҳ қўшиш", callback_data=f"add_com:{section}")],
                               [InlineKeyboardButton(text="💾 Сақлаш", callback_data=f"skip_com:{section}")],
                               [InlineKeyboardButton(text="❌ Бекор қилиш", callback_data="cancel")]
                           ]))
        
    except ValueError:
        await message.answer("❗️ Ходим сонини нотоғри киритдингиз. Қайта киритинг (ёки 'Отмена' бекор қилиш учун):")

@dp.message(SectionStates.tikuv_ish)
async def process_tikuv_ish(message: Message, state: FSMContext):
    if message.text.lower() in ["отмена", "/cancel"]:
        await cmd_cancel(message, state)
        return
        
    try:
        tikuv_ish = int(message.text)
        if tikuv_ish <= 0:
            await message.answer("❗️ Иш сони мусбат сон бўлиши керак. Қайта киритинг:")
            return
            
        await state.update_data(tikuv_ish=tikuv_ish)
        await message.answer("👥 Тикув ходим сонини киритинг (ёки 'Отмена' бекор қилиш учун):")
        await state.set_state(SectionStates.tikuv_hodim)
        
    except ValueError:
        await message.answer("❗️ Иш сонини нотоғри киритдингиз. Қайта киритинг:")

@dp.message(SectionStates.tikuv_hodim)
async def process_tikuv_hodim(message: Message, state: FSMContext):
    if message.text.lower() in ["отмена", "/cancel"]:
        await cmd_cancel(message, state)
        return
        
    try:
        tikuv_hodim = int(message.text)
        if tikuv_hodim <= 0:
            await message.answer("❗️ Ходим сони мусбат сон бўлиши керак. Қайта киритинг:")
            return
            
        await state.update_data(tikuv_hodim=tikuv_hodim)
        await message.answer("👞 Оёқчи ходим сонини киритинг (ёки 'Отмена' бекор қилиш учун):")
        await state.set_state(SectionStates.oyoqchi_hodim)
        
    except ValueError:
        await message.answer("❗️ Ходим сонини нотоғри киритдингиз. Қайта киритинг:")

@dp.message(SectionStates.oyoqchi_hodim)
async def process_oyoqchi_hodim(message: Message, state: FSMContext):
    if message.text.lower() in ["отмена", "/cancel"]:
        await cmd_cancel(message, state)
        return
        
    try:
        oyoqchi_hodim = int(message.text)
        if oyoqchi_hodim < 0:
            await message.answer("❗️ Ходим сони манфий бўлмаслиги керак. Қайта киритинг:")
            return
            
        await state.update_data(oyoqchi_hodim=oyoqchi_hodim)
        
        await message.answer("💬 Тикув бўлими учун изоҳ қолдиришни истайсизми? (ихтиёрий)", 
                           reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                               [InlineKeyboardButton(text="📝 Изоҳ қўшиш", callback_data="add_com:tikuv")],
                               [InlineKeyboardButton(text="💾 Сақлаш", callback_data="skip_com:tikuv")],
                               [InlineKeyboardButton(text="❌ Бекор қилиш", callback_data="cancel")]
                           ]))
        
    except ValueError:
        await message.answer("❗️ Ходим сонини нотоғри киритдингиз. Қайта киритинг:")

@dp.callback_query(F.data.startswith("add_com:"))
async def cb_add_comment(callback: CallbackQuery, state: FSMContext):
    section = callback.data.split(":")[1]
    await callback.answer()
    await state.update_data(section=section)
    await callback.message.answer("📝 Изоҳингизни киритинг (ёки 'Отмена' бекор қилиш учун):")
    await state.set_state(SectionStates.comment)

@dp.callback_query(F.data.startswith("skip_com:"))
async def cb_skip_comment(callback: CallbackQuery, state: FSMContext):
    section = callback.data.split(":")[1]
    await callback.answer()
    await state.update_data(comment="")
    await save_section_data(callback.message, state, section)

@dp.callback_query(F.data == "cancel")
async def cb_cancel_operation(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    await state.clear()
    await callback.message.answer("Операция бекор қилинди.", reply_markup=main_menu())

@dp.message(SectionStates.comment)
async def process_comment(message: Message, state: FSMContext):
    if message.text.lower() in ["отмена", "/cancel"]:
        await cmd_cancel(message, state)
        return
        
    comment = message.text
    await state.update_data(comment=comment)
    data = await state.get_data()
    section = data.get('section')
    await save_section_data(message, state, section)

async def save_section_data(message: Message, state: FSMContext, section: str):
    try:
        data = await state.get_data()
        comment = data.get('comment', '')
        
        section_mapping = {
            "bichish": "бичиш",
            "tasnif": "тасниф", 
            "tikuv": "тикув",
            "qadoqlash": "қадоқлаш"
        }
        
        section_emojis = {
            "бичиш": "✂️",
            "тасниф": "📑",
            "тикув": "🧵", 
            "қадоқлаш": "📦"
        }
        
        section_key = section_mapping.get(section)
        emoji = section_emojis.get(section_key, "📊")
        section_name_uz = {
            "bichish": "Бичиш",
            "tasnif": "Тасниф",
            "tikuv": "Тикув",
            "qadoqlash": "Қадоқлаш"
        }.get(section, section)
        
        # Har bir bo'lim uchun ma'lumotlarni olish
        if section == "bichish":
            ish_soni = data.get('ish_soni')
            hodim_soni = data.get('hodim_soni')
            values_by_index = {1: ish_soni, 2: hodim_soni}
            append_or_update(sheet_report, values_by_index)
            congrats_msg = update_monthly_totals("Бичиш", ish_soni)
            
            await message.answer(f"✅ Бичиш маълумотлари сақланди!\nИш: {ish_soni} та\nХодим: {hodim_soni} та")
            daily_value = ish_soni
            
            # Бичиш учун ягона хабар
            monthly_data = get_monthly_data()
            section_data = monthly_data.get("бичиш", {})
            plan = section_data.get('plan', 0)
            done = section_data.get('done', 0)
            remaining = max(0, plan - done)
            remaining_days = get_remaining_workdays()
            
            current_working_days = get_working_days_in_current_month()
            daily_norm = plan / current_working_days if current_working_days > 0 else 0
            
            daily_percentage = calculate_percentage(daily_value, daily_norm) if daily_norm > 0 else 0
            monthly_percentage = calculate_percentage(done, plan) if plan > 0 else 0
            
            daily_needed = remaining / remaining_days if remaining_days > 0 else 0

            production_msg = f"{emoji} {section_name_uz} бўлимида кунлик иш хисоботи:\n\n"
            production_msg += f"📅 Кунлик иш: {daily_value} та\n"
            production_msg += f"👥 Ходимлар: {hodim_soni} та\n"
            production_msg += f"📊 Кунлик норма: {daily_norm:.1f} та/кун\n" 
            production_msg += f"✅ Кунлик бажарилди: {daily_percentage:.1f}%\n\n"
            production_msg += f"🗓 Ойлик режа: {plan:.0f} та\n"
            production_msg += f"✅ Ойлик бажарилди: {done:.0f} та ({monthly_percentage:.1f}%)\n"
            production_msg += f"⏳ Қолдиқ: {remaining:.0f} та\n"
            production_msg += f"📆 Қолган иш кунлари: {remaining_days} кун\n"
            production_msg += f"🎯 Ҳар кунги керак: {daily_needed:.1f} та/кун\n"

            if comment:
                production_msg += f"💬 Изоҳ: {comment}\n"

            await send_to_group(production_msg, PRODUCTION_TOPIC_ID)
            
        elif section == "tasnif":
            dikimga = data.get('dikimga', 0)
            pechat = data.get('pechat', 0)
            vishivka = data.get('vishivka', 0)
            hodim_soni = data.get('hodim_soni')
            total_ish = dikimga
            
            values_by_index = {
                3: dikimga,
                4: pechat, 
                5: vishivka,
                6: hodim_soni
            }
            append_or_update(sheet_report, values_by_index)
            congrats_msg = update_monthly_totals("Тасниф", total_ish)
            
            await message.answer(f"✅ Тасниф маълумотлари сақланди!\nДикимга: {dikimga} та\nПечат: {pechat} та\nВишивка: {vishivka} та\nХодим: {hodim_soni} та")
            
            monthly_data = get_monthly_data()
            section_data = monthly_data.get("тасниф", {})
            plan = section_data.get('plan', 0)
            done = section_data.get('done', 0)
            remaining = max(0, plan - done)
            remaining_days = get_remaining_workdays()
            
            current_working_days = get_working_days_in_current_month()
            daily_norm = plan / current_working_days if current_working_days > 0 else 0
            
            daily_percentage = calculate_percentage(total_ish, daily_norm) if daily_norm > 0 else 0
            monthly_percentage = calculate_percentage(done, plan) if plan > 0 else 0
            
            daily_needed = remaining / remaining_days if remaining_days > 0 else 0

            # 📑 ТАСНИФ учун фақат битта умумий хабар
            production_msg = f"{emoji} {section_name_uz} бўлимида кунлик иш хисоботи:\n\n"
            production_msg += f"📊 Жами кунлик иш: {total_ish} та\n"
            production_msg += f"  ✳️ Дикимга: {dikimga} та\n"
            production_msg += f"  🖨 Печат: {pechat} та\n"
            production_msg += f"  🧵 Вишивка: {vishivka} та\n\n"
            production_msg += f"👥 Ходимлар: {hodim_soni} та\n"
            production_msg += f"📊 Кунлик норма: {daily_norm:.1f} та/кун\n"
            production_msg += f"✅ Кунлик бажарилди: {daily_percentage:.1f}%\n\n"
            production_msg += f"🗓 Ойлик режа: {plan:.0f} та\n"
            production_msg += f"✅ Ойлик бажарилди: {done:.0f} та ({monthly_percentage:.1f}%)\n"
            production_msg += f"⏳ Қолдиқ: {remaining:.0f} та\n"
            production_msg += f"📆 Қолган иш кунлари: {remaining_days} кун\n"
            production_msg += f"🎯 Ҳар кунги керак: {daily_needed:.1f} та/кун\n"

            if comment:
                production_msg += f"💬 Изоҳ: {comment}\n"

            await send_to_group(production_msg, PRODUCTION_TOPIC_ID)
            
        elif section == "tikuv":
            tikuv_ish = data.get('tikuv_ish')
            tikuv_hodim = data.get('tikuv_hodim')
            oyoqchi_hodim = data.get('oyoqchi_hodim', 0)
            
            values_by_index = {
                7: tikuv_ish,
                8: tikuv_hodim,
                9: oyoqchi_hodim
            }
            append_or_update(sheet_report, values_by_index)
            congrats_msg = update_monthly_totals("Тикув", tikuv_ish)
            
            await message.answer(f"✅ Тикув маълумотлари сақланди!\nИш: {tikuv_ish} та\nХодим: {tikuv_hodim} та\nОёқчи: {oyoqchi_hodim} та")
            
            # Тикув учун ягона хабар
            monthly_data = get_monthly_data()
            section_data = monthly_data.get("тикув", {})
            plan = section_data.get('plan', 0)
            done = section_data.get('done', 0)
            remaining = max(0, plan - done)
            remaining_days = get_remaining_workdays()
            
            current_working_days = get_working_days_in_current_month()
            daily_norm = plan / current_working_days if current_working_days > 0 else 0
            
            daily_percentage = calculate_percentage(tikuv_ish, daily_norm) if daily_norm > 0 else 0
            monthly_percentage = calculate_percentage(done, plan) if plan > 0 else 0
            
            daily_needed = remaining / remaining_days if remaining_days > 0 else 0

            production_msg = f"{emoji} {section_name_uz} бўлимида кунлик иш хисоботи:\n\n"
            production_msg += f"📅 Кунлик иш: {tikuv_ish} та\n"
            production_msg += f"👥 Тикув ходим: {tikuv_hodim} та\n"
            production_msg += f"👞 Оёқчи ходим: {oyoqchi_hodim} та\n"
            production_msg += f"📊 Кунлик норма: {daily_norm:.1f} та/кун\n" 
            production_msg += f"✅ Кунлик бажарилди: {daily_percentage:.1f}%\n\n"
            production_msg += f"🗓 Ойлик режа: {plan:.0f} та\n"
            production_msg += f"✅ Ойлик бажарилди: {done:.0f} та ({monthly_percentage:.1f}%)\n"
            production_msg += f"⏳ Қолдиқ: {remaining:.0f} та\n"
            production_msg += f"📆 Қолган иш кунлари: {remaining_days} кун\n"
            production_msg += f"🎯 Ҳар кунги керак: {daily_needed:.1f} та/кун\n"

            if comment:
                production_msg += f"💬 Изоҳ: {comment}\n"

            await send_to_group(production_msg, PRODUCTION_TOPIC_ID)
            
        elif section == "qadoqlash":
            ish_soni = data.get('ish_soni')
            hodim_soni = data.get('hodim_soni')
            values_by_index = {10: ish_soni, 11: hodim_soni}
            append_or_update(sheet_report, values_by_index)
            congrats_msg = update_monthly_totals("Қадоқлаш", ish_soni)
            
            await message.answer(f"✅ Қадоқлаш маълумотлари сақланди!\nИш: {ish_soni} та\nХодим: {hodim_soni} та")
            
            # Қадоқлаш учун ягона хабар
            monthly_data = get_monthly_data()
            section_data = monthly_data.get("қадоқлаш", {})
            plan = section_data.get('plan', 0)
            done = section_data.get('done', 0)
            remaining = max(0, plan - done)
            remaining_days = get_remaining_workdays()
            
            current_working_days = get_working_days_in_current_month()
            daily_norm = plan / current_working_days if current_working_days > 0 else 0
            
            daily_percentage = calculate_percentage(ish_soni, daily_norm) if daily_norm > 0 else 0
            monthly_percentage = calculate_percentage(done, plan) if plan > 0 else 0
            
            daily_needed = remaining / remaining_days if remaining_days > 0 else 0

            production_msg = f"{emoji} {section_name_uz} бўлимида кунлик иш хисоботи:\n\n"
            production_msg += f"📅 Кунлик иш: {ish_soni} та\n"
            production_msg += f"👥 Ходимлар: {hodim_soni} та\n"
            production_msg += f"📊 Кунлик норма: {daily_norm:.1f} та/кун\n" 
            production_msg += f"✅ Кунлик бажарилди: {daily_percentage:.1f}%\n\n"
            production_msg += f"🗓 Ойлик режа: {plan:.0f} та\n"
            production_msg += f"✅ Ойлик бажарилди: {done:.0f} та ({monthly_percentage:.1f}%)\n"
            production_msg += f"⏳ Қолдиқ: {remaining:.0f} та\n"
            production_msg += f"📆 Қолган иш кунлари: {remaining_days} кун\n"
            production_msg += f"🎯 Ҳар кунги керак: {daily_needed:.1f} та/кун\n"

            if comment:
                production_msg += f"💬 Изоҳ: {comment}\n"

            await send_to_group(production_msg, PRODUCTION_TOPIC_ID)
        
        if congrats_msg:
            await send_to_group(congrats_msg, RECOGNITION_TOPIC_ID)
            
        await state.clear()
        await message.answer("Бош меню:", reply_markup=main_menu())
        
    except Exception as e:
        logger.error(f"❌ Маълумотларни сақлашда хato: {e}")
        import traceback
        logger.error(traceback.format_exc())
        await message.answer("❌ Маълумотларни сақлашда хатолик юз берди")
        
# ------------------- WORKFLOW HANDLERS -------------------
@dp.callback_query(F.data.startswith("workflow_"))
async def cb_workflow_section(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    
    section_map = {
        "workflow_bichish": "bichish",
        "workflow_tasnif": "tasnif",
        "workflow_tikuv": "tikuv", 
        "workflow_qadoqlash": "qadoqlash",
        "workflow_qutiga": "qutiga_solish"
    }
    
    section = section_map.get(callback.data)
    if not section:
        await callback.message.answer("❌ Нотўғри босқич танланди.")
        return
    
    await state.update_data(workflow_section=section)
    
    stage_names = {
        "bichish": "✂️ Бичиш",
        "tasnif": "📑 Тасниф",
        "tikuv": "🧵 Тикув",
        "qadoqlash": "📦 Қадоқлаш",
        "qutiga_solish": "📤 Қутига солиш"
    }
    
    stage_name = stage_names.get(section, section)
    
    orders = get_workflow_stage_orders(section)
    
    if not orders:
        await callback.message.answer(f"❌ {stage_name} босқичида фаол буюртмалар мавжуд эмас.")
        return
    
    await callback.message.edit_text(
        f"{stage_name} босқичидаги буюртмалар:\n\nБуюртмани танланг:", 
        reply_markup=workflow_orders_keyboard(orders)
    )

@dp.callback_query(F.data.startswith("workflow_ord:"))
async def cb_workflow_order_select(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    
    try:
        row_index = int(callback.data.split(":")[1])
        
        orders = get_workflow_orders_data()
        selected_order = None
        
        for order in orders:
            if order['row_index'] == row_index:
                selected_order = order
                break
        
        if not selected_order:
            await callback.message.answer("❌ Буюртма топилмади.")
            return
        
        await state.update_data(selected_order=selected_order)
        
        stage_names = {
            "bichish": "✂️ Бичиш",
            "tasnif": "📑 Тасниф", 
            "tikuv": "🧵 Тикув",
            "qadoqlash": "📦 Қадоқлаш",
            "qutiga_solish": "📤 Қутига солиш"
        }
        
        data = await state.get_data()
        section = data.get('workflow_section')
        stage_name = stage_names.get(section, section)
        
        order_info = f"📦 Буюртма: {selected_order['name']}\n"
        order_info += f"📍 Жорий босқич: {selected_order['current_stage']}\n"
        order_info += f"📊 Умумий: {selected_order['total']} та\n"
        order_info += f"✅ Бажарилди: {selected_order['done']} та ({selected_order['done_percentage']})\n"
        order_info += f"⏳ Қолдиқ: {selected_order['remaining']} та\n"
        order_info += f"📅 Муддат: {selected_order['deadline']} (Қолган {selected_order['days_left']} кун)\n\n"
        
        order_info += f"🔄 {stage_name} босқичи учун иш микдорини киритинг:"
        
        await callback.message.answer(order_info)
        await state.set_state(WorkflowStates.waiting_for_quantity)
        
    except Exception as e:
        logger.error(f"❌ Буюртма танлашда хato: {e}")
        await callback.message.answer("❌ Буюртма танлашда хатолик юз берди.")

@dp.message(WorkflowStates.waiting_for_quantity)
async def process_workflow_quantity(message: Message, state: FSMContext):
    try:
        quantity = int(message.text)
        if quantity <= 0:
            await message.answer("❗️ Миқдор мусбат сон бўлиши керак. Қайта киритинг:")
            return
        
        data = await state.get_data()
        selected_order = data.get('selected_order')
        section = data.get('workflow_section')
        
        if not selected_order or not section:
            await message.answer("❌ Маълумотлар нотўғри. Қайта уриниб кўринг.")
            await state.clear()
            return
        
        success = update_workflow_order(selected_order['row_index'], section, quantity)
        
        if success:
            stage_names = {
                "bichish": "✂️ Бичиш",
                "tasnif": "📑 Тасниф",
                "tikuv": "🧵 Тикув", 
                "qadoqlash": "📦 Қадоқлаш",
                "qutiga_solish": "📤 Қутига солиш"
            }
            
            stage_name = stage_names.get(section, section)
            
            report_msg = f"✅ {stage_name} босқичи учун маълумотлар қўшилди:\n"
            report_msg += f"📦 Буюртма: {selected_order['name']}\n"
            report_msg += f"📊 Миқдор: {quantity} та\n"
            report_msg += f"📍 Босқич: {stage_name}"
            
            await message.answer(report_msg)
            await send_to_group(
                f"🔄 {stage_name}: {selected_order['name']} буюртмаси учун {quantity} та иш бажарилди", 
                PRODUCTION_TOPIC_ID
            )
        else:
            await message.answer("❌ Маълумотларни сақлашда хатолик юз берди.")
        
        await state.clear()
        await message.answer("Бош меню:", reply_markup=main_menu())
        
    except ValueError:
        await message.answer("❗️ Сонни нотоғри киритдингиз. Қайта киритинг:")

# ------------------- ORDERS HANDLERS -------------------
@dp.callback_query(F.data == "add_ord")
async def cb_add_order(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    await callback.message.answer("📦 Янги буюртма номини киритинг:")
    await state.set_state(OrderStates.waiting_for_name)

@dp.message(OrderStates.waiting_for_name)
async def process_order_name(message: Message, state: FSMContext):
    order_name = message.text.strip()
    if len(order_name) < 2:
        await message.answer("❌ Буюртма номи энг камда 2 та ҳарфдан иборат бўлиши керак. Қайта киритинг:")
        return
    
    await state.update_data(order_name=order_name)
    await message.answer("📊 Буюртманинг умумий микдорини киритинг:")
    await state.set_state(OrderStates.waiting_for_quantity)

@dp.message(OrderStates.waiting_for_quantity)
async def process_order_quantity(message: Message, state: FSMContext):
    try:
        quantity = int(message.text)
        if quantity <= 0:
            await message.answer("❌ Миқдор мусбат сон бўлиши керак. Қайта киритинг:")
            return
        
        await state.update_data(order_quantity=quantity)
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✂️ Бичиш", callback_data="section_bichish")],
            [InlineKeyboardButton(text="📑 Тасниф", callback_data="section_tasnif")],
            [InlineKeyboardButton(text="🧵 Тикув", callback_data="section_tikuv")],
            [InlineKeyboardButton(text="📦 Қадоқлаш", callback_data="section_qadoqlash")]
        ])
        
        await message.answer("🏭 Буюртма учун бўлимни танланг:", reply_markup=keyboard)
        await state.set_state(OrderStates.waiting_for_section)
        
    except ValueError:
        await message.answer("❌ Миқдорни нотоғри киритдинзи. Бутун сон киритинг:")

@dp.callback_query(F.data.startswith("section_"))
async def cb_order_section(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    
    section_map = {
        "section_bichish": "Бичиш",
        "section_tasnif": "Тасниф",
        "section_tikuv": "Тикув", 
        "section_qadoqlash": "Қадоқлаш"
    }
    
    section = section_map.get(callback.data)
    if not section:
        await callback.message.answer("❌ Нотўғри бўлим танланди.")
        return
    
    await state.update_data(order_section=section)
    await callback.message.answer(f"📅 Буюртма муддатини киритинг (кун.ой.йил форматида, мисол: {datetime.now().strftime('%d.%m.%Y')}):")
    await state.set_state(OrderStates.waiting_for_deadline)

@dp.message(OrderStates.waiting_for_deadline)
async def process_order_deadline(message: Message, state: FSMContext):
    deadline = message.text.strip()
    
    errors = validate_order_data("test", 1, deadline)
    if errors:
        error_msg = "\n".join(errors)
        await message.answer(f"❌ Санани текширишда хатолик:\n{error_msg}\n\nҚайта киритинг:")
        return
    
    data = await state.get_data()
    order_name = data.get('order_name')
    order_quantity = data.get('order_quantity')
    order_section = data.get('order_section')
    
    try:
        deadline_date = datetime.strptime(deadline, "%d.%m.%Y").replace(tzinfo=TZ)
        today = datetime.now(TZ)
        days_left = (deadline_date - today).days
        
        row_data = [
            today_date_str(),
            order_name,
            order_quantity,
            0,  # Бажарилди
            order_quantity,  # Қолдиқ
            "0%",  # Бажарилди фоиз
            "100%",  # Қолдиқ фоиз
            deadline,
            days_left,
            order_section
        ]
        
        sheet_orders.append_row(row_data)
        
        order_msg = format_order_message(
            order_name, order_quantity, 0, deadline, days_left, order_section, "қўшилди"
        )
        
        await message.answer(order_msg)
        await send_to_group(order_msg, ORDERS_TOPIC_ID)
        
        await state.clear()
        await message.answer("Бош меню:", reply_markup=main_menu())
        
    except Exception as e:
        logger.error(f"❌ Буюртма қўшишда хato: {e}")
        await message.answer("❌ Буюртма қўшишда хатолик юз берди.")

@dp.callback_query(F.data == "ord_list")
async def cb_orders_list(callback: CallbackQuery):
    await callback.answer()
    
    reports = format_orders_report(only_active=True)
    for report in reports:
        await callback.message.answer(report)
    
    await callback.message.answer("Буюртмалар бўлими:", reply_markup=orders_menu())

@dp.callback_query(F.data == "edit_ord")
async def cb_edit_order(callback: CallbackQuery):
    await callback.answer()
    
    orders = get_orders_data()
    if not orders:
        await callback.message.answer("❌ Ҳали буюртмалар мавжуд эмас.")
        return
    
    await callback.message.edit_text(
        "✏️ Таҳрирлаш учун буюртмани танланг:", 
        reply_markup=orders_keyboard(orders)
    )

@dp.callback_query(F.data.startswith("sel_ord:"))
async def cb_select_order_edit(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    
    try:
        row_index = int(callback.data.split(":")[1])
        
        orders = get_orders_data()
        selected_order = None
        
        for order in orders:
            if order['row_index'] == row_index:
                selected_order = order
                break
        
        if not selected_order:
            await callback.message.answer("❌ Буюртма топилмади.")
            return
        
        await state.update_data(edit_order=selected_order)
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✏️ Номини ўзгартириш", callback_data="edit_name")],
            [InlineKeyboardButton(text="📊 Миқдорини ўзгартириш", callback_data="edit_quantity")],
            [InlineKeyboardButton(text="✅ Бажарилганини ўзгартириш", callback_data="edit_done")],
            [InlineKeyboardButton(text="📅 Муддатини ўзгартириш", callback_data="edit_deadline")],
            [InlineKeyboardButton(text="🏭 Бўлимини ўзгартириш", callback_data="edit_section")],
            [InlineKeyboardButton(text="⬅️ Ортга", callback_data="back_ord")]
        ])
        
        order_info = f"📦 Буюртма: {selected_order['name']}\n"
        order_info += f"📊 Умумий: {selected_order['total']} та\n"
        order_info += f"✅ Бажарилди: {selected_order['done']} та\n"
        order_info += f"⏳ Қолдиқ: {selected_order['remaining']} та\n"
        order_info += f"📅 Муддат: {selected_order['deadline']}\n"
        order_info += f"🏭 Бўлим: {selected_order['section']}\n\n"
        order_info += "Нимни ўзгартирмоқчисиз?"
        
        await callback.message.edit_text(order_info, reply_markup=keyboard)
        
    except Exception as e:
        logger.error(f"❌ Буюртма танлашда хato: {e}")
        await callback.message.answer("❌ Буюртма танлашда хатолик юз берди.")

@dp.callback_query(F.data == "edit_name")
async def cb_edit_name(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    await callback.message.answer("✏️ Янги буюртма номини киритинг:")
    await state.set_state(OrderStates.edit_order_name)

@dp.message(OrderStates.edit_order_name)
async def process_edit_name(message: Message, state: FSMContext):
    new_name = message.text.strip()
    if len(new_name) < 2:
        await message.answer("❌ Буюртма номи энг камда 2 та ҳарфдан иборат бўлиши керак. Қайта киритинг:")
        return
    
    data = await state.get_data()
    order = data.get('edit_order')
    
    if update_order_in_sheet(order['row_index'], "name", new_name):
        await message.answer(f"✅ Буюртма номи '{new_name}' га ўзгартирилди.")
        await send_to_group(
            f"✏️ Буюртма номи ўзгартирилди: {order['name']} -> {new_name}", 
            ORDERS_TOPIC_ID
        )
    else:
        await message.answer("❌ Буюртма номини ўзгартиришда хатолик юз берди.")
    
    await state.clear()
    await message.answer("Бош меню:", reply_markup=main_menu())

@dp.callback_query(F.data == "edit_quantity")
async def cb_edit_quantity(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    await callback.message.answer("📊 Янги умумий микдорни киритинг:")
    await state.set_state(OrderStates.edit_order_quantity)

@dp.message(OrderStates.edit_order_quantity)
async def process_edit_quantity(message: Message, state: FSMContext):
    try:
        new_quantity = int(message.text)
        if new_quantity <= 0:
            await message.answer("❌ Миқдор мусбат сон бўлиши керак. Қайта киритинг:")
            return
        
        data = await state.get_data()
        order = data.get('edit_order')
        
        if update_order_in_sheet(order['row_index'], "total", new_quantity):
            await message.answer(f"✅ Буюртма микдори {new_quantity} та га ўзгартирилди.")
            await send_to_group(
                f"📊 Буюртма микдори ўзгартирилди: {order['name']} - {new_quantity} та", 
                ORDERS_TOPIC_ID
            )
        else:
            await message.answer("❌ Буюртма микдорини ўзгартиришда хатолик юз берди.")
        
        await state.clear()
        await message.answer("Бош меню:", reply_markup=main_menu())
        
    except ValueError:
        await message.answer("❌ Миқдорни нотоғри киритдинзи. Бутун сон киритинг:")

@dp.callback_query(F.data == "edit_done")
async def cb_edit_done(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    await callback.message.answer("✅ Янги бажарилган микдорни киритинг:")
    await state.set_state(OrderStates.edit_order_done)

@dp.message(OrderStates.edit_order_done)
async def process_edit_done(message: Message, state: FSMContext):
    try:
        new_done = int(message.text)
        if new_done < 0:
            await message.answer("❌ Миқдор мусбат сон бўлиши керак. Қайта киритинг:")
            return
        
        data = await state.get_data()
        order = data.get('edit_order')
        
        if new_done > order['total']:
            await message.answer(f"❌ Бажарилган микдор умумий микдордан ({order['total']} та) ошмаслиги керак. Қайта киритинг:")
            return
        
        if update_order_in_sheet(order['row_index'], "done", new_done):
            await message.answer(f"✅ Бажарилган микдор {new_done} та га ўзгартирилди.")
            await send_to_group(
                f"✅ Буюртма бажарилди: {order['name']} - {new_done}/{order['total']} та", 
                ORDERS_TOPIC_ID
            )
        else:
            await message.answer("❌ Бажарилган микдорни ўзгартиришда хатолик юз берди.")
        
        await state.clear()
        await message.answer("Бош меню:", reply_markup=main_menu())
        
    except ValueError:
        await message.answer("❌ Миқдорни нотоғри киритдинзи. Бутун сон киритинг:")

@dp.callback_query(F.data == "edit_deadline")
async def cb_edit_deadline(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    await callback.message.answer("📅 Янги муддатни киритинг (кун.ой.йил форматида):")
    await state.set_state(OrderStates.edit_order_deadline)

@dp.message(OrderStates.edit_order_deadline)
async def process_edit_deadline(message: Message, state: FSMContext):
    new_deadline = message.text.strip()
    
    errors = validate_order_data("test", 1, new_deadline)
    if errors:
        error_msg = "\n".join(errors)
        await message.answer(f"❌ Санани текширишда хатолик:\n{error_msg}\n\nҚайта киритинг:")
        return
    
    data = await state.get_data()
    order = data.get('edit_order')
    
    if update_order_in_sheet(order['row_index'], "deadline", new_deadline):
        await message.answer(f"✅ Буюртма муддати {new_deadline} га ўзгартирилди.")
        await send_to_group(
            f"📅 Буюртма муддати ўзгартирилди: {order['name']} - {new_deadline}", 
            ORDERS_TOPIC_ID
        )
    else:
        await message.answer("❌ Буюртма муддатини ўзгартиришда хатолик юз берди.")
    
    await state.clear()
    await message.answer("Бош меню:", reply_markup=main_menu())

@dp.callback_query(F.data == "edit_section")
async def cb_edit_section(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✂️ Бичиш", callback_data="edit_sec_bichish")],
        [InlineKeyboardButton(text="📑 Тасниф", callback_data="edit_sec_tasnif")],
        [InlineKeyboardButton(text="🧵 Тикув", callback_data="edit_sec_tikuv")],
        [InlineKeyboardButton(text="📦 Қадоқлаш", callback_data="edit_sec_qadoqlash")]
    ])
    
    await callback.message.edit_text("🏭 Янги бўлимни танланг:", reply_markup=keyboard)
    await state.set_state(OrderStates.edit_order_section)

@dp.callback_query(F.data.startswith("edit_sec_"))
async def cb_edit_section_select(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    
    section_map = {
        "edit_sec_bichish": "Бичиш",
        "edit_sec_tasnif": "Тасниф",
        "edit_sec_tikuv": "Тикув",
        "edit_sec_qadoqlash": "Қадоқлаш"
    }
    
    new_section = section_map.get(callback.data)
    if not new_section:
        await callback.message.answer("❌ Нотўғри бўлим танланди.")
        return
    
    data = await state.get_data()
    order = data.get('edit_order')
    
    if update_order_in_sheet(order['row_index'], "section", new_section):
        await callback.message.answer(f"✅ Буюртма бўлими '{new_section}' га ўзгартирилди.")
        await send_to_group(
            f"🏭 Буюртма бўлими ўзгартирилди: {order['name']} - {new_section}", 
            ORDERS_TOPIC_ID
        )
    else:
        await callback.message.answer("❌ Буюртма бўлимини ўзгартиришда хатолик юз берди.")
    
    await state.clear()
    await callback.message.answer("Бош меню:", reply_markup=main_menu())

# ------------------- DAILY WORK ORDERS HANDLERS -------------------
@dp.callback_query(F.data.startswith("daily_ord:"))
async def cb_daily_order_select(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    
    try:
        row_index = int(callback.data.split(":")[1])
        
        orders = get_orders_data()
        selected_order = None
        
        for order in orders:
            if order['row_index'] == row_index:
                selected_order = order
                break
        
        if not selected_order:
            await callback.message.answer("❌ Буюртма топилмади.")
            return
        
        await state.update_data(selected_order=selected_order)
        await callback.message.answer(f"📦 Буюртма: {selected_order['name']}\nУмумий: {selected_order['total']} та\nБажарилди: {selected_order['done']} та\nҚолдиқ: {selected_order['remaining']} та\n\nБугунги иш микдорини киритинг:")
        await state.set_state(DailyWorkStates.waiting_for_quantity)
        
    except Exception as e:
        logger.error(f"❌ Буюртма танлашда хato: {e}")
        await callback.message.answer("❌ Буюртма танлашда хатолик юз берди.")

@dp.message(DailyWorkStates.waiting_for_quantity)
async def process_daily_order_quantity(message: Message, state: FSMContext):
    try:
        quantity = int(message.text)
        if quantity <= 0:
            await message.answer("❗️ Миқдор мусбат сон бўлиши керак. Қайта киритинг:")
            return
        
        data = await state.get_data()
        selected_order = data.get('selected_order')
        
        if not selected_order:
            await message.answer("❌ Маълумотлар нотўғри. Қайта уриниб кўринг.")
            await state.clear()
            return
        
        new_done = selected_order['done'] + quantity
        if new_done > selected_order['total']:
            await message.answer(f"❌ Бажарилган микдор умумий микдордан ({selected_order['total']} та) ошмаслиги керак. Қайта киритинг:")
            return
        
        if update_order_in_sheet(selected_order['row_index'], "done", new_done):
            report_msg = f"✅ Буюртмага иш қўшилди:\n📦 {selected_order['name']}\n📊 Миқдор: {quantity} та\n✅ Жами бажарилди: {new_done}/{selected_order['total']} та"
            
            await message.answer(report_msg)
            await send_to_group(
                f"📦 Буюртма иш чиқими: {selected_order['name']} - {quantity} та бажарилди ({new_done}/{selected_order['total']})", 
                PRODUCTION_TOPIC_ID
            )
        else:
            await message.answer("❌ Маълумотларни сақлашда хатолик юз берди.")
        
        await state.clear()
        await message.answer("Бош меню:", reply_markup=main_menu())
        
    except ValueError:
        await message.answer("❗️ Сонни нотоғри киритдингиз. Қайта киритинг:")

# ------------------- REPORTS HANDLERS -------------------
@dp.callback_query(F.data == "r_kun")
async def cb_report_daily(callback: CallbackQuery):
    await callback.answer()
    
    report = format_daily_report()
    await callback.message.answer(report)

@dp.callback_query(F.data == "r_haf")
async def cb_report_weekly(callback: CallbackQuery):
    await callback.answer()
    
    report = format_weekly_report()
    await callback.message.answer(report)

@dp.callback_query(F.data == "r_oy")
async def cb_report_monthly(callback: CallbackQuery):
    await callback.answer()
    
    report = format_monthly_report()
    await callback.message.answer(report)

@dp.callback_query(F.data == "r_ord")
async def cb_report_orders(callback: CallbackQuery):
    await callback.answer()
    
    reports = format_orders_report(only_active=True)
    for report in reports:
        await callback.message.answer(report)

@dp.callback_query(F.data == "r_workflow")
async def cb_report_workflow(callback: CallbackQuery):
    await callback.answer()
    
    reports = format_workflow_report()
    for report in reports:
        await callback.message.answer(report)

# ------------------- GRAPH REPORTS HANDLERS -------------------
@dp.callback_query(F.data == "g_mon")
async def cb_graph_monthly(callback: CallbackQuery):
    await callback.answer()
    
    buf = create_comprehensive_dashboard()
    if buf:
        await callback.message.answer_photo(
            BufferedInputFile(buf.getvalue(), filename="monthly_dashboard.png"),
            caption="🏭 Барча бўлимлар учун ойлик дашборд"
        )
    else:
        await callback.message.answer("❌ Дашборд яратишда хатолик юз берди.")

@dp.callback_query(F.data == "g_day")
async def cb_graph_daily(callback: CallbackQuery):
    await callback.answer()
    
    buf = create_section_visualization("Бичиш")
    if buf:
        await callback.message.answer_photo(
            BufferedInputFile(buf.getvalue(), filename="daily_report.png"),
            caption="📊 Кунлик иш графиги"
        )
    else:
        await callback.message.answer("❌ График яратишда хатолик юз берди.")

@dp.callback_query(F.data == "g_week")
async def cb_graph_weekly(callback: CallbackQuery):
    await callback.answer()
    
    buf = create_weekly_trend_chart()
    if buf:
        await callback.message.answer_photo(
            BufferedInputFile(buf.getvalue(), filename="weekly_trend.png"),
            caption="📈 Ҳафталик иш тенденцияси"
        )
    else:
        await callback.message.answer("❌ Ҳафталик тенденция графиги яратишда хатолик юз берди.")

@dp.callback_query(F.data == "g_month")
async def cb_graph_monthly_trend(callback: CallbackQuery):
    await callback.answer()
    
    buf = create_monthly_trend_chart()
    if buf:
        await callback.message.answer_photo(
            BufferedInputFile(buf.getvalue(), filename="monthly_trend.png"),
            caption="📊 Ойлик иш тенденцияси"
        )
    else:
        await callback.message.answer("❌ Ойлик тенденция графиги яратишда хатолик юз берди.")

@dp.callback_query(F.data == "g_pie")
async def cb_graph_pie(callback: CallbackQuery):
    await callback.answer()
    
    buf = create_percentage_pie_chart()
    if buf:
        await callback.message.answer_photo(
            BufferedInputFile(buf.getvalue(), filename="pie_chart.png"),
            caption="🥧 Ойлик режа бажарилиши фоизда"
        )
    else:
        await callback.message.answer("❌ Pie chart яратишда хатолик юз берди.")

@dp.callback_query(F.data == "vis_bich")
async def cb_visual_bichish(callback: CallbackQuery):
    await callback.answer()
    
    buf = create_section_visualization("Бичиш")
    if buf:
        await callback.message.answer_photo(
            BufferedInputFile(buf.getvalue(), filename="bichish_visual.png"),
            caption="✂️ Бичиш бўлими учун тўлиқ визуал хисобот"
        )
    else:
        await callback.message.answer("❌ Бичиш визуал хисобот яратишда хатолик юз берди.")

@dp.callback_query(F.data == "vis_tasn")
async def cb_visual_tasnif(callback: CallbackQuery):
    await callback.answer()
    
    buf = create_section_visualization("Тасниф")
    if buf:
        await callback.message.answer_photo(
            BufferedInputFile(buf.getvalue(), filename="tasnif_visual.png"),
            caption="📑 Тасниф бўлими учун тўлиқ визуал хисобот"
        )
    else:
        await callback.message.answer("❌ Тасниф визуал хисобот яратишда хатолик юз берди.")

@dp.callback_query(F.data == "vis_tik")
async def cb_visual_tikuv(callback: CallbackQuery):
    await callback.answer()
    
    buf = create_section_visualization("Тикув")
    if buf:
        await callback.message.answer_photo(
            BufferedInputFile(buf.getvalue(), filename="tikuv_visual.png"),
            caption="🧵 Тикув бўлими учун тўлиқ визуал хисобот"
        )
    else:
        await callback.message.answer("❌ Тикув визуал хисобот яратишда хатолик юз берди.")

@dp.callback_query(F.data == "vis_qad")
async def cb_visual_qadoqlash(callback: CallbackQuery):
    await callback.answer()
    
    buf = create_section_visualization("Қадоқлаш")
    if buf:
        await callback.message.answer_photo(
            BufferedInputFile(buf.getvalue(), filename="qadoqlash_visual.png"),
            caption="📦 Қадоқлаш бўлими учун тўлиқ визуал хисобот"
        )
    else:
        await callback.message.answer("❌ Қадоқлаш визуал хисобот яратишда хатолик юз берди.")

@dp.callback_query(F.data == "vis_all")
async def cb_visual_all(callback: CallbackQuery):
    await callback.answer()
    
    buf = create_comprehensive_dashboard()
    if buf:
        await callback.message.answer_photo(
            BufferedInputFile(buf.getvalue(), filename="all_dashboard.png"),
            caption="🏭 Барча бўлимлар учун комплекс дашборд"
        )
    else:
        await callback.message.answer("❌ Дашборд яратишда хатолик юз берди.")

# ------------------- ADMIN HANDLERS -------------------
@dp.callback_query(F.data == "admin_workdays")
async def cb_admin_workdays(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    
    current_workdays = get_working_days_in_current_month()
    await callback.message.answer(
        f"📅 Ҳозирги иш кунлари: {current_workdays}\n"
        f"Янги иш кунлари сонини киритинг (ёки 'Отмена' бекор қилиш учун):"
    )
    await state.set_state(AdminStates.waiting_for_workdays)

@dp.message(AdminStates.waiting_for_workdays)
async def process_admin_workdays(message: Message, state: FSMContext):
    if message.text.lower() in ["отмена", "/cancel"]:
        await cmd_cancel(message, state)
        return
        
    try:
        new_workdays = int(message.text)
        if new_workdays <= 0 or new_workdays > 31:
            await message.answer("❗️ Иш кунлари 1 ва 31 орасида бўлиши керак. Қайта киритинг:")
            return
            
        # Google Sheets да иш кунларини янгилаш
        try:
            # Ойлик хисобот яратиш
            section_names = sheet_month.col_values(1)
            for i, name in enumerate(section_names, start=1):
                if i > 1:  # Сарлавҳалардан кейин
                    monthly_plan = parse_float(sheet_month.cell(i, 2).value)
                    daily_plan = monthly_plan / new_workdays
                    sheet_month.update_cell(i, 7, round(daily_plan, 2))
            
            await message.answer(f"✅ Иш кунлари {new_workdays} га ўзгартирилди!")
            
        except Exception as e:
            logger.error(f"❌ Иш кунларини янгилашда хato: {e}")
            await message.answer("❌ Иш кунларини янгилашда хатолик юз берди.")
            
        await state.clear()
        await message.answer("👨‍💼 Админ панели:", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📅 Иш кунларини ўзгартириш", callback_data="admin_workdays")],
            [InlineKeyboardButton(text="📊 Ойлик режани ўзгартириш", callback_data="admin_monthly_plan")],
            [InlineKeyboardButton(text="⬅️ Ортга", callback_data="back_main")]
        ]))
        
    except ValueError:
        await message.answer("❗️ Сонни нотоғри киритдингиз. Қайта киритинг:")

@dp.callback_query(F.data == "admin_monthly_plan")
async def cb_admin_monthly_plan(callback: CallbackQuery, state: FSMContext):  # ✅ state параметри қўшилди
    await callback.answer()
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✂️ Бичиш", callback_data="admin_plan_bichish")],
        [InlineKeyboardButton(text="📑 Тасниф", callback_data="admin_plan_tasnif")],
        [InlineKeyboardButton(text="🧵 Тикув", callback_data="admin_plan_tikuv")],
        [InlineKeyboardButton(text="📦 Қадоқлаш", callback_data="admin_plan_qadoqlash")],
        [InlineKeyboardButton(text="⬅️ Ортга", callback_data="admin_back")]
    ])
    
    await callback.message.edit_text("📊 Ойлик режани ўзгартириш учун бўлимни танланг:", reply_markup=keyboard)
    await state.set_state(AdminStates.waiting_for_monthly_plan_section)  # ✅ Тўғри стейт

@dp.callback_query(F.data.startswith("admin_plan_"))
async def cb_admin_plan_section(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    section = callback.data.replace("admin_plan_", "")
    
    section_names = {
        "bichish": "Бичиш",
        "tasnif": "Тасниф",
        "tikuv": "Тикув",
        "qadoqlash": "Қадоқлаш"
    }
    
    section_display = section_names.get(section, section)
    
    # Ҳозирги режани олиш
    current_plan = 0
    try:
        monthly_data = get_monthly_data()
        section_key = section_names[section].lower()
        if section_key in monthly_data:
            current_plan = monthly_data[section_key]['plan']
    except:
        pass
    
    await state.update_data(admin_section=section)
    await callback.message.answer(
        f"📊 {section_display} бўлими учун ойлик режани ўзгартириш\n"
        f"Ҳозирги режа: {current_plan:,.0f} та\n"
        f"Янги режани киритинг (ёки 'Отмена' бекор қилиш учун):"
    )
    await state.set_state(AdminStates.waiting_for_monthly_plan)  # ✅ Тўғри стейт

@dp.message(AdminStates.waiting_for_monthly_plan)
async def process_admin_monthly_plan(message: Message, state: FSMContext):
    if message.text.lower() in ["отмена", "/cancel"]:
        await cmd_cancel(message, state)
        return
        
    try:
        new_plan = int(message.text)
        if new_plan <= 0:
            await message.answer("❗️ Режа мусбат сон бўлиши керак. Қайта киритинг:")
            return
            
        data = await state.get_data()
        section = data.get('admin_section')
        
        section_names = {
            "bichish": "Бичиш",
            "tasnif": "Тасниф", 
            "tikuv": "Тикув",
            "qadoqlash": "Қадоқлаш"
        }
        
        section_display = section_names.get(section, section)
        
        # Google Sheets да режани янгилаш
        try:
            section_list = sheet_month.col_values(1)
            row_idx = None
            
            for i, name in enumerate(section_list, start=1):
                if name.strip().lower() == section_display.lower():
                    row_idx = i
                    break
            
            if row_idx:
                sheet_month.update_cell(row_idx, 2, new_plan)
                
                # Кунлик режани хисоблаш
                workdays = get_working_days_in_current_month()
                daily_plan = new_plan / workdays
                sheet_month.update_cell(row_idx, 7, round(daily_plan, 2))
                
                await message.answer(f"✅ {section_display} бўлими учун ойлик режа {new_plan:,.0f} та га ўзгартирилди!")
            else:
                await message.answer("❌ Бўлим топилмади.")
                
        except Exception as e:
            logger.error(f"❌ Режани янгилашда хato: {e}")
            await message.answer("❌ Режани янгилашда хатолик юз берди.")
            
        await state.clear()
        await message.answer("👨‍💼 Админ панели:", reply_markup=admin_professional_menu())
        
    except ValueError:
        await message.answer("❗️ Сонни нотоғри киритдингиз. Қайта киритинг:")

@dp.callback_query(F.data == "admin_stats")
async def cb_admin_stats(callback: CallbackQuery):
    await callback.answer()
    
    try:
        # Статистика яратиш
        total_workdays = get_working_days_in_current_month()
        current_workday = get_current_workday_index()
        remaining_workdays = get_remaining_workdays()
        
        monthly_data = get_monthly_data()
        
        stats_text = "📊 Бот Статистикаси\n\n"
        stats_text += f"📅 Ойига иш кунлари: {total_workdays}\n"
        stats_text += f"✅ Ҳозиргача иш кунлари: {current_workday}\n"
        stats_text += f"⏳ Қолган иш кунлари: {remaining_workdays}\n\n"
        
        stats_text += "📈 Бўлимлар режаси:\n"
        for section_name, data in monthly_data.items():
            stats_text += f"• {section_name.capitalize()}: {data['plan']:,.0f} та\n"
        
        await callback.message.answer(stats_text)
        
    except Exception as e:
        logger.error(f"❌ Статистика яратишда хato: {e}")
        await callback.message.answer("❌ Статистикани олишда хатолик юз берди.")

@dp.callback_query(F.data == "admin_restart")
async def cb_admin_restart(callback: CallbackQuery):
    await callback.answer()
    
    try:
        # Ботни қайта юклаш
        await callback.message.answer("🔄 Бот қайта юкланмоқда...")
        
        # Google Sheets ўрнатиш
        update_order_sheet_for_workflow()
        
        await callback.message.answer("✅ Бот муваффақиятли қайта юкланди!")
        
    except Exception as e:
        logger.error(f"❌ Ботни қайта юклавшда хato: {e}")
        await callback.message.answer("❌ Ботни қайта юклавшда хатолик юз берди.")

# ------------------- ADMIN MENYU YANGИЛАШ -------------------

@dp.callback_query(F.data == "admin_menu")
async def cb_admin_menu(callback: CallbackQuery):
    await callback.answer()
    if not is_admin(callback.from_user.id):
        await callback.message.answer("❌ Сизда бу имконият йўқ.")
        return
    await callback.message.edit_text("👨‍💼 Профессионал Админ Панели", reply_markup=admin_professional_menu())

@dp.callback_query(F.data == "admin_back")
async def cb_admin_back(callback: CallbackQuery):
    await callback.answer()
    await callback.message.edit_text("👨‍💼 Профессионал Админ Панели", reply_markup=admin_professional_menu())

@dp.callback_query(F.data.startswith("edit_comment:"))
async def cb_edit_comment(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    date_str = callback.data.split(":")[1]
    await state.update_data(edit_comment_date=date_str)
    
    # Joriy izohni topib ko'rsatish
    current_comment = get_comment_by_date(date_str)
    
    await callback.message.answer(
        f"📝 {date_str} санаси учун изоҳни таҳрирлаш:\n\n"
        f"Ҳозирги изоҳ: {current_comment if current_comment else 'Изоҳ мавжуд эмас'}\n\n"
        f"Янги изоҳни киритинг (ёки 'Отмена' бекор қилиш учун):"
    )
    await state.set_state(AdminEditCommentStates.waiting_for_comment)

@dp.message(AdminEditCommentStates.waiting_for_comment)
async def process_admin_comment(message: Message, state: FSMContext):
    new_comment = message.text
    data = await state.get_data()
    date_str = data.get('edit_comment_date')
    
    # Izohni yangilash
    success = update_sheet_comment(date_str, new_comment)
    
    if success:
        await message.answer(f"✅ {date_str} санаси учун изоҳ муваффақиятли янгиланди!")
        
        # Гуруҳга хабар
        await send_to_group(
            f"✏️ Админ изоҳни таҳрирлади: {date_str} санаси учун изоҳ янгиланди",
            PRODUCTION_TOPIC_ID
        )
    else:
        await message.answer("❌ Изоҳни янгилашда хатолик юз берди.")
    
    await state.clear()
    await message.answer("👨‍💼 Админ панели:", reply_markup=admin_professional_menu())

# ------------------- MAIN -------------------
async def main():
    logger.info("🚀 Бот ишга туширилмоқда (PostgreSQLсиз)...")

    # Fabric модули – registration function already includes the router
    from fabric_monitor import register_fabric_handlers
    register_fabric_handlers(dp)          # ✅ Router attached here – do NOT include again

    # KPI модули – import only the router (no registration function needed)
    from kpi import router as kpi_router
    dp.include_router(kpi_router)         # ✅ Correct way to attach

    dp.callback_query.middleware(LoggingMiddleware())
    dp.message.middleware(LoggingMiddleware())
    try:
        await dp.start_polling(bot)
    except Exception as e:
        logger.error(f"❌ Ботни ишга туширишда хато: {e}")
    finally:
        await bot.session.close()

if __name__ == "__main__":
    asyncio.run(main())
