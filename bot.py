import asyncio
import logging
import os
import re
import sqlite3
from pathlib import Path
from typing import Optional

from aiogram import Bot, Dispatcher, F
from aiogram.enums import ParseMode
from aiogram.filters import CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import Message, ReplyKeyboardMarkup, KeyboardButton
from aiogram.client.default import DefaultBotProperties
from dotenv import load_dotenv

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
DB_PATH = os.getenv("DB_PATH", "/data/crm.sqlite").strip() or "/data/crm.sqlite"

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is not set")

Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def conn():
    c = sqlite3.connect(DB_PATH)
    c.row_factory = sqlite3.Row
    return c


def init_db():
    with conn() as db:
        db.executescript(
            """
            PRAGMA foreign_keys = ON;

            CREATE TABLE IF NOT EXISTS users (
                telegram_id INTEGER PRIMARY KEY,
                full_name TEXT,
                workspace_id INTEGER,
                role TEXT DEFAULT 'guest',
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(workspace_id) REFERENCES workspaces(id)
            );

            CREATE TABLE IF NOT EXISTS workspaces (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                owner_telegram_id INTEGER NOT NULL UNIQUE,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS clients (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                workspace_id INTEGER NOT NULL,
                last_name TEXT,
                first_name TEXT,
                middle_name TEXT,
                phone TEXT,
                social_contact TEXT,
                rating INTEGER DEFAULT 3 CHECK(rating BETWEEN 0 AND 5),
                bonus_points INTEGER DEFAULT 0,
                note TEXT,
                created_by INTEGER,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(workspace_id) REFERENCES workspaces(id) ON DELETE CASCADE
            );

            CREATE INDEX IF NOT EXISTS idx_clients_workspace_phone ON clients(workspace_id, phone);
            CREATE INDEX IF NOT EXISTS idx_clients_workspace_rating ON clients(workspace_id, rating);
            """
        )


def normalize_phone(value: str) -> str:
    return re.sub(r"\D", "", value or "")


def user_record(telegram_id: int):
    with conn() as db:
        return db.execute("SELECT * FROM users WHERE telegram_id = ?", (telegram_id,)).fetchone()


def workspace_record(workspace_id: int):
    with conn() as db:
        return db.execute("SELECT * FROM workspaces WHERE id = ?", (workspace_id,)).fetchone()


def create_workspace(owner_id: int, owner_name: str, workspace_name: str):
    with conn() as db:
        cur = db.execute(
            "INSERT INTO workspaces(name, owner_telegram_id) VALUES(?, ?)",
            (workspace_name.strip(), owner_id),
        )
        workspace_id = cur.lastrowid
        db.execute(
            "INSERT OR REPLACE INTO users(telegram_id, full_name, workspace_id, role) VALUES(?, ?, ?, 'owner')",
            (owner_id, owner_name, workspace_id),
        )
        db.commit()
        return workspace_id


def add_staff(owner_workspace_id: int, telegram_id: int, full_name: str):
    with conn() as db:
        db.execute(
            "INSERT OR REPLACE INTO users(telegram_id, full_name, workspace_id, role) VALUES(?, ?, ?, 'staff')",
            (telegram_id, full_name, owner_workspace_id),
        )
        db.commit()


def add_client(workspace_id: int, data: dict, created_by: int):
    with conn() as db:
        db.execute(
            """
            INSERT INTO clients(
                workspace_id, last_name, first_name, middle_name, phone, social_contact,
                rating, bonus_points, note, created_by
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                workspace_id,
                data.get("last_name", "").strip(),
                data.get("first_name", "").strip(),
                data.get("middle_name", "").strip(),
                normalize_phone(data.get("phone", "")),
                data.get("social_contact", "").strip(),
                int(data.get("rating", 3)),
                int(data.get("bonus_points", 0)),
                data.get("note", "").strip(),
                created_by,
            ),
        )
        db.commit()


def search_clients(workspace_id: int, query: str):
    q = query.strip()
    qp = normalize_phone(q)
    with conn() as db:
        if qp:
            rows = db.execute(
                """
                SELECT * FROM clients
                WHERE workspace_id = ?
                  AND replace(replace(replace(ifnull(phone,''), '+',''), ' ', ''), '-', '') LIKE ?
                ORDER BY updated_at DESC, id DESC
                LIMIT 10
                """,
                (workspace_id, f"%{qp}%"),
            ).fetchall()
            if rows:
                return rows
        return db.execute(
            """
            SELECT * FROM clients
            WHERE workspace_id = ?
              AND (
                lower(ifnull(last_name,'')) LIKE lower(?) OR
                lower(ifnull(first_name,'')) LIKE lower(?) OR
                lower(ifnull(middle_name,'')) LIKE lower(?) OR
                lower(ifnull(social_contact,'')) LIKE lower(?)
              )
            ORDER BY updated_at DESC, id DESC
            LIMIT 10
            """,
            (workspace_id, f"%{q}%", f"%{q}%", f"%{q}%", f"%{q}%"),
        ).fetchall()


def low_rating_clients(workspace_id: int):
    with conn() as db:
        return db.execute(
            """
            SELECT * FROM clients
            WHERE workspace_id = ? AND rating <= 2
            ORDER BY rating ASC, updated_at DESC, id DESC
            LIMIT 100
            """,
            (workspace_id,),
        ).fetchall()


def global_low_rating_clients():
    with conn() as db:
        return db.execute(
            """
            SELECT c.*, w.name as workspace_name
            FROM clients c
            JOIN workspaces w ON w.id = c.workspace_id
            WHERE c.rating <= 2
            ORDER BY c.rating ASC, c.updated_at DESC, c.id DESC
            LIMIT 200
            """
        ).fetchall()


def stats_for_workspace(workspace_id: int):
    with conn() as db:
        total = db.execute("SELECT COUNT(*) FROM clients WHERE workspace_id = ?", (workspace_id,)).fetchone()[0]
        low = db.execute("SELECT COUNT(*) FROM clients WHERE workspace_id = ? AND rating <= 2", (workspace_id,)).fetchone()[0]
        return total, low


def main_keyboard(has_workspace: bool) -> ReplyKeyboardMarkup:
    if not has_workspace:
        return ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="🏢 Створити кабінет")]],
            resize_keyboard=True,
        )
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="➕ Додати клієнта"), KeyboardButton(text="🔎 Пошук")],
            [KeyboardButton(text="⚠️ Низький рейтинг"), KeyboardButton(text="🌐 Глобальний список 0-2")],
            [KeyboardButton(text="👥 Додати працівника"), KeyboardButton(text="📊 Статистика")],
        ],
        resize_keyboard=True,
    )


class CreateWorkspace(StatesGroup):
    waiting_for_name = State()


class AddStaff(StatesGroup):
    waiting_for_telegram_id = State()
    waiting_for_name = State()


class AddClient(StatesGroup):
    last_name = State()
    first_name = State()
    middle_name = State()
    phone = State()
    social_contact = State()
    rating = State()
    bonus_points = State()
    note = State()


class SearchClient(StatesGroup):
    query = State()


dp = Dispatcher(storage=MemoryStorage())


@dp.message(CommandStart())
async def start(message: Message, state: FSMContext):
    await state.clear()
    user = user_record(message.from_user.id)
    text = (
        "Вітаю. Це Telegram CRM бот.\n\n"
        "Тут один спільний бот, але окремі кабінети для кожного власника."
    )
    if not user:
        text += "\n\nНатисніть <b>🏢 Створити кабінет</b>, щоб почати."
    else:
        ws = workspace_record(user["workspace_id"]) if user["workspace_id"] else None
        text += f"\n\nВаш кабінет: <b>{ws['name']}</b>" if ws else ""
    await message.answer(text, reply_markup=main_keyboard(bool(user and user['workspace_id'])), parse_mode=ParseMode.HTML)


@dp.message(F.text == "🏢 Створити кабінет")
async def create_cabinet(message: Message, state: FSMContext):
    user = user_record(message.from_user.id)
    if user and user["workspace_id"]:
        await message.answer("У вас уже є кабінет.", reply_markup=main_keyboard(True))
        return
    await state.set_state(CreateWorkspace.waiting_for_name)
    await message.answer("Введіть назву вашої системи або магазину.")


@dp.message(CreateWorkspace.waiting_for_name)
async def save_cabinet(message: Message, state: FSMContext):
    name = message.text.strip()
    if len(name) < 2:
        await message.answer("Назва занадто коротка. Введіть нормальну назву.")
        return
    workspace_id = create_workspace(message.from_user.id, message.from_user.full_name, name)
    await state.clear()
    await message.answer(
        f"Готово. Кабінет <b>{name}</b> створено. ID: <code>{workspace_id}</code>",
        parse_mode=ParseMode.HTML,
        reply_markup=main_keyboard(True),
    )


def ensure_workspace(message: Message) -> Optional[sqlite3.Row]:
    user = user_record(message.from_user.id)
    if not user or not user["workspace_id"]:
        return None
    return user


@dp.message(F.text == "👥 Додати працівника")
async def add_staff_start(message: Message, state: FSMContext):
    user = ensure_workspace(message)
    if not user:
        await message.answer("Спочатку створіть кабінет.", reply_markup=main_keyboard(False))
        return
    if user["role"] != "owner":
        await message.answer("Додавати працівників може лише власник кабінету.")
        return
    await state.set_state(AddStaff.waiting_for_telegram_id)
    await message.answer("Надішліть Telegram ID працівника цифрами. Працівник має хоча б раз відкрити бота /start.")


@dp.message(AddStaff.waiting_for_telegram_id)
async def add_staff_id(message: Message, state: FSMContext):
    raw = message.text.strip()
    if not raw.isdigit():
        await message.answer("Потрібен Telegram ID цифрами.")
        return
    await state.update_data(telegram_id=int(raw))
    await state.set_state(AddStaff.waiting_for_name)
    await message.answer("Тепер введіть ім'я працівника або як його підписати в системі.")


@dp.message(AddStaff.waiting_for_name)
async def add_staff_name(message: Message, state: FSMContext):
    data = await state.get_data()
    owner = user_record(message.from_user.id)
    add_staff(owner["workspace_id"], data["telegram_id"], message.text.strip())
    await state.clear()
    await message.answer("Працівника додано до вашого кабінету.", reply_markup=main_keyboard(True))


@dp.message(F.text == "➕ Додати клієнта")
async def add_client_start(message: Message, state: FSMContext):
    user = ensure_workspace(message)
    if not user:
        await message.answer("Спочатку створіть кабінет.", reply_markup=main_keyboard(False))
        return
    await state.clear()
    await state.set_state(AddClient.last_name)
    await message.answer("Прізвище клієнта:")


@dp.message(AddClient.last_name)
async def add_client_last(message: Message, state: FSMContext):
    await state.update_data(last_name=message.text)
    await state.set_state(AddClient.first_name)
    await message.answer("Ім'я:")


@dp.message(AddClient.first_name)
async def add_client_first(message: Message, state: FSMContext):
    await state.update_data(first_name=message.text)
    await state.set_state(AddClient.middle_name)
    await message.answer("По батькові. Якщо немає - напишіть -")


@dp.message(AddClient.middle_name)
async def add_client_middle(message: Message, state: FSMContext):
    value = "" if message.text.strip() == "-" else message.text
    await state.update_data(middle_name=value)
    await state.set_state(AddClient.phone)
    await message.answer("Телефон. Якщо немає - напишіть -")


@dp.message(AddClient.phone)
async def add_client_phone(message: Message, state: FSMContext):
    value = "" if message.text.strip() == "-" else message.text
    await state.update_data(phone=value)
    await state.set_state(AddClient.social_contact)
    await message.answer("Instagram / Telegram / інший контакт. Якщо немає - напишіть -")


@dp.message(AddClient.social_contact)
async def add_client_social(message: Message, state: FSMContext):
    value = "" if message.text.strip() == "-" else message.text
    await state.update_data(social_contact=value)
    await state.set_state(AddClient.rating)
    await message.answer("Рейтинг 0-5:")


@dp.message(AddClient.rating)
async def add_client_rating(message: Message, state: FSMContext):
    value = message.text.strip()
    if value not in {"0", "1", "2", "3", "4", "5"}:
        await message.answer("Введіть рейтинг числом від 0 до 5.")
        return
    await state.update_data(rating=int(value))
    await state.set_state(AddClient.bonus_points)
    await message.answer("ББ 🎁. Введіть кількість бонусних балів або 0:")


@dp.message(AddClient.bonus_points)
async def add_client_bonus(message: Message, state: FSMContext):
    value = message.text.strip()
    if not re.fullmatch(r"-?\d+", value):
        await message.answer("Введіть число.")
        return
    await state.update_data(bonus_points=int(value))
    await state.set_state(AddClient.note)
    await message.answer("Примітка. Якщо немає - напишіть -")


@dp.message(AddClient.note)
async def add_client_note(message: Message, state: FSMContext):
    value = "" if message.text.strip() == "-" else message.text
    await state.update_data(note=value)
    data = await state.get_data()
    user = user_record(message.from_user.id)
    add_client(user["workspace_id"], data, message.from_user.id)
    await state.clear()
    fio = " ".join(filter(None, [data.get("last_name"), data.get("first_name"), data.get("middle_name")]))
    await message.answer(f"Клієнта <b>{fio}</b> додано.", parse_mode=ParseMode.HTML, reply_markup=main_keyboard(True))


@dp.message(F.text == "🔎 Пошук")
async def search_start(message: Message, state: FSMContext):
    user = ensure_workspace(message)
    if not user:
        await message.answer("Спочатку створіть кабінет.", reply_markup=main_keyboard(False))
        return
    await state.set_state(SearchClient.query)
    await message.answer("Введіть телефон, прізвище або нік у соцмережі.")


@dp.message(SearchClient.query)
async def search_process(message: Message, state: FSMContext):
    user = user_record(message.from_user.id)
    rows = search_clients(user["workspace_id"], message.text)
    await state.clear()
    if not rows:
        await message.answer("Нічого не знайдено.", reply_markup=main_keyboard(True))
        return
    texts = []
    for r in rows:
        fio = " ".join(filter(None, [r["last_name"], r["first_name"], r["middle_name"]])) or "Без ПІБ"
        contact = r["phone"] or r["social_contact"] or "немає"
        texts.append(
            f"<b>{fio}</b>\n"
            f"Контакт: <code>{contact}</code>\n"
            f"Рейтинг: <b>{r['rating']}/5</b>\n"
            f"ББ 🎁: <b>{r['bonus_points']}</b>\n"
            f"Примітка: {r['note'] or '-'}"
        )
    await message.answer("\n\n".join(texts), parse_mode=ParseMode.HTML, reply_markup=main_keyboard(True))


@dp.message(F.text == "⚠️ Низький рейтинг")
async def low_rating(message: Message):
    user = ensure_workspace(message)
    if not user:
        await message.answer("Спочатку створіть кабінет.", reply_markup=main_keyboard(False))
        return
    rows = low_rating_clients(user["workspace_id"])
    if not rows:
        await message.answer("У вашому кабінеті немає клієнтів з рейтингом 0-2.", reply_markup=main_keyboard(True))
        return
    chunks = []
    for r in rows:
        fio = " ".join(filter(None, [r["last_name"], r["first_name"], r["middle_name"]])) or "Без ПІБ"
        contact = r["phone"] or r["social_contact"] or "немає"
        chunks.append(f"<b>{fio}</b>\nКонтакт: <code>{contact}</code>\nРейтинг: <b>{r['rating']}/5</b>")
    await message.answer("\n\n".join(chunks[:20]), parse_mode=ParseMode.HTML, reply_markup=main_keyboard(True))


@dp.message(F.text == "🌐 Глобальний список 0-2")
async def global_low_rating(message: Message):
    user = ensure_workspace(message)
    if not user:
        await message.answer("Спочатку створіть кабінет.", reply_markup=main_keyboard(False))
        return
    rows = global_low_rating_clients()
    if not rows:
        await message.answer("Глобальний список зараз порожній.", reply_markup=main_keyboard(True))
        return
    chunks = []
    for r in rows:
        fio = " ".join(filter(None, [r["last_name"], r["first_name"], r["middle_name"]])) or "Без ПІБ"
        contact = r["phone"] or r["social_contact"] or "немає"
        chunks.append(
            f"<b>{fio}</b>\nКонтакт: <code>{contact}</code>\n"
            f"Рейтинг: <b>{r['rating']}/5</b>\nКабінет: {r['workspace_name']}"
        )
    await message.answer("\n\n".join(chunks[:20]), parse_mode=ParseMode.HTML, reply_markup=main_keyboard(True))


@dp.message(F.text == "📊 Статистика")
async def stats(message: Message):
    user = ensure_workspace(message)
    if not user:
        await message.answer("Спочатку створіть кабінет.", reply_markup=main_keyboard(False))
        return
    total, low = stats_for_workspace(user["workspace_id"])
    ws = workspace_record(user["workspace_id"])
    await message.answer(
        f"<b>{ws['name']}</b>\nУсього клієнтів: <b>{total}</b>\nНизький рейтинг 0-2: <b>{low}</b>",
        parse_mode=ParseMode.HTML,
        reply_markup=main_keyboard(True),
    )


@dp.message()
async def fallback(message: Message):
    user = user_record(message.from_user.id)
    await message.answer(
        "Не зрозумів команду. Натисніть кнопку в меню або /start.",
        reply_markup=main_keyboard(bool(user and user['workspace_id'])),
    )


async def main():
    init_db()
    bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
