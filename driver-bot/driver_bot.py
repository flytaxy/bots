#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
FlyTaxi Driver Bot — Telegram (aiogram v3) + RabbitMQ integration
Single-file implementation with:
- Registration & approval flow for drivers
- Online/offline toggle and settings
- Location updates (GPS share or manual address -> geocode via OSM)
- Orders pipeline backed by RabbitMQ:
  - Consumes new orders from QUEUE_ORDERS
  - Offers to nearby/eligible online drivers
  - On accept -> publishes confirmation into QUEUE_CONFIRMATIONS
- Local JSON storage (drivers, pending applications, orders, settings)

Env variables (.env supported):
  DRIVER_BOT_TOKEN or BOT_TOKEN
  RABBITMQ_HOST=localhost
  QUEUE_ORDERS=orders
  QUEUE_CONFIRMATIONS=confirmations

Run:
  pip install aiogram==3.* python-dotenv pika requests
  python driver_bot.py
"""
import os
import json
import asyncio
import logging
import time as time_module  # для sleep()

from datetime import datetime, timezone, timedelta
from datetime import time as dtime  # для time(5,0)
import math
import threading
from typing import Dict, Any, Tuple, List, Optional

from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command, CommandStart
from aiogram.types import (
    ReplyKeyboardMarkup,
    KeyboardButton,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    InputMediaPhoto,
    ReplyKeyboardRemove,
)
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import User


from aiogram import F, Router
from aiogram.types import CallbackQuery
from asyncio import run_coroutine_threadsafe, get_running_loop


class PaymentFSM(StatesGroup):
    waiting_for_receipt = State()


from dotenv import load_dotenv
import requests
import pika


def save_json(path, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4, ensure_ascii=False)


def _load_json(path: str, default):
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return default
    return default


def _save_json(path: str, data):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


def save_drivers(data):
    _save_json(DRIVERS_FILE, data)


def save_pending():
    _save_json(PENDING_FILE, pending)


def save_orders():
    _save_json(ORDERS_FILE, orders_state)


def save_settings():
    _save_json(SETTINGS_FILE, settings)


def load_drivers():
    return _load_json(DRIVERS_FILE, {})


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def fmt_price(p):
    try:
        return f"{int(round(float(p)))} грн"
    except Exception:
        return f"{p} грн"


def haversine_km(a: Tuple[float, float], b: Tuple[float, float]) -> float:
    lat1, lon1 = a
    lat2, lon2 = b
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    lat1 = math.radians(lat1)
    lat2 = math.radians(lat2)
    h = (
        math.sin(dlat / 2) ** 2
        + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    )
    return 2 * R * math.asin(math.sqrt(h))


settings = _load_json("settings.json", {})


# ----------------------------
# Env & Globals
# ----------------------------

# Env & Globals
# ----------------------------
load_dotenv()

API_TOKEN = os.getenv("DRIVER_BOT_TOKEN") or os.getenv("BOT_TOKEN")
if not API_TOKEN:
    raise RuntimeError("Please set DRIVER_BOT_TOKEN (or BOT_TOKEN) in .env")

# RabbitMQ config (with sane defaults)
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
QUEUE_ORDERS = os.getenv("QUEUE_ORDERS", "orders")
QUEUE_CONFIRMATIONS = os.getenv("QUEUE_CONFIRMATIONS", "confirmations")

logging.basicConfig(format="%(asctime)s %(levelname)s: %(message)s", level=logging.INFO)

bot = Bot(API_TOKEN)
dp = Dispatcher(storage=MemoryStorage())
router = Router()  # ✅ тут тепер router замість dp

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DRIVERS_FILE = os.path.join(BASE_DIR, "drivers.json")
PENDING_FILE = os.path.join(BASE_DIR, "pending_drivers.json")
ORDERS_FILE = os.path.join(BASE_DIR, "orders.json")
SETTINGS_FILE = os.path.join(BASE_DIR, "settings.json")

drivers: Dict[str, Any] = _load_json(DRIVERS_FILE, {})
pending: Dict[str, Any] = _load_json(PENDING_FILE, {})
orders_state: Dict[str, Any] = _load_json(ORDERS_FILE, {"orders": []})
settings: Dict[str, Any] = _load_json(SETTINGS_FILE, {"ADMIN_CHAT_ID": None})


# Keep a simple in-memory map of raw orders by id (useful for confirmations)
ORDERS_BY_ID: Dict[str, Dict[str, Any]] = {}

# Optional hard-coded admin IDs (can keep empty)
ADMIN_IDS = set()

# Offer timeout
OFFER_TIMEOUT_SEC = 120

# ----------------------------
# UI Builders
# ----------------------------


def driver_main_menu(user_id: int) -> ReplyKeyboardMarkup:
    drivers = load_drivers()
    uid = str(user_id)
    online = drivers.get(uid, {}).get("online", False)  # ✅ тільки online

    toggle = "🔴 Офлайн" if online else "🟢 Онлайн"

    rows = [
        [KeyboardButton(text=toggle)],
        [KeyboardButton(text="📊 Статус"), KeyboardButton(text="📈 Моя статистика")],
        [
            KeyboardButton(text="⚙️ Налаштування"),
            KeyboardButton(text="📍 Оновити локацію"),
        ],
        [
            KeyboardButton(text="💳 Оплатити добу"),
            KeyboardButton(text="🆘 Служба підтримки"),
        ],
    ]

    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True)


def offer_inline_kb(order_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="✅ Прийняти", callback_data=f"accept:{order_id}"
                ),
                InlineKeyboardButton(
                    text="❌ Відхилити", callback_data=f"decline:{order_id}"
                ),
            ]
        ]
    )


def ontrip_inline_kb(order_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="📍 Прибув", callback_data=f"arrived:{order_id}"
                )
            ],
            [
                InlineKeyboardButton(
                    text="▶️ Почав поїздку", callback_data=f"start:{order_id}"
                )
            ],
            [
                InlineKeyboardButton(
                    text="✅ Завершив поїздку", callback_data=f"finish:{order_id}"
                )
            ],
            [
                InlineKeyboardButton(
                    text="🛑 Скасувати поїздку", callback_data=f"cancel:{order_id}"
                )
            ],
        ]
    )


def format_order_card(order: Dict[str, Any]) -> str:
    pickup = order.get("pickup", {})
    drop = order.get("dropoff", {})
    tariff = order.get("tariff", "-")
    price = order.get("price", "-")
    payment = order.get("payment", "-")
    dist = float(order.get("distance_km", 0.0) or 0.0)
    eta = int(order.get("eta_min", 5) or 5)
    return (
        f"🚖 <b>Замовлення #{order.get('id','')}</b>\n"
        f"📍 Подача: {pickup.get('address','-')}\n"
        f"🏁 Фініш: {drop.get('address','-')}\n"
        f"🛣 Відстань: {dist:.1f} км\n"
        f"🚗 Тариф: {tariff}\n"
        f"💳 Оплата: {payment}\n"
        f"💵 Ціна: {fmt_price(price)}\n"
        f"⏱ Подача до клієнта: ~{eta} хв\n"
    )


# ----------------------------
# States
# ----------------------------


class Reg(StatesGroup):
    waiting_name = State()
    waiting_phone = State()
    waiting_car_brand_model = State()
    waiting_car_color_plate = State()
    waiting_dl_front = State()
    waiting_dl_back = State()
    waiting_sts = State()
    waiting_selfie = State()
    waiting_interior = State()
    waiting_car_photo = State()
    waiting_submit = State()


class LocationUpdate(StatesGroup):
    waiting_for_geo = State()
    waiting_for_address = State()


# ----------------------------
# Utils
# ----------------------------


def is_admin(user_id: Optional[int], chat_id: Optional[int] = None) -> bool:
    if user_id is not None and user_id in ADMIN_IDS:
        return True
    if chat_id is not None and settings.get("ADMIN_CHAT_ID") == chat_id:
        return True
    return False


def ensure_driver_skeleton(user_id: int):
    if str(user_id) not in drivers:
        drivers[str(user_id)] = {
            "docs": {},
            "online": False,  # стан онлайн/офлайн
            "last_location": None,
            "today": {
                "trips": 0,
                "earn": 0.0,
                "accepted": 0,
                "declined": 0,
                "canceled": 0,
            },
            "active_order_id": None,
            "approved": False,  # підтвердження адміном
            "approved_at": None,
            "pickup_km": 3.0,
            "payment_method": "both",
            "paid_until": None,
            "payment_status": "pending",
            "classes": ["standard"],  # 🔹 нове поле для тарифів
        }
        save_drivers(drivers)


# ----------------------------
# Handlers — auth & profile
# ----------------------------


@router.message(CommandStart())
async def start(message: types.Message, state: FSMContext):
    uid = str(message.from_user.id)
    ensure_driver_skeleton(message.from_user.id)

    d = drivers[uid]
    if not d.get("approved"):
        if uid in pending:
            await message.answer(
                "Ваша заявка на реєстрацію отримана ✅\nОчікуйте підтвердження адміністратора."
            )
            return
        await message.answer(
            "👋 Вітаємо у FlyTaxi Drivers!\n"
            "Для роботи потрібна одноразова реєстрація.\n\n"
            "✍️ Вкажіть ваше ПІБ (як у правах):"
        )
        await state.set_state(Reg.waiting_name)
        return

    callsign = d.get("callsign") or "—"
    car = d.get("car") or {}
    car_line = f"{car.get('brand','—')} {car.get('model','')}"
    if car.get("color"):
        car_line += f", {car.get('color')}"
    if car.get("plate"):
        car_line += f" • {car.get('plate')}"
    phone = d.get("phone") or "—"

    await message.answer(
        f"Привіт, {callsign}! 👋\n"
        f"🚗 Авто: {car_line}\n"
        f"📞 Телефон: {phone}\n\n"
        "Готові до роботи.",
        reply_markup=driver_main_menu(message.from_user.id),
    )


@router.message(Reg.waiting_name)
async def reg_name(message: types.Message, state: FSMContext):
    await state.update_data(name=message.text.strip())
    kb = ReplyKeyboardMarkup(
        resize_keyboard=True,
        keyboard=[[KeyboardButton(text="📱 Надіслати номер", request_contact=True)]],
    )
    await message.answer("📞 Надішліть номер телефону (кнопка нижче):", reply_markup=kb)
    await state.set_state(Reg.waiting_phone)


# --- Кнопки зв'язку через Telegram ---
def contact_inline_kb(passenger: Dict[str, Any]) -> Optional[InlineKeyboardMarkup]:
    btns: List[List[InlineKeyboardButton]] = []

    if not passenger:
        return None

    p_uid = passenger.get("user_id") or passenger.get("id")
    p_username = passenger.get("username")

    if p_username:
        btns.append(
            [
                InlineKeyboardButton(
                    text="💬 Написати пасажиру в Telegram",
                    url=f"https://t.me/{p_username}",
                )
            ]
        )
    elif p_uid:
        btns.append(
            [
                InlineKeyboardButton(
                    text="💬 Написати пасажиру в Telegram", url=f"tg://user?id={p_uid}"
                )
            ]
        )

    if not btns:
        return None
    return InlineKeyboardMarkup(inline_keyboard=btns)


@router.message(Reg.waiting_phone, F.contact)
async def reg_phone_contact(message: types.Message, state: FSMContext):
    await state.update_data(phone=message.contact.phone_number)
    await message.answer(
        "🚗 Введіть <b>марку та модель</b> авто (наприклад: <i>Renault Megane 3</i>):",
        parse_mode="HTML",
        reply_markup=ReplyKeyboardRemove(),
    )
    await state.set_state(Reg.waiting_car_brand_model)


@router.message(Reg.waiting_phone)
async def reg_phone_text(message: types.Message, state: FSMContext):
    await state.update_data(phone=message.text.strip())
    await message.answer(
        "🚗 Введіть <b>марку та модель</b> авто (наприклад: <i>Renault Megane 3</i>):",
        parse_mode="HTML",
        reply_markup=ReplyKeyboardRemove(),
    )
    await state.set_state(Reg.waiting_car_brand_model)


@router.message(Reg.waiting_car_brand_model)
async def reg_car_brand_model(message: types.Message, state: FSMContext):
    await state.update_data(car_brand_model=message.text.strip())
    await message.answer(
        "🎨 Введіть <b>колір</b> та <b>номерний знак</b> через крапку з комою:\n<i>білий; AA1234AA</i>",
        parse_mode="HTML",
    )
    await state.set_state(Reg.waiting_car_color_plate)


@router.message(Reg.waiting_car_color_plate)
async def reg_car_color_plate(message: types.Message, state: FSMContext):
    txt = message.text.strip()
    try:
        color, plate = [s.strip() for s in txt.split(";")]
    except Exception:
        await message.answer(
            "Будь ласка, введіть у форматі: <b>колір; НОМЕР</b>\nНапр.: <i>білий; AA1234AA</i>",
            parse_mode="HTML",
        )
        return
    await state.update_data(car_color=color, car_plate=plate)
    await message.answer(
        "📷 Надішліть фото <b>водійського посвідчення (лицьова сторона)</b>:",
        parse_mode="HTML",
    )
    await state.set_state(Reg.waiting_dl_front)


@router.message(Reg.waiting_dl_front, F.photo)
async def reg_dl_front(message: types.Message, state: FSMContext):
    await state.update_data(dl_front=message.photo[-1].file_id)
    await message.answer(
        "📷 Тепер надішліть фото <b>водійського посвідчення (зворотна сторона)</b>:",
        parse_mode="HTML",
    )
    await state.set_state(Reg.waiting_dl_back)


@router.message(Reg.waiting_dl_back, F.photo)
async def reg_dl_back(message: types.Message, state: FSMContext):
    await state.update_data(dl_back=message.photo[-1].file_id)
    await message.answer(
        "📷 Надішліть фото <b>свідоцтва про реєстрацію ТЗ</b>:", parse_mode="HTML"
    )
    await state.set_state(Reg.waiting_sts)


@router.message(Reg.waiting_sts, F.photo)
async def reg_sts(message: types.Message, state: FSMContext):
    await state.update_data(sts=message.photo[-1].file_id)
    await message.answer(
        "🤳 Надішліть <b>селфі</b> (обличчя чітко видно):", parse_mode="HTML"
    )
    await state.set_state(Reg.waiting_selfie)


@router.message(Reg.waiting_selfie, F.photo)
async def reg_selfie(message: types.Message, state: FSMContext):
    await state.update_data(selfie=message.photo[-1].file_id)
    await message.answer(
        "🪑 Тепер надішліть фото <b>салону авто</b> (передні сидіння/торпедо):",
        parse_mode="HTML",
    )
    await state.set_state(Reg.waiting_interior)


@router.message(Reg.waiting_interior, F.photo)
async def reg_interior(message: types.Message, state: FSMContext):
    await state.update_data(interior=message.photo[-1].file_id)
    await message.answer(
        "📷 І останнє: фото <b>автомобіля</b> (вид збоку/півоберта):", parse_mode="HTML"
    )
    await state.set_state(Reg.waiting_car_photo)


@router.message(Reg.waiting_car_photo, F.photo)
async def reg_car_photo(message: types.Message, state: FSMContext):
    await state.update_data(car_photo=message.photo[-1].file_id)
    data = await state.get_data()
    review = (
        f"Перевірте дані:\n\n"
        f"👤 ПІБ: {data.get('name')}\n"
        f"📞 Телефон: {data.get('phone')}\n"
        f"🚗 Авто: {data.get('car_brand_model')}\n"
        f"🎨 Колір: {data.get('car_color')}\n"
        f"🔖 Номер: {data.get('car_plate')}\n\n"
        f"Надіслати на перевірку адміністратору?"
    )
    kb = ReplyKeyboardMarkup(
        resize_keyboard=True,
        keyboard=[
            [KeyboardButton(text="📤 Надіслати на перевірку")],
            [KeyboardButton(text="🔁 Почати спочатку")],
        ],
    )
    await message.answer(review, reply_markup=kb)
    await state.set_state(Reg.waiting_submit)


@router.message(Reg.waiting_submit)
async def submit_registration(message: types.Message, state: FSMContext):
    uid = str(message.from_user.id)
    admin_id = settings.get("ADMIN_CHAT_ID")
    if not admin_id:
        await message.answer(
            "❗️Адмін-чат не налаштований. Зайди у потрібний канал/групу як адмін і виконай /set_admin_chat."
        )
        return
    admin_id = int(admin_id)  # 🔹 ВАЖЛИВО: приводимо до int

    data = await state.get_data()

    name = data.get("name", "-")
    phone = data.get("phone", "-")
    car = data.get("car_brand_model", "-")
    color = data.get("car_color", "-")
    plate = data.get("car_plate", "-")
    color_plate = f"{color}; {plate}"
    username = getattr(message.from_user, "username", None) or "_"

    text_caption = (
        "🚖 <b>Нова заявка водія</b>\n"
        f"🆔 ID: <code>{uid}</code>\n"
        f"👤 Ім'я: {name}\n"
        f"📞 Телефон: {phone}\n"
        f"🚗 Авто: {car}\n"
        f"🎨 Колір/Номер: {color_plate}\n"
        f"👤 Username: @{username}\n"
        "\nПеревірте фото нижче та натисніть дію:"
    )

    # --- Формуємо альбом фото ---
    media = []
    if data.get("selfie"):
        media.append(
            types.InputMediaPhoto(
                media=data["selfie"], caption=text_caption, parse_mode="HTML"
            )
        )
    if data.get("car_photo"):
        media.append(types.InputMediaPhoto(media=data["car_photo"]))
    if data.get("dl_front"):
        media.append(types.InputMediaPhoto(media=data["dl_front"]))
    if data.get("dl_back"):
        media.append(types.InputMediaPhoto(media=data["dl_back"]))
    if data.get("sts"):
        media.append(types.InputMediaPhoto(media=data["sts"]))
    if data.get("interior"):
        media.append(types.InputMediaPhoto(media=data["interior"]))

    if media:
        await bot.send_media_group(admin_id, media)
    else:
        # fallback — якщо немає фото
        await bot.send_message(admin_id, text_caption, parse_mode="HTML")

    # --- Кнопки для підтвердження ---
    inline_keyboard = [
        [
            InlineKeyboardButton(
                text="✅ Standard", callback_data=f"approve:{uid}:standard"
            ),
            InlineKeyboardButton(
                text="✅ Comfort", callback_data=f"approve:{uid}:comfort"
            ),
        ],
        [
            InlineKeyboardButton(
                text="✅ Standard+Comfort",
                callback_data=f"approve:{uid}:standard,comfort",
            ),
            InlineKeyboardButton(
                text="✅ Standard+Business+Comfort",
                callback_data=f"approve:{uid}:standard,business,comfort",
            ),
        ],
        [
            InlineKeyboardButton(text="❌ Відхилити", callback_data=f"reject:{uid}"),
        ],
    ]
    kb = InlineKeyboardMarkup(inline_keyboard=inline_keyboard)

    await bot.send_message(
        admin_id, "⬆️ Фото заявки. Оберіть дію:", reply_markup=kb, parse_mode="HTML"
    )

    # --- Зберігаємо в pending ---
    pending[uid] = {
        "user_id": uid,
        "name": name,
        "phone": phone,
        "car_brand_model": car,
        "car_color": color,
        "car_plate": plate,
        "selfie": data.get("selfie"),
        "dl_front": data.get("dl_front"),
        "dl_back": data.get("dl_back"),
        "sts": data.get("sts"),
        "interior": data.get("interior"),
        "car_photo": data.get("car_photo"),
        "username": username,
        "created_at": now_iso(),
        "car": {"brand_model": car, "color": color, "plate": plate},
        "photos": {
            "dl_front": data.get("dl_front"),
            "dl_back": data.get("dl_back"),
            "sts": data.get("sts"),
            "car": data.get("car_photo"),
            "selfie": data.get("selfie"),
            "interior": data.get("interior"),
        },
        "review": {"status": "pending", "by": None, "note": None},
    }
    save_pending()

    # send media group
    media = []

    def add(fid, cap=None):
        if fid:
            media.append(
                InputMediaPhoto(media=fid, caption=cap or None, parse_mode="HTML")
            )

    add(data.get("selfie"), "🤳 Селфі")
    add(data.get("dl_front"), "🪪 Права (лицьова)")
    add(data.get("dl_back"), "🪪 Права (зворот)")
    add(data.get("sts"), "📄 Техпаспорт")
    add(data.get("interior"), "🪑 Салон")
    add(data.get("car_photo"), "🚗 Авто")
    if media:
        try:
            await bot.send_media_group(admin_id, media=media)
        except Exception:
            pass

    await message.answer("✅ Заявку надіслано адміну. Очікуйте рішення.")
    await state.clear()


@router.message(Command("set_admin_chat"))
async def set_admin_chat(message: types.Message):
    if message.chat.type not in ("group", "supergroup", "channel"):
        await message.answer("❌ Надішліть цю команду в групі або каналі.")
        return
    if ADMIN_IDS and (
        message.from_user.id not in ADMIN_IDS
        and message.chat.id != settings.get("ADMIN_CHAT_ID")
    ):
        await message.answer("❌ У вас немає прав для встановлення адмін-чату.")
        return
    settings["ADMIN_CHAT_ID"] = message.chat.id
    save_settings()
    await message.answer("✅ Цей чат збережено як адмін-чат для заявок.")
    try:
        await bot.send_message(
            settings["ADMIN_CHAT_ID"], "🔔 Тест: адмін-чат успішно налаштований."
        )
    except Exception as e:
        logging.error(f"Не вдалося надіслати тестове повідомлення: {e}")


@router.channel_post(Command("set_admin_chat"))
async def set_admin_chat_channel(message: types.Message):
    settings["ADMIN_CHAT_ID"] = message.chat.id
    save_settings()
    await message.answer("✅ Цей канал збережено як адмін-чат для заявок.")
    try:
        await bot.send_message(
            settings["ADMIN_CHAT_ID"], "🔔 Тест: адмін-чат успішно налаштований."
        )
    except Exception as e:
        logging.error(f"Не вдалося надіслати тестове повідомлення: {e}")


@router.callback_query(F.data.startswith("approve:"))
async def cb_approve(call: types.CallbackQuery):
    if not is_admin(call.from_user.id, getattr(call.message.chat, "id", None)):
        await call.answer("Немає прав.", show_alert=True)
        return

    parts = call.data.split(":")
    uid = parts[1]
    classes = (
        parts[2].split(",") if len(parts) > 2 else ["standard"]
    )  # 🔹 парсимо класи

    data = pending.get(uid)
    if not data:
        await call.answer("Заявка не знайдена.", show_alert=True)
        return

    car_brand_model = data.get("car_brand_model") or (data.get("car") or {}).get(
        "brand_model"
    )
    car_color = data.get("car_color") or (data.get("car") or {}).get("color")
    car_plate = data.get("car_plate") or (data.get("car") or {}).get("plate")

    photos = data.get("photos") or {}
    dl_front = data.get("dl_front") or photos.get("dl_front")
    dl_back = data.get("dl_back") or photos.get("dl_back")
    sts = data.get("sts") or photos.get("sts")
    car_photo = data.get("car_photo") or photos.get("car")
    selfie = data.get("selfie") or photos.get("selfie")
    interior = data.get("interior") or photos.get("interior")

    brand, model = "", ""
    if car_brand_model:
        tokens = car_brand_model.split()
        brand = tokens[0]
        model = " ".join(tokens[1:]) if len(tokens) > 1 else ""

    drivers[uid] = {
        "callsign": None,
        "phone": data.get("phone"),
        "car": {
            "brand": brand,
            "model": model,
            "color": car_color,
            "plate": car_plate,
            "photo_id": car_photo,
        },
        "docs": {
            "dl_front": dl_front,
            "dl_back": dl_back,
            "sts": sts,
            "selfie": selfie,
            "interior": interior,
        },
        "online": False,
        "last_location": None,
        "today": {"trips": 0, "earn": 0, "accepted": 0, "declined": 0, "canceled": 0},
        "active_order_id": None,
        "approved": True,
        "approved_at": now_iso(),
        "pickup_km": drivers.get(uid, {}).get("pickup_km", 3.0),
        "payment_method": drivers.get(uid, {}).get("payment_method", "both"),
        "classes": classes,  # 🔹 сюди зберігаємо обрані адміном класи
    }

    pending.pop(uid, None)
    save_drivers(drivers)
    save_pending()

    try:
        await call.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass

    await call.answer("✅ Заявку підтверджено", show_alert=True)

    try:
        await bot.send_message(
            int(uid),
            f"✅ Вас підтверджено як водія.\nДоступні класи: {', '.join(classes)}",
            reply_markup=driver_main_menu(int(uid)),
        )
    except Exception:
        pass

    # повідомлення у чат оплат
    payment_chat = settings.get("PAYMENT_CHAT_ID")
    if payment_chat:
        try:
            await bot.send_message(
                int(payment_chat),
                f"💵 Водій {uid} підтверджений. Не забудьте оплатити добу ({DAILY_FEE} грн).\n"
                f"Посилання: tg://user?id={uid}",
            )
        except Exception as e:
            logging.error(e)


# ----------------------------
# Адмін: відхилити заявку (кадрова, не про оплату)
# ----------------------------
@router.callback_query(F.data.startswith("reject:"))
async def cb_reject(call: types.CallbackQuery):
    if not is_admin(call.from_user.id, getattr(call.message.chat, "id", None)):
        await call.answer("Немає прав.", show_alert=True)
        return

    uid = call.data.split(":", 1)[1]
    if uid not in pending:
        await call.answer("Заявка не знайдена.", show_alert=True)
        return

    pending.pop(uid, None)
    save_pending()
    try:
        await call.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass
    await call.answer("❌ Заявку відхилено", show_alert=True)

    try:
        await bot.send_message(
            int(uid),
            "❌ Вашу заявку відхилено. Ви можете подати її повторно після виправлення.",
        )
    except Exception:
        pass


# ----------------------------
# Оплата доби
# ----------------------------
@router.message(F.text == "💳 Оплатити добу")
async def pay_day(message: types.Message, state: FSMContext):
    await message.answer(
        "💳 Реквізити для оплати:\n\n"
        "Картка: 5375 **** **** 1234\n"
        "Сума: 100 грн\n\n"
        "Після оплати натисніть кнопку нижче:",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(
                        text="✅ Я оплатив", callback_data="confirm_payment"
                    )
                ]
            ]
        ),
    )


@router.callback_query(F.data == "confirm_payment")
async def confirm_payment(call: types.CallbackQuery, state: FSMContext):
    await call.message.answer("📸 Надішліть, будь ласка, фото або скріншот чеку.")
    await state.set_state(PaymentFSM.waiting_for_receipt)
    await call.answer()


@router.message(PaymentFSM.waiting_for_receipt, F.photo)
async def process_receipt(message: types.Message, state: FSMContext):
    uid = str(message.from_user.id)
    drivers = load_drivers()
    d = drivers.get(uid, {})

    payment_chat = int(settings.get("PAYMENT_CHAT_ID"))  # важливо: int

    caption = (
        "🆕 Новий платіж від водія\n"
        f"🚖 Позивний: {d.get('callsign', '-')}\n"
        f"🆔 ID: {uid}\n"
        f"📞 Телефон: {d.get('phone', '-')}\n"
        f"📅 Дата: {datetime.now().strftime('%Y-%m-%d %H:%M')}\n"
        "💰 Сума: 100 грн"
    )

    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="✅ Підтвердити", callback_data=f"approve_{uid}"
                ),
                InlineKeyboardButton(
                    text="❌ Відхилити", callback_data=f"reject_{uid}"
                ),
            ]
        ]
    )

    await bot.send_photo(
        chat_id=payment_chat,
        photo=message.photo[-1].file_id,
        caption=caption,
        reply_markup=keyboard,
    )

    await message.answer("✅ Дякуємо! Ваш чек надіслано адміну на перевірку.")
    await state.clear()


# ----------------------------
# Підтвердити / Відхилити оплату
# ----------------------------
@router.callback_query(F.data.startswith("approve_"))
async def approve_payment(callback: types.CallbackQuery):
    drivers = load_drivers()
    uid = callback.data.replace("approve_", "")

    if uid not in drivers:
        await callback.answer("Водій не знайдений ❌", show_alert=True)
        return

    # зберігаємо дедлайн в UTC
    now_utc = datetime.now(timezone.utc)
    end_of_day_utc = datetime.combine(
        now_utc.date(), dtime(23, 59, 59), tzinfo=timezone.utc
    )

    drivers[uid]["approved"] = True
    drivers[uid]["approved_at"] = now_utc.isoformat()
    drivers[uid]["paid_until"] = end_of_day_utc.isoformat()
    drivers[uid]["payment_status"] = "approved"
    save_drivers(drivers)

    await callback.answer("✅ Оплату підтверджено")
    await callback.message.edit_caption(
        caption=(callback.message.caption or "") + "\n\n✅ Оплату підтверджено",
        reply_markup=None,
    )

    # повідомлення водію
    try:
        end_local = end_of_day_utc.astimezone().strftime("%Y-%m-%d %H:%M")
        await bot.send_message(
            int(uid), f"✅ Оплату підтверджено. Доступ активний до {end_local}."
        )
    except Exception as e:
        logging.exception(f"[TG] Не вдалось надіслати підтвердження водію {uid}: {e}")


@router.callback_query(lambda c: c.data.startswith("reject_"))
async def reject_payment(callback_query: types.CallbackQuery):
    uid = callback_query.data.split("_", 1)[1]

    drivers = load_drivers()
    if uid in drivers:
        drivers[uid]["payment_status"] = "rejected"
        save_drivers(drivers)

    await callback_query.answer("❌ Оплату відхилено", show_alert=True)
    await callback_query.message.edit_reply_markup()

    try:
        await bot.send_message(
            int(uid), "❌ Оплату відхилено. Завантажте правильний чек."
        )
    except Exception as e:
        logging.exception(f"[TG] Помилка надсилання водію {uid}: {e}")


# ----------------------------
# Online / Offline
# ----------------------------
@router.message(F.text == "🟢 Онлайн")
async def go_online(message: types.Message, state: FSMContext):
    uid = str(message.from_user.id)
    drivers = load_drivers()

    if uid not in drivers:
        await message.answer("❌ Ви ще не зареєстровані як водій.")
        return

    driver = drivers[uid]

    # локальний час для вікна 05:00–23:59
    now_utc = datetime.now(timezone.utc)
    current_time_local = now_utc.astimezone().time()

    start_time = dtime(5, 0)
    end_time = dtime(23, 59)
    if not (start_time <= current_time_local <= end_time):
        await message.answer(
            "⏰ Зараз доступ закритий. Працювати можна з 05:00 до 23:59."
        )
        return

    # схвалення
    if not driver.get("approved"):
        await message.answer("❌ Ваш профіль ще не схвалено адміністратором.")
        return

    # оплата: обидві дати aware у UTC
    paid_until_str = driver.get("paid_until")
    if not paid_until_str:
        await message.answer("❌ Ваш доступ закінчився. Сплатіть добу для роботи.")
        return
    try:
        paid_until = datetime.fromisoformat(paid_until_str)
        if paid_until.tzinfo is None:
            paid_until = paid_until.replace(tzinfo=timezone.utc)
        else:
            paid_until = paid_until.astimezone(timezone.utc)
    except Exception:
        await message.answer("⚠️ Помилка у даних оплати. Зверніться до адміністратора.")
        return

    if now_utc > paid_until:
        await message.answer("❌ Ваш доступ закінчився. Сплатіть добу для роботи.")
        return

    # ✅ все добре – онлайн
    driver["online"] = True
    drivers[uid] = driver
    save_drivers(drivers)

    await message.answer(
        "🟢 Ви вийшли онлайн. Замовлення будуть надходити автоматично.",
        reply_markup=driver_main_menu(uid),
    )


# 🔴 Вихід офлайн
@router.message(F.text == "🔴 Офлайн")
async def go_offline(message: types.Message, state: FSMContext):
    uid = str(message.from_user.id)
    drivers = load_drivers()

    if uid in drivers:
        drivers[uid]["online"] = False
        save_drivers(drivers)
        drivers = load_drivers()

    await message.answer(
        "🔴 Ви вийшли офлайн.",
        reply_markup=driver_main_menu(uid),
    )


@router.message(F.text == "🆘 Служба підтримки")
async def support(message: types.Message):
    await message.answer("Підтримка: @FlyTaxiSupport (8:00–23:00)")


async def show_order_waiting_screen(chat_id: int):
    try:
        await bot.send_message(
            chat_id,
            "🚖 Очікування нових замовлень…",
            reply_markup=driver_main_menu(chat_id),
        )
    except Exception as e:
        logging.error(f"Waiting screen error: {e}")


@router.message(F.text == "⚙️ Налаштування")
async def settings_menu(message: types.Message):
    uid = str(message.from_user.id)
    cur_dist = drivers.get(uid, {}).get("pickup_km")
    pay = drivers.get(uid, {}).get("payment_method")
    pay_h = (
        "💵 Готівка"
        if pay == "cash"
        else (
            "💳 Картка" if pay == "card" else ("💵💳 Обидва" if pay == "both" else "—")
        )
    )
    dist_h = f"{float(cur_dist):.1f} км" if isinstance(cur_dist, (int, float)) else "—"

    distances = [i / 2 for i in range(1, 21)]  # 0.5..10.0
    rows: List[List[InlineKeyboardButton]] = []
    row: List[InlineKeyboardButton] = []
    for i, dval in enumerate(distances, start=1):
        row.append(
            InlineKeyboardButton(
                text=f"{dval:.1f} км", callback_data=f"setpick:{dval:.1f}"
            )
        )
        if i % 4 == 0:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append(
        [
            InlineKeyboardButton(text="💵 Лише готівка", callback_data="setpay:cash"),
            InlineKeyboardButton(text="💳 Лише картка", callback_data="setpay:card"),
            InlineKeyboardButton(text="💵💳 Обидва", callback_data="setpay:both"),
        ]
    )

    text = (
        "⚙️ <b>Налаштування</b>\n"
        "Оберіть максимальну дистанцію <i>від вас до пасажира</i> для підбору замовлень, та тип оплати.\n\n"
        f"Поточна дистанція: <b>{dist_h}</b>\n"
        f"Поточна оплата: <b>{pay_h}</b>"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=rows)
    await message.answer(text, parse_mode="HTML", reply_markup=kb)

    # --- Адмін перевірка оплат ---


@router.callback_query(F.data.startswith("approve_payment"))
async def approve_payment(call: CallbackQuery):
    today = datetime.now()

    # доступ з 05:00 до 23:59 сьогодні
    end_of_day = datetime.combine(today.date(), dtime(23, 59, 59))
    uid = str(call.from_user.id)

    drivers[uid]["approved"] = True
    drivers[uid]["approved_at"] = today.isoformat()
    drivers[uid]["paid_until"] = end_of_day.isoformat()

    save_json(DRIVERS_FILE, drivers)

    await call.message.edit_caption(
        "✅ Оплата підтверджена!\n"
        f"Доступ активний до <b>{end_of_day.strftime('%H:%M %d-%m-%Y')}</b>",
        parse_mode="HTML",
    )


@router.callback_query(F.data.startswith("reject_"))
async def reject_payment(call: types.CallbackQuery):
    uid = call.data.split("_")[1]
    await call.message.edit_caption(
        caption=call.message.caption + "\n❌ Оплату відхилено адміністратором"
    )
    await bot.send_message(
        uid, "❌ Ваш платіж відхилено. Зв’яжіться з адміністратором."
    )
    await call.answer()


@router.callback_query(F.data.startswith("setpick:"))
async def set_pickup_distance(call: types.CallbackQuery):
    uid = str(call.from_user.id)
    try:
        val = float(call.data.split(":", 1)[1])
    except Exception:
        await call.answer("Некоректне значення.", show_alert=True)
        return
    drivers.setdefault(uid, {})["pickup_km"] = val
    save_drivers(drivers)

    pay = drivers.get(uid, {}).get("payment_method")
    if pay in ("cash", "card", "both"):
        await call.answer("Збережено")
        try:
            await call.message.delete()
        except Exception:
            pass
        await show_order_waiting_screen(call.from_user.id)
        return

    await call.answer("Дистанцію збережено")
    text = (
        "⚙️ Поточний вибір:\n"
        f"• Дистанція: <b>{val:.1f} км</b>\n"
        "• Оплата: <b>не обрано</b>\n\n"
        "Оберіть тип оплати нижче ⤵️"
    )
    await bot.send_message(call.from_user.id, text, parse_mode="HTML")
    dummy = types.Message(
        message_id=call.message.message_id,
        date=call.message.date,
        chat=call.message.chat,
    )
    dummy.from_user = User(
        id=getattr(call.from_user, "id", 0),
        is_bot=getattr(call.from_user, "is_bot", False),
        first_name=getattr(call.from_user, "first_name", ""),
        last_name=getattr(call.from_user, "last_name", None),
        username=getattr(call.from_user, "username", None),
        language_code=getattr(call.from_user, "language_code", None),
    )
    await settings_menu(dummy)


@router.message(F.text.contains("Статус"))
async def my_status(message: types.Message):
    uid = str(message.from_user.id)
    drivers = load_drivers()  # ✅ підвантажуємо свіжі дані
    d = drivers.get(uid, {})
    car = d.get("car") or {}

    # --- Авто ---
    car_line = f"{car.get('brand','-')} {car.get('model','')}"
    if car.get("color"):
        car_line += f", {car.get('color')}"
    if car.get("plate"):
        car_line += f" • {car.get('plate')}"

    # --- Телефон ---
    phone = d.get("phone") or "-"

    # --- Радіус підбору ---
    pick = d.get("pickup_km")
    pick_h = f"{float(pick):.1f} км" if isinstance(pick, (int, float)) else "-"

    # --- Оплата (тип) ---
    pay = d.get("payment_method")
    if pay == "cash":
        pay_h = "💵 Готівка"
    elif pay == "card":
        pay_h = "💳 Картка"
    elif pay == "both":
        pay_h = "💵💳 Обидва"
    else:
        pay_h = "-"

    # --- Оплата (підписка) ---
    paid_until = d.get("paid_until")
    if paid_until:
        try:
            paid_text = datetime.fromisoformat(paid_until).strftime("%Y-%m-%d %H:%M")
        except Exception:
            paid_text = "❌ некоректна дата"
    else:
        paid_text = "❌ немає активної оплати"

    # --- Текст статусу ---
    txt = (
        f"<b>Ваш статус</b>\n"
        f"🚗 Авто: {car_line}\n"
        f"📞 Телефон: {phone}\n"
        f"📍 Радіус підбору: {pick_h}\n"
        f"💰 Тип оплати: {pay_h}\n"
        f"💵 Оплата дійсна до: {paid_text}\n"
        f"📡 Статус: {'Онлайн' if d.get('online') else 'Офлайн'}\n"
    )

    await message.answer(
        txt, parse_mode="HTML", reply_markup=driver_main_menu(message.from_user.id)
    )


@router.message(F.text == "📈 Моя статистика")
async def my_stats(message: types.Message):
    uid = str(message.from_user.id)
    d = drivers.get(uid, {})
    t = d.get("today", {})
    trips = int(t.get("trips", 0) or 0)
    earn = float(t.get("earn", 0) or 0)
    accepted = int(t.get("accepted", 0) or 0)
    declined = int(t.get("declined", 0) or 0)
    canceled = int(t.get("canceled", 0) or 0)
    avg = (earn / trips) if trips else 0.0
    txt = (
        "📊 <b>Моя статистика (сьогодні)</b>\n"
        f"Поїздок: <b>{trips}</b>\n"
        f"Прийнято: {accepted}  •  Відхилено: {declined}  •  Скасовано: {canceled}\n"
        f"Заробіток: <b>{int(earn)} грн</b>\n"
        f"Середній чек: {avg:.0f} грн"
    )
    await message.answer(
        txt, parse_mode="HTML", reply_markup=driver_main_menu(message.from_user.id)
    )


# ----------------------------
# Orders + Locks
# ----------------------------

_order_locks: Dict[str, asyncio.Lock] = {}


def get_order_lock(order_id: str) -> asyncio.Lock:
    if order_id not in _order_locks:
        _order_locks[order_id] = asyncio.Lock()
    return _order_locks[order_id]


async def dispatch_order_to_nearby_drivers(order: dict):
    from math import radians, sin, cos, sqrt, atan2

    def haversine(coord1, coord2):
        """Рахує відстань (у км) між двома координатами (lat, lon)"""
        R = 6371.0  # радіус Землі в км
        lat1, lon1 = coord1
        lat2, lon2 = coord2

        dlat = radians(lat2 - lat1)
        dlon = radians(lon2 - lon1)

        a = (
            sin(dlat / 2) ** 2
            + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon / 2) ** 2
        )
        c = 2 * atan2(sqrt(a), sqrt(1 - a))

        return R * c

    nearby: List[int] = []
    pickup_coords = None

    try:
        pickup_coords = tuple(
            order.get("route", {}).get("start", {}).get("coords") or []
        )
        if len(pickup_coords) != 2:
            pickup_coords = None
        logging.info(f"[DISPATCH] Pickup coords = {pickup_coords}")
    except Exception as e:
        logging.error(f"[DISPATCH] Error extracting pickup coords: {e}")
        pickup_coords = None

    drivers = load_drivers()

    for uid, d in drivers.items():
        logging.info(f"[FILTER] Checking driver {uid}: {d}")

        # --- 1. Перевірка на approve + online ---
        if not d.get("approved") or not d.get("online"):
            logging.info(f"[FILTER] Driver {uid} skipped (not approved/online)")
            continue

        logging.info(f"[FILTER] Driver {uid} ✅ passed approve/online")

        # --- 2. Перевірка способу оплати ---
        PAYMENT_ALIASES = {
            "💵 готівка": "cash",
            "готівка": "cash",
            "💳 переказ на картку водію": "card",
            "карта": "card",
            "на картку": "card",
        }

        order_payment = str(
            order.get("payment_type") or order.get("payment") or ""
        ).lower()
        order_payment = PAYMENT_ALIASES.get(order_payment, order_payment)

        driver_payment = d.get("payment_method", "both").lower()

        if driver_payment != "both" and order_payment != driver_payment:
            logging.info(
                f"[FILTER] Driver {uid} skipped (payment mismatch: order={order_payment}, driver={driver_payment})"
            )
            continue

        logging.info(f"[FILTER] Driver {uid} ✅ passed payment filter")

        # --- 3. Перевірка класу авто ---
        TARIFF_ALIASES = {
            # українська → англійська
            "Стандарт": "standard",
            "Комфорт": "comfort",
            "Бізнес": "business",
            # англійська → англійська
            "standard": "standard",
            "comfort": "comfort",
            "business": "business",
        }

        order_tariff = str(order.get("tariff", "")).strip()
        order_tariff = TARIFF_ALIASES.get(order_tariff, order_tariff)

        driver_classes = [c.lower() for c in d.get("classes", ["standard"])]

        if order_tariff and order_tariff not in driver_classes:
            logging.info(
                f"[FILTER] Driver {uid} skipped (no class match: order={order_tariff}, driver={driver_classes})"
            )
            continue

        logging.info(f"[FILTER] Driver {uid} ✅ passed class filter")

        # --- 3. Перевірка відстані ---
        driver_loc = d.get("last_location")
        if not driver_loc or len(driver_loc) != 2:
            logging.info(f"[FILTER] Driver {uid} skipped (no/invalid last_location)")
            continue

        if not pickup_coords or len(pickup_coords) != 2:
            logging.info(f"[FILTER] Driver {uid} skipped (no pickup coords)")
            continue

        try:
            dist = haversine(driver_loc, pickup_coords)
            if dist > float(d.get("pickup_km", 5.0)):
                logging.info(
                    f"[FILTER] Driver {uid} skipped (distance {dist:.2f} km > {d.get('pickup_km')} km)"
                )
                continue
        except Exception as e:
            logging.error(f"[ERROR] Distance check failed for driver {uid}: {e}")
            continue

        # Якщо водій пройшов усі фільтри — додаємо його
        nearby.append(int(uid))

    logging.info(f"[FILTER] Total nearby drivers = {len(nearby)}")

    if not nearby:
        logging.info("No nearby drivers for order %s", order.get("id"))
        return

    order["status"] = "offered"
    order["accepted_by"] = None
    order["offered_at"] = now_iso()
    save_orders()

    kb = offer_inline_kb(order["id"])
    text = (
        format_order_card(order) + f"\n⏳ У вас {OFFER_TIMEOUT_SEC} сек, щоб прийняти."
    )

    for did in nearby:
        try:
            await bot.send_message(did, text, parse_mode="HTML", reply_markup=kb)
        except Exception as e:
            logging.error(f"[DISPATCH] Error sending offer to driver {did}: {e}")

    # --- Таймер завершення оферу ---
    async def expire():
        await asyncio.sleep(OFFER_TIMEOUT_SEC)
        lock = get_order_lock(order["id"])
        async with lock:
            for o in orders_state["orders"]:
                if (
                    o["id"] == order["id"]
                    and o.get("accepted_by") is None
                    and o.get("status") == "offered"
                ):
                    o["status"] = "expired"
                    save_orders()
                    logging.info(
                        f"[DISPATCH] Order {order['id']} expired (no driver accepted)"
                    )
                    break

    asyncio.create_task(expire())


# ----------------------------
# Trip lifecycle callbacks
# ----------------------------


def build_driver_card(uid: str) -> dict:
    d = drivers.get(str(uid), {}) or {}
    car_info = d.get("car") or {}
    _brand = car_info.get("brand") or ""
    _model_name = car_info.get("model") or ""
    _color = car_info.get("color") or ""
    _plate = car_info.get("plate") or ""
    _model = f"{_brand} {_model_name}".strip()
    if _color:
        _model = f"{_model} ({_color})"
    return {
        "id": int(uid) if str(uid).isdigit() else uid,
        "name": d.get("callsign") or (d.get("phone") or f"ID {uid}"),
        "phone": d.get("phone"),
        "car": {
            "model": _model,
            "brand": _brand,
            "model_name": _model_name,
            "plate": _plate,
            "color": _color,
        },
    }


def publish_confirmation(confirmation: Dict[str, Any]):
    """Publish confirmation to RabbitMQ in a short-lived connection."""
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(RABBITMQ_HOST))
        channel = connection.channel()
        # Ensure driver card present
        try:
            if "driver" not in confirmation and confirmation.get("driver_id"):
                confirmation["driver"] = build_driver_card(
                    str(confirmation["driver_id"])
                )
        except Exception:
            pass
        channel.queue_declare(queue=QUEUE_CONFIRMATIONS, durable=True)
        channel.basic_publish(
            exchange="",
            routing_key=QUEUE_CONFIRMATIONS,
            body=json.dumps(confirmation, ensure_ascii=False).encode("utf-8"),
        )
        connection.close()
        logging.info("[SEND] confirmations → %s", confirmation)
    except Exception as e:
        logging.exception(f"Failed to publish confirmation: {e}")


@router.callback_query(F.data.startswith("accept:"))
async def cb_accept(call: types.CallbackQuery):
    order_id = call.data.split(":", 1)[1]
    uid = str(call.from_user.id)
    d = drivers.get(uid)
    if not d or not d.get("approved"):
        await call.answer("Спочатку завершіть реєстрацію.", show_alert=True)
        return

    lock = get_order_lock(order_id)
    async with lock:
        target = next(
            (o for o in orders_state["orders"] if o.get("id") == order_id), None
        )
        if not target:
            await call.answer("Замовлення не знайдено.", show_alert=True)
            return
        if target.get("accepted_by") and int(target.get("accepted_by")) != int(uid):
            await call.answer("Замовлення вже прийняте іншим водієм.", show_alert=True)
            return
        target["status"] = "accepted"
        target["accepted_by"] = int(uid)
        target["accepted_at"] = now_iso()
        drivers[uid]["active_order_id"] = order_id
        drivers[uid]["today"]["accepted"] = drivers[uid]["today"].get("accepted", 0) + 1
        save_orders()
        save_drivers(drivers)

    await call.message.edit_text(
        format_order_card(target) + "\n✅ Ви прийняли замовлення.", parse_mode="HTML"
    )
    # --- Кнопки для водія: Прибув / Почати / Завершити + Зв’язок + Навігація ---
    passenger = target.get("passenger", {})
    p_id = passenger.get("user_id")
    p_username = passenger.get("username")

    coords = (target.get("pickup") or {}).get("coords") or []

    rows = [
        [InlineKeyboardButton(text="✅ Прибув", callback_data=f"arrived:{order_id}")],
        [
            InlineKeyboardButton(
                text="🚖 Почати поїздку", callback_data=f"start:{order_id}"
            )
        ],
        [
            InlineKeyboardButton(
                text="🏁 Завершити поїздку", callback_data=f"finish:{order_id}"
            )
        ],
    ]

    # кнопка зв’язку з пасажиром
    if p_username:
        rows.append(
            [
                InlineKeyboardButton(
                    text="💬 Написати в Telegram", url=f"https://t.me/{p_username}"
                )
            ]
        )
    elif p_id:
        rows.append(
            [
                InlineKeyboardButton(
                    text="💬 Написати в Telegram", url=f"tg://user?id={p_id}"
                )
            ]
        )

    # кнопки навігації
    if isinstance(coords, (list, tuple)) and len(coords) == 2:
        lat, lon = coords
        google_url = f"https://www.google.com/maps/dir/?api=1&destination={lat},{lon}"
        waze_url = f"https://waze.com/ul?ll={lat},{lon}&navigate=yes"
        rows.append(
            [
                InlineKeyboardButton(text="🗺 Google Maps", url=google_url),
                InlineKeyboardButton(text="🚖 Waze", url=waze_url),
            ]
        )

    kb = InlineKeyboardMarkup(inline_keyboard=rows)
    await call.message.edit_reply_markup(reply_markup=kb)

    await call.answer("Прийнято")

    # Publish confirmation to MQ
    raw = ORDERS_BY_ID.get(order_id, {})
    confirmation = {"status": "accepted", "order_id": order_id, "driver_id": int(uid)}
    for key in ("from", "to", "pickup", "dropoff"):
        if key in raw:
            confirmation[key] = raw[key]
    publish_confirmation(confirmation)


@router.callback_query(F.data.startswith("decline:"))
async def cb_decline(call: types.CallbackQuery):
    uid = str(call.from_user.id)
    if uid in drivers:
        drivers[uid]["today"]["declined"] = drivers[uid]["today"].get("declined", 0) + 1
        save_drivers(drivers)

        await call.message.delete()
        await call.message.delete()
    await call.answer("Відхилено.")


@router.callback_query(F.data.startswith("arrived:"))
async def cb_arrived(call: types.CallbackQuery):
    uid = str(call.from_user.id)
    order_id = call.data.split(":", 1)[1]
    if drivers.get(uid, {}).get("active_order_id") != order_id:
        await call.answer("Це не ваше активне замовлення.", show_alert=True)
        return
    target = next((o for o in orders_state["orders"] if o.get("id") == order_id), None)
    if not target:
        await call.answer("Замовлення не знайдено.", show_alert=True)
        return
    target["status"] = "arrived"
    target["arrived_at"] = now_iso()
    save_orders()
    await call.message.answer("📍 Позначено як 'Прибув'.")
    await call.answer()
    # Publish ARRIVED to MQ
    publish_confirmation(
        {
            "status": "arrived",
            "order_id": order_id,
            "driver_id": int(uid),
            "free_wait_min": 3,
        }
    )


@router.callback_query(F.data.startswith("start:"))
async def cb_start(call: types.CallbackQuery):
    uid = str(call.from_user.id)
    order_id = call.data.split(":", 1)[1]

    if drivers.get(uid, {}).get("active_order_id") != order_id:
        await call.answer("Це не ваше активне замовлення.", show_alert=True)
        return

    target = next((o for o in orders_state["orders"] if o.get("id") == order_id), None)
    if not target:
        await call.answer("Замовлення не знайдено.", show_alert=True)
        return

    target["status"] = "in_progress"
    target["started_at"] = now_iso()
    save_orders()

    # === Кнопки для відкриття навігатора ===
    coords = (target.get("dropoff") or {}).get("coords") or []
    if isinstance(coords, (list, tuple)) and len(coords) == 2:
        lat, lon = coords
        google_url = f"https://www.google.com/maps/dir/?api=1&destination={lat},{lon}"
        waze_url = f"https://waze.com/ul?ll={lat},{lon}&navigate=yes"
        kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="🗺 Google Maps", url=google_url)],
                [InlineKeyboardButton(text="🚦 Waze", url=waze_url)],
            ]
        )
        await call.message.answer(
            "▶️ Поїздку розпочато. Оберіть навігатор:", reply_markup=kb
        )
    else:
        await call.message.answer("▶️ Поїздку розпочато.")
        rows = []

    await call.answer()


@router.callback_query(F.data.startswith("finish:"))
async def cb_finish(call: types.CallbackQuery):
    uid = str(call.from_user.id)
    order_id = call.data.split(":", 1)[1]

    if drivers.get(uid, {}).get("active_order_id") != order_id:
        await call.answer("Це не ваше активне замовлення.", show_alert=True)
        return

    target = next((o for o in orders_state["orders"] if o.get("id") == order_id), None)
    if not target:
        await call.answer("Замовлення не знайдено.", show_alert=True)
        return

    # --- Завершення замовлення ---
    target["status"] = "done"
    target["finished_at"] = now_iso()
    save_orders()

    # --- Додаємо статистику ---
    price = float(target.get("price", 0) or 0)
    drivers[uid]["today"]["trips"] = drivers[uid]["today"].get("trips", 0) + 1
    drivers[uid]["today"]["earn"] = drivers[uid]["today"].get("earn", 0) + price

    # --- Оновлюємо локацію на кінцеву точку замовлення ---
    if "dropoff" in target and "coords" in target["dropoff"]:
        drivers[uid]["last_location"] = target["dropoff"]["coords"]

    # --- Звільняємо водія від активного замовлення ---
    drivers[uid]["active_order_id"] = None

    # --- Водій залишається онлайн ---
    drivers[uid]["online"] = True

    save_drivers(drivers)

    # --- Повідомлення водію ---
    await call.message.answer(
        f"✅ Поїздку завершено. Локацію оновлено. Заробіток: {fmt_price(price)}"
    )
    await call.answer()

    # --- Відправляємо підтвердження MQ ---
    publish_confirmation(
        {"status": "finished", "order_id": order_id, "driver_id": int(uid)}
    )

    # --- Відправляємо підтвердження MQ ---
    publish_confirmation(
        {"status": "finished", "order_id": order_id, "driver_id": int(uid)}
    )


@router.callback_query(F.data.startswith("cancel:"))
async def cb_cancel(call: types.CallbackQuery):
    uid = str(call.from_user.id)
    driver = drivers.get(uid)
    if not driver:
        await call.answer("Помилка: водія не знайдено.", show_alert=True)
        return

    order_id = driver.get("active_order_id")
    if not order_id:
        await call.answer("У вас немає активного замовлення.", show_alert=True)
        return

    target = next((o for o in orders_state["orders"] if o.get("id") == order_id), None)
    if not target:
        await call.answer("Замовлення не знайдено.", show_alert=True)
        return

    # 1) фіксуємо факт скасування
    target["status"] = "canceled"
    target["canceled_at"] = now_iso()
    target["accepted_by"] = None
    target.pop("sent_to", None)
    target.pop("viewed_by", None)
    target["created_at"] = now_iso()

    # водій вільний від замовлення і лишається онлайн
    driver["active_order_id"] = None
    driver["online"] = True

    save_orders()
    save_drivers(drivers)

    await call.message.edit_text("❌ Ви скасували поїздку.")
    await call.answer()

    # 2) повідомляємо пасажирський бот через MQ
    publish_confirmation(
        {
            "status": "driver_cancelled",
            "order_id": order_id,
            "passenger_id": target.get("passenger_id"),
        }
    )

    # 3) повертаємо замовлення у пошук іншого водія
    target["status"] = "searching"
    target["accepted_by"] = None
    save_orders()
    publish_order(target)


# ----------------------------
# Misc commands
# ----------------------------


@router.message(Command("ping"))
async def ping(message: types.Message):
    await message.answer("pong")


@router.message(Command("id"))
async def my_id(message: types.Message):
    await message.answer(
        f"👤 user_id: <code>{message.from_user.id}</code>\n💬 chat_id: <code>{message.chat.id}</code>",
        parse_mode="HTML",
    )


# ----------------------------
# Location update
# ----------------------------


@router.message(F.text == "📍 Оновити локацію")
async def update_location_menu(message: types.Message, state: FSMContext):
    kb = ReplyKeyboardMarkup(
        resize_keyboard=True,
        keyboard=[
            [KeyboardButton(text="📌 Поділитися геопозицією", request_location=True)],
            [KeyboardButton(text="🏠 Ввести адресу вручну")],
            [KeyboardButton(text="⬅️ Назад до меню")],
        ],
    )
    await message.answer("Оновіть свою локацію:", reply_markup=kb)
    await state.set_state(LocationUpdate.waiting_for_geo)


@router.message(F.location, LocationUpdate.waiting_for_geo)
async def save_geo_location(message: types.Message, state: FSMContext):
    uid = str(message.from_user.id)
    if uid in drivers:
        drivers[uid]["last_location"] = [
            message.location.latitude,
            message.location.longitude,
        ]
        save_drivers(drivers)

        await message.answer(
            f"✅ Локація оновлена\n🌍 https://maps.google.com/?q={message.location.latitude},{message.location.longitude}",
            reply_markup=driver_main_menu(int(uid)),
        )
    else:
        await message.answer("❌ Спочатку зареєструйтесь у системі.")
    await state.clear()


@router.message(F.text == "🏠 Ввести адресу вручну", LocationUpdate.waiting_for_geo)
async def ask_for_address(message: types.Message, state: FSMContext):
    await message.answer(
        "Введіть свою адресу (наприклад: Київ, Хрещатик 1):",
        reply_markup=ReplyKeyboardRemove(),
    )
    await state.set_state(LocationUpdate.waiting_for_address)


@router.message(LocationUpdate.waiting_for_address)
async def save_address_location(message: types.Message, state: FSMContext):
    uid = str(message.from_user.id)
    address = message.text.strip()
    try:
        url = "https://nominatim.openstreetmap.org/search"
        params = {"format": "json", "q": address, "limit": 1}
        headers = {"User-Agent": "FlyTaxiBot/1.0 (contact@example.com)"}
        r = requests.get(url, params=params, headers=headers, timeout=10)
        data = r.json()
        if data:
            lat = float(data[0]["lat"])
            lon = float(data[0]["lon"])
            if uid in drivers:
                drivers[uid]["last_location"] = [lat, lon]
                save_drivers(drivers)

                await message.answer(
                    f"✅ Локація оновлена\n🌍 https://maps.google.com/?q={lat},{lon}",
                    reply_markup=driver_main_menu(int(uid)),
                )
            else:
                await message.answer("❌ Спочатку зареєструйтесь у системі.")
        else:
            await message.answer("❌ Адресу не знайдено. Спробуйте ще раз.")
            return
    except Exception as e:
        await message.answer(f"❌ Помилка геокодування: {e}")
        return
    await state.clear()


@router.message(F.text == "⬅️ Назад до меню")
async def back_to_menu(message: types.Message, state: FSMContext):
    await state.clear()
    await message.answer(
        "Повертаємо в головне меню.",
        reply_markup=driver_main_menu(message.from_user.id),
    )


# ----------------------------
# RabbitMQ consumer (background thread)
# ----------------------------


class MQConsumerThread(threading.Thread):
    def __init__(self, loop: asyncio.AbstractEventLoop):
        super().__init__(daemon=True)
        self.loop = loop
        self._stop = threading.Event()
        self.connection = None
        self.channel = None

    def run(self):
        while not self._stop.is_set():
            try:
                params = pika.ConnectionParameters(
                    RABBITMQ_HOST, heartbeat=30, blocked_connection_timeout=300
                )
                self.connection = pika.BlockingConnection(params)
                self.channel = self.connection.channel()
                self.channel.queue_declare(queue=QUEUE_ORDERS, durable=True)
                logging.info(f"[MQ] Connected. Consuming from '{QUEUE_ORDERS}'")

                for method_frame, properties, body in self.channel.consume(
                    QUEUE_ORDERS, inactivity_timeout=1, auto_ack=True
                ):
                    if self._stop.is_set():
                        break
                    if body is None:
                        continue
                    try:
                        order = json.loads(body)

                    except Exception:
                        logging.exception("Invalid JSON from MQ")
                        continue

                    # Guarantee ID
                    oid = str(order.get("id") or int(time.time() * 1000))
                    order["id"] = oid

                    # Store raw for pass-through to confirmation
                    ORDERS_BY_ID[oid] = order

                    # Put into orders_state list (ensure single instance)
                    existing = next(
                        (o for o in orders_state["orders"] if o.get("id") == oid), None
                    )
                    if existing:
                        existing.update(order)
                    else:
                        orders_state["orders"].append(order)
                    save_orders()

                    # Schedule coroutine into bot loop
                    asyncio.run_coroutine_threadsafe(
                        dispatch_order_to_nearby_drivers(order.copy()), self.loop
                    )

                try:
                    if self.connection and self.connection.is_open:
                        self.connection.close()
                except Exception:
                    pass

            except Exception as e:
                logging.error(f"[MQ] Connection error: {e}. Reconnecting in 3s...")
                time.sleep(3)

    def stop(self):
        self._stop.set()
        try:
            if self.connection and self.connection.is_open:
                self.connection.close()
        except Exception:
            pass


mq_thread: Optional[MQConsumerThread] = None


async def on_startup():
    global mq_thread
    loop = asyncio.get_running_loop()
    if mq_thread is None or not mq_thread.is_alive():
        mq_thread = MQConsumerThread(loop)
        mq_thread.start()
        logging.info("[MQ] Consumer thread started")


async def on_shutdown():
    global mq_thread
    if mq_thread and mq_thread.is_alive():
        mq_thread.stop()
        logging.info("[MQ] Consumer thread stopped")


dp.include_router(router)


async def main():
    await on_startup()
    try:
        await dp.start_polling(bot)
    finally:
        await on_shutdown()


if __name__ == "__main__":
    asyncio.run(main())


# ----------------------------
# DEBUG: Ловимо всі callback-и
# ----------------------------
@router.callback_query(lambda c: True)
async def debug_all_callbacks(callback_query: CallbackQuery):
    logging.info("[DEBUG DRIVER] Callback received: %s", callback_query.data)
