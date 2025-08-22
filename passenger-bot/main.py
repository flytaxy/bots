# -*- coding: utf-8 -*-
# === FlyTaxi Passenger Bot (Aiogram v3) — ORIGINAL FEATURES + RabbitMQ integration ===
# Збережено всі функції з "копія main (13).py" і додано:
#   • Публікацію замовлень у RabbitMQ (QUEUE_ORDERS)
#   • Фоновий споживач підтверджень з RabbitMQ (QUEUE_CONFIRMATIONS)
#   • Зв'язок order_id → chat_id/user_id для коректної доставки нотифікацій
#
# Env (.env):
#   BOT_TOKEN=...
#   GOOGLE_MAPS_API_KEY=...
#   RABBITMQ_HOST=localhost
#   QUEUE_ORDERS=orders
#   QUEUE_CONFIRMATIONS=confirmations
#
# pip install aiogram==3.* python-dotenv geopy pika pytz
#
import os
import json
import logging
import threading
import time as _time
from datetime import datetime, time
import pytz
import asyncio
import uuid


from keyboards import class_keyboard
from math import radians, sin, cos, asin, sqrt
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import CommandStart, Command, StateFilter
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from dotenv import load_dotenv

from cd import calculate_price
from maps import build_route

from geopy.geocoders import GoogleV3
from geopy.location import Location as GeoLocation

import pika  # NEW

load_dotenv()

API_TOKEN = os.getenv("BOT_TOKEN")
GOOGLE_MAPS_API_KEY = os.getenv("GOOGLE_MAPS_API_KEY")
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")  # NEW
QUEUE_ORDERS = os.getenv("QUEUE_ORDERS", "orders")  # NEW
QUEUE_CONFIRMATIONS = os.getenv("QUEUE_CONFIRMATIONS", "confirmations")  # NEW
SERVICE_CENTER_LAT = float(os.getenv("SERVICE_CENTER_LAT", "50.4501"))
SERVICE_CENTER_LON = float(os.getenv("SERVICE_CENTER_LON", "30.5234"))
SERVICE_RADIUS_KM = float(os.getenv("SERVICE_RADIUS_KM", "100"))
SERVICE_AREA_NAME = os.getenv("SERVICE_AREA_NAME", "Києва")

logging.basicConfig(level=logging.INFO)

bot = Bot(token=API_TOKEN)
dp = Dispatcher(storage=MemoryStorage())

user_states = {}
USERS_FILE = "users.json"
ORDERS_FILE = "orders.json"
ORDERS_INDEX_FILE = (
    "orders_index.json"  # NEW: order_id → {chat_id, user_id, await_rating?}
)

TARIFFS = ["Стандарт", "Комфорт", "Бізнес"]
RESTART_TEXT = "🔄 Перезапустити"
PROFILE_TEXT = "👤 Мій профіль"
SUPPORT_TEXT = "🆘 Служба підтримки"
SUPPORT_BUTTON = KeyboardButton(text=SUPPORT_TEXT)
ADD_STOP_TEXT = "➕ Додати зупинку"
NEXT_TEXT = "✅ Далі"
CONFIRM_TEXT = "✅ Підтвердити"
CANCEL_TEXT = "❌ Скасувати"

# Favorites/UI texts
SAVE_HOME_TEXT = "🏠 Зберегти як Дім"
SAVE_WORK_TEXT = "🏢 Зберегти як Робота"
SKIP_SAVE_TEXT = "⏭️ Не зараз"

HOME_TEXT = "🏠 Дім"
WORK_TEXT = "🏢 Робота"
FAVORITES_PICK_TEXT = "⭐ Обране"
MANUAL_INPUT_TEXT = "✍️ Ввести адресу вручну"
SHARE_GEO_TEXT = "📍 Поділитися локацією"

# Final destination save texts
SAVE_HOME_FINAL_TEXT = "🏠 Зберегти фініш як Дім"
SAVE_WORK_FINAL_TEXT = "🏢 Зберегти фініш як Робота"
ADD_FAVORITE_FINAL_TEXT = "⭐ Додати фініш в Обране"
CONTINUE_TEXT = "▶️ Продовжити"

EXTRA_STOP_FEE = 30  # грн
MAX_STOPS = 5

RESTART_BUTTON = KeyboardButton(text=RESTART_TEXT)
PROFILE_BUTTON = KeyboardButton(text=PROFILE_TEXT)

NEGATIVE_REASONS = [
    "Некультурний водій",
    "Брудне авто",
    "Запах у салоні",
    "Небезпечне водіння",
    "Запізнення водія",
    "Невірний маршрут/об'їзди",
    "Не працює кондиціонер/обігрів",
    "Інше",
]

# ——— ETA (подача авто) ———
ETA_BASE_MIN = {"Стандарт": 6, "Комфорт": 8, "Бізнес": 10}


def _haversine_km(a, b):
    lat1, lon1 = a
    lat2, lon2 = b
    R = 6371.0
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    lat1 = radians(lat1)
    lat2 = radians(lat2)
    h = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    return 2 * R * asin(sqrt(h))


def _inside_service(lat, lon) -> bool:
    return (
        _haversine_km((lat, lon), (SERVICE_CENTER_LAT, SERVICE_CENTER_LON))
        <= SERVICE_RADIUS_KM
    )


async def _guard_point(message, lat, lon, what: str) -> bool:
    if _inside_service(lat, lon):
        return True
    dist = _haversine_km((lat, lon), (SERVICE_CENTER_LAT, SERVICE_CENTER_LON))
    await message.answer(
        f"⛔ {what} поза зоною обслуговування ({SERVICE_RADIUS_KM:.0f} км від {SERVICE_AREA_NAME}). "
        f"Відстань ≈ {dist:.1f} км. Оберіть іншу адресу."
    )
    return False


def eta_minutes(tariff: str, multiplier: float) -> int:
    base = ETA_BASE_MIN.get(tariff, 7)
    add = 0
    if multiplier >= 1.7:
        add = 5
    elif multiplier >= 1.25:
        add = 2
    return base + add


# === PEAK HOURS ===
def is_peak_hour() -> float:
    kyiv_now = datetime.now(pytz.timezone("Europe/Kiev"))
    current_time = kyiv_now.time()
    weekday = kyiv_now.weekday()  # Пн=0, Нд=6

    if weekday < 5:  # будні
        peak_periods_30 = [
            (time(5, 0), time(6, 0)),
            (time(7, 30), time(10, 30)),
            (time(16, 30), time(19, 30)),
            (time(21, 30), time(23, 59, 59)),
        ]
        for start, end in peak_periods_30:
            if start <= current_time <= end:
                return 1.3
    else:  # субота або неділя
        peak_periods_30 = [
            (time(5, 0), time(6, 0)),
            (time(9, 0), time(11, 0)),
            (time(14, 0), time(17, 0)),
        ]
        peak_periods_75 = [
            (time(21, 30), time(23, 59, 59)),
        ]
        for start, end in peak_periods_75:
            if start <= current_time <= end:
                return 1.75
        for start, end in peak_periods_30:
            if start <= current_time <= end:
                return 1.3
    return 1.0


# === UTILS ===
def _load_json(path, default):
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            try:
                return json.load(f)
            except json.JSONDecodeError:
                return default
    return default


def _save_json(path, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)


# === USERS ===
if os.path.exists(USERS_FILE):
    with open(USERS_FILE, "r", encoding="utf-8") as f:
        registered_users = json.load(f)
else:
    registered_users = {}


def _ensure_user(user_id: str):
    u = registered_users.setdefault(user_id, {})
    u.setdefault("name", "")
    u.setdefault("phone", "")
    u.setdefault("favorites", {"home": None, "work": None, "other": []})
    u.setdefault("favorites_prompted", False)
    u.setdefault("home_prompted_final", False)
    u.setdefault("work_prompted_final", False)


def _save_users():
    _save_json(USERS_FILE, registered_users)


# === ORDERS INDEX (NEW) ===
orders_index = _load_json(
    ORDERS_INDEX_FILE, {}
)  # {order_id: {chat_id, user_id, await_rating?, payload?}}


def _save_orders_index():
    _save_json(ORDERS_INDEX_FILE, orders_index)


# === STATES ===
class OrderTaxi(StatesGroup):
    waiting_for_phone = State()
    waiting_for_location = State()
    waiting_for_destination = State()
    waiting_for_additional_stops = State()
    waiting_for_tariff = State()
    waiting_for_payment_type = State()
    waiting_for_confirmation = State()
    waiting_for_driver_confirmation = State()
    waiting_for_rating = State()
    waiting_for_feedback_reason = State()
    waiting_for_feedback_other = State()
    waiting_for_save_favorite = State()
    waiting_for_save_final = State()


# === HELPERS ===
def kb_with_common_rows(rows):
    base = list(rows) if rows else []
    base.append([SUPPORT_BUTTON])
    base.append([PROFILE_BUTTON])
    base.append([RESTART_BUTTON])
    return ReplyKeyboardMarkup(keyboard=base, resize_keyboard=True)


def kb_start_selection(user_id: str):
    _ensure_user(user_id)
    fav = registered_users[user_id]["favorites"]
    rows = []
    rows.append([KeyboardButton(text=SHARE_GEO_TEXT, request_location=True)])
    hw = []
    if fav.get("home"):
        hw.append(KeyboardButton(text=HOME_TEXT))
    if fav.get("work"):
        hw.append(KeyboardButton(text=WORK_TEXT))
    if hw:
        rows.append(hw)
    if fav.get("other"):
        rows.append([KeyboardButton(text=FAVORITES_PICK_TEXT)])
    rows.append([KeyboardButton(text=MANUAL_INPUT_TEXT)])
    return kb_with_common_rows(rows)


def kb_dest_selection(user_id: str):
    _ensure_user(user_id)
    fav = registered_users[user_id]["favorites"]
    rows = []
    hw = []
    if fav.get("home"):
        hw.append(KeyboardButton(text=f"🏁 {HOME_TEXT}"))
    if fav.get("work"):
        hw.append(KeyboardButton(text=f"🏁 {WORK_TEXT}"))
    if hw:
        rows.append(hw)
    if fav.get("other"):
        rows.append([KeyboardButton(text=FAVORITES_PICK_TEXT)])
    rows.append([KeyboardButton(text=MANUAL_INPUT_TEXT)])
    return kb_with_common_rows(rows)


def save_favorite(user_id: str, which: str, coords, address: str):
    _ensure_user(user_id)
    registered_users[user_id]["favorites"][which] = {
        "coords": coords,
        "address": address,
    }
    _save_users()


def add_other_favorite(user_id: str, coords, address: str):
    _ensure_user(user_id)
    other = registered_users[user_id]["favorites"].get("other", [])
    if not any(isinstance(x, dict) and x.get("address") == address for x in other):
        other.append({"coords": coords, "address": address})
        registered_users[user_id]["favorites"]["other"] = other
        _save_users()


def human_address_from_coords(lat, lng) -> str:
    try:
        geolocator = GoogleV3(api_key=GOOGLE_MAPS_API_KEY)
        loc: GeoLocation = geolocator.reverse((lat, lng))
        return loc.address if loc else f"{lat:.5f},{lng:.5f}"
    except Exception:
        return f"{lat:.5f},{lng:.5f}"


def list_other_favorites_kb(user_id: str, as_destination: bool):
    _ensure_user(user_id)
    other = registered_users[user_id]["favorites"].get("other", [])
    if not other:
        return None
    buttons = []
    for item in other:
        address = item["address"] if isinstance(item, dict) else str(item)
        label = f"🏁 {address}" if as_destination else address
        buttons.append([KeyboardButton(text=label)])
    return kb_with_common_rows(buttons)


# === PROFILE VIEW ===
async def show_profile(message: types.Message):
    user_id = str(message.from_user.id)
    _ensure_user(user_id)
    info = registered_users.get(user_id, {})
    name = info.get("name") or message.from_user.full_name
    phone = info.get("phone", "—")
    fav = info.get("favorites", {})
    home = fav.get("home")
    work = fav.get("work")
    other = fav.get("other", [])
    fav_block = []
    if home:
        fav_block.append(
            f"🏠 Дім: {home.get('address') if isinstance(home, dict) else home}"
        )
    if work:
        fav_block.append(
            f"🏢 Робота: {work.get('address') if isinstance(work, dict) else work}"
        )
    if other:
        fav_block.append(
            "⭐ Інші: "
            + ", ".join(
                (it["address"] if isinstance(it, dict) else str(it)) for it in other[:5]
            )
            + ("" if len(other) <= 5 else f" …(+{len(other)-5})")
        )
    if not fav_block:
        fav_block.append("Обрані адреси ще не збережені.")
    text = (
        f"👤 <b>Мій профіль</b>\n\n"
        f"Ім'я: {name}\nТелефон: {phone}\n\n"
        f"⭐ <b>Обрані адреси</b>:\n" + "\n".join(fav_block)
    )
    kb = kb_with_common_rows([])
    await message.answer(text, reply_markup=kb, parse_mode="HTML")


# === GLOBAL HANDLERS ===
@dp.message(F.text == RESTART_TEXT, StateFilter("*"))
async def restart_global(message: types.Message, state: FSMContext):
    await state.clear()
    await start(message, state)


@dp.message(F.text == PROFILE_TEXT, StateFilter("*"))
async def profile_global(message: types.Message, state: FSMContext):
    await show_profile(message)


@dp.message(F.text == SUPPORT_TEXT, StateFilter("*"))
async def support_global(message: types.Message, state: FSMContext):
    await message.answer(
        "🆘 Підтримка: <b>@FlyTaxiSupport</b> (8:00–23:00)",
        parse_mode="HTML",
    )


# === SCENARIO ===
@dp.message(CommandStart())
async def start(message: types.Message, state: FSMContext):
    user_id = str(message.from_user.id)
    _ensure_user(user_id)
    multiplier = is_peak_hour()
    user_name = message.from_user.first_name or "пасажир"
    greet_text = f"👋 Вітаю, {user_name}! 🚖 Вас вітає FlyTaxi."
    if multiplier > 1.0:
        pct = int((multiplier - 1.0) * 100)
        greet_text += f"\n⚠️ Зараз діють пікові тарифи: ціни вищі на {pct}%"
    await message.answer(greet_text)

    if not registered_users[user_id].get("phone"):
        kb = kb_with_common_rows(
            [[KeyboardButton(text="📱 Надіслати номер", request_contact=True)]]
        )
        await message.answer(
            "📱 Надішліть номер телефону (кнопка нижче).", reply_markup=kb
        )
        await state.set_state(OrderTaxi.waiting_for_phone)
        return

    kyiv_time = datetime.now(pytz.timezone("Europe/Kiev"))
    if 0 <= kyiv_time.hour < 5:
        await message.answer(
            "⛔ Замовлення таксі неможливе з 00:00 до 05:00 (комендантська година)."
        )
        return

    kb = kb_start_selection(user_id)
    await message.answer(
        "Оберіть спосіб задання старту або введіть адресу:", reply_markup=kb
    )
    await state.set_state(OrderTaxi.waiting_for_location)


@dp.message(OrderTaxi.waiting_for_phone, F.contact)
async def get_phone(message: types.Message, state: FSMContext):
    user_id = str(message.from_user.id)
    _ensure_user(user_id)
    registered_users[user_id]["name"] = message.from_user.full_name
    registered_users[user_id]["username"] = message.from_user.username
    registered_users[user_id]["phone"] = message.contact.phone_number
    _save_users()
    kb = kb_start_selection(user_id)
    await message.answer(
        "✅ Реєстрацію завершено.\nОберіть спосіб задання старту:", reply_markup=kb
    )
    await state.set_state(OrderTaxi.waiting_for_location)


@dp.message(OrderTaxi.waiting_for_phone)
async def remind_phone(message: types.Message, state: FSMContext):
    kb = kb_with_common_rows(
        [[KeyboardButton(text="📱 Надіслати номер", request_contact=True)]]
    )
    await message.answer("Будь ласка, надішліть номер (кнопка нижче).", reply_markup=kb)


# === START selection buttons ===
@dp.message(OrderTaxi.waiting_for_location, F.text == MANUAL_INPUT_TEXT)
async def start_manual_prompt(message: types.Message, state: FSMContext):
    await message.answer(
        "Введіть ПОЧАТКОВУ адресу текстом:", reply_markup=types.ReplyKeyboardRemove()
    )


@dp.message(OrderTaxi.waiting_for_location, F.text == FAVORITES_PICK_TEXT)
async def start_pick_favorite(message: types.Message, state: FSMContext):
    user_id = str(message.from_user.id)
    kb = list_other_favorites_kb(user_id, as_destination=False)
    if not kb:
        await message.answer("⭐ У вас ще немає збережених обраних адрес.")
        return
    await message.answer("Оберіть старт із обраних адрес:", reply_markup=kb)


@dp.message(OrderTaxi.waiting_for_location, F.text.in_({HOME_TEXT, WORK_TEXT}))
async def start_choose_home_or_work(message: types.Message, state: FSMContext):
    user_id = str(message.from_user.id)
    fav = registered_users[user_id]["favorites"]
    which = "home" if message.text == HOME_TEXT else "work"
    entry = fav.get(which)
    if not entry or not isinstance(entry, dict) or not entry.get("coords"):
        await message.answer("Ця адреса ще не збережена або не має координат.")
        return
    coords = tuple(entry["coords"])
    await state.update_data(
        start_coords=coords, route_addresses=[], route_coords=[coords], distance_km=0.0
    )
    kb = kb_dest_selection(user_id)
    await message.answer(
        "🏁 Введіть КІНЦЕВУ адресу або оберіть варіант нижче:", reply_markup=kb
    )
    await state.set_state(OrderTaxi.waiting_for_destination)


@dp.message(OrderTaxi.waiting_for_location)
async def get_location_or_address(message: types.Message, state: FSMContext):
    user_id = str(message.from_user.id)
    _ensure_user(user_id)
    geolocator = GoogleV3(api_key=GOOGLE_MAPS_API_KEY)
    if message.location:
        lat, lng = message.location.latitude, message.location.longitude
        if not await _guard_point(message, lat, lng, "Точка"):
            return  # зупиняємо, якщо поза зоною
        await state.update_data(
            start_coords=(lat, lng),
            route_addresses=[],
            route_coords=[(lat, lng)],
            distance_km=0.0,
        )

        if not registered_users[user_id].get("favorites_prompted", False):
            rows = [
                [
                    KeyboardButton(text=SAVE_HOME_TEXT),
                    KeyboardButton(text=SAVE_WORK_TEXT),
                ],
                [KeyboardButton(text=SKIP_SAVE_TEXT)],
            ]
            kb = kb_with_common_rows(rows)
            registered_users[user_id]["favorites_prompted"] = True
            _save_users()
            await message.answer(
                "Зберегти цю локацію як Дім або Робота? (питаємо лише один раз)",
                reply_markup=kb,
            )
            await state.set_state(OrderTaxi.waiting_for_save_favorite)
            return
        kb = kb_dest_selection(user_id)
        await message.answer(
            "🏁 Введіть КІНЦЕВУ адресу або оберіть варіант нижче:", reply_markup=kb
        )
        await state.set_state(OrderTaxi.waiting_for_destination)
    else:
        if message.text in {
            SHARE_GEO_TEXT,
            FAVORITES_PICK_TEXT,
            HOME_TEXT,
            WORK_TEXT,
            MANUAL_INPUT_TEXT,
        }:
            return
        location = geolocator.geocode(message.text)
        if not location:
            kb = kb_start_selection(user_id)
            await message.answer(
                "❌ Не знайшов такої адреси. Спробуйте ще або оберіть інший спосіб.",
                reply_markup=kb,
            )
            # Перевірка кінцевої точки
        if not await _guard_point(
            message, location.latitude, location.longitude, "Кінцева точка"
        ):
            return

        await state.update_data(
            start_coords=(location.latitude, location.longitude),
            route_addresses=[],
            route_coords=[(location.latitude, location.longitude)],
            distance_km=0.0,
        )
        kb = kb_dest_selection(user_id)
        await message.answer(
            "🏁 Введіть КІНЦЕВУ адресу або оберіть варіант нижче:", reply_markup=kb
        )
        await state.set_state(OrderTaxi.waiting_for_destination)


@dp.message(
    OrderTaxi.waiting_for_save_favorite,
    F.text.in_({SAVE_HOME_TEXT, SAVE_WORK_TEXT, SKIP_SAVE_TEXT}),
)
async def maybe_save_favorite(message: types.Message, state: FSMContext):
    user_id = str(message.from_user.id)
    data = await state.get_data()
    coords = data.get("start_coords")
    if message.text != SKIP_SAVE_TEXT and coords:
        which = "home" if message.text == SAVE_HOME_TEXT else "work"
        addr = human_address_from_coords(coords[0], coords[1])
        save_favorite(user_id, which, coords, addr)
        await message.answer(f"✅ Збережено як {which.upper()}: {addr}")
    kb = kb_dest_selection(user_id)
    await message.answer(
        "🏁 Введіть КІНЦЕВУ адресу або оберіть варіант нижче:", reply_markup=kb
    )
    await state.set_state(OrderTaxi.waiting_for_destination)


# === Destination helpers & handlers ===
@dp.message(OrderTaxi.waiting_for_destination, F.text == MANUAL_INPUT_TEXT)
async def dest_manual_prompt(message: types.Message, state: FSMContext):
    await message.answer(
        "Введіть КІНЦЕВУ адресу текстом:", reply_markup=types.ReplyKeyboardRemove()
    )


@dp.message(OrderTaxi.waiting_for_destination, F.text == FAVORITES_PICK_TEXT)
async def dest_pick_favorite(message: types.Message, state: FSMContext):
    user_id = str(message.from_user.id)
    kb = list_other_favorites_kb(user_id, as_destination=True)
    if not kb:
        await message.answer("⭐ У вас ще немає збережених обраних адрес.")
        return
    await message.answer("Оберіть місце призначення з обраних адрес:", reply_markup=kb)


@dp.message(
    OrderTaxi.waiting_for_destination,
    F.text.in_({f"🏁 {HOME_TEXT}", f"🏁 {WORK_TEXT}"}),
)
async def choose_home_or_work_as_final(message: types.Message, state: FSMContext):
    user_id = str(message.from_user.id)
    which = "home" if HOME_TEXT in message.text else "work"
    fav_entry = registered_users[user_id]["favorites"].get(which)
    if not fav_entry:
        await message.answer("Ця адреса ще не збережена.")
        return
    address = fav_entry["address"] if isinstance(fav_entry, dict) else str(fav_entry)
    data = await state.get_data()
    start_coords = data["route_coords"][-1]
    dest_coords, leg_km, map_file = build_route(start_coords, address)
    if not dest_coords:
        await message.answer("❌ Не вдалося побудувати маршрут до цієї адреси.")
        return
    await state.update_data(
        final_address=address,
        final_coords=dest_coords,
        stops_addresses=[],
        stops_coords=[],
        distance_km=leg_km,
        leg_to_final_km=leg_km,
        multiplier=is_peak_hour(),
    )
    await message.answer_photo(
        types.FSInputFile(map_file),
        caption=f"Старт → Фініш: {leg_km} км\nСумарно: {leg_km:.2f} км",
    )
    rows = [
        [KeyboardButton(text=ADD_FAVORITE_FINAL_TEXT)],
        [KeyboardButton(text=CONTINUE_TEXT)],
    ]
    kb = kb_with_common_rows(rows)
    await message.answer(
        "Хочеш зберегти фінішну адресу в Обране або продовжити?", reply_markup=kb
    )
    await state.set_state(OrderTaxi.waiting_for_save_final)


@dp.message(OrderTaxi.waiting_for_destination, F.text.regexp(r"^🏁 .+"))
async def choose_other_favorite_as_final(message: types.Message, state: FSMContext):
    user_id = str(message.from_user.id)
    address = message.text[2:].strip()
    data = await state.get_data()
    start_coords = data["route_coords"][-1]
    dest_coords, leg_km, map_file = build_route(start_coords, address)
    if not dest_coords:
        await message.answer("❌ Не вдалося побудувати маршрут до цієї адреси.")
        return
    await state.update_data(
        final_address=address,
        final_coords=dest_coords,
        stops_addresses=[],
        stops_coords=[],
        distance_km=leg_km,
        leg_to_final_km=leg_km,
        multiplier=is_peak_hour(),
    )
    await message.answer_photo(
        types.FSInputFile(map_file),
        caption=f"Старт → Фініш: {leg_km} км\nСумарно: {leg_km:.2f} км",
    )
    rows = [
        [KeyboardButton(text=ADD_FAVORITE_FINAL_TEXT)],
        [KeyboardButton(text=CONTINUE_TEXT)],
    ]
    kb = kb_with_common_rows(rows)
    await message.answer(
        "Хочеш зберегти фінішну адресу в Обране або продовжити?", reply_markup=kb
    )
    await state.set_state(OrderTaxi.waiting_for_save_final)


@dp.message(OrderTaxi.waiting_for_destination)
async def get_final_destination(message: types.Message, state: FSMContext):
    user_id = str(message.from_user.id)
    data = await state.get_data()
    if not data.get("route_coords"):
        await message.answer("Спочатку задайте старт.")
        return
    start_coords = data["route_coords"][-1]
    final_address = message.text.strip()
    dest_coords, leg_km, map_file = build_route(start_coords, final_address)
    if not dest_coords:
        kb = kb_dest_selection(user_id)
        await message.answer(
            "❌ Не вдалося знайти маршрут. Спробуйте іншу адресу або оберіть варіант нижче.",
            reply_markup=kb,
        )
        return
    await state.update_data(
        final_address=final_address,
        final_coords=dest_coords,
        stops_addresses=[],
        stops_coords=[],
        distance_km=leg_km,
        leg_to_final_km=leg_km,
        multiplier=is_peak_hour(),
    )
    await message.answer_photo(
        types.FSInputFile(map_file),
        caption=f"Старт → Фініш: {leg_km} км\nСумарно: {leg_km:.2f} км",
    )

    fav = registered_users[user_id]["favorites"]
    offer_home = (fav.get("home") is None) and (
        not registered_users[user_id].get("home_prompted_final", False)
    )
    offer_work = (fav.get("work") is None) and (
        not registered_users[user_id].get("work_prompted_final", False)
    )

    rows = []
    if offer_home:
        rows.append([KeyboardButton(text=SAVE_HOME_FINAL_TEXT)])
    if offer_work:
        rows.append([KeyboardButton(text=SAVE_WORK_FINAL_TEXT)])
    rows.append([KeyboardButton(text=ADD_FAVORITE_FINAL_TEXT)])
    rows.append([KeyboardButton(text=CONTINUE_TEXT), KeyboardButton(text=CANCEL_TEXT)])
    kb = kb_with_common_rows(rows)
    await message.answer("Хочеш зберегти фінішну адресу?", reply_markup=kb)
    await state.set_state(OrderTaxi.waiting_for_save_final)


@dp.message(
    OrderTaxi.waiting_for_save_final,
    F.text.in_(
        {
            SAVE_HOME_FINAL_TEXT,
            SAVE_WORK_FINAL_TEXT,
            ADD_FAVORITE_FINAL_TEXT,
            CONTINUE_TEXT,
            CANCEL_TEXT,
        }
    ),
)
async def handle_save_final_choice(message: types.Message, state: FSMContext):
    user_id = str(message.from_user.id)
    data = await state.get_data()
    final_address = data.get("final_address")
    final_coords = data.get("final_coords")

    if message.text == SAVE_HOME_FINAL_TEXT:
        registered_users[user_id]["home_prompted_final"] = True
        save_favorite(user_id, "home", final_coords, final_address)
        await message.answer(f"✅ Збережено Дім: {final_address}")
    elif message.text == SAVE_WORK_FINAL_TEXT:
        registered_users[user_id]["work_prompted_final"] = True
        save_favorite(user_id, "work", final_coords, final_address)
        await message.answer(f"✅ Збережено Робота: {final_address}")
    elif message.text == ADD_FAVORITE_FINAL_TEXT:
        add_other_favorite(user_id, final_coords, final_address)
        registered_users[user_id]["home_prompted_final"] = True
        registered_users[user_id]["work_prompted_final"] = True
        await message.answer("✅ Додано в Обране.")
    elif message.text in {CONTINUE_TEXT, CANCEL_TEXT}:
        registered_users[user_id]["home_prompted_final"] = True
        registered_users[user_id]["work_prompted_final"] = True

    _save_users()
    kb = kb_with_common_rows(
        [[KeyboardButton(text=ADD_STOP_TEXT), KeyboardButton(text=NEXT_TEXT)]]
    )
    await message.answer(
        "Можете додати проміжні зупинки (до 5) або перейти далі.", reply_markup=kb
    )
    await state.set_state(OrderTaxi.waiting_for_additional_stops)


@dp.message(OrderTaxi.waiting_for_additional_stops, F.text == ADD_STOP_TEXT)
async def ask_next_stop(message: types.Message, state: FSMContext):
    data = await state.get_data()
    stops = data.get("stops_addresses", [])
    if len(stops) >= MAX_STOPS:
        kb = kb_with_common_rows([[KeyboardButton(text=NEXT_TEXT)]])
        await message.answer(
            f"ℹ️ Досягнуто ліміт {MAX_STOPS} зупинок. Натисніть «✅ Далі».",
            reply_markup=kb,
        )
        return
    await message.answer(
        f"Введіть адресу зупинки #{len(stops)+1}:",
        reply_markup=types.ReplyKeyboardRemove(),
    )


@dp.message(OrderTaxi.waiting_for_additional_stops, F.text == NEXT_TEXT)
async def proceed_to_tariff(message: types.Message, state: FSMContext):
    data = await state.get_data()
    distance_km = data.get("distance_km", 0.0)
    extra_stops = len(data.get("stops_addresses", []))
    surcharge = extra_stops * EXTRA_STOP_FEE
    multiplier = data.get("multiplier", 1.0)

    tariff_rows = []
    for tariff in TARIFFS:
        base = calculate_price(distance_km, tariff)
        total = int(round(base * multiplier + surcharge))
        eta = eta_minutes(tariff, multiplier)
        tariff_rows.append([KeyboardButton(text=f"{tariff} — {total} грн • ~{eta} хв")])

    kb = kb_with_common_rows(tariff_rows)
    await message.answer(f"🛣 Загальна відстань маршруту: {distance_km:.2f} км")
    await message.answer(f"🅿️ Проміжних зупинок: {extra_stops}")
    if extra_stops > 0:
        await message.answer(
            f"➕ Додаткові зупинки: {extra_stops} × {EXTRA_STOP_FEE} грн = {surcharge} грн (додано до вартості)"
        )
    if multiplier > 1.0:
        pct = int((multiplier - 1.0) * 100)
        await message.answer(f"⚠️ Ураховано піковий тариф: +{pct}%")
    await message.answer("🚗 Оберіть клас авто:", reply_markup=kb)
    await state.set_state(OrderTaxi.waiting_for_tariff)


@dp.message(OrderTaxi.waiting_for_additional_stops)
async def add_intermediate_stop(message: types.Message, state: FSMContext):
    data = await state.get_data()
    final_address = data.get("final_address")
    final_coords = data.get("final_coords")
    if not final_address or not final_coords:
        kb = kb_with_common_rows([[KeyboardButton(text=NEXT_TEXT)]])
        await message.answer("Спочатку вкажіть кінцеву адресу.", reply_markup=kb)
        return

    stops_addresses = data.get("stops_addresses", [])
    stops_coords = data.get("stops_coords", [])
    if len(stops_addresses) >= MAX_STOPS:
        kb = kb_with_common_rows([[KeyboardButton(text=NEXT_TEXT)]])
        await message.answer(
            f"ℹ️ Досягнуто ліміт {MAX_STOPS} зупинок. Натисніть «✅ Далі».",
            reply_markup=kb,
        )
        return

    if stops_coords:
        prev_coords = stops_coords[-1]
    else:
        prev_coords = data["route_coords"][0]

    dest_coords_stop, leg_prev_to_stop, map_file_stop = build_route(
        prev_coords, message.text.strip()
    )
    if not dest_coords_stop:
        kb = kb_with_common_rows(
            [[KeyboardButton(text=ADD_STOP_TEXT), KeyboardButton(text=NEXT_TEXT)]]
        )
        await message.answer(
            "❌ Не вдалося знайти цю зупинку. Спробуйте іншу адресу або натисніть «✅ Далі».",
            reply_markup=kb,
        )
        return

    _, leg_stop_to_final, _ = build_route(dest_coords_stop, final_address)
    old_tail = data.get("leg_to_final_km", 0.0)
    new_total = (
        max(0.0, data.get("distance_km", 0.0) - old_tail)
        + leg_prev_to_stop
        + leg_stop_to_final
    )

    stops_addresses.append(message.text.strip())
    stops_coords.append(dest_coords_stop)

    await state.update_data(
        stops_addresses=stops_addresses,
        stops_coords=stops_coords,
        distance_km=new_total,
        leg_to_final_km=leg_stop_to_final,
    )

    kb = kb_with_common_rows(
        [[KeyboardButton(text=ADD_STOP_TEXT), KeyboardButton(text=NEXT_TEXT)]]
    )
    route_preview = (
        " → ".join(stops_addresses + [final_address])
        if stops_addresses
        else final_address
    )
    await message.answer_photo(
        types.FSInputFile(map_file_stop),
        caption=(
            f"Додано зупинку: {message.text.strip()}\n"
            f"Оновлена сумарна дистанція: {new_total:.2f} км\n"
            f"Маршрут: {route_preview}"
        ),
    )
    await message.answer("Можете додати ще або натиснути «✅ Далі».", reply_markup=kb)


@dp.message(OrderTaxi.waiting_for_tariff)
async def choose_tariff(message: types.Message, state: FSMContext):
    chosen = message.text.split(" — ")[0]
    if chosen not in TARIFFS:
        data = await state.get_data()
        distance_km = data.get("distance_km", 1.0)
        multiplier = data.get("multiplier", 1.0)
        extra_stops = len(data.get("stops_addresses", []))
        surcharge = extra_stops * EXTRA_STOP_FEE
        rows = []
        for t in TARIFFS:
            base = calculate_price(distance_km, t)
            total = int(round(base * multiplier + surcharge))
            eta = eta_minutes(t, multiplier)
            rows.append([KeyboardButton(text=f"{t} — {total} грн • ~{eta} хв")])
        kb = kb_with_common_rows(rows)
        await message.answer("❌ Оберіть тариф з кнопок нижче.", reply_markup=kb)
        return

    data = await state.get_data()
    distance_km = data.get("distance_km", 0.0)
    multiplier = data.get("multiplier", 1.0)
    extra_stops = len(data.get("stops_addresses", []))
    surcharge = extra_stops * EXTRA_STOP_FEE

    base_price = calculate_price(distance_km, chosen)
    price = int(round(base_price * multiplier + surcharge))

    # SAVE TARIFF HERE (fixes KeyError 'tariff')
    await state.update_data(tariff=chosen, price=price)

    kb = kb_with_common_rows(
        [
            [KeyboardButton(text="💵 Готівка")],
            [KeyboardButton(text="💳 Переказ на картку водію")],
        ]
    )
    eta = eta_minutes(chosen, multiplier)
    pct_line = (
        f"\n⚠️ Піковий тариф: +{int((multiplier-1.0)*100)}%" if multiplier > 1.0 else ""
    )
    await message.answer(
        f"💵 Вартість: {price} грн{pct_line}\n⏱ Орієнтовна подача: ~{eta} хв\n\nОберіть спосіб оплати:",
        reply_markup=kb,
    )
    await state.set_state(OrderTaxi.waiting_for_payment_type)


@dp.message(F.text, OrderTaxi.waiting_for_payment_type)
async def choose_payment(message: types.Message, state: FSMContext):
    if message.text not in ["💵 Готівка", "💳 Переказ на картку водію"]:
        kb = kb_with_common_rows(
            [
                [KeyboardButton(text="💵 Готівка")],
                [KeyboardButton(text="💳 Переказ на картку водію")],
            ]
        )
        await message.answer(
            "❌ Оберіть спосіб оплати з кнопок нижче.", reply_markup=kb
        )
        return

    data = await state.get_data()

    payload = {
        "id": str(uuid.uuid4()),
        "user_id": message.from_user.id,  # хто зробив замовлення
        "username": message.from_user.username,
        "chat_id": message.chat.id,  # 👈 потрібно для повідомлень
        "pickup": data.get("pickup"),
        "destination": data.get("destination"),
        "tariff": data.get("tariff"),
        "price": data.get("price"),
        "payment_method": message.text,
        "created_at": datetime.utcnow().isoformat(),
    }

    # відправляємо в RabbitMQ
    async def choose_payment(message: types.Message, state: FSMContext):
        data = await state.get_data()

    payload = {
        "id": str(uuid.uuid4()),
        "user_id": message.from_user.id,
        "username": message.from_user.username,
        "chat_id": message.chat.id,
        "pickup": data.get("pickup"),
        "destination": data.get("destination"),
        "tariff": data.get("tariff"),
        "price": data.get("price"),
        "payment_method": message.text,
        "created_at": datetime.utcnow().isoformat(),
    }

    # ⚠️ Більше не штовхаємо payload в RabbitMQ тут!
    await state.update_data(payment_type=message.text)

    # Формуємо попередній перегляд маршруту
    route_preview = (
        " ➝ ".join(data.get("stops_addresses", []))
        + f" ➝ {data.get('final_address', '-')}"
        if data.get("stops_addresses")
        else data.get("final_address", "-")
    )
    tariff = data.get("tariff", "-")
    price = data.get("price", "-")

    kb = kb_with_common_rows(
        [[KeyboardButton(text=CONFIRM_TEXT), KeyboardButton(text=CANCEL_TEXT)]]
    )

    await message.answer(
        f"🚖 Маршрут: {route_preview}\n"
        f"🚘 Клас авто: {tariff}\n"
        f"💰 Вартість: {price} грн\n"
        f"💳 Оплата: {message.text}\n\n"
        "Підтвердити замовлення?",
        reply_markup=kb,
    )

    await state.set_state(OrderTaxi.waiting_for_confirmation)


# === RabbitMQ PUBLISH (NEW) ===
def _publish_order_to_mq(payload: dict):
    try:
        conn = pika.BlockingConnection(pika.ConnectionParameters(RABBITMQ_HOST))
        ch = conn.channel()
        ch.queue_declare(queue=QUEUE_ORDERS, durable=True)
        ch.basic_publish(
            exchange="",
            routing_key=QUEUE_ORDERS,
            body=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        )
        conn.close()
        logging.info("Published order %s to %s", payload.get("id"), QUEUE_ORDERS)
    except Exception as e:
        logging.exception("RabbitMQ publish error: %s", e)


async def schedule_republish(order_id: str, delay: int = 30, max_attempts: int = 3):
    """Перепубліковуємо замовлення, якщо його не підтвердили, максимум max_attempts разів."""
    attempts = 0

    while attempts < max_attempts:
        await asyncio.sleep(delay)
        od = orders_index.get(order_id)
        if not od or od.get("confirmed"):
            return  # замовлення вже підтверджене або видалене

        attempts += 1
        logging.info(f"⏳ Замовлення {order_id} не підтверджене. Спроба #{attempts}")

        if attempts < max_attempts:
            _publish_order_to_mq(od["payload"])

        else:
            od = orders_index.get(order_id)

    if not od:  # якщо замовлення вже видалено
        return

    chat_id = od.get("chat_id")

    if chat_id:
        # Повідомлення користувачу
        await bot.send_message(
            chat_id,
            "🚫 На жаль, авто не знайдено.\nБудь ласка, виберіть інший клас авто:",
        )

        # Переводимо у стан вибору класу авто
        ctx = dp.fsm.get_context(bot=bot, chat_id=chat_id, user_id=chat_id)
        await ctx.set_state(OrderTaxi.waiting_for_tariff)

        # Використовуємо готову клавіатуру
        await bot.send_message(
            chat_id, "Оберіть клас авто:", reply_markup=class_keyboard
        )


def _make_order_payload(user: types.User, data: dict, order_id: str) -> dict:
    # Збираємо корисне навантаження для водійського боку
    start_lat, start_lng = data["route_coords"][0]
    payload = {
        "id": order_id,
        "created_at": datetime.now(pytz.timezone("Europe/Kiev")).isoformat(),
        "passenger": {
            "user_id": user.id,
            "username": user.username,
            "first_name": user.first_name,
            "last_name": user.last_name,
            "phone": (registered_users.get(str(user.id), {}) or {}).get("phone"),
        },
        # ORIGINAL nested shape
        "route": {
            "start": {"coords": [start_lat, start_lng]},
            "stops": data.get("stops_addresses", []),
            "final": {
                "address": data.get("final_address"),
                "coords": list(data.get("final_coords") or []),
            },
            "distance_km": round(float(data.get("distance_km", 0.0)), 2),
        },
        "pricing": {
            "tariff": data.get("tariff"),
            "multiplier": data.get("multiplier", 1.0),
            "extra_stops_fee": EXTRA_STOP_FEE,
            "price_total": int(data.get("price", 0)),
        },
        "payment_type": data.get("payment_type"),
        "eta_min": eta_minutes(
            data.get("tariff", "Стандарт"), data.get("multiplier", 1.0)
        ),
        "status": "new",
        # FLAT mirror for driver_bot compatibility
        "pickup": {
            "address": f"{start_lat:.6f},{start_lng:.6f}",
            "coords": [start_lat, start_lng],
        },
        "dropoff": {
            "address": data.get("final_address") or "Невідомо",
            "coords": list(data.get("final_coords") or []),
        },
        "distance_km": round(float(data.get("distance_km", 0.0)), 2),
        "tariff": data.get("tariff"),
        "payment": data.get("payment_type"),
        "price": int(data.get("price", 0)),
    }
    # Ensure required fields have defaults

    if not payload.get("pickup"):
        payload["pickup"] = "Невідомо"

    if not payload.get("destination"):
        payload["destination"] = "Невідомо"

    if not payload.get("distance_km"):
        payload["distance_km"] = "Невідомо"

    if not payload.get("tariff"):
        payload["tariff"] = "Невідомо"

    if not payload.get("payment"):
        payload["payment"] = "Невідомо"

    if not payload.get("price"):
        payload["price"] = "Невідомо"

    if not payload.get("eta_min"):
        payload["eta_min"] = "Невідомо"

    # Ensure required fields have defaults

    if not payload.get("pickup"):
        payload["pickup"] = "Невідомо"

    if not payload.get("destination"):
        payload["destination"] = "Невідомо"

    if not payload.get("distance_km"):
        payload["distance_km"] = "Невідомо"

    if not payload.get("tariff"):
        payload["tariff"] = "Невідомо"

    if not payload.get("payment"):
        payload["payment"] = "Невідомо"

    if not payload.get("price"):
        payload["price"] = "Невідомо"

    if not payload.get("eta_min"):
        payload["eta_min"] = "Невідомо"

    return payload


@dp.message(OrderTaxi.waiting_for_confirmation, F.text == CONFIRM_TEXT)
async def confirm_order_publish(message: types.Message, state: FSMContext):
    # Формуємо замовлення і шлемо у RabbitMQ
    data = await state.get_data()
    order_id = str(int(_time.time() * 1000))
    payload = _make_order_payload(message.from_user, data, order_id)

    # Збережемо індекс доставки
    orders_index[order_id] = {
        "chat_id": message.chat.id,
        "user_id": message.from_user.id,
        "await_rating": False,
        "payload": payload,
    }
    _save_orders_index()

    # Публікація
    _publish_order_to_mq(payload)

    await message.answer("🚖 Заявку відправлено водіям. Очікуємо підтвердження...")
    await state.set_state(OrderTaxi.waiting_for_driver_confirmation)
    asyncio.create_task(schedule_republish(order_id, 30, max_attempts=3))


@dp.message(OrderTaxi.waiting_for_confirmation, F.text == CANCEL_TEXT)
async def cancel_order(message: types.Message, state: FSMContext):
    kb = kb_with_common_rows([])
    await message.answer("❌ Замовлення скасовано.", reply_markup=kb)
    await state.clear()


@dp.message(OrderTaxi.waiting_for_driver_confirmation)
async def waiting_for_driver_confirmation_state(
    message: types.Message, state: FSMContext
):
    # Якщо користувач ввів оцінку від 1 до 5 — одразу запускаємо обробку рейтингу
    text = (message.text or "").strip()
    if text in {"1", "2", "3", "4", "5"}:
        await _handle_rating_common(message, state, allow_without_state=True)
        return

    # Інакше просто показуємо, що чекаємо підтвердження
    await message.answer("⏳ Очікуємо підтвердження водієм. Будь ласка, зачекайте...")


# === CONFIRMATIONS CONSUMER (NEW) ===
class ConfirmConsumer(threading.Thread):
    def __init__(self, loop: asyncio.AbstractEventLoop, bot: Bot):
        super().__init__(daemon=True)
        self.loop = loop
        self.bot = bot
        self._stop = threading.Event()

    def run(self):
        while not self._stop.is_set():
            try:
                params = pika.ConnectionParameters(
                    RABBITMQ_HOST, heartbeat=30, blocked_connection_timeout=300
                )
                conn = pika.BlockingConnection(params)
                ch = conn.channel()
                ch.queue_declare(queue=QUEUE_CONFIRMATIONS, durable=True)
                logging.info("[MQ] Listening '%s'", QUEUE_CONFIRMATIONS)
                for method, props, body in ch.consume(
                    QUEUE_CONFIRMATIONS, inactivity_timeout=1, auto_ack=True
                ):
                    if self._stop.is_set():
                        break
                    if not body:
                        continue
                    try:
                        msg = json.loads(body.decode("utf-8"))
                    except Exception:
                        logging.exception("Invalid JSON in confirmations")
                        continue

                    order_id = str(msg.get("order_id") or msg.get("id") or "")
                    status = msg.get("status", "accepted")
                    info = orders_index.get(order_id)
                    if not info:
                        continue
                    chat_id = info["chat_id"]

                    if status == "accepted":
                        text = "✅ Ваше замовлення прийнято водієм!\n"
                        if msg.get("driver"):
                            d = msg["driver"]
                            name = d.get("name") or "-"
                            dr_id = d.get("id") or ""
                            car = d.get("car") or {}
                            car_line = ""
                            if car:
                                car_line = f"\nАвто: {car.get('model','-')} {car.get('plate','')}"
                            text += f"Водій: {name} #{dr_id}{car_line}"
                        text += f"\nЗамовлення №{order_id}"
                        asyncio.run_coroutine_threadsafe(
                            self.bot.send_message(chat_id, text), self.loop
                        )
                        driver_phone = d.get("phone")
                        driver_username = d.get("username")
                        if driver_phone:
                            asyncio.run_coroutine_threadsafe(
                                self.bot.send_contact(
                                    chat_id,
                                    phone_number=driver_phone,
                                    first_name=name or "Водій",
                                ),
                                self.loop,
                            )

                        from aiogram.types import (
                            InlineKeyboardMarkup,
                            InlineKeyboardButton,
                        )

                        rows = []
                        if driver_phone:
                            rows.append(
                                [
                                    InlineKeyboardButton(
                                        text="📞 Подзвонити водію",
                                        url=f"tel:{driver_phone}",
                                    )
                                ]
                            )
                        if driver_username:
                            rows.append(
                                [
                                    InlineKeyboardButton(
                                        text="💬 Написати в Telegram",
                                        url=f"https://t.me/{driver_username}",
                                    )
                                ]
                            )
                        elif dr_id:
                            rows.append(
                                [
                                    InlineKeyboardButton(
                                        text="💬 Написати в Telegram",
                                        url=f"tg://user?id={dr_id}",
                                    )
                                ]
                            )
                        if rows:
                            kb = InlineKeyboardMarkup(inline_keyboard=rows)
                            asyncio.run_coroutine_threadsafe(
                                self.bot.send_message(
                                    chat_id, "Зв’язок із водієм:", reply_markup=kb
                                ),
                                self.loop,
                            )

                        od = orders_index.get(order_id)
                        if od:
                            od["confirmed"] = True

                    elif status == "arrived":
                        wait_min = int(msg.get("free_wait_min", 3))
                        asyncio.run_coroutine_threadsafe(
                            self.bot.send_message(
                                chat_id,
                                f"🚖 Ваш водій на місці.\n🕒 Розпочалось безкоштовне очікування {wait_min} хв.",
                            ),
                            self.loop,
                        )

                    elif status == "driver_cancelled":
                        asyncio.run_coroutine_threadsafe(
                            self.bot.send_message(
                                chat_id,
                                "🚫 Водій скасував поїздку.\n🔍 Продовжуємо пошук нового водія...",
                            ),
                            self.loop,
                        )
                        # Отримуємо дані замовлення з локального індексу
                        order_data = orders_index.get(order_id)
                        if order_data:
                            # Повертаємо в пошук
                            _publish_order_to_mq(order_data["payload"])

                    elif status in ("completed", "finished"):
                        info["await_rating"] = True
                        orders_index[order_id] = info
                        _save_orders_index()
                        asyncio.run_coroutine_threadsafe(
                            self.bot.send_message(
                                chat_id,
                                "✅ Дякуємо, що скористались FlyTaxi!\n🧾 Поїздку завершено. Оцініть від 1 до 5.",
                            ),
                            self.loop,
                        )

                try:
                    if conn and conn.is_open:
                        conn.close()
                except Exception:
                    pass
            except Exception as e:
                logging.error(
                    "[MQ] confirmations consumer error: %s. Reconnect in 3s...", e
                )
                _time.sleep(3)

    def stop(self):
        self._stop.set()


confirm_thread: ConfirmConsumer | None = None


# === RATINGS ===
@dp.message(OrderTaxi.waiting_for_rating)
async def rate_driver_stateful(message: types.Message, state: FSMContext):
    await _handle_rating_common(message, state)


@dp.message(F.text.regexp(r"^[1-5]$"))
async def rate_driver_stateless(message: types.Message, state: FSMContext):
    """Дублер: якщо стан не виставлено, але RabbitMQ надіслав нам 'completed'."""
    await _handle_rating_common(message, state, allow_without_state=True)


async def _handle_rating_common(
    message: types.Message, state: FSMContext, allow_without_state: bool = False
):
    user_id = str(message.from_user.id)
    try:
        rating = int(message.text)
    except ValueError:
        return
    if not (1 <= rating <= 5):
        return

    # Знайдемо останнє замовлення цього юзера, що чекає рейтинг
    pending_order_id = None
    for oid, info in reversed(list(orders_index.items())):
        if info.get("user_id") == message.from_user.id and info.get("await_rating"):
            pending_order_id = oid
            break

    if not pending_order_id and not allow_without_state:
        # Якщо ми строго у стані — підкажемо
        await message.answer("Зараз немає поїздки, що очікує оцінку.")
        return

    if rating <= 2:
        reason_rows = [[KeyboardButton(text=reason)] for reason in NEGATIVE_REASONS]
        kb = kb_with_common_rows(reason_rows)
        await message.answer(
            "😔 Нам шкода, що поїздка вам не сподобалася.\nОберіть причину низької оцінки:",
            reply_markup=kb,
        )
        await state.update_data(rating=rating, pending_order_id=pending_order_id)
        await state.set_state(OrderTaxi.waiting_for_feedback_reason)
        return
    else:
        await _finalize_feedback(user_id, rating, "—", pending_order_id)
        kb = kb_with_common_rows([])
        await message.answer(f"⭐ Дякуємо за оцінку {rating}!", reply_markup=kb)
        await state.clear()
        await start(message, state)  # Повернення в головне меню


def save_feedback(user_id, rating, reason, order_id=None):
    orders = _load_json(ORDERS_FILE, [])
    if not isinstance(orders, list):
        orders = []
    orders.append(
        {
            "user_id": user_id,
            "rating": rating,
            "reason": reason,
            "order_id": order_id,
            "timestamp": datetime.now(pytz.timezone("Europe/Kiev")).strftime(
                "%Y-%m-%d %H:%M:%S"
            ),
        }
    )
    _save_json(ORDERS_FILE, orders)


async def _finalize_feedback(user_id, rating, reason, order_id):
    if order_id:
        info = orders_index.get(order_id, {})
        info["await_rating"] = False
        # Закриваємо замовлення, щоб не було повторного очікування водія
        info["status"] = "completed"
        orders_index[order_id] = info
        _save_orders_index()
    save_feedback(user_id, rating, reason, order_id)


@dp.message(OrderTaxi.waiting_for_feedback_reason)
async def feedback_reason_chosen(message: types.Message, state: FSMContext):
    reason = message.text
    data = await state.get_data()
    rating = data.get("rating", "—")
    pending_order_id = data.get("pending_order_id")
    user_id = str(message.from_user.id)

    if reason == "Інше":
        await message.answer(
            "✍️ Будь ласка, опишіть причину детальніше:",
            reply_markup=types.ReplyKeyboardRemove(),
        )
        await state.set_state(OrderTaxi.waiting_for_feedback_other)
        return

    await _finalize_feedback(user_id, rating, reason, pending_order_id)
    kb = kb_with_common_rows([])
    await message.answer("✅ Дякуємо за відгук!", reply_markup=kb)
    await state.clear()
    await start(message, state)  # Повернення в головне меню


@dp.message(OrderTaxi.waiting_for_feedback_other)
async def feedback_other_text(message: types.Message, state: FSMContext):
    other_reason = message.text.strip()
    data = await state.get_data()
    rating = data.get("rating", "—")
    pending_order_id = data.get("pending_order_id")
    user_id = str(message.from_user.id)

    await _finalize_feedback(user_id, rating, other_reason, pending_order_id)
    kb = kb_with_common_rows([])
    await message.answer("✅ Дякуємо за детальний відгук!", reply_markup=kb)
    await state.clear()
    await start(message, state)  # Повернення в головне меню


# === Reset favorites command ===
@dp.message(Command("reset_favorites"))
async def reset_favorites(message: types.Message, state: FSMContext):
    user_id = str(message.from_user.id)
    _ensure_user(user_id)
    registered_users[user_id]["favorites"] = {"home": None, "work": None, "other": []}
    registered_users[user_id]["favorites_prompted"] = False
    registered_users[user_id]["home_prompted_final"] = False
    registered_users[user_id]["work_prompted_final"] = False
    _save_users()
    await message.answer(
        "✅ Очищено Дім/Робота/Обране та скинуто пропозиції збереження."
    )


async def main():
    # Стартуємо фоновий consumer підтверджень
    loop = asyncio.get_running_loop()
    global confirm_thread
    confirm_thread = ConfirmConsumer(loop, bot)
    confirm_thread.start()

    await bot.delete_webhook(drop_pending_updates=True)
    try:
        await dp.start_polling(bot)
    finally:
        if confirm_thread and confirm_thread.is_alive():
            confirm_thread.stop()


if __name__ == "__main__":
    asyncio.run(main())
