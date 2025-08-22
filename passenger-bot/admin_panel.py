import logging
import os
from aiogram import Router, types
from aiogram.filters import Command

from tariff_store import load_tariffs_cfg, upsert_tariff, save_tariffs_cfg

# ===== Читаємо ADMIN_IDS з .env =====
ADMIN_IDS = [int(x) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip()]

# ===== Ініціалізація =====
router = Router()
logger = logging.getLogger(__name__)


# ===== Команда /set_tariff =====
@router.message(Command("set_tariff"))
async def set_tariff_cmd(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        return

    try:
        parts = message.text.split()
        if len(parts) != 4:
            await message.answer("⚠️ Використання: /set_tariff <назва> <база> <за_км>")
            return

        _, name, base, per_km = parts
        base, per_km = float(base), float(per_km)

        cfg = load_tariffs_cfg()
        upsert_tariff(cfg, name, base, per_km)
        save_tariffs_cfg(cfg)

        await message.answer(f"✅ Тариф {name} оновлено: база={base}, км={per_km}")
    except Exception as e:
        logger.exception("Помилка у set_tariff_cmd")
        await message.answer(f"❌ Помилка: {e}")


# ===== Команда /show_tariffs =====
@router.message(Command("show_tariffs"))
async def show_tariffs_cmd(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        return

    cfg = load_tariffs_cfg()
    txt = "📊 Поточні тарифи:\n"
    for name, vals in cfg.get("tariffs", {}).items():
        txt += f"{name}: базова {vals.get('base', 0)} грн, за км {vals.get('per_km', 0)} грн\n"

    await message.answer(txt)


# ===== Команда /save_tariffs =====
@router.message(Command("save_tariffs"))
async def save_tariffs_cmd(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        return

    cfg = load_tariffs_cfg()
    save_tariffs_cfg(cfg)
    await message.answer("💾 Тарифи збережено у файл.")
