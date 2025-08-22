from aiogram.types import ReplyKeyboardMarkup, KeyboardButton

class_keyboard = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="Економ")],
        [KeyboardButton(text="Комфорт")],
        [KeyboardButton(text="Бізнес")],
    ],
    resize_keyboard=True,
)
