import asyncio
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.client.default import DefaultBotProperties

API_TOKEN = "7841476557:AAEY9IRm-a1gWPsmXXvozuJ1LQAHziN6iQM"

# правильний спосіб
bot = Bot(token=API_TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
dp = Dispatcher()


@dp.message(Command("id"))
async def send_chat_id(message: types.Message):
    chat_id = message.chat.id
    await message.reply(f"👉 CHAT ID: <code>{chat_id}</code>")
    print(f"👉 CHAT ID: {chat_id}")


async def main():
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
