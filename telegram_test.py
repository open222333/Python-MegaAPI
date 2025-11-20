from configparser import ConfigParser
from src.telegram import send_telegram_message
import os


conf = ConfigParser()
conf.read(os.path.join('conf', 'config.ini'))

TELEGRAM_BOT_TOKEN = conf.get('TELEGRAM', 'TELEGRAM_BOT_TOKEN', fallback=None)
TELEGRAM_CHAT_ID = conf.get('TELEGRAM', 'TELEGRAM_CHAT_ID', fallback=None)

if __name__ == "__main__":
    send_telegram_message("測試訊息", TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID)
