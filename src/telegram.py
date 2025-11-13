import requests


def send_telegram_message(message, bot_token, chat_id, send=True):
    """發送 telegram 訊息

    Args:
        message (_type_): 訊息內容
        bot_token (_type_): 機器人 token
        chat_id (_type_): 聊天 ID
        send (bool, optional): 是否發送訊息. Defaults to True.
    """
    if send == True:
        url = f'https://api.telegram.org/bot{bot_token}/sendMessage?chat_id={chat_id}&text={message}'
        requests.get(url)
