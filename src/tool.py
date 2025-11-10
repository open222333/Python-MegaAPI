from src.exception import ConfigError
from time import sleep
import logging


def wait_for_user_confirmation(logger: logging.Logger = None, always_yes: bool = False, sleep_seconds: int = 10):
    while True:
        if always_yes:
            message = f"自動確認繼續執行（跳過提示）。 {sleep_seconds} 秒後繼續執行"
            if logger:
                logger.info(message)
            else:
                print(f"{message}\n")
            break

        choice = input("🟡 是否繼續執行？(Y/N，預設為 N)：").strip().lower()
        if choice == "y":
            message = "繼續執行..."
            if logger:
                logger.info(message)
            else:
                print(f"{message}\n")
            break
        elif choice == "n" or choice == "":
            message = "使用者選擇中斷，程式終止。"
            if logger:
                logger.info(message)
            else:
                print(message)
            exit(0)
        else:
            message = "無效輸入，請輸入 Y 或 N（Enter 預設為 N）。"
            if logger:
                logger.warning(message)
            else:
                print("無效輸入，請輸入 Y 或 N（Enter 預設為 N）。")


def check_required_vars(required_vars: dict, logger: logging.Logger = None):
    """
    檢查必要參數是否完整。

    參數：
        required_vars (dict): key=參數名稱, value=對應值
        logger (logging.Logger): 日誌記錄器，可選

    若有缺少參數，會列出名稱及其當前值，並丟出 ConfigError。
    """
    missing = {k: v for k, v in required_vars.items() if not v}

    if missing:
        # lines = [f"{k} = {repr(v)}" for k, v in missing.items()]
        # message = "必要參數未設定完整，缺少或為空：\n" + "\n".join(lines)

        message = f"必要參數未設定完整，缺少或為空: {', '.join(missing)}"
        raise ConfigError(message)
    else:
        if logger:
            logger.info("必要參數檢查通過，所有參數均已設定。")
