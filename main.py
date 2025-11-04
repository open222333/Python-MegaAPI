from configparser import ConfigParser
from argparse import ArgumentParser
from logging.handlers import RotatingFileHandler
from src.mega_exception import ConfigError
# from src.mega_api import Mega
from src.mega_s4 import MegaS4
import logging
import os


def wait_for_user_confirmation():
    while True:
        choice = input("🟡 是否繼續執行？(Y/N，預設為 N)：").strip().lower()
        if choice == "y":
            print("✅ 繼續執行...\n")
            break
        elif choice == "n" or choice == "":
            print("🛑 使用者選擇中斷，程式終止。")
            exit(0)
        else:
            print("⚠️ 無效輸入，請輸入 Y 或 N（Enter 預設為 N）。")


if __name__ == '__main__':
    parser = ArgumentParser(description='MegaAPI 指令列介面')
    config_group = parser.add_argument_group("config", "設定相關參數")
    config_group.add_argument('--config_path', type=str,
                              default=os.path.join('conf', 'config.ini'),
                              help='路徑至設定檔（預設：conf/config.ini）')
    log_group = parser.add_argument_group("log", "日誌相關參數")
    log_group.add_argument('--log_path', type=str,
                           default=os.path.join('logs', 'MegaAPIMain.log'),
                           help='路徑至日誌檔（預設：logs/MegaAPIMain.log）')
    log_group.add_argument('--log_level', type=str,
                           choices=['DEBUG', 'INFO',
                                    'WARNING', 'ERROR', 'CRITICAL'],
                           default='DEBUG',
                           help='日誌等級（預設：DEBUG）')
    log_group.add_argument('--no_console', action='store_true',
                           help='不輸出日誌至控制台')
    log_group.add_argument('--no_file', action='store_true',
                           help='不輸出日誌至檔案')
    log_group.add_argument('--max_bytes', type=int,
                           default=10 * 1024 * 1024,
                           help='單一日誌檔最大位元組數（預設：10MB）')
    log_group.add_argument('--backup_count', type=int,
                           default=5,
                           help='保留的舊日誌檔案數量（預設：5）')
    test_group = parser.add_argument_group("test", "測試相關參數")
    test_group.add_argument('--local_test_file', type=str,
                            default=None,
                            help='本地測試檔案路徑（預設：無）')

    args = parser.parse_args()

    logger = logging.getLogger('MegaAPIMain')
    logger.setLevel(getattr(logging, args.log_level))
    log_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger.propagate = False

    if not args.no_file:
        log_path = os.path.abspath(args.log_path)
        os.makedirs(os.path.dirname(log_path), exist_ok=True)
        file_handler = RotatingFileHandler(
            log_path,
            maxBytes=args.max_bytes,
            backupCount=args.backup_count,
            encoding='utf-8'
        )
        file_handler.setFormatter(log_formatter)
        # 只記錄 ERROR+
        file_handler.setLevel(logging.ERROR)
        logger.addHandler(file_handler)
        logger.info(f"日誌將輸出到檔案：{log_path}")

    if not args.no_console:
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(log_formatter)
        logger.addHandler(console_handler)
        logger.info("已啟用控制台日誌輸出")

    if args.no_file and args.no_console:
        print("⚠️ 警告：未啟用任何日誌輸出（無檔案、無控制台）")

    if args.config_path:
        if not os.path.isfile(args.config_path):
            logger.error(f"設定檔不存在：{args.config_path}")
            exit(1)
        try:
            conf = ConfigParser()
            conf.read(args.config_path, encoding='utf-8')
            logger.debug(f"設定檔載入完成：{args.config_path}")
        except Exception as e:
            logger.error(f"無法載入設定檔：{args.config_path}，錯誤訊息：{e}")
            exit(1)

    ACCOUNT = conf.get('MEGA', 'ACCOUNT', fallback=None)
    PASSWORD = conf.get('MEGA', 'PASSWORD', fallback=None)

    ACCESS_KEY = conf.get('MEGA_S4', 'ACCESS_KEY', fallback=None)
    SECRET_KEY = conf.get('MEGA_S4', 'SECRET_KEY', fallback=None)

    REGION = conf.get('MEGA_S4', 'REGION', fallback=None)
    BUCKET_NAME = conf.get('MEGA_S4', 'BUCKET_NAME', fallback=None)
    ENDPOINT_URL = f"https://{BUCKET_NAME}.s3.{REGION}.s4.mega.io" if REGION and BUCKET_NAME else None

    # 檢查必要參數
    required_vars = {
        "ACCOUNT": ACCOUNT,
        "PASSWORD": PASSWORD,
        "ACCESS_KEY": ACCESS_KEY,
        "SECRET_KEY": SECRET_KEY,
        "ENDPOINT_URL": ENDPOINT_URL
    }

    missing = [k for k, v in required_vars.items() if not v]

    if missing:
        message = f"必要參數未設定完整，缺少: {', '.join(missing)}"
        logger.error(message)
        raise ConfigError(message)

    logger.info("=== 參數設定確認 ===")
    logger.info(f"設定檔路徑：{args.config_path}")
    logger.info(f"日誌等級：{args.log_level}")
    logger.info(f"日誌檔路徑：{args.log_path if not args.no_file else '無'}")
    logger.info(f"本地測試檔案：{args.local_test_file if args.local_test_file else '無'}")
    logger.info("=== MegaS4 設定 ===")
    logger.info(f"ENDPOINT_URL：{ENDPOINT_URL if ENDPOINT_URL else '未設定'}")
    logger.info(f"REGION：{REGION if REGION else '未設定'}")
    logger.info(f"BUCKET_NAME：{BUCKET_NAME if BUCKET_NAME else '未設定'}")

    if args.local_test_file:
        if not os.path.isfile(args.local_test_file):
            logger.error(f"本地測試檔案不存在：{args.local_test_file}")
            exit(1)

    logger.info("請確認以上設定是否正確。")

    wait_for_user_confirmation()

    # mega = Mega(
    #     email=ACCOUNT,
    #     password=PASSWORD,
    #     log_level=args.log_level,
    #     log_max_bytes=args.max_bytes,
    #     log_backup_count=args.backup_count
    # )
    # files = mega.list_files()
    # if files is not None:
    #     for file in files:
    #         logger.debug(file)
    # else:
    #     logger.error("無法列出檔案，請確認是否登入成功。")

    mega = MegaS4(
        access_key=ACCESS_KEY,
        secret_key=SECRET_KEY,
        endpoint_url=ENDPOINT_URL,
        region_name=REGION,
        log_level=args.log_level,
        log_max_bytes=args.max_bytes,
        log_backup_count=args.backup_count
    )

    mega.upload_file_to_s4(
        bucket_name=BUCKET_NAME,
        local_file_path=args.local_test_file,
        remote_key=args.local_test_file
    )
