from configparser import ConfigParser
from argparse import ArgumentParser
from logging.handlers import RotatingFileHandler
from src.mega_api import Mega
import logging
import os


if __name__ == '__main__':
    parser = ArgumentParser(description='MegaAPI 指令列介面')
    config_group = parser.add_argument_group("config", "設定相關參數")
    config_group.add_argument('--config_path', type=str,
                        default=os.path.join('conf', 'config.ini'),
                        help='路徑至設定檔（預設：conf/config.ini）')
    log_group = parser.add_argument_group("log", "日誌相關參數")
    log_group.add_argument('--log_path', type=str,
                        default=os.path.join('logs', 'MegaAPI.log'),
                        help='路徑至日誌檔（預設：logs/MegaAPI.log）')
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

    args = parser.parse_args()

    logger = logging.getLogger('MegaAPIMain')
    logger.setLevel(getattr(logging, args.log_level))
    log_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

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

    mega = Mega(ACCOUNT, PASSWORD)
    files = mega.list_files()
    if files is not None:
        for file in files:
            logger.debug(file)
    else:
        logger.error("無法列出檔案，請確認是否登入成功。")
