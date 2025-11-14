from argparse import ArgumentParser
from configparser import ConfigParser
from logging.handlers import RotatingFileHandler
from src.amazon_s3 import AmazonS3
from src.mega_s4 import MegaS4
from src.timer import human_time_ct_str
from src.tool import wait_for_user_confirmation, check_required_vars
import logging
import os


if __name__ == '__main__':
    parser = ArgumentParser(description='MegaAPI 指令列介面')
    config_group = parser.add_argument_group("config", "設定相關參數")
    config_group.add_argument(
        '--config_path', type=str,
        default=os.path.join('conf', 'config.ini'),
        help='路徑至設定檔（預設：conf/config.ini）'
    )
    log_group = parser.add_argument_group("log", "日誌相關參數")
    log_group.add_argument(
        '--log_path', type=str,
        default=os.path.join('logs', 'MegaAPIMain.log'),
        help='路徑至日誌檔（預設：logs/MegaAPIMain.log）'
    )
    log_group.add_argument(
        '--log_level', type=str,
        choices=['DEBUG', 'INFO',
                 'WARNING', 'ERROR', 'CRITICAL'],
        default='DEBUG',
        help='日誌等級（預設：DEBUG）'
    )
    log_group.add_argument(
        '--no_console', action='store_true',
        help='不輸出日誌至控制台'
    )
    log_group.add_argument(
        '--no_file', action='store_true',
        help='不輸出日誌至檔案'
    )
    log_group.add_argument(
        '--max_bytes', type=int,
        default=10 * 1024 * 1024,
        help='單一日誌檔最大位元組數（預設：10MB）'
    )
    log_group.add_argument(
        '--backup_count', type=int,
        default=5,
        help='保留的舊日誌檔案數量（預設：5）'
    )
    log_group.add_argument(
        '-T', '--enable_telegram_message', action='store_true',
        help='啟用 Telegram 通知功能'
    )
    test_group = parser.add_argument_group("test", "測試相關參數")
    test_group.add_argument(
        '-l', '--local_upload_test_file', type=str,
        default=None,
        help='本地測試上傳檔案路徑（預設：無）'
    )
    test_group.add_argument(
        '--code', type=str,
        default=None,
        help='測試用代碼參數（預設：無）'
    )
    execution_group = parser.add_argument_group("execution", "執行行為相關參數")
    execution_group.add_argument(
        '-y', '--always_yes',
        action='store_true',
        help='自動回答「是」以跳過提示，直接執行操作'
    )
    execution_group.add_argument(
        '--dry_run',
        action='store_true',
        help='模擬執行，不進行實際操作'
    )
    execution_group.add_argument(
        '--skip_existing',
        action='store_true',
        help='跳過已存在的檔案，不進行下載或上傳'
    )
    execution_group.add_argument(
        '--download_type', type=str,
        choices=['s3', 'resume'],
        default='resume',
        help='下載類型選擇 s3 resume （預設：resume）'
    )
    performance_group = parser.add_argument_group("performance", "效能相關參數")
    performance_group.add_argument(
        '--max_workers', type=int,
        default=5,
        help='同時處理的最大工作緒數（預設：5）'
    )
    performance_group.add_argument(
        '--max_retries',
        type=int,
        default=3,
        help='失敗重試次數（預設：3）'
    )
    performance_group.add_argument(
        '--per_count', type=int,
        default=100,
        help='每次處理的文件數量（預設：100）'
    )

    args = parser.parse_args()

    logger = logging.getLogger('MegaAPIMain')
    logger.setLevel(getattr(logging, args.log_level))
    log_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
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
        print("警告：未啟用任何日誌輸出（無檔案、無控制台）")

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

    TELEGRAM_BOT_TOKEN = conf.get('TELEGRAM', 'TELEGRAM_BOT_TOKEN', fallback=None)
    TELEGRAM_CHAT_ID = conf.get('TELEGRAM', 'TELEGRAM_CHAT_ID', fallback=None)

    MONGO_HOST = conf.get('MONGO', 'MONGO_HOST', fallback="127.0.0.1")
    MONGO_PORT = conf.get('MONGO', 'MONGO_PORT', fallback="27017")
    MONGO_DATABASE_NAME = conf.get('MONGO', 'MONGO_DATABASE_NAME', fallback=None)
    MONGO_COLLECTION_NAME = conf.get('MONGO', 'MONGO_COLLECTION_NAME', fallback=None)
    MONGO_METADATA_COLLECTION_NAME = conf.get('MONGO', 'MONGO_METADATA_COLLECTION_NAME', fallback=None)

    MEGA_S4_ACCESS_KEY = conf.get('MEGA_S4', 'MEGA_S4_ACCESS_KEY', fallback=None)
    MEGA_S4_SECRET_KEY = conf.get('MEGA_S4', 'MEGA_S4_SECRET_KEY', fallback=None)
    MEGA_S4_REGION = conf.get('MEGA_S4', 'MEGA_S4_REGION', fallback=None)
    MEGA_S4_BUCKET_NAME = conf.get('MEGA_S4', 'MEGA_S4_BUCKET_NAME', fallback=None)
    # MEGA S4 的 endpoint 結構 <bucket-name>.s3.<region>.s4.mega.io
    MEGA_S4_ENDPOINT_URL = f"https://{MEGA_S4_BUCKET_NAME}.s3.{MEGA_S4_REGION}.s4.mega.io" if MEGA_S4_REGION and MEGA_S4_BUCKET_NAME else None

    AMAZON_S3_ACCESS_KEY = conf.get('AMAZON_S3', 'AMAZON_S3_ACCESS_KEY', fallback=None)
    AMAZON_S3_SECRET_KEY = conf.get('AMAZON_S3', 'AMAZON_S3_SECRET_KEY', fallback=None)
    AMAZON_S3_REGION = conf.get('AMAZON_S3', 'AMAZON_S3_REGION', fallback=None)
    AMAZON_S3_BUCKET_NAME = conf.get('AMAZON_S3', 'AMAZON_S3_BUCKET_NAME', fallback=None)
    AMAZON_S3_URL = conf.get('AMAZON_S3', 'AMAZON_S3_URL', fallback=None)

    # 檢查必要參數
    required_vars = {
        "MEGA_S4_ACCESS_KEY": MEGA_S4_ACCESS_KEY,
        "MEGA_S4_SECRET_KEY": MEGA_S4_SECRET_KEY,
        "MEGA_S4_ENDPOINT_URL": MEGA_S4_ENDPOINT_URL,
        "MEGA_S4_REGION": MEGA_S4_REGION,
        "MEGA_S4_BUCKET_NAME": MEGA_S4_BUCKET_NAME,
        "AMAZON_S3_ACCESS_KEY": AMAZON_S3_ACCESS_KEY,
        "AMAZON_S3_SECRET_KEY": AMAZON_S3_SECRET_KEY,
        "AMAZON_S3_REGION": AMAZON_S3_REGION,
        "AMAZON_S3_BUCKET_NAME": AMAZON_S3_BUCKET_NAME,
        "AMAZON_S3_URL": AMAZON_S3_URL,
        "MONGO_DATABASE_NAME": MONGO_DATABASE_NAME,
        "MONGO_COLLECTION_NAME": MONGO_COLLECTION_NAME
    }

    check_required_vars(required_vars, logger)

    logger.info("=== 參數設定確認 ===")
    logger.info(f"設定檔路徑：{args.config_path}")
    logger.info(f"日誌等級：{args.log_level}")
    logger.info(f"日誌檔路徑：{args.log_path if not args.no_file else '無'}")
    logger.info(f"本地測試檔案：{args.local_upload_test_file if args.local_upload_test_file else '無'}")
    logger.info(f"測試代碼參數：{args.code if args.code else '無'}")
    logger.info(f"模擬執行：{'是' if args.dry_run else '否'}")
    logger.info(f"跳過已存在檔案：{'是' if args.skip_existing else '否'}")
    logger.info(f"最大工作緒數：{args.max_workers}")
    logger.info(f"失敗重試次數：{args.max_retries}")
    logger.info(f"每次處理文件數量：{args.per_count}")
    logger.info(f"下載方式：{'透過 s3 金鑰' if args.download_type == 's3' else '斷點續傳下載'}")
    logger.info("=== MegaS4 設定 ===")
    logger.info(f"MEGA_S4_ENDPOINT_URL{MEGA_S4_ENDPOINT_URL if MEGA_S4_ENDPOINT_URL else '未設定'}")
    logger.info(f"MEGA_S4_REGION{MEGA_S4_REGION if MEGA_S4_REGION else '未設定'}")
    logger.info(f"MEGA_S4_BUCKET_NAME{MEGA_S4_BUCKET_NAME if MEGA_S4_BUCKET_NAME else '未設定'}")
    logger.info("=== AmazonS3 設定 ===")
    logger.info(f"AMAZON_S3_REGION{AMAZON_S3_REGION if AMAZON_S3_REGION else '未設定'}")
    logger.info(f"AMAZON_S3_BUCKET_NAME{AMAZON_S3_BUCKET_NAME if AMAZON_S3_BUCKET_NAME else '未設定'}")
    logger.info(f"AMAZON_S3_URL{AMAZON_S3_URL if AMAZON_S3_URL else '未設定'}")
    logger.info("=== MongoDB 設定 ===")
    logger.info(f"MONGO 主機: {MONGO_HOST}:{MONGO_PORT}")
    logger.info(f"MONGO 資料庫: {MONGO_DATABASE_NAME}")
    logger.info(f"MONGO 集合: {MONGO_COLLECTION_NAME}")
    logger.info(f"MONGO Metadata 集合: {MONGO_METADATA_COLLECTION_NAME}")
    logger.info("=== Telegram 設定 ===")
    logger.info(f"TELEGRAM_BOT_TOKEN: {'已設定' if TELEGRAM_BOT_TOKEN else '未設定'}")
    logger.info(f"TELEGRAM_CHAT_ID: {'已設定' if TELEGRAM_CHAT_ID else '未設定'}")
    logger.info("=====================")

    if args.local_upload_test_file:
        if not os.path.isfile(args.local_upload_test_file):
            logger.error(f"本地測試檔案不存在：{args.local_upload_test_file}")
            exit(1)

    logger.info("請確認以上設定是否正確。")

    # 等待使用者確認
    wait_for_user_confirmation(
        logger=logger,
        always_yes=args.always_yes
    )

    mega_s4 = MegaS4(
        access_key=MEGA_S4_ACCESS_KEY,
        secret_key=MEGA_S4_SECRET_KEY,
        endpoint_url=MEGA_S4_ENDPOINT_URL,
        region_name=MEGA_S4_REGION,
        log_level=args.log_level,
        log_max_bytes=args.max_bytes,
        log_backup_count=args.backup_count
    )

    awason_s3 = AmazonS3(
        aws_access_key_id=AMAZON_S3_ACCESS_KEY,
        aws_secret_access_key=AMAZON_S3_SECRET_KEY,
        region=AMAZON_S3_REGION,
        log_level=args.log_level,
        log_max_bytes=args.max_bytes,
        log_backup_count=args.backup_count
    )

    if args.local_upload_test_file:
        # 測試 mega 上傳
        logger.info("進入本地測試上傳檔案處理模式。")
        upload_result, upload_sec = mega_s4.upload_file(
            bucket_name=MEGA_S4_BUCKET_NAME,
            local_file_path=args.local_upload_test_file,
            remote_key=os.path.basename(args.local_upload_test_file),
            show_progress=True
        )
        logger.info(f"本地測試檔案上傳結果: {'成功' if upload_result else '失敗'} 經過時間: {human_time_ct_str(upload_sec)} ")
        logger.info("本地測試上傳檔案處理完成。")
