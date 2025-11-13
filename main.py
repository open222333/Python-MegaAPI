from argparse import ArgumentParser
from concurrent.futures import ThreadPoolExecutor, as_completed
from configparser import ConfigParser
from datetime import datetime
from logging.handlers import RotatingFileHandler
from pymongo import MongoClient
from src.amazon_s3 import AmazonS3
from src.downloader import Downloader
from src.exception import DownloadError, UploadError, FileNotFoundError
from src.mega_s4 import MegaS4
from src.telegram import send_telegram_message
from src.timer import human_time_ct_str
from src.timer import timed
from src.tool import wait_for_user_confirmation, check_required_vars
from tqdm import tqdm
import logging
import os

# ========================
# 單筆處理函式
# ========================


@timed(print_result=False)
def process_file(bucket_name, s3_remote_key, s4_remote_key, s3_client: AmazonS3, s4_client: MegaS4, s4_bucket, skip_existing=True, dry_run=False, tmp_dir="temp", show_progress=True, logger: logging.Logger = None) -> bool:
    """
    處理單一檔案：
        Amazon S3 -> 下載到本地 -> 上傳到 Mega S4

    參數：
        bucket_name (str): S3 儲存桶名稱
        s3_remote_key (str): 檔案在 S3 上的路徑
        s4_remote_key (str): 檔案在 S4 上的路徑
        s3_client (AmazonS3): AmazonS3 客戶端實例
        s4_client (MegaS4): MegaS4 客戶端實例
        s4_bucket (str): MegaS4 儲存桶名稱
        skip_existing (bool): 是否跳過已存在的檔案
        dry_run (bool): 是否為模擬執行（不進行實際下載或上傳）
        tmp_dir (str): 暫存目錄路徑
        show_progress (bool): 是否顯示下載進度條
        logger (logging.Logger): 日誌記錄器
    """
    # 暫存檔案路徑
    local_path = os.path.join(tmp_dir, os.path.basename(s3_remote_key))
    os.makedirs(tmp_dir, exist_ok=True)

    try:
        # 若 S4 已存在，直接略過
        if skip_existing == True:
            if s4_client.exists(s4_bucket, s4_remote_key):
                logger.info(f"已存在於 S4，略過：{s3_remote_key}")
                return True

        # 從 S3 下載
        logger.info(f"下載中：s3://{bucket_name}/{s3_remote_key}")

        if dry_run:
            logger.info(f"[模擬執行] 已下載：s3://{bucket_name}/{s3_remote_key} → {local_path}")
        else:
            download_result, download_sec = s3_client.download_file(
                bucket_name=bucket_name,
                remote_key=s3_remote_key,
                local_file_path=local_path,
                show_progress=show_progress,
            )

            if not download_result:
                raise DownloadError(f"S3 下載失敗：{s3_remote_key}")
            else:
                logger.info(f"下載完成 經過時間: {human_time_ct_str(download_sec)} ")

        # 上傳至 S4
        logger.info(f"上傳中：{local_path} → {s4_remote_key}")

        if dry_run:
            logger.info(f"[模擬執行] 已上傳: {local_path} → s4://{s4_bucket}/{s4_remote_key}")
        else:
            upload_result, upload_sec = s4_client.upload_file(
                bucket_name=s4_bucket,
                local_file_path=local_path,
                remote_key=s4_remote_key,
            )

            if not upload_result:
                raise UploadError(f"S4 上傳失敗：{local_path} -> {s4_remote_key}")
            else:
                logger.info(f"上傳完成 經過時間: {human_time_ct_str(upload_sec)} ")

        return True

    except Exception as e:
        logger.error(f"處理失敗 {s3_remote_key}：{e}")
        return False
    finally:
        # 清理暫存檔案
        if os.path.exists(local_path):
            try:
                os.remove(local_path)
                logger.info(f"已刪除暫存檔案：{local_path}")
            except Exception as e:
                logger.error(f"無法刪除暫存檔案 {local_path}：{e}")


@timed(print_result=False)
def process_file_by_url(url, s4_remote_key, s4_client: MegaS4, s4_bucket, skip_existing=True, dry_run=False, tmp_dir="temp", show_progress=True, logger: logging.Logger = None) -> bool:
    """
    處理單一檔案：
        Amazon S3 -> 下載到本地 -> 上傳到 Mega S4

    參數：
        url (str): 檔案 URL
        s4_remote_key (str): 檔案在 S4 上的路徑
        s4_client (MegaS4): MegaS4 客戶端實例
        s4_bucket (str): MegaS4 儲存桶名稱
        skip_existing (bool): 是否跳過已存在的檔案
        dry_run (bool): 是否為模擬執行（不進行實際下載或上傳）
        tmp_dir (str): 暫存目錄路徑
        show_progress (bool): 是否顯示下載進度條
        logger (logging.Logger): 日誌記錄器
    """
    # 暫存檔案路徑
    local_path = os.path.join(tmp_dir, os.path.basename(s4_remote_key))
    os.makedirs(tmp_dir, exist_ok=True)

    try:
        # 若 S4 已存在，直接略過
        if skip_existing == True:
            if s4_client.exists(s4_bucket, s4_remote_key):
                logger.info(f"已存在於 S4，略過：{url}")
                return True

        # 從 S3 下載
        logger.info(f"下載中：{url}")

        if dry_run:
            logger.info(f"[模擬執行] 已下載：{url} → {local_path}")
        else:
            downloader = Downloader()
            download_result, download_sec = downloader.download_file_with_resume(
                url=url,
                file_path=local_path,
                print_bar=show_progress,
            )

            if not download_result:
                raise DownloadError(f"S3 下載失敗：{url}")
            else:
                logger.info(f"下載完成 經過時間: {human_time_ct_str(download_sec)} ")

        # 上傳至 S4
        logger.info(f"上傳中：{local_path} → {s4_remote_key}")

        if dry_run:
            logger.info(f"[模擬執行] 已上傳: {local_path} → s4://{s4_bucket}/{s4_remote_key}")
        else:
            upload_result, upload_sec = s4_client.upload_file(
                bucket_name=s4_bucket,
                local_file_path=local_path,
                remote_key=s4_remote_key,
            )

            if not upload_result:
                raise UploadError(f"S4 上傳失敗：{local_path} -> {s4_remote_key}")
            else:
                logger.info(f"上傳完成 經過時間: {human_time_ct_str(upload_sec)} ")

        return True

    except Exception as e:
        logger.error(f"處理失敗 {url}：{e}")
        return False
    finally:
        # 清理暫存檔案
        if os.path.exists(local_path):
            try:
                os.remove(local_path)
                logger.info(f"已刪除暫存檔案：{local_path}")
            except Exception as e:
                logger.error(f"無法刪除暫存檔案 {local_path}：{e}")


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
    logger.info(f"下載方式：{"透過 s3 金鑰" if args.download_type == "s3" else '斷點續傳下載'}")
    logger.info("=== MegaS4 設定 ===")
    logger.info(f"MEGA_S4_ENDPOINT_URL{MEGA_S4_ENDPOINT_URL if MEGA_S4_ENDPOINT_URL else '未設定'}")
    logger.info(f"MEGA_S4_REGION{MEGA_S4_REGION if MEGA_S4_REGION else '未設定'}")
    logger.info(f"MEGA_S4_BUCKET_NAME{MEGA_S4_BUCKET_NAME if MEGA_S4_BUCKET_NAME else '未設定'}")
    logger.info("=== AmazonS3 設定 ===")
    logger.info(f"AMAZON_S3_REGION{AMAZON_S3_REGION if AMAZON_S3_REGION else '未設定'}")
    logger.info(f"AMAZON_S3_BUCKET_NAME{AMAZON_S3_BUCKET_NAME if AMAZON_S3_BUCKET_NAME else '未設定'}")
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
    else:
        mongo_client = MongoClient(host=MONGO_HOST, port=int(MONGO_PORT))
        col = mongo_client[MONGO_DATABASE_NAME][MONGO_COLLECTION_NAME]
        metadata_col = mongo_client[MONGO_DATABASE_NAME][MONGO_METADATA_COLLECTION_NAME]

        if args.code:
            # === 單筆資料模式 ===
            logger.info(f"開始處理本地測試檔案（單筆模式）:{args.code}")
            doc = col.find_one({"code": args.code})
            process_result, process_sec = process_file(
                file_doc=doc,
                s3_client=awason_s3,
                s4_client=mega_s4,
                s4_bucket=MEGA_S4_BUCKET_NAME,
                skip_existing=args.skip_existing,
                dry_run=args.dry_run,
                tmp_dir="temp",
                show_progress=True,
                logger=logger
            )
            logger.info("本地測試檔案處理完成。")
            logger.info(f"本地測試檔案處理結果: {'成功' if process_result else '失敗'} 經過時間: {human_time_ct_str(process_sec)} ")
        else:
            logger.info("開始處理所有待處理檔案（多筆模式）。")

            col_total = col.count_documents({})
            metadata_col_total = metadata_col.count_documents({})
            if col_total != metadata_col_total:
                logger.info(f"檔案集合與 Metadata 集合的文件數量不一致 檔案集合: {col_total} 筆，Metadata 集合: {metadata_col_total} 筆")

                while True:

                    datas = col.find({}).batch_size(args.per_count)

                    for data in datas:

                        metadata_doc = metadata_col.find_one({"code": data.get("code")})

                        if not metadata_doc:
                            # 在 Metadata 新增資料
                            now_datetime = datetime.now()
                            metadata = {
                                "code": data.get("code"),
                                "origin": data.get("origin"),
                                "remote_key": data.get("remote_key"),
                                "status": "pending",
                                "status_updated_at": now_datetime,
                                "creation_date": now_datetime,
                                "modified_date": now_datetime
                            }
                            metadata_col.insert_one(data)

            while True:
                # === 多筆資料模式 ===
                docs = list(col.find({}))  # 範例篩選條件
                total_files = len(docs)

                if total_files == 0:
                    logger.info("沒有待處理的檔案。")
                    exit(0)

                logger.info(f"共讀取 {total_files} 筆資料，開始使用 {args.threads} 執行緒處理。")

                # === 建立多執行緒執行池 ===
                with ThreadPoolExecutor(max_workers=args.threads) as executor:
                    futures = []
                    for file_doc in docs:
                        futures.append(executor.submit(
                            process_file,
                            file_doc=file_doc,
                            s3_client=awason_s3,
                            s4_client=mega_s4,
                            s4_bucket=MEGA_S4_BUCKET_NAME,
                            skip_existing=args.skip_existing,
                            dry_run=args.dry_run,
                            tmp_dir="temp",
                            show_progress=False,  # 多執行緒時建議關閉個別 tqdm
                            logger=logger
                        ))

                    # === 使用 tqdm 顯示整體進度 ===
                    for _ in tqdm(as_completed(futures), total=total_files, desc="整體進度", unit="file"):
                        pass

                logger.info("全部檔案處理完成")
                send_telegram_message(
                    message=f"MegaAPI 檔案處理完成，共處理 {total_files} 筆檔案。",
                    bot_token=TELEGRAM_BOT_TOKEN,
                    chat_id=TELEGRAM_CHAT_ID,
                    send=args.enable_telegram_message
                )
