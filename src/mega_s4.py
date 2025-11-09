import os
import boto3
from botocore.client import Config
from datetime import datetime, timezone, timedelta
from logging.handlers import RotatingFileHandler
from src.timer import timed
import logging


class MegaS4():

    def __init__(self, access_key, secret_key, endpoint_url, region_name, name="MegaS4Client", log_level="INFO", log_max_bytes=5*1024*1024, log_backup_count=3):
        """ 初始化 Mega S4 客戶端。

        Args:
            access_key (str): S4 存取金鑰。
            secret_key (str): S4 秘密金鑰。
            endpoint_url (str): S4 端點 URL。 範例 "https://s3.g.s4.mega.io"。
            region_name (str): S4 區域名稱。 範例 "g"。
            name (str): 日誌記錄器名稱。 預設為 "MegaS4Client"。
            log_level (str): 日誌等級。 預設為 "INFO"。
            log_max_bytes (int): 單一日誌檔最大位元組數。 預設為 5MB。
            log_backup_count (int): 保留的舊日誌檔案數量。 預設為 3。
        """
        self.client = boto3.client(
            "s3",
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            endpoint_url=endpoint_url,
            region_name=region_name,
            config=Config(signature_version='s3v4')
        )

        # 建立 logger
        self.logger = logging.getLogger(name)
        self.logger.setLevel(getattr(logging, log_level.upper()))
        self.logger.propagate = False  # 避免重複輸出

        # === 檔案輸出 handler (只記錄 ERROR 以上) ===
        log_path = os.path.join('logs', f"{name}.log")
        file_handler = RotatingFileHandler(
            log_path,
            maxBytes=log_max_bytes,
            backupCount=log_backup_count,
            encoding='utf-8'
        )
        file_handler.setLevel(logging.ERROR)  # 只記錄 ERROR+

        # === 終端機輸出 handler ===
        console_handler = logging.StreamHandler()
        console_handler.setLevel(getattr(logging, log_level.upper()))

        # === 統一格式 ===
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)

        # === 加入 handler ===
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)

    @timed(print_result=False)
    def upload_file_to_s4(self, bucket_name, local_file_path, remote_key):
        """上傳本地檔案到 S4 儲存桶。"""
        if not os.path.isfile(local_file_path):
            raise FileNotFoundError(f"Local file not found: {local_file_path}")

        self.logger.info(f"上傳中 {local_file_path} to s3://{bucket_name}/{remote_key}...")
        self.client.upload_file(local_file_path, bucket_name, remote_key)
        self.logger.info(f"已上傳: {local_file_path} → s3://{bucket_name}/{remote_key}")

    def delete_old_files(self, bucket_name, prefix="", days=7):
        """刪除指定 prefix 下超過 N 天的舊檔案。"""
        cutoff_date = datetime.now(timezone.utc) - timedelta(days=days)
        deleted_count = 0

        response = self.client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        if "Contents" not in response:
            self.logger.error("沒有符合的檔案")
            return

        for obj in response["Contents"]:
            last_modified = obj["LastModified"]
            key = obj["Key"]
            if last_modified < cutoff_date:
                self.client.delete_object(Bucket=bucket_name, Key=key)
                deleted_count += 1
                self.logger.info(f"刪除舊檔: {key} (LastModified={last_modified})")

        self.logger.info(f"完成刪除 {deleted_count}")


# 使用方法
if __name__ == "__main__":
    # === 設定 ===
    ACCESS_KEY = "你的_ACCESS_KEY"
    SECRET_KEY = "你的_SECRET_KEY"
    ENDPOINT_URL = "https://s3.eu-central-1.s4.mega.io"
    REGION = "eu-central-1"
    BUCKET_NAME = "your-bucket-name"

    LOCAL_FILE = "/path/to/your/file.txt"
    REMOTE_KEY = "uploads/file.txt"

    PREFIX = "uploads/"   # 刪除該目錄下的檔案
    RETENTION_DAYS = 7    # 預設 7 天，可自行調整

    # === 建立客戶端 ===
    s4 = MegaS4(ACCESS_KEY, SECRET_KEY, ENDPOINT_URL, REGION)

    # === 上傳檔案 ===
    s4.upload_file_to_s4(BUCKET_NAME, LOCAL_FILE, REMOTE_KEY)

    # === 刪除舊檔案 ===
    s4.delete_old_files(BUCKET_NAME, PREFIX, days=RETENTION_DAYS)
