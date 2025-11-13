from botocore.exceptions import ClientError, BotoCoreError
from logging.handlers import RotatingFileHandler
from src.base_storage_client import BaseStorageClient
from src.timer import timed
from tqdm import tqdm
import boto3
import logging
import os
import requests


class AmazonS3(BaseStorageClient):
    """ Amazon S3 客戶端，用於下載及刪除檔案。 """

    def __init__(
            self, aws_access_key_id: str, aws_secret_access_key: str, aws_session_token: str, region: str, name="AmazonS3", log_level="INFO", log_max_bytes=5*1024*1024, log_backup_count=3):
        """
        初始化 S3Downloader。

        參數：
            aws_access_key_id : AWS Access Key ID
            aws_secret_access_key : AWS Secret Access Key
            aws_session_token : 可選 Session Token（例如臨時憑證）
            region : AWS 區域名稱
            name : logger 名稱
            log_level : logger 等級
            log_max_bytes : logger 檔案最大尺寸
            log_backup_count : logger 備份檔案數量
        """
        self.client = boto3.client(
            "s3",
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_session_token=aws_session_token,
            region_name=region,
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
    def upload_file(self, bucket_name, remote_key, local_file_path, show_progress=True) -> bool:
        """上傳檔案到 S3（含進度列）。

        參數：
            bucket_name : S3 Bucket 名稱
            local_file_path : 本地檔案路徑
            remote_key : 檔案在 S3 上的路徑
            show_progress : 是否顯示上傳進度列
        """
        try:
            if not os.path.isfile(local_file_path):
                raise FileNotFoundError(f"找不到本地檔案: {local_file_path}")

            file_size = os.path.getsize(local_file_path)
            self.logger.info(
                f"上傳中 {local_file_path} → s3://{bucket_name}/{remote_key} ({file_size / 1024 / 1024:.2f} MB)"
            )

            with tqdm(
                total=file_size,
                unit="B",
                unit_scale=True,
                desc=f"上傳中：{os.path.basename(local_file_path)}",
                disable=not show_progress,
            ) as pbar:

                def progress_hook(bytes_amount):
                    pbar.update(bytes_amount)

                self.client.upload_file(
                    local_file_path,
                    bucket_name,
                    remote_key,
                    Callback=progress_hook,
                )

            self.logger.info(f"已上傳: {local_file_path} → s3://{bucket_name}/{remote_key}")
            return True

        except (ClientError, BotoCoreError) as e:
            self.logger.error(f"S3 上傳失敗：{e}")
            return False
        except Exception as e:
            self.logger.error(f"未預期錯誤：{e}")
            return False

    @timed(print_result=False)
    def download_file(self, bucket_name, remote_key, local_file_path, show_progress=True) -> bool:
        """
        下載 S3 檔案到指定路徑。

        參數：
            bucket_name : S3 Bucket 名稱
            remote_key : 檔案在 S3 上的路徑
            local_file_path : 本地儲存檔案的完整路徑
            show_progress : 是否顯示下載進度列

        回傳：
            True  -> 成功
            False -> 失敗
        """
        try:
            # 取得檔案大小
            meta = self.client.head_object(Bucket=bucket_name, Key=remote_key)
            total_length = meta.get("ContentLength", 0)

            # 建立目錄
            os.makedirs(os.path.dirname(local_file_path), exist_ok=True)

            # 開始下載
            with open(local_file_path, "wb") as f:
                with tqdm(
                    total=total_length,
                    unit="B",
                    unit_scale=True,
                    desc=f"下載中：{os.path.basename(local_file_path)}",
                    disable=not show_progress,
                ) as pbar:
                    self.client.download_fileobj(
                        Bucket=bucket_name,
                        Key=remote_key,
                        Fileobj=f,
                        Callback=lambda bytes_transferred: pbar.update(bytes_transferred),
                    )

            self.logger.info(f"下載完成：{local_file_path}")
            return True

        except (ClientError, BotoCoreError) as e:
            self.logger.error(f"S3 下載失敗：{e}")
            return False
        except Exception as e:
            self.logger.error(f"未預期錯誤：{e}")
            return False

    def delete_file(self, bucket_name, remote_key) -> bool:
        """刪除 S3 上指定的單一檔案。

        參數：
            bucket_name : S3 Bucket 名稱
            remote_key : 檔案在 S3 上的路徑
        """
        try:
            self.client.delete_object(Bucket=bucket_name, Key=remote_key)
            self.logger.info(f"已刪除檔案：s3://{bucket_name}/{remote_key}")
            return True
        except Exception as e:
            self.logger.error(f"刪除失敗：{remote_key}，原因：{e}")
            return False

    def exists(self, bucket_name, remote_key) -> bool:
        """檢查 S3 檔案是否存在。
        參數：
            bucket_name : S3 Bucket 名稱
            remote_key : 檔案在 S3 上的路徑
        回傳：
            True  -> 檔案存在
            False -> 檔案不存在
        """
        try:
            self.client.head_object(Bucket=bucket_name, Key=remote_key)
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                return False
            else:
                self.logger.error(f"檔案不存在：s3://{bucket_name}/{remote_key}，原因：{e}")
                return None

    def list_files(self, bucket_name, prefix="") -> list:
        """列出指定 prefix 的所有檔案

        參數：
            bucket_name : S3 Bucket 名稱
            prefix : 前綴字串
        回傳：
            符合前綴的檔案清單(list)
        """
        try:
            paginator = self.client.get_paginator("list_objects_v2")
            page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=prefix)

            all_files = []
            for page in page_iterator:
                for obj in page.get("Contents", []):
                    all_files.append(obj["Key"])

            self.logger.info(f"列出 {len(all_files)} 個檔案 (prefix={prefix})")
            return all_files
        except Exception as e:
            self.logger.error(f"列出檔案失敗 (prefix={prefix})，原因：{e}")
            return []
