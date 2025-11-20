from botocore.exceptions import ClientError, BotoCoreError
from logging.handlers import RotatingFileHandler
from src.base_storage_client import BaseStorageClient
from src.timer import timed
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type, before_sleep_log
from tqdm import tqdm
import boto3
import logging
import os


class AmazonS3(BaseStorageClient):
    """ Amazon S3 客戶端，用於下載及刪除檔案。 """

    def __init__(
            self, aws_access_key_id: str, aws_secret_access_key: str, region: str, aws_session_token: str = None, name="AmazonS3", log_level="INFO", log_max_bytes=5*1024*1024, log_backup_count=3):
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
    def upload_file(self, bucket_name, remote_key, local_file_path, show_progress=True, retry_time=3) -> bool:
        """
        上傳本地檔案至 S3（含 tenacity 自動重試與 tqdm 進度列）。

        此方法會先取得檔案大小並建立 tqdm 進度列；在上傳過程中若發生
        連線中斷、伺服器端異常或其他錯誤，將根據 retry_time 進行自動重試。
        重試間隔採指數退避（Exponential Backoff），每次重試前會記錄 WARNING。

        參數：
            bucket_name (str): S3 Bucket 名稱。
            remote_key (str): 上傳至 S3 的完整檔案 Key。
            local_file_path (str): 本地待上傳的檔案路徑。
            show_progress (bool): 是否顯示上傳進度列，預設 True。
            retry_time (int): 最大重試次數（預設 3）。

        回傳：
            bool: True 上傳成功；False 上傳失敗。
        """

        if not os.path.isfile(local_file_path):
            self.logger.error(f"找不到本地檔案: {local_file_path}")
            return False

        file_size = os.path.getsize(local_file_path)
        self.logger.debug(
            f"上傳中 {local_file_path} → s3://{bucket_name}/{remote_key} "
            f"({file_size / 1024 / 1024:.2f} MB)"
        )

        try:
            # tqdm 進度列
            with tqdm(
                total=file_size,
                unit="B",
                unit_scale=True,
                desc=f"上傳中：{os.path.basename(local_file_path)}",
                disable=not show_progress,
            ) as pbar:

                def progress_hook(bytes_amount):
                    """每次上傳 chunk 時由 boto3 呼叫，用於更新進度列。"""
                    pbar.update(bytes_amount)

                # === 加入 tenacity retry ===
                @retry(
                    stop=stop_after_attempt(retry_time),
                    wait=wait_exponential(multiplier=1, min=1, max=8),
                    retry=retry_if_exception_type((ClientError, BotoCoreError, Exception)),
                    before_sleep=before_sleep_log(self.logger, logging.WARNING)
                )
                def do_upload():
                    """實際執行 S3 上傳（支援自動重試）。"""
                    self.client.upload_file(
                        local_file_path,
                        bucket_name,
                        remote_key,
                        Callback=progress_hook,
                    )

                do_upload()

            self.logger.info(f"已上傳：{local_file_path} → s3://{bucket_name}/{remote_key}")
            return True

        except Exception as e:
            self.logger.error(f"S3 上傳失敗：{e}", exc_info=True)
            return False

    @timed(print_result=False)
    def download_file(self, bucket_name, remote_key, local_file_path, show_progress=True, retry_time=3) -> bool:
        """
        下載 S3 檔案到本地端（含 tenacity 自動重試與 tqdm 進度列）。

        此方法會先使用 head_object 取得檔案大小，以建立進度列；下載過程
        若發生連線異常、伺服器錯誤或其他例外，將根據 retry_time 進行自動重試。
        重試採用指數退避策略，在每次重試之前會記錄 WARNING，用於除錯。

        參數：
            bucket_name (str): S3 Bucket 名稱。
            remote_key (str): 下載目標檔案在 S3 的完整 Key。
            local_file_path (str): 下載後的本地完整路徑；不存在的資料夾會自動建立。
            show_progress (bool): 是否顯示 tqdm 下載進度列。
            retry_time (int): 最大重試次數（預設 3）。

        回傳：
            bool: True 下載成功；False 下載失敗。
        """

        try:
            # 先取得檔案大小，用來建立進度列
            meta = self.client.head_object(Bucket=bucket_name, Key=remote_key)
            total_length = meta.get("ContentLength", 0)

            # 建立本地目錄
            os.makedirs(os.path.dirname(local_file_path), exist_ok=True)

            with open(local_file_path, "wb") as f:
                with tqdm(
                    total=total_length,
                    unit="B",
                    unit_scale=True,
                    desc=f"下載中：{os.path.basename(local_file_path)}",
                    disable=not show_progress,
                ) as pbar:

                    # === 加入 tenacity retry ===
                    @retry(
                        stop=stop_after_attempt(retry_time),
                        wait=wait_exponential(multiplier=1, min=1, max=8),
                        retry=retry_if_exception_type((ClientError, BotoCoreError, Exception)),
                        before_sleep=before_sleep_log(self.logger, logging.WARNING)
                    )
                    def do_download():
                        """實際執行下載（支援自動重試）。"""
                        self.client.download_fileobj(
                            Bucket=bucket_name,
                            Key=remote_key,
                            Fileobj=f,
                            Callback=lambda bytes_transferred: pbar.update(bytes_transferred),
                        )

                    do_download()

            self.logger.info(f"下載完成：{local_file_path}")
            return True

        except Exception as e:
            self.logger.error(f"S3 下載失敗：{e}", exc_info=True)
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
                self.logger.error(f"檔案不存在：s3://{bucket_name}/{remote_key}，原因：{e}", exc_info=True)
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
            self.logger.error(f"列出檔案失敗 (prefix={prefix})，原因：{e}", exc_info=True)
            return []
