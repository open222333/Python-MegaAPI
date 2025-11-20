from botocore.client import Config
from botocore.exceptions import ClientError, BotoCoreError
from logging.handlers import RotatingFileHandler
from src.base_storage_client import BaseStorageClient
from src.timer import timed
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type, before_sleep_log
from tqdm import tqdm
import boto3
import logging
import os


class MegaS4(BaseStorageClient):
    """ Mega S4 客戶端，用於上傳、下載及刪除檔案。 """

    def __init__(self, access_key, secret_key, endpoint_url, region_name, name="MegaS4Client", log_level="INFO", log_max_bytes=5*1024*1024, log_backup_count=3):
        """初始化 Mega S4 客戶端。

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
    def upload_file(self, bucket_name, local_file_path, remote_key, show_progress=True, retry_time=3) -> bool:
        """
        上傳本地檔案到 S4 儲存桶（含 tenacity 重試機制與 tqdm 進度條）。

        此方法會在上傳過程中監控進度、遇到網路或 S4 端錯誤時自動重試，並在每次重試前輸出警告訊息。
        適用於大型檔案或不穩定網路環境。

        Args:
            bucket_name (str):
                目標 S4 Bucket 名稱。

            local_file_path (str):
                本地端檔案的完整路徑。

            remote_key (str):
                上傳到 S4 時使用的 Key（包含路徑與檔名）。

            show_progress (bool, optional):
                是否顯示 tqdm 上傳進度條。預設為 True。

            retry_time (int, optional):
                上傳失敗時最多自動重試次數。預設為 3。

        Returns:
            bool:
                - True：上傳成功
                - False：檔案不存在或超過重試次數仍上傳失敗

        Notes:
            - 使用 tenacity 自動重試，等待時間採用指數回退（1 → 2 → 4 → 最大 8 秒）。
            - 可捕捉 ClientError、BotoCoreError 以及所有 Exception。
            - 每次重試前會透過 logger 輸出 WARNING 訊息，以利偵錯。
            - tqdm 進度條會依照實際已上傳 bytes 即時更新。
        """
        if not os.path.isfile(local_file_path):
            self.logger.error(f"本地檔案不存在: {local_file_path}")
            return False

        file_size = os.path.getsize(local_file_path)

        try:
            # === 進度條只建立一次 ===
            with tqdm(
                total=file_size,
                unit="B",
                unit_scale=True,
                unit_divisor=1024,
                desc=f"上傳中：{os.path.basename(local_file_path)}",
                ascii=True,
                disable=not show_progress,
            ) as pbar:

                """
                如果發生 ClientError、BotoCoreError 或任何 Exception
                會自動重試最多 retry 次
                每次 retry 之前會用 logger 寫 WARNING 訊息
                重試間隔採用 1 → 2 → 4 → 8 秒的指數等待（最大 8 秒）
                """
                @retry(
                    stop=stop_after_attempt(retry_time),
                    wait=wait_exponential(multiplier=1, min=1, max=8),
                    retry=retry_if_exception_type((ClientError, BotoCoreError, Exception)),
                    before_sleep=before_sleep_log(self.logger, logging.WARNING)
                )
                def do_upload():
                    """真正執行上傳（可 retry）"""
                    self.client.upload_file(
                        Filename=local_file_path,
                        Bucket=bucket_name,
                        Key=remote_key,
                        Callback=lambda bytes_amount: pbar.update(bytes_amount)
                    )

                # === 執行上傳（包含 retry ）===
                do_upload()

            self.logger.info(f"已上傳：{local_file_path} → s4://{bucket_name}/{remote_key}")
            return True

        except Exception as e:
            self.logger.error(f"S4 上傳失敗：{e}", exc_info=True)
            return False

    @timed(print_result=False)
    def download_file(self, bucket_name, remote_key, local_file_path, show_progress=True, retry_time=3) -> bool:
        """
        從 S4 下載檔案至本地端（含 tenacity 自動重試與 tqdm 下載進度列）。

        此方法會先使用 head_object 取得遠端檔案大小，以建立對應的 tqdm
        下載進度條。當下載過程中發生網路波動、伺服器端錯誤或其他異常時，
        tenacity 會自動進行重試。重試策略採用指數退避（Exponential Backoff），
        並在每次重試前輸出 WARNING 記錄，以利追蹤問題。

        Args:
            bucket_name (str):
                S4 Bucket 名稱。

            remote_key (str):
                S4 上檔案的完整路徑（Key）。

            local_file_path (str):
                要將檔案下載到本地的完整儲存路徑；若資料夾不存在會自動建立。

            show_progress (bool, optional):
                是否顯示 tqdm 下載進度列，預設為 True。

            retry_time (int, optional):
                最大重試次數。預設為 3 次。
                若下載過程發生：
                    - ClientError
                    - BotoCoreError
                    - Exception
                則會進行重試。

        Returns:
            bool:
                - True：下載成功
                - False：下載失敗（如找不到檔案、重試次數用盡、連線錯誤等）

        Notes:
            - 函式會先透過 head_object 取得 ContentLength，以便建立進度條。
            - tqdm 透過 download_fileobj 的 Callback 每次更新已傳輸 bytes。
            - 遇到任何指定的異常時會自動重試，間隔採用指數退避：
                1 → 2 → 4 → 最長 8 秒。
            - before_sleep_log 會在每次重試前記錄 WARNING 訊息至 logger。
            - local_file_path 若包含不存在的資料夾，會自動建立避免寫入失敗。
            - 若最終仍失敗會返回 False 並記錄錯誤訊息。
        """
        try:
            # 檔案大小
            meta = self.client.head_object(Bucket=bucket_name, Key=remote_key)
            total_length = meta.get("ContentLength", 0)

            os.makedirs(os.path.dirname(local_file_path), exist_ok=True)

            with open(local_file_path, "wb") as f:
                with tqdm(
                    total=total_length,
                    unit="B",
                    unit_scale=True,
                    unit_divisor=1024,
                    desc=f"下載中：{os.path.basename(local_file_path)}",
                    ascii=True,
                    disable=not show_progress,
                ) as pbar:

                    """
                    如果發生 ClientError、BotoCoreError 或任何 Exception
                    會自動重試最多 retry 次
                    每次 retry 之前會用 logger 寫 WARNING 訊息
                    重試間隔採用 1 → 2 → 4 → 8 秒的指數等待（最大 8 秒）
                    """
                    @retry(
                        stop=stop_after_attempt(retry_time),
                        wait=wait_exponential(multiplier=1, min=1, max=8),
                        retry=retry_if_exception_type((ClientError, BotoCoreError, Exception)),
                        before_sleep=before_sleep_log(self.logger, logging.WARNING)
                    )
                    def do_download():
                        # 每次 retry 都要從頭寫
                        f.seek(0)
                        self.client.download_fileobj(
                            Bucket=bucket_name,
                            Key=remote_key,
                            Fileobj=f,
                            Callback=lambda bytes_transferred: pbar.update(bytes_transferred)
                        )

                    # 執行下載（含 retry）
                    do_download()

            self.logger.info(f"已下載：s4://{bucket_name}/{remote_key} → {local_file_path}")
            return True

        except Exception as e:
            self.logger.error(f"下載失敗：{remote_key}，原因：{e}", exc_info=True)
            return False

    def delete_file(self, bucket_name, remote_key) -> bool:
        """刪除 S4 上指定的單一檔案。

        參數：
            bucket_name : S4 Bucket 名稱
            remote_key : 檔案在 S4 上的路徑
        """
        try:
            self.client.delete_object(Bucket=bucket_name, Key=remote_key)
            self.logger.info(f"已刪除檔案：s4://{bucket_name}/{remote_key}")
            return True
        except Exception as e:
            self.logger.error(f"刪除失敗：{remote_key}，原因：{e}", exc_info=True)
            return False

    def exists(self, bucket_name, remote_key) -> bool:
        """檢查 Mega 檔案是否存在

        參數：
            bucket_name : S4 Bucket 名稱
            remote_key : 檔案在 Mega 上的路徑
        回傳：
            True  -> 檔案存在
            False -> 檔案不存在
        """
        file = self.client.find(remote_key)
        return bool(file)

    def list_files(self, bucket_name, prefix="") -> list:
        """
        列出 Mega 所有檔案（模擬 prefix）

        參數：
            bucket_name : S4 Bucket 名稱
            prefix : 前綴字串
        回傳：
            符合前綴的檔案清單(list)
        """
        try:
            all_files = self.client.get_files_in_node(None)  # 取得所有節點檔案
            matched = [name for name in all_files.keys() if name.startswith(prefix)]
            self.logger.info(f"列出 {len(matched)} 個檔案 (prefix={prefix})")
            return matched
        except Exception as e:
            self.logger.error(f"列出檔案失敗，原因：{e}", exc_info=True)
            return []
