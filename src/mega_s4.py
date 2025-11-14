from botocore.client import Config
from botocore.exceptions import ClientError, BotoCoreError
from logging.handlers import RotatingFileHandler
from src.base_storage_client import BaseStorageClient
from src.timer import timed
from tqdm import tqdm
import boto3
import logging
import os


class MegaS4(BaseStorageClient):
    """ Mega S4 客戶端，用於上傳、下載及刪除檔案。 """

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
    def upload_file(self, bucket_name, local_file_path, remote_key, show_progress=True) -> bool:
        """上傳本地檔案到 S4 儲存桶（含進度列）。

        參數：
            bucket_name : S4 Bucket 名稱
            local_file_path : 本地檔案路徑
            remote_key : 檔案在 S4 上的路徑
            show_progress : 是否顯示上傳進度列
        回傳：
            True  -> 上傳成功
            False -> 上傳失敗
        """
        try:
            if not os.path.isfile(local_file_path):
                raise FileNotFoundError(f"本地檔案不存在: {local_file_path}")

            file_size = os.path.getsize(local_file_path)
            self.logger.info(
                f"上傳中 {local_file_path} → s4://{bucket_name}/{remote_key} "
                f"({file_size / 1024 / 1024:.2f} MB)"
            )

            # 顯示 tqdm 進度列
            with tqdm(
                total=file_size,
                unit="B",
                unit_scale=True,
                unit_divisor=1024,
                desc=f"上傳中：{os.path.basename(local_file_path)}",
                ascii=True,
                disable=not show_progress,
            ) as pbar:

                def progress_hook(bytes_amount):
                    """每次上傳一個 chunk 時被呼叫。"""
                    pbar.update(bytes_amount)

                # 使用 boto3 的 Callback 參數
                self.client.upload_file(
                    Filename=local_file_path,
                    Bucket=bucket_name,
                    Key=remote_key,
                    Callback=progress_hook,
                )

            self.logger.info(f"已上傳: {local_file_path} → s4://{bucket_name}/{remote_key}")
            return True

        except (ClientError, BotoCoreError) as e:
            self.logger.error(f"S4 上傳失敗：{e}")
            return False
        except Exception as e:
            self.logger.error(f"未預期錯誤：{e}")
            return False

    @timed(print_result=False)
    def download_file(self, bucket_name, remote_key, local_file_path, show_progress=True) -> bool:
        """從 S4 下載檔案至本地端（含進度列）。

        參數：
            bucket_name : S4 Bucket 名稱
            remote_key : 檔案在 S4 上的路徑
            local_file_path : 本地儲存檔案完整路徑
            show_progress : 是否顯示下載進度列
        回傳：
            True  -> 下載成功
            False -> 下載失敗
        """
        try:
            # 取得檔案大小
            meta = self.client.head_object(Bucket=bucket_name, Key=remote_key)
            total_length = meta.get("ContentLength", 0)

            # 建立本地資料夾
            os.makedirs(os.path.dirname(local_file_path), exist_ok=True)

            # 開始下載
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
                    self.client.download_fileobj(
                        Bucket=bucket_name,
                        Key=remote_key,
                        Fileobj=f,
                        Callback=lambda bytes_transferred: pbar.update(bytes_transferred),
                    )

            self.logger.info(f"已下載：s3://{bucket_name}/{remote_key} → {local_file_path}")
            return True

        except (ClientError, BotoCoreError) as e:
            self.logger.error(f"下載失敗：{remote_key}，原因：{e}")
            return False
        except Exception as e:
            self.logger.error(f"未預期錯誤：{e}")
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
            self.logger.error(f"刪除失敗：{remote_key}，原因：{e}")
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
            self.logger.error(f"列出檔案失敗，原因：{e}")
            return []
