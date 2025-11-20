from logging.handlers import RotatingFileHandler
from src.timer import timed
from tenacity import retry, stop_after_attempt, wait_fixed
from tqdm import tqdm
import logging
import os
import requests


def retry_with(retry_time):
    """建立可調整 retry 次數的動態 decorator"""
    return retry(
        stop=stop_after_attempt(retry_time),
        wait=wait_fixed(2),
        reraise=True
    )


class Downloader:

    def __init__(self, name="Downloader", log_level="INFO", log_max_bytes=5*1024*1024, log_backup_count=3):
        """下載

        Args:
            name (str, optional): logger 名稱. Defaults to "Downloader".
            log_level (str, optional): logger 檔案最大尺寸. Defaults to "INFO".
            log_max_bytes (_type_, optional): logger 備份檔案數量. Defaults to 5*1024*1024.
            log_backup_count (int, optional): logger 名稱. Defaults to 3.
        """
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
    def download_file_with_resume(
        self,
        url: str,
        file_path: str,
        show_progress=False,
        chunk_size=1024000,
        retry_time=3
    ):
        @retry_with(retry_time)              # <-- retry_time 真正生效
        def _run():
            return self._download_core(url, file_path, show_progress, chunk_size)

        return _run()

    def _download_core(self, url: str, file_path: str, show_progress=False, chunk_size=1024000):
        """斷點續傳下載檔案（含 tenacity retry）

        此方法支援：
            - 斷點續傳（Range header）
            - 自動偵測本地檔案大小
            - 若伺服器不支援續傳則自動重載
            - tqdm 進度條（可選）
            - 失敗時會進行 retry（由 tenacity 控制）

        Args:
            url (str):Ｆ
                要下載的檔案網址。
            file_path (str):
                本地儲存路徑，例如 "/data/video.mp4"。
            show_progress (bool, optional):
                是否顯示 tqdm 進度條。預設 False。
            chunk_size (int, optional):
                每次下載的資料量大小（bytes）。預設 1MB。

        Returns:
            bool | str:
                - 成功回傳 True
                - 失敗回傳錯誤訊息字串（包含錯誤原因）
        """
        try:
            # ---------------------------------------------------------
            # 判斷是否已有部分檔案，準備斷點續傳
            # ---------------------------------------------------------
            if os.path.exists(file_path):
                file_size = os.path.getsize(file_path)
                open_file_mode = 'ab'
                r_first = requests.get(url, stream=True, timeout=15)
                remote_file_size = int(r_first.headers.get('content-length', 0))

                # 若伺服器變更檔案，則刪除重來
                if remote_file_size < file_size:
                    os.remove(file_path)
                    open_file_mode = 'wb'
                    file_size = 0
                elif remote_file_size == file_size:
                    # 已完整下載
                    return True

                bpct = True  # 支援斷點
            else:
                open_file_mode = 'wb'
                file_size = 0
                bpct = False

            # ---------------------------------------------------------
            # 發送下載請求（加上 Range header）
            # ---------------------------------------------------------
            headers = {'Range': f'bytes={file_size}-'} if bpct else {}
            r = requests.get(url, stream=True, timeout=15, headers=headers)
            total = int(r.headers.get('content-length', 0))

            # 若伺服器不支援斷點（206），則刪除檔案重新下載
            if bpct and r.status_code != 206:
                msg = f"不支援斷點下載，刪除後重載: {file_path} code:{r.status_code}"
                self.logger.warning(msg)
                os.remove(file_path)
                return self.download_file_with_resume(url, file_path, show_progress, chunk_size)

            # 明確的下載錯誤
            if r.status_code == 404:
                raise FileExistsError(f'網址錯誤 code 404 url:{url}')

            # ---------------------------------------------------------
            # 建立目錄
            # ---------------------------------------------------------
            os.makedirs(os.path.dirname(file_path), exist_ok=True)

            # ---------------------------------------------------------
            # 執行下載寫入
            # ---------------------------------------------------------
            with open(file_path, open_file_mode) as f:
                if show_progress:
                    progress = tqdm(
                        total=total + file_size,
                        initial=file_size,
                        unit='B',
                        unit_scale=True,
                        desc=os.path.basename(file_path),
                        ascii=True
                    )
                    for chunk in r.iter_content(chunk_size=chunk_size):
                        if chunk:
                            f.write(chunk)
                            progress.update(len(chunk))
                    progress.close()
                else:
                    for chunk in r.iter_content(chunk_size=chunk_size):
                        if chunk:
                            f.write(chunk)

            return True

        except Exception as err:
            # tenacity 會自動重試 Exception，最終失敗才會進到這裡
            msg = f'下載發生錯誤: {err}'
            self.logger.error(msg, exc_info=True)

            # 清理錯誤檔案
            if os.path.exists(file_path) and os.path.isfile(file_path):
                os.remove(file_path)

            return msg
