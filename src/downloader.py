from src.timer import timed
from tqdm import tqdm
import os
import requests
import logging
from logging.handlers import RotatingFileHandler


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
    def download_file_with_resume(self, url: str, file_path: str, print_bar=False, chunk_size=1024000):
        """斷點續傳下載檔案

        Args:
            url (str): 網址
            file_path (str): 檔案位置
            print_bar (bool, optional): 顯示進度條. Defaults to False.
            chunk_size (int, optional): 下載chunk大小. Defaults to 1024000.

        Returns:
            bool | str:
                - 成功時回傳 True
                - 失敗時回傳錯誤訊息字串（msg），內容包含錯誤原因與相關資訊
        """
        try:
            # 檢查已下載的檔案大小
            if os.path.exists(file_path):
                file_size = os.path.getsize(file_path)
                open_file_mode = 'ab'
                r_first = requests.get(url, stream=True, timeout=15)
                remote_file_size = int(r_first.headers.get('content-length', 0))
                if remote_file_size < file_size:
                    os.remove(file_path)
                    open_file_mode = 'wb'
                    file_size = 0
                elif remote_file_size == file_size:
                    return True
                bpct = True
            else:
                open_file_mode = 'wb'
                file_size = 0
                bpct = False

            # 進行下載請求
            headers = {'Range': f'bytes={file_size}-'} if bpct else {}
            r = requests.get(url, stream=True, timeout=15, headers=headers)
            total = int(r.headers.get('content-length', 0))

            if bpct and r.status_code != 206:
                msg = f"不支援斷點下載，刪除後重載: {file_path} code:{r.status_code}"
                self.logger.warning(msg)
                os.remove(file_path)
                return self.download_file_with_resume(url, file_path, print_bar, chunk_size)

            if r.status_code == 404:
                raise FileExistsError(f'網址錯誤 code 404 url:{url}')

            # 建立目錄
            os.makedirs(os.path.dirname(file_path), exist_ok=True)

            # 顯示 tqdm 進度條（僅當 print_bar 為 True 時）
            with open(file_path, open_file_mode) as f:
                if print_bar:
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
            msg = f'下載發生錯誤: {err}'
            self.logger.error(msg)
            if os.path.exists(file_path):
                if os.path.isfile(file_path):
                    os.remove(file_path)
            return msg
