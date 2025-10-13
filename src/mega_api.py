import base64
import requests
import logging
import random
import json
import os
from logging.handlers import RotatingFileHandler
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes


# ============================================================
# Base64 URL 編碼與解碼
# ============================================================


def base64url_decode(data):
    """
    將 Base64 URL 安全格式字串解碼為 bytes。
    MEGA API 使用 URL-safe Base64（即用 -_ 取代 +/）
    """
    # 補足 Base64 長度必須為 4 的倍數
    data += '=' * (-len(data) % 4)
    return base64.urlsafe_b64decode(data)


def base64url_encode(data):
    """
    將 bytes 編碼為 Base64 URL 安全格式（不包含結尾的 '='）
    """
    return base64.urlsafe_b64encode(data).decode().rstrip('=')

# ============================================================
# 資料型別轉換：string ↔ 32-bit integer array (a32)
# ============================================================


def str_to_a32(s):
    """
    將位元組字串轉成 32-bit 整數陣列。
    例如：b'ABCDEFGH' -> [0x41424344, 0x45464748]
    這是 MEGA 的內部資料結構。
    將 bytes 轉換成 32-bit 整數陣列 (每 4 bytes 一組)
    """
    # 確保長度是 4 的倍數（使用 b'\0' 填充）
    s += b'\0' * (-len(s) % 4)
    return [int.from_bytes(s[i:i + 4], 'big') for i in range(0, len(s), 4)]


def a32_to_str(a):
    """
    將 32-bit 整數陣列轉回位元組字串。
    例如：[0x41424344, 0x45464748] -> b'ABCDEFGH'
    """
    return b''.join(x.to_bytes(4, 'big') for x in a)

# ============================================================
# AES 加密（使用 ECB 模式）
# ============================================================


def aes_cbc_encrypt(data, key):
    """
    使用 AES-ECB 模式加密資料。
    注意：雖然名稱是 CBC，但實際上這裡是 ECB 模式。
    MEGA 的金鑰導出流程使用 ECB 而非 CBC。
    """
    cipher = Cipher(algorithms.AES(a32_to_str(key)), modes.ECB())
    encryptor = cipher.encryptor()
    return encryptor.update(data) + encryptor.finalize()


def aes_ecb_encrypt(data: bytes, key):
    """AES-ECB 模式加密（MEGA 金鑰推導使用）"""
    cipher = Cipher(algorithms.AES(a32_to_str(key)), modes.ECB())
    encryptor = cipher.encryptor()

    # 確保長度是 16 bytes 的倍數（AES block size）
    if len(data) % 16 != 0:
        data += b'\0' * (16 - len(data) % 16)

    return encryptor.update(data) + encryptor.finalize()


# ============================================================
# 密碼轉換為 Master Key（登入前置步驟）
# ============================================================


def prepare_key(password: str):
    """
    由密碼字串導出 master key
    參考 MEGA SDK: password → AES key
    """
    password_bytes = password.encode('utf-8')
    key = [0x93C467E3, 0x7DB0C7A4, 0xD1BE3F81, 0x0152CB56]  # 初始固定 key
    aes = None

    for _ in range(0x10000):  # 65536 次
        for i in range(0, len(password_bytes), 16):
            block = password_bytes[i:i+16]
            if len(block) < 16:
                block += b'\0' * (16 - len(block))

            aes = Cipher(algorithms.AES(a32_to_str(key)), modes.ECB()).encryptor()
            key = str_to_a32(aes.update(block))

    return key

# ============================================================
# 登入雜湊（用於驗證帳號密碼）
# ============================================================


def stringhash(email, key):
    """
    計算登入所需的雜湊值 (uh)
    依 MEGA API 登入邏輯：
    - email 轉小寫
    - 以 key 為 AES-ECB 金鑰
    - 對 email 反覆加密 16384 次
    - 最後取 h[0], h[2] 組成結果
    """
    email_bytes = email.lower().encode()
    aes = Cipher(algorithms.AES(a32_to_str(key)), modes.ECB()).encryptor()

    # 初始 4 個 32-bit 整數（16 bytes）
    h = [0, 0, 0, 0]

    # 每 16 bytes 一個 block
    for i in range(0, len(email_bytes), 16):
        block = email_bytes[i:i + 16]
        if len(block) < 16:
            block += b'\0' * (16 - len(block))

        # XOR 合併進 h
        for j in range(4):
            h[j] ^= int.from_bytes(block[j*4:j*4+4], 'big')

    # 重複加密 16384 次
    for _ in range(0x4000):  # 16384
        h = str_to_a32(aes.update(a32_to_str(h)))

    # 最後取 h[0] 與 h[2]
    out = a32_to_str([h[0], h[2]])

    # 回傳 base64url 安全格式
    return base64url_encode(out)

# ============================================================
# 登入主程式
# ============================================================


class Mega:

    def __init__(self,
                 email,
                 password,
                 name='MegaAPI',
                 log_max_bytes=10*1024*1024,
                 log_backup_count=5,
                 log_level="DEBUG"):
        os.makedirs('logs', exist_ok=True)

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

        self.email = email
        self.password = password
        self.session = self.mega_login(email, password)
        self.sid = self.session.get("csid") if self.session else None

    # ============================================================
    # API 通用請求函式
    # ============================================================

    def mega_api_request(self, data, base_url="https://g.api.mega.co.nz/cs"):
        """向 MEGA API 發送請求"""
        if not isinstance(data, list):
            data = [data]

        payload = json.dumps(data)
        headers = {"Content-Type": "application/json"}
        # MEGA 的 /cs API 必須包含一個隨機 id 參數
        params = {"id": random.randint(0, 0xFFFFFFFF)}

        try:
            res = requests.post(
                base_url,
                params=params,
                data=payload,
                headers=headers, timeout=15
            )

            # Debug 輸出非 JSON 回應
            if res.status_code != 200:
                error_msg = res.text.strip() or "<no response body>"
                self.logger.error(f"HTTP {res.status_code}: {error_msg}")
                self.logger.debug(f"HTTP {res.status_code}: {error_msg} | URL={base_url} | Payload={data}")
                raise Exception(f"MEGA API error: HTTP {res.status_code} - {error_msg}")

            # 確認回應內容是否為 JSON
            try:
                json_data = res.json()
            except json.JSONDecodeError:
                self.logger.error(
                    f"Non-JSON response: {res.text[:200]}")  # 只顯示前 200 字
                raise Exception("Invalid JSON response from MEGA API")

            return json_data[0] if json_data else None

        except requests.exceptions.RequestException as e:
            self.logger.exception("MEGA API connection failed")
            raise e

    # ------------------------------------------------------
    # 登入方法
    # ------------------------------------------------------
    def mega_login(self, email, password):
        """
        登入 MEGA：
        1. 透過密碼生成 master key
        2. 使用 master key 與 email 生成 uh 雜湊
        3. 呼叫 MEGA API 的 us (user session) 命令
        成功後會回傳 session ID (csid)
        """
        key = prepare_key(password)
        uh = stringhash(email.lower(), key)
        self.logger.debug(f"Login hash (uh): {uh}")
        data = {"a": "us", "user": email, "uh": uh}
        result = self.mega_api_request(data)

        if isinstance(result, dict) and "csid" in result:
            self.logger.info(f"登入成功！Session ID: {result['csid']}")
            return result
        else:
            self.logger.error(f"登入失敗: {result}")
            return None

    # ------------------------------------------------------
    # 列出所有檔案與資料夾
    # ------------------------------------------------------
    def list_files(self):
        """
        列出使用者雲端所有檔案與資料夾
        """
        if not self.sid:
            self.logger.error("尚未登入，無法列出檔案。")
            return None

        data = {"a": "f", "c": 1}  # "f" = files, "c"=1 表示要返回整個目錄結構
        result = self.mega_api_request(data, self.sid)

        if isinstance(result, dict) and "f" in result:
            files = result["f"]
            self.logger.info(f"共取得 {len(files)} 個檔案/資料夾。")
            return files
        else:
            self.logger.error(f"列出檔案失敗: {result}")
            return None

    # ------------------------------------------------------
    # 上傳檔案
    # ------------------------------------------------------
    def upload_file(self, file_path, folder_node=None):
        """
        上傳檔案到指定資料夾（預設根目錄）
        注意：這是簡化版上傳（未加密）
        """
        if not self.sid:
            self.logger.error("尚未登入，無法上傳檔案。")
            return None

        if not os.path.exists(file_path):
            self.logger.error(f"找不到檔案：{file_path}")
            return None

        file_name = os.path.basename(file_path)
        size = os.path.getsize(file_path)

        # 這裡示範「匿名暫存上傳 URL」方式（實際 MEGA 用更複雜 AES 加密）
        data = {
            "a": "u",       # upload
            "s": size,      # file size
            "t": folder_node or "n"  # 目標資料夾 (預設根目錄)
        }

        result = self.mega_api_request(data, self.sid)
        if not isinstance(result, str):
            self.logger.error(f"初始化上傳失敗: {result}")
            return None

        upload_url = result
        self.logger.info(f"取得上傳 URL: {upload_url}")

        # 進行實際上傳
        with open(file_path, "rb") as f:
            upload_res = requests.post(upload_url, data=f.read())
        if upload_res.status_code == 200:
            self.logger.info(f"檔案上傳成功: {file_name}")
            return True
        else:
            self.logger.error(
                f"上傳失敗 ({upload_res.status_code}): {upload_res.text}")
            return False

    # ------------------------------------------------------
    # 刪除檔案
    # ------------------------------------------------------
    def delete_file(self, node_id):
        """
        刪除指定檔案（node id）
        """
        if not self.sid:
            self.logger.error("尚未登入，無法刪除檔案。")
            return None

        data = {"a": "d", "n": node_id}
        result = self.mega_api_request(data, self.sid)

        if result == 0:
            self.logger.info(f"檔案刪除成功：{node_id}")
            return True
        else:
            self.logger.error(f"檔案刪除失敗: {result}")
            return False
