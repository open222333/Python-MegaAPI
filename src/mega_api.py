import base64
import requests
import logging
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


def prepare_key(password):
    """
    根據使用者密碼計算 MEGA Master Key。
    流程：
    1. 將密碼轉為 32-bit 陣列
    2. 以固定初始金鑰 XOR 疊代 65536 次
    3. 每次疊代後再用 AES-ECB 加密一次
    這個過程相當於一種 key derivation function (KDF)
    """
    p = str_to_a32(password.encode())
    key = [0x93c467e3, 0x7db0c7a4, 0xd1be3f81, 0x0152cb56]
    for _ in range(65536):  # 重複 65536 次以增加暴力破解成本
        for i in range(0, len(p), 4):
            # XOR 合併目前的 key 與密碼區塊
            key = [x ^ y for x, y in zip(key, p[i:i + 4])]
            # 使用 AES 再加密一次（以 [0,0,0,0] 為固定 key）
            key = str_to_a32(aes_cbc_encrypt(a32_to_str(key), [0, 0, 0, 0]))
    return key

# ============================================================
# 登入雜湊（用於驗證帳號密碼）
# ============================================================


def stringhash(email, key):
    """
    計算登入所需的雜湊值 (uh)
    MEGA 用 email 與導出的 master key 產生一個 AES 雜湊。
    """
    email_bytes = email.encode()
    aes = Cipher(algorithms.AES(a32_to_str(key)), modes.ECB()).encryptor()

    # 初始雜湊值為 4 個 32-bit 整數（共 16 bytes）
    h = [0, 0, 0, 0]

    # 每 16 bytes email 做一個 block 加密
    for i in range(0, len(email_bytes), 16):
        block = email_bytes[i:i + 16]
        # 不足 16 bytes 的部分補 0
        if len(block) < 16:
            block += b'\0' * (16 - len(block))

        # XOR 合併目前雜湊
        for j in range(4):
            h[j] ^= int.from_bytes(block[j*4:j*4+4], 'big')

        # 將 XOR 結果再加密一次
        h = str_to_a32(aes.update(a32_to_str(h)))

    out = a32_to_str(h)
    # 回傳 Base64 URL 安全格式（取前 16 bytes）
    return base64url_encode(out[:16])

# ============================================================
# API 通用請求函式
# ============================================================


def mega_api_request(data):
    """
    傳送 JSON-RPC API 請求到 MEGA。
    參數 data 應該是一個 dict，例如：{"a":"uq"}
    """
    url = "https://g.api.mega.co.nz/cs"
    # 注意：MEGA API 要求 JSON body 是陣列格式
    res = requests.post(url, json=[data])
    # 回傳第一個結果
    return res.json()[0]

# ============================================================
# 登入主程式
# ============================================================


class Mega:

    def __init__(self, email, password, name='MegaAPI'):
        os.makedirs('logs', exist_ok=True)

        self.logger = logging.getLogger(name)
        log_handler = RotatingFileHandler(
            os.path.join('logs', f"{name}.log"),
            maxBytes=10 * 1024 * 1024,  # 10 MB
            backupCount=5,
            encoding='utf-8'
        )
        log_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        log_handler.setFormatter(log_formatter)
        self.logger.addHandler(log_handler)
        self.logger.setLevel(logging.INFO)

        self.email = email
        self.password = password
        self.session = self.mega_login(email, password)
        self.sid = self.session.get("csid") if self.session else None

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
        data = {"a": "us", "user": email, "uh": uh}
        result = mega_api_request(data)

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
        result = mega_api_request(data, self.sid)

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

        result = mega_api_request(data, self.sid)
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
        result = mega_api_request(data, self.sid)

        if result == 0:
            self.logger.info(f"檔案刪除成功：{node_id}")
            return True
        else:
            self.logger.error(f"檔案刪除失敗: {result}")
            return False
