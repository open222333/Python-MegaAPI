from pathlib import Path
from src.exception import ConfigError
import logging
import os
import shutil
import tarfile


def log_config_summary(logger: logging.Logger, sections: dict):
    """在執行前印出所有參數供人工核對。

    Args:
        logger: 日誌記錄器
        sections: {section_title: [(label, value), ...]}
    """
    for section_title, items in sections.items():
        logger.info(f"=== {section_title} ===")
        for label, value in items:
            logger.info(f"{label}：{value}")
    logger.info("=" * 21)


def wait_for_user_confirmation(logger: logging.Logger = None, always_yes: bool = False, sleep_seconds: int = 10):
    while True:
        if always_yes:
            message = f"自動確認繼續執行（跳過提示）。 {sleep_seconds} 秒後繼續執行"
            if logger:
                logger.info(message)
            else:
                print(f"{message}\n")
            break

        choice = input("🟡 是否繼續執行？(Y/N，預設為 N)：").strip().lower()
        if choice == "y":
            message = "繼續執行..."
            if logger:
                logger.info(message)
            else:
                print(f"{message}\n")
            break
        elif choice == "n" or choice == "":
            message = "使用者選擇中斷，程式終止。"
            if logger:
                logger.info(message)
            else:
                print(message)
            exit(0)
        else:
            message = "無效輸入，請輸入 Y 或 N（Enter 預設為 N）。"
            if logger:
                logger.warning(message)
            else:
                print("無效輸入，請輸入 Y 或 N（Enter 預設為 N）。")


def check_required_vars(required_vars: dict, logger: logging.Logger = None):
    """
    檢查必要參數是否完整。

    參數：
        required_vars (dict): key=參數名稱, value=對應值
        logger (logging.Logger): 日誌記錄器，可選

    若有缺少參數，會列出名稱及其當前值，並丟出 ConfigError。
    """
    missing = {k: v for k, v in required_vars.items() if not v}

    if missing:
        # lines = [f"{k} = {repr(v)}" for k, v in missing.items()]
        # message = "必要參數未設定完整，缺少或為空：\n" + "\n".join(lines)

        message = f"必要參數未設定完整，缺少或為空: {', '.join(missing)}"
        raise ConfigError(message)
    else:
        if logger:
            logger.info("必要參數檢查通過，所有參數均已設定。")


def list_all_files(root_dir: str):
    """
    取得指定資料夾內的所有檔案（包含子目錄）

    此函式會使用 Path.rglob("*") 遞迴搜尋所有項目，
    並回傳「所有是檔案的路徑」。

    Args:
        root_dir (str): 要搜尋的根目錄路徑

    Returns:
        list[str]: 所有檔案的完整路徑（字串列表）
    """
    return [str(p) for p in Path(root_dir).rglob("*") if p.is_file()]


def compress_to_tar(root_dir: str, tar_name: str = None):
    """
    將指定資料夾壓縮為 tar 檔，並保持原始的目錄結構。

    此函式會：
    1. 遞迴取得資料夾內所有檔案
    2. 自動建立 tar 檔（無壓縮；如需 gzip 可改成 "w:gz"）
    3. 將檔案以「相對於 root_dir 的路徑」存入 tar 中

    Args:
        root_dir (str): 要壓縮的資料夾路徑
        tar_name (str, optional): 輸出 tar 檔名稱（不含 .tar）
                                  若未提供，預設為資料夾名稱

    Returns:
        str: 產生的 tar 檔名稱（含 .tar 副檔名）
    """
    # 移除結尾的斜線，避免路徑處理錯誤
    root_dir = root_dir.rstrip("/")

    # 若未指定 tar 檔名，則使用資料夾名稱（ex: foo/bar → bar.tar）
    dir_name = os.path.dirname(root_dir)
    tar_name = tar_name if tar_name else os.path.basename(root_dir)
    tar_file_name = os.path.join(dir_name, f'{tar_name}.tar')

    # 取得資料夾內所有檔案（含子資料夾）
    files = list_all_files(root_dir)

    # 建立 tar 檔
    with tarfile.open(tar_file_name, "w") as tar:
        for file_path in files:
            # 在 tar 內的存放路徑（相對於 root_dir）
            arcname = os.path.relpath(file_path, start=root_dir)

            # 加入檔案並保持原始目錄結構
            tar.add(file_path, arcname=arcname)

    return tar_file_name


def delete(path: str):
    """刪除指定路徑（自動判斷檔案或資料夾）

    Args:
        path (str): 要刪除的路徑，可以是檔案或資料夾。

    功能說明：
        - 若路徑不存在，則直接跳過。
        - 若是檔案，使用 os.remove() 刪除。
        - 若是資料夾，使用 shutil.rmtree() 遞迴刪除整個資料夾。

    注意：
        - shutil.rmtree() 會刪除資料夾中的所有內容，請小心使用。
    """

    if not os.path.exists(path):
        return  # 不存在就直接跳過

    # 是檔案就刪檔案
    if os.path.isfile(path):
        os.remove(path)
        return

    # 是資料夾就遞迴刪除
    if os.path.isdir(path):
        shutil.rmtree(path)
