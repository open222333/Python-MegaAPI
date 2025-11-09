import time
from functools import wraps
from typing import Callable


def human_time_ct_str(seconds: float) -> str:
    """將秒數轉換成中文時間格式（若為 0 不顯示該部分）

    參數：
        seconds : float    秒數
    回傳：
        str : 例如 "1 小時 2 分 3 秒"
    """
    seconds = int(seconds)
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    secs = seconds % 60

    parts = []
    if hours > 0:
        parts.append(f"{hours} 小時")
    if minutes > 0:
        parts.append(f"{minutes} 分")
    if secs > 0 or not parts:  # 全部為 0 時顯示 "0 秒"
        parts.append(f"{secs} 秒")

    return " ".join(parts)


def timed(print_result: bool = False):
    """裝飾器：測量同步函式執行時間。

    參數：
        print_result (bool): 是否印出花費時間（預設 True）
    回傳：
        若原函式有回傳值 → (result, elapsed_seconds)
        若原函式無回傳值 → elapsed_seconds
    """
    def decorator(func: Callable):
        @wraps(func)
        def wrapper(*args, **kwargs):
            start = time.perf_counter()
            result = func(*args, **kwargs)
            end = time.perf_counter()
            elapsed = end - start
            if print_result:
                print(f"[timed] {func.__name__} took {elapsed:.6f} seconds")
            # 若原函式有回傳值，回傳 (result, elapsed)
            return (result, elapsed) if result is not None else elapsed
        return wrapper
    return decorator
