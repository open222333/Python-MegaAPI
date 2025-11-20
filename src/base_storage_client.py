from abc import ABC, abstractmethod


class BaseStorageClient(ABC):
    """雲端儲存服務的抽象基底類別。"""

    @abstractmethod
    def upload_file(self, bucket_name: str, remote_key: str, local_file_path: str) -> bool:
        """上傳本地檔案到儲存桶。

        參數：
            bucket_name : 儲存桶名稱
            remote_key : 檔案在儲存桶上的路徑
            local_file_path : 本地檔案路徑
        回傳：
            True  -> 上傳成功
            False -> 上傳失敗
        """
        pass

    @abstractmethod
    def download_file(self, bucket_name: str, remote_key: str, local_file_path: str) -> bool:
        """從儲存桶下載檔案到本地端。

        參數：
            bucket_name : 儲存桶名稱
            remote_key : 檔案在儲存桶上的路徑
            local_file_path : 本地檔案路徑
        回傳：
            True  -> 下載成功
            False -> 下載失敗
        """
        pass

    @abstractmethod
    def delete_file(self, bucket_name: str, remote_key: str) -> bool:
        """刪除儲存桶內的指定檔案。

        參數：
            bucket_name : 儲存桶名稱
            remote_key : 檔案在儲存桶上的路徑
        回傳：
            True  -> 刪除成功
            False -> 刪除失敗
        """
        pass

    @abstractmethod
    def exists(self, bucket_name: str, remote_key: str) -> bool:
        """檢查指定檔案是否存在。

        參數：
            bucket_name : 儲存桶名稱
            remote_key : 檔案在儲存桶上的路徑
        回傳：
            True  -> 檔案存在
            False -> 檔案不存在
        """
        pass

    @abstractmethod
    def list_files(self, bucket_name: str, prefix: str = "") -> list:
        """列出儲存桶中符合前綴的檔案清單。

        參數：
            bucket_name : 儲存桶名稱
            prefix : 前綴字串
        回傳：
            符合前綴的檔案清單(list)
        """
        pass
