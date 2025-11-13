from abc import ABC, abstractmethod
from datetime import datetime
import os
import uuid


class RemotePathHandler(ABC):
    """抽象類別：用於統一上傳路徑與檔名處理邏輯"""

    def __init__(self, base_path: str):
        """
        :param base_path: 上傳根目錄，例如 "uploads" 或 "s3://mybucket/"
        """
        self.base_path = base_path.rstrip("/")

    @abstractmethod
    def generate_filename(self, original_name: str) -> str:
        """根據原始檔名產生新檔名（可避免重複）"""
        pass

    @abstractmethod
    def build_remote_path(self, filename: str) -> str:
        """根據檔名產生完整上傳路徑"""
        pass

    def get_upload_path(self, original_name: str) -> str:
        """提供統一的上傳路徑產生方法"""
        new_name = self.generate_filename(original_name)
        return self.build_remote_path(new_name)
