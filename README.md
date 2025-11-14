# Python-MegaAPI

```
MEGA 官方並沒有一份完整公開、詳盡以 HTTP/JSON 介面為主的「API 文件」（像傳統 REST API 那樣）。
```

## 目錄

- [Python-MegaAPI](#python-megaapi)
  - [目錄](#目錄)
  - [參考資料](#參考資料)
- [測試](#測試)
  - [建立 sparse 檔案（快速，不真實寫入）](#建立-sparse-檔案快速不真實寫入)
  - [建立真實 1GB（會寫入、較慢）](#建立真實-1gb會寫入較慢)
- [status 狀態](#status-狀態)

## 參考資料

[MegaApiClient Github](https://github.com/gpailler/MegaApiClient)

# 測試

## 建立 sparse 檔案（快速，不真實寫入）

```sh
truncate -s 1G dummy_1GB.bin
```

## 建立真實 1GB（會寫入、較慢）

```sh
dd if=/dev/zero of=dummy_1GB.bin bs=4M count=256
```

# status 狀態

| 狀態值                    | 說明          | 觸發時機                   |
| ---------------------- | ----------- | ---------------------- |
| `"pending"`            | 等待處理        | 剛插入或未開始處理              |
| `"processing"`         | 正在下載 / 上傳中  | 進入任務前設定                |
| `"s3_download_failed"` | 從 S3 下載失敗   | S3 檔案不存在或網路錯誤          |
| `"s4_upload_failed"`   | 上傳 S4 失敗    | 上傳時錯誤或認證失敗             |
| `"done"`               | 已完成上傳       | 成功下載並上傳到 S4            |
| `"error"`              | 其他非預期錯誤     | 例如 JSON 格式錯誤、IOError   |
