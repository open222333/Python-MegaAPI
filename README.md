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
- [metadata MongoDB 文件欄位設計](#metadata-mongodb-文件欄位設計)
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

# metadata MongoDB 文件欄位設計

| 欄位名                 | 型別         | 說明                                                                                            |
| ------------------- | ---------- | --------------------------------------------------------------------------------------------- |
| `code`              | `str`      | 檔案代碼或唯一識別碼（例如影片代號、任務代號）                                                                       |
| `bucket_name`       | `str`      | 檔案來源的 S3 bucket 名稱                                                                            |
| `remote_key`        | `str`      | 檔案在 S3 的路徑（key）                                                                               |
| `s4_key`            | `str`      | 上傳到 S4 後的路徑（key，可驗證）                                                                          |
| `status`            | `str`      | 處理狀態（`pending`、`processing`、`done`、`s3_download_failed`、`s4_upload_failed`、`error`） |
| `status_updated_at` | `datetime` | 每次狀態更新時間                                                                                      |
| `creation_date`     | `datetime` | 文件建立時間（首次寫入時設定）                                                                               |
| `modified_date`     | `datetime` | 文件最後修改時間（每次更新時可覆寫）                                                                            |
| `retry_count`       | `int`      | 重新嘗試次數（預設 0，每失敗一次 +1）                                                                         |
| `error_message`     | `str`      | 錯誤訊息摘要（僅在失敗或例外時寫入）                                                                            |


# status 狀態

| 狀態值                    | 說明          | 觸發時機                   |
| ---------------------- | ----------- | ---------------------- |
| `"pending"`            | 等待處理        | 剛插入或未開始處理              |
| `"processing"`         | 正在下載 / 上傳中  | 進入任務前設定                |
| `"s3_download_failed"` | 從 S3 下載失敗   | S3 檔案不存在或網路錯誤          |
| `"s4_upload_failed"`   | 上傳 S4 失敗    | 上傳時錯誤或認證失敗             |
| `"done"`               | 已完成上傳       | 成功下載並上傳到 S4            |
| `"error"`              | 其他非預期錯誤     | 例如 JSON 格式錯誤、IOError   |
