# Python-MegaAPI

```
MEGA 官方並沒有一份完整公開、詳盡以 HTTP/JSON 介面為主的「API 文件」（像傳統 REST API 那樣）。
```

使用 MEGA S4（S3 相容 API）接收任務，從 Amazon S3 下載檔案後上傳至 MEGA S4。透過 MongoDB 儲存任務狀態，支援 Telegram 通知。

## 目錄

- [專案說明](#專案說明)
- [執行方式](#執行方式)
- [設定檔說明](#設定檔說明)
- [執行流程](#執行流程)
- [status 狀態](#status-狀態)
- [測試](#測試)
- [參考資料](#參考資料)
- [注意事項](#注意事項)

## 專案說明

本工具作為 S3 → MEGA S4 的自動搬運服務。主程式從 MongoDB 撈取 `pending` 狀態的任務，從 Amazon S3 下載對應檔案，再上傳至 MEGA S4，並在過程中即時更新任務狀態。支援 Telegram Bot 推送執行結果通知。

## 執行方式

```bash
python main.py
```

### Docker 部署

```bash
docker-compose up -d
```

## 設定檔說明

### conf/config.ini

```ini
[MONGO]
MONGO_HOST=
MONGO_PORT=
MONGO_DATABASE_NAME=
MONGO_COLLECTION_NAME=

[MEGA_S4]
MEGA_S4_ACCESS_KEY=
MEGA_S4_SECRET_KEY=
MEGA_S4_REGION=
MEGA_S4_BUCKET_NAME=

[AMAZON_S3]
AMAZON_S3_ACCESS_KEY=
AMAZON_S3_SECRET_KEY=
AMAZON_S3_REGION=
AMAZON_S3_BUCKET_NAME=
AMAZON_S3_URL=

[TELEGRAM]
TELEGRAM_BOT_TOKEN=
TELEGRAM_CHAT_ID=
```

| 區段 | 說明 |
|------|------|
| `[MONGO]` | MongoDB 連線資訊，用於讀取任務與更新狀態 |
| `[MEGA_S4]` | MEGA S4（S3 相容）上傳目標設定 |
| `[AMAZON_S3]` | 來源 Amazon S3 設定，`AMAZON_S3_URL` 可指定自訂 endpoint |
| `[TELEGRAM]` | 選填，設定後可透過 Telegram Bot 推送執行通知 |

## 執行流程

```
從 MongoDB 查詢 status = "pending" 的任務
  → 將任務狀態更新為 "processing"
    → 從 Amazon S3 下載檔案
      → 成功：將檔案上傳至 MEGA S4
        → 成功：更新狀態為 "done"，發送 Telegram 通知
        → 失敗：更新狀態為 "s4_upload_failed"
      → 失敗：更新狀態為 "s3_download_failed"
    → 其他例外（JSON 格式錯誤、IOError 等）：更新狀態為 "error"
```

## status 狀態

| 狀態值 | 說明 | 觸發時機 |
|--------|------|----------|
| `"pending"` | 等待處理 | 剛插入或未開始處理 |
| `"processing"` | 正在下載 / 上傳中 | 進入任務前設定 |
| `"s3_download_failed"` | 從 S3 下載失敗 | S3 檔案不存在或網路錯誤 |
| `"s4_upload_failed"` | 上傳 S4 失敗 | 上傳時錯誤或認證失敗 |
| `"done"` | 已完成上傳 | 成功下載並上傳到 S4 |
| `"error"` | 其他非預期錯誤 | 例如 JSON 格式錯誤、IOError |

## 測試

### 建立 sparse 檔案（快速，不真實寫入）

```bash
truncate -s 1G dummy_1GB.bin
```

### 建立真實 1GB（會寫入、較慢）

```bash
dd if=/dev/zero of=dummy_1GB.bin bs=4M count=256
```

## 參考資料

- [MegaApiClient Github](https://github.com/gpailler/MegaApiClient)

## 注意事項

- MongoDB 中的任務文件需包含對應 S3 檔案的路徑資訊，格式請參考 `src/` 內的 model 定義。
- `AMAZON_S3_URL` 可用於指定非標準 S3 endpoint（如自架 MinIO），若使用 AWS 官方 S3 可留空。
- `processing` 狀態的任務在程式異常終止後不會自動重設，需手動將狀態改回 `pending` 再重跑。
- Telegram 設定為選填，若不需通知可留空，不影響主要功能。
- 建議搭配 Docker Compose 部署，避免環境差異問題。
