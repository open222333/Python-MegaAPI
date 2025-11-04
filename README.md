# Python-MegaAPI

```
MEGA 官方並沒有一份完整公開、詳盡以 HTTP/JSON 介面為主的「API 文件」（像傳統 REST API 那樣）。
```

## 目錄

- [Python-MegaAPI](#python-megaapi)
  - [目錄](#目錄)
  - [參考資料](#參考資料)

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