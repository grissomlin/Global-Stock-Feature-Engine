# -*- coding: utf-8 -*-
import os, sys, sqlite3, json, time, socket, io
import pandas as pd
from datetime import datetime
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
from dotenv import load_dotenv

# 💡 載入環境變數
load_dotenv() 
socket.setdefaulttimeout(600)

# 💡 環境變數讀取 (優先從環境變數抓取，若無則可填寫預設值作為備援)
GDRIVE_FOLDER_ID = os.environ.get('GDRIVE_FOLDER_ID')
SERVICE_ACCOUNT_FILE = 'citric-biplane-319514-75fead53b0f5.json'

# 💡 導入特徵加工模組 (processor.py)
try:
    from processor import process_market_data
except ImportError:
    print("⚠️ 系統提示：找不到 processor.py，將跳過特徵工程。")
    process_market_data = None

# 💡 導入通知模組 (修正：若找不到 notifier.py 則跳過，不崩潰)
try:
    from notifier import StockNotifier
    notifier = StockNotifier()
except (ImportError, ModuleNotFoundError, Exception) as e:
    print(f"⚠️ 系統提示：Notifier 初始化跳過 (原因: {e})")
    notifier = None

import downloader_tw, downloader_us, downloader_cn, downloader_hk, downloader_jp, downloader_kr

# 📊 預期數量監控
EXPECTED_MIN_STOCKS = {
    'tw': 2500, 'us': 5684, 'cn': 5496, 'hk': 2689, 'jp': 4315, 'kr': 2000
}

# ========== Google Drive 服務函式 ==========

def get_drive_service():
    """初始化 Google Drive API 服務"""
    env_json = os.environ.get('GDRIVE_SERVICE_ACCOUNT')
    try:
        if env_json:
            info = json.loads(env_json)
            creds = service_account.Credentials.from_service_account_info(
                info, scopes=['https://www.googleapis.com/auth/drive']
            )
        elif os.path.exists(SERVICE_ACCOUNT_FILE):
            creds = service_account.Credentials.from_service_account_file(
                SERVICE_ACCOUNT_FILE, scopes=['https://www.googleapis.com/auth/drive']
            )
        else:
            print("❌ 找不到 Google 服務帳號憑證 (Env 或 JSON 檔案)")
            return None
        return build('drive', 'v3', credentials=creds, cache_discovery=False)
    except Exception as e:
        print(f"❌ Drive 服務初始化失敗: {e}")
        return None

def download_db_from_drive(service, file_name):
    """從雲端下載資料庫檔案"""
    if not GDRIVE_FOLDER_ID:
        print("❌ 錯誤：未設定 GDRIVE_FOLDER_ID")
        return False
        
    query = f"name = '{file_name}' and '{GDRIVE_FOLDER_ID}' in parents and trashed = false"
    try:
        results = service.files().list(q=query, fields="files(id)").execute()
        items = results.get('files', [])
        if not items:
            print(f"🔍 雲端尚未有資料庫檔案: {file_name}")
            return False
            
        file_id = items[0]['id']
        print(f"📡 從雲端下載資料庫: {file_name}")
        request = service.files().get_media(fileId=file_id)
        with io.FileIO(file_name, 'wb') as fh:
            downloader = MediaIoBaseDownload(fh, request, chunksize=5*1024*1024)
            done = False
            while not done:
                _, done = downloader.next_chunk()
        return True
    except Exception as e:
        print(f"⚠️ 下載失敗: {e}")
        return False

def upload_db_to_drive(service, file_path):
    """上傳資料庫檔案至雲端"""
    if not GDRIVE_FOLDER_ID:
        print("❌ 錯誤：未設定 GDRIVE_FOLDER_ID")
        return False

    file_name = os.path.basename(file_path)
    media = MediaFileUpload(file_path, mimetype='application/x-sqlite3', resumable=True)
    query = f"name = '{file_name}' and '{GDRIVE_FOLDER_ID}' in parents and trashed = false"
    
    try:
        results = service.files().list(q=query, fields="files(id)").execute()
        items = results.get('files', [])
        if items:
            # 檔案已存在，執行更新
            service.files().update(fileId=items[0]['id'], media_body=media).execute()
        else:
            # 檔案不存在，執行新建
            meta = {'name': file_name, 'parents': [GDRIVE_FOLDER_ID]}
            service.files().create(body=meta, media_body=media).execute()
        print(f"✅ 上傳成功: {file_name}")
        return True
    except Exception as e:
        print(f"⚠️ 上傳失敗: {e}")
        return False

def get_db_summary(db_path, market_id):
    """統計資料庫內的標的數量與最後日期"""
    if not os.path.exists(db_path): return None
    try:
        conn = sqlite3.connect(db_path)
        df_stats = pd.read_sql("SELECT COUNT(DISTINCT symbol) as s, MAX(date) as d2 FROM stock_prices", conn)
        conn.close()
        success_count = int(df_stats['s'][0]) if df_stats['s'][0] else 0
        latest_date = df_stats['d2'][0] if df_stats['d2'][0] else "N/A"
        expected = EXPECTED_MIN_STOCKS.get(market_id, 1)
        coverage = (success_count / expected) * 100
        return {
            "market": market_id.upper(), "expected": expected, "success": success_count,
            "coverage": f"{coverage:.1f}%", "end_date": latest_date,
            "status": "✅" if 80 <= coverage <= 120 else "⚠️"
        }
    except: return None

# ========== 主程式邏輯 ==========

def main():
    # 支援命令列參數，例如 python main.py tw
    target_market = sys.argv[1].lower() if len(sys.argv) > 1 else None
    module_map = {
        'tw': downloader_tw, 'us': downloader_us, 'cn': downloader_cn, 
        'hk': downloader_hk, 'jp': downloader_jp, 'kr': downloader_kr
    }
    
    markets_to_run = [target_market] if target_market in module_map else list(module_map.keys())
    
    # 啟動 Google Drive 服務
    service = get_drive_service()
    all_summaries = []

    for m in markets_to_run:
        db_file = f"{m}_stock_warehouse.db"
        print(f"\n--- 🌍 市場啟動: {m.upper()} ---")

        # 1. 嘗試同步雲端舊檔 (配合 GitHub Actions 快取)
        if service and not os.path.exists(db_file):
            download_db_from_drive(service, db_file)

        # 2. 執行數據下載
        target_module = module_map.get(m)
        print(f"🚀 正在下載/更新原始數據...")
        # 預設抓取 2024 至今的數據
        exec_results = target_module.run_sync(start_date="2024-01-01", end_date="2025-12-31")
        
        # 3. 執行特徵工程
        if process_market_data and os.path.exists(db_file):
            print(f"🧪 正在執行特徵工程 (技術指標 & 資料清洗)...")
            process_market_data(db_file)
        
        # 4. 生成摘要
        summary = get_db_summary(db_file, m)
        if summary:
            all_summaries.append(summary)
            print(f"📊 摘要: {summary['market']} | 涵蓋率: {summary['coverage']} | 最後日期: {summary['end_date']}")

        # 5. 優化與同步回雲端
        if service and os.path.exists(db_file):
            print(f"🧹 優化資料庫檔案並同步雲端...")
            try:
                conn = sqlite3.connect(db_file)
                conn.execute("VACUUM")
                conn.close()
                upload_db_to_drive(service, db_file)
            except Exception as e:
                print(f"❌ 同步雲端失敗: {e}")

    # 6. 發送報告 (如果有 notifier)
    if notifier and all_summaries:
        print("📨 正在發送 Email 報告...")
        try:
            notifier.send_stock_report_email(all_summaries)
        except Exception as e:
            print(f"❌ 報告發送失敗: {e}")

if __name__ == "__main__":
    main()
