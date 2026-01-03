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

# 💡 修正點 1：從環境變數讀取資料夾 ID，若無則使用預設值
GDRIVE_FOLDER_ID = os.environ.get('GDRIVE_FOLDER_ID', '1ltKCQ209k9MFuWV6FIxQ1coinV2fxSyl')
SERVICE_ACCOUNT_FILE = 'citric-biplane-319514-75fead53b0f5.json'

# 💡 修正點 2：正確導入 processor 函式 (假設 processor.py 在根目錄)
try:
    from processor import process_market_data
except ImportError:
    print("⚠️ 找不到 processor.py 中的 process_market_data，將跳過特徵工程。")
    process_market_data = None

try:
    from notifier import StockNotifier
    notifier = StockNotifier()
except Exception as e:
    print(f"❌ Notifier 初始化失敗: {e}")
    notifier = None

import downloader_tw, downloader_us, downloader_cn, downloader_hk, downloader_jp, downloader_kr

# 📊 門檻門檻
EXPECTED_MIN_STOCKS = {
    'tw': 2500, 'us': 5684, 'cn': 5496, 'hk': 2689, 'jp': 4315, 'kr': 2000
}

# ========== Google Drive 服務 ==========
def get_drive_service():
    env_json = os.environ.get('GDRIVE_SERVICE_ACCOUNT')
    try:
        if env_json:
            info = json.loads(env_json)
            creds = service_account.Credentials.from_service_account_info(info, scopes=['https://www.googleapis.com/auth/drive'])
        elif os.path.exists(SERVICE_ACCOUNT_FILE):
            creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=['https://www.googleapis.com/auth/drive'])
        else:
            return None
        return build('drive', 'v3', credentials=creds, cache_discovery=False)
    except Exception as e:
        print(f"❌ 無法初始化 Drive 服務: {e}")
        return None

def download_db_from_drive(service, file_name):
    # 💡 這裡會自動使用上面定義的變數 GDRIVE_FOLDER_ID
    query = f"name = '{file_name}' and '{GDRIVE_FOLDER_ID}' in parents and trashed = false"
    try:
        results = service.files().list(q=query, fields="files(id)").execute()
        items = results.get('files', [])
        if not items: return False
        file_id = items[0]['id']
        print(f"📡 正在從雲端下載數據庫: {file_name}")
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
    file_name = os.path.basename(file_path)
    media = MediaFileUpload(file_path, mimetype='application/x-sqlite3', resumable=True)
    query = f"name = '{file_name}' and '{GDRIVE_FOLDER_ID}' in parents and trashed = false"
    try:
        results = service.files().list(q=query, fields="files(id)").execute()
        items = results.get('files', [])
        if items:
            service.files().update(fileId=items[0]['id'], media_body=media).execute()
        else:
            meta = {'name': file_name, 'parents': [GDRIVE_FOLDER_ID]}
            service.files().create(body=meta, media_body=media).execute()
        print(f"✅ 上傳成功: {file_name} ({os.path.getsize(file_path)/1024/1024:.2f} MB)")
        return True
    except Exception as e:
        print(f"⚠️ 上傳失敗: {e}")
        return False

# ========== 核心邏輯 ==========
def get_db_summary(db_path, market_id):
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

def main():
    target_market = sys.argv[1].lower() if len(sys.argv) > 1 else None
    
    START_DATE = "2024-01-01"
    END_DATE = "2025-12-31"

    module_map = {
        'tw': downloader_tw, 'us': downloader_us, 'cn': downloader_cn,
        'hk': downloader_hk, 'jp': downloader_jp, 'kr': downloader_kr
    }
    
    markets_to_run = [target_market] if target_market in module_map else list(module_map.keys())
    service = get_drive_service()
    all_summaries = []

    for m in markets_to_run:
        db_file = f"{m}_stock_warehouse.db"
        print(f"\n--- 🌍 市場啟動: {m.upper()} ---")

        if service and not os.path.exists(db_file):
            download_db_from_drive(service, db_file)

        target_module = module_map.get(m)
        print(f"🚀 正在下載原始數據...")
        exec_results = target_module.run_sync(start_date=START_DATE, end_date=END_DATE)
        
        # 💡 修正點 3：使用正確導入的 process_market_data 函式
        if process_market_data and exec_results.get('success', 0) > 0:
            print(f"🧪 正在執行特徵工程 (技術指標 & 未來報酬)...")
            process_market_data(db_file)
        
        summary = get_db_summary(db_file, m)
        if summary:
            all_summaries.append(summary)

        if service:
            print(f"🧹 正在優化資料庫並同步至雲端...")
            try:
                conn = sqlite3.connect(db_file)
                conn.execute("VACUUM")
                conn.close()
                upload_db_to_drive(service, db_file)
            except Exception as e:
                print(f"❌ 同步失敗: {e}")

    if notifier and all_summaries:
        notifier.send_stock_report_email(all_summaries)

if __name__ == "__main__":
    main()
