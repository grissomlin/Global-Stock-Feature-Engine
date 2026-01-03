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

# 💡 環境變數讀取
GDRIVE_FOLDER_ID = os.environ.get('GDRIVE_FOLDER_ID')
SERVICE_ACCOUNT_FILE = 'citric-biplane-319514-75fead53b0f5.json'

# 💡 導入模組
try:
    from processor import process_market_data
except ImportError:
    print("⚠️ 系統提示：找不到 processor.py，將跳過特徵工程。")
    process_market_data = None

try:
    from notifier import StockNotifier
    notifier = StockNotifier()
except Exception as e:
    print(f"⚠️ 系統提示：Notifier 初始化跳過 (原因: {e})")
    notifier = None

import downloader_tw, downloader_us, downloader_cn, downloader_hk, downloader_jp, downloader_kr

# 📊 預期數量監控
EXPECTED_MIN_STOCKS = {
    'tw': 2500, 'us': 5684, 'cn': 5496, 'hk': 2689, 'jp': 4315, 'kr': 2000
}

# ========== Google Drive 服務函式 ==========

def get_drive_service():
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
            return None
        return build('drive', 'v3', credentials=creds, cache_discovery=False)
    except Exception as e:
        print(f"❌ Drive 服務初始化失敗: {e}")
        return None

def download_db_from_drive(service, file_name):
    if not GDRIVE_FOLDER_ID: return False
    query = f"name = '{file_name}' and '{GDRIVE_FOLDER_ID}' in parents and trashed = false"
    try:
        results = service.files().list(q=query, fields="files(id)").execute()
        items = results.get('files', [])
        if not items: return False
        file_id = items[0]['id']
        print(f"📡 從雲端下載資料庫: {file_name}")
        request = service.files().get_media(fileId=file_id)
        with io.FileIO(file_name, 'wb') as fh:
            downloader = MediaIoBaseDownload(fh, request, chunksize=5*1024*1024)
            done = False
            while not done: _, done = downloader.next_chunk()
        return True
    except: return False

def upload_to_drive(service, file_path, mimetype='application/x-sqlite3'):
    """通用上傳函式，支援更新與新建"""
    if not GDRIVE_FOLDER_ID or not os.path.exists(file_path): return False
    file_name = os.path.basename(file_path)
    media = MediaFileUpload(file_path, mimetype=mimetype, resumable=True)
    query = f"name = '{file_name}' and '{GDRIVE_FOLDER_ID}' in parents and trashed = false"
    try:
        results = service.files().list(q=query, fields="files(id)").execute()
        items = results.get('files', [])
        if items:
            service.files().update(fileId=items[0]['id'], media_body=media).execute()
        else:
            meta = {'name': file_name, 'parents': [GDRIVE_FOLDER_ID]}
            service.files().create(body=meta, media_body=media).execute()
        print(f"✅ 上傳成功: {file_name}")
        return True
    except Exception as e:
        print(f"⚠️ {file_name} 上傳失敗: {e}")
        return False

def get_db_summary(db_path, market_id):
    if not os.path.exists(db_path): return None
    try:
        conn = sqlite3.connect(db_path)
        df_stats = pd.read_sql("SELECT COUNT(DISTINCT symbol) as s, MAX(date) as d2 FROM stock_prices", conn)
        conn.close()
        success_count = int(df_stats['s'][0]) if df_stats['s'][0] else 0
        latest_date = str(df_stats['d2'][0]) if df_stats['d2'][0] else "N/A"
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
    target_market = sys.argv[1].lower() if len(sys.argv) > 1 else None
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
        print(f"🚀 正在下載/更新原始數據...")
        target_module.run_sync(start_date="2024-01-01", end_date="2025-12-31")
        
        if process_market_data and os.path.exists(db_file):
            print(f"🧪 正在執行特徵工程...")
            process_market_data(db_file)
        
        summary = get_db_summary(db_file, m)
        if summary:
            all_summaries.append(summary)
            print(f"📊 摘要: {summary['market']} | 涵蓋率: {summary['coverage']} | 最後日期: {summary['end_date']}")
            # 💡 新增：生成該市場的獨立摘要文件
            market_summary_file = f"summary_{m}.json"
            with open(market_summary_file, "w", encoding="utf-8") as f:
                json.dump(summary, f, ensure_ascii=False, indent=4)
            print(f"📝 已生成市場摘要文件: {market_summary_file}")
        if service and os.path.exists(db_file):
            print(f"🧹 優化並同步雲端...")
            try:
                conn = sqlite3.connect(db_file)
                conn.execute("VACUUM")
                conn.close()
                upload_to_drive(service, db_file)
            except Exception as e:
                print(f"❌ 雲端同步失敗: {e}")

    # --- 🌍 新增：全球特徵摘要 JSON 生成與上傳 ---
    if all_summaries:
        print("\n🌍 正在生成全球市場特徵摘要...")
        if len(markets_to_run) > 1:
            json_file = "global_summary.json"
            with open(json_file, "w", encoding="utf-8") as f:
                json.dump(all_summaries, f, ensure_ascii=False, indent=4)
            
            if service:
                print(f"📡 正在同步 {json_file} 至雲端...")
                upload_to_drive(service, json_file, mimetype='application/json')

    # 6. 發送報告
    if notifier and all_summaries:
        print("📨 正在發送 Email 報告...")
        try:
            notifier.send_stock_report_email(all_summaries)
        except Exception as e:
            print(f"❌ 報告發送失敗: {e}")

if __name__ == "__main__":
    main()

