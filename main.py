# -*- coding: utf-8 -*-
import os, sys, sqlite3, json, time, socket, io
import pandas as pd
from datetime import datetime
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
from dotenv import load_dotenv

load_dotenv() 
socket.setdefaulttimeout(600)

# 💡 修正：優先讀取 GitHub Secret
GDRIVE_FOLDER_ID = os.environ.get('GDRIVE_FOLDER_ID', '1ltKCQ209k9MFuWV6FIxQ1coinV2fxSyl')
SERVICE_ACCOUNT_FILE = 'citric-biplane-319514-75fead53b0f5.json'

# 💡 修正：導入 process_market_data
try:
    from processor import process_market_data
except ImportError:
    print("⚠️ 找不到 processor.py，將跳過特徵工程。")
    process_market_data = None

try:
    from notifier import StockNotifier
    notifier = StockNotifier()
except Exception as e:
    print(f"❌ Notifier 初始化失敗: {e}")
    notifier = None

import downloader_tw, downloader_us, downloader_cn, downloader_hk, downloader_jp, downloader_kr

EXPECTED_MIN_STOCKS = {
    'tw': 2500, 'us': 5684, 'cn': 5496, 'hk': 2689, 'jp': 4315, 'kr': 2000
}

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
    query = f"name = '{file_name}' and '{GDRIVE_FOLDER_ID}' in parents and trashed = false"
    try:
        results = service.files().list(q=query, fields="files(id)").execute()
        items = results.get('files', [])
        if not items: return False
        file_id = items[0]['id']
        print(f"📡 下載資料庫: {file_name}")
        request = service.files().get_media(fileId=file_id)
        with io.FileIO(file_name, 'wb') as fh:
            downloader = MediaIoBaseDownload(fh, request, chunksize=5*1024*1024)
            done = False
            while not done:
                _, done = downloader.next_chunk()
        return True
    except: return False

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
        print(f"✅ 上傳成功: {file_name}")
        return True
    except: return False

def main():
    target_market = sys.argv[1].lower() if len(sys.argv) > 1 else None
    module_map = {'tw': downloader_tw, 'us': downloader_us, 'cn': downloader_cn, 'hk': downloader_hk, 'jp': downloader_jp, 'kr': downloader_kr}
    markets_to_run = [target_market] if target_market in module_map else list(module_map.keys())
    service = get_drive_service()
    all_summaries = []

    for m in markets_to_run:
        db_file = f"{m}_stock_warehouse.db"
        if service and not os.path.exists(db_file):
            download_db_from_drive(service, db_file)

        target_module = module_map.get(m)
        exec_results = target_module.run_sync(start_date="2024-01-01", end_date="2025-12-31")
        
        # 💡 執行特徵工程
        if process_market_data and exec_results.get('success', 0) > 0:
            print(f"🧪 正在執行特徵工程...")
            process_market_data(db_file)
        
        if service:
            conn = sqlite3.connect(db_file)
            conn.execute("VACUUM")
            conn.close()
            upload_db_to_drive(service, db_file)

if __name__ == "__main__":
    main()
