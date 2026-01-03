# -*- coding: utf-8 -*-
import sys, os
from processor import process_market_data
from main import get_drive_service, download_db_from_drive, upload_db_to_drive

def run_remote_process(market):
    db_file = f"{market}_stock_warehouse.db"
    service = get_gdrive_service() # 借用 main.py 的連線功能
    
    if service:
        # 1. 如果本地沒檔案(快取失效)，去 Google Drive 抓
        if not os.path.exists(db_file):
            print(f"📡 快取不存在，從雲端抓取 {db_file}...")
            download_db_from_drive(service, db_file)
        
        # 2. 執行特徵工程 (processor.py)
        if os.path.exists(db_file):
            print(f"🧪 開始對 {market.upper()} 執行資料清洗與特徵加工...")
            process_market_data(db_file)
            
            # 3. 加工完後，傳回雲端覆蓋舊檔
            print(f"📤 將加工後的資料庫傳回雲端...")
            upload_db_to_drive(service, db_file)
        else:
            print("❌ 雲端也找不到檔案，請確認市場代碼是否正確。")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        target_market = sys.argv[1].lower()
        run_remote_process(target_market)
    else:
        print("請帶入參數，例如: python only_feature.py tw")
