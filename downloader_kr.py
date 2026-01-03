# -*- coding: utf-8 -*-
import os, io, time, random, sqlite3, requests, logging
import pandas as pd
import yfinance as yf
from datetime import datetime
from tqdm import tqdm

# ========== 1. 環境設定 ==========
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "kr_stock_warehouse.db")
# 💡 定義清單路徑 (假設 main.py 會下載到同一個目錄)
LIST_CSV_PATH = os.path.join(BASE_DIR, "kr_list_all.csv")

def log(msg: str):
    print(f"{pd.Timestamp.now():%H:%M:%S}: {msg}", flush=True)

logging.getLogger('yfinance').setLevel(logging.CRITICAL)

# ========== 2. 獲取韓股清單 (四重保險機制) ==========
def get_kr_stock_list():
    log("📡 啟動韓股清單獲取任務...")
    items = []

    # --- 🛡️ 保險 0：優先嘗試本地 CSV (Colab 產出的那份) ---
    if os.path.exists(LIST_CSV_PATH):
        try:
            log(f"📁 [保險 0] 偵測到雲端同步清單 {LIST_CSV_PATH}，讀取中...")
            df_list = pd.read_csv(LIST_CSV_PATH)
            # 根據您 Colab 產出的格式 (code, name, board) 進行轉換
            # board: KS -> .KS (KOSPI), KQ -> .KQ (KOSDAQ)
            for _, row in df_list.iterrows():
                code = str(row['code']).zfill(6)
                board = str(row['board']).upper()
                symbol = f"{code}.KS" if board == "KS" else f"{code}.KQ"
                market = "KOSPI" if board == "KS" else "KOSDAQ"
                # 格式: (symbol, name, sector, market)
                items.append((symbol, row['name'], "Stock", market))
            
            if items:
                log(f"✅ 從 CSV 成功載入 {len(items)} 檔名單")
                return items
        except Exception as e:
            log(f"⚠️ 讀取 CSV 清單失敗: {e}")

    # --- 保險 1：嘗試 pykrx (官方對接) ---
    try:
        from pykrx import stock as krx
        log("🔍 [保險 1] 嘗試透過 pykrx 獲取即時清單...")
        today = datetime.now().strftime("%Y%m%d")
        for mk, suffix in [("KOSPI", ".KS"), ("KOSDAQ", ".KQ")]:
            tickers = krx.get_market_ticker_list(today, market=mk)
            for t in tickers:
                code = str(t).strip().zfill(6)
                name = krx.get_market_ticker_name(t)
                items.append((f"{code}{suffix}", name, "Stock", mk))
        if items:
            log(f"✅ pykrx 獲取成功: {len(items)} 檔")
            return items
    except Exception as e:
        log(f"⚠️ pykrx 失敗 (通常是 GitHub IP 被封): {e}")

    # --- 保險 2：從資料庫讀取既有名單 (Resume 模式) ---
    if os.path.exists(DB_PATH):
        log("🔍 [保險 2] 嘗試從本地資料庫讀取既有名單...")
        try:
            conn = sqlite3.connect(DB_PATH)
            db_items = conn.execute("SELECT symbol, name, sector, market FROM stock_info").fetchall()
            conn.close()
            if db_items:
                log(f"✅ 從資料庫恢復了 {len(db_items)} 檔名單")
                return db_items
        except:
            pass

    # --- 保險 3：嘗試 Yahoo Finance 常用權值股 (最後保底) ---
    if not items:
        log("🔍 [保險 3] 嘗試最後保底名單 (權值股)...")
        items = [
            ("005930.KS", "Samsung Electronics", "Stock", "KOSPI"),
            ("000660.KS", "SK Hynix", "Stock", "KOSPI"),
            ("035420.KQ", "NAVER", "Stock", "KOSDAQ")
        ]

    return items

# ========== 3. 下載核心與 4. 主流程 (保持原樣即可) ==========
# ... [其餘代碼不變] ...
