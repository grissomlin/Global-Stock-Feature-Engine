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

# ========== 3. 下載核心 (強化連線穩定度) ==========
def download_one_kr(symbol, start_date, end_date):
    # 韓國市場下載最怕 429 錯誤，這裡強制隨機等待
    time.sleep(random.uniform(0.1, 0.5))
    
    for attempt in range(2):
        try:
            df = yf.download(symbol, start=start_date, end=end_date, progress=False, 
                             auto_adjust=True, threads=False, timeout=30)
            if df is not None and not df.empty:
                if isinstance(df.columns, pd.MultiIndex):
                    df.columns = df.columns.get_level_values(0)
                df.reset_index(inplace=True)
                df.columns = [c.lower() for c in df.columns]
                date_col = 'date' if 'date' in df.columns else df.columns[0]
                df['date_str'] = pd.to_datetime(df[date_col]).dt.tz_localize(None).dt.strftime('%Y-%m-%d')
                
                df_final = df[['date_str', 'open', 'high', 'low', 'close', 'volume']].copy()
                df_final.columns = ['date', 'open', 'high', 'low', 'close', 'volume']
                df_final['symbol'] = symbol
                return df_final
        except Exception as e:
            log(f"  ❌ {symbol} 重試中 ({attempt+1}/2): {e}")
            time.sleep(2)
    return None

# ========== 4. 主流程 (增加空數據檢查) ==========
def run_sync(start_date="2024-01-01", end_date="2025-12-31"):
    start_time = time.time()
    
    # 初始化
    conn = sqlite3.connect(DB_PATH)
    conn.execute('CREATE TABLE IF NOT EXISTS stock_prices (date TEXT, symbol TEXT, open REAL, high REAL, low REAL, close REAL, volume INTEGER, PRIMARY KEY (date, symbol))')
    conn.execute('CREATE TABLE IF NOT EXISTS stock_info (symbol TEXT PRIMARY KEY, name TEXT, sector TEXT, market TEXT, updated_at TEXT)')
    conn.close()
    
    items = get_kr_stock_list()
    if not items:
        log("❌ 關鍵錯誤：所有清單獲取管道均失效，跳過韓股。")
        return {"success": 0, "has_changed": False}

    log(f"🚀 開始下載... (區間: {start_date} ~ {end_date})")

    success_count = 0
    conn = sqlite3.connect(DB_PATH, timeout=60)
    
    for item in tqdm(items, desc="KR同步"):
        symbol = item[0]
        df_res = download_one_kr(symbol, start_date, end_date)
        
        if df_res is not None:
            df_res.to_sql('stock_prices', conn, if_exists='append', index=False, 
                          method=lambda table, conn, keys, data_iter: 
                          conn.executemany(f"INSERT OR REPLACE INTO {table.name} ({', '.join(keys)}) VALUES ({', '.join(['?']*len(keys))})", data_iter))
            
            # 更新 Info 表 (確保下次失敗時能用)
            conn.execute("INSERT OR REPLACE INTO stock_info VALUES (?, ?, ?, ?, ?)", 
                         (symbol, item[1], item[2], item[3], datetime.now().strftime("%Y-%m-%d")))
            success_count += 1
            
        # 每 100 筆 commit 一次，增加效率與安全性
        if success_count % 100 == 0:
            conn.commit()

    conn.commit()
    log("🧹 資料庫 VACUUM...")
    conn.execute("VACUUM")
    conn.close()
    
    log(f"📊 同步完成！更新成功: {success_count} / {len(items)}")
    return {"success": success_count, "total": len(items), "has_changed": success_count > 0}

if __name__ == "__main__":
    run_sync()

