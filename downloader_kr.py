# -*- coding: utf-8 -*-
import os, io, time, random, sqlite3, requests, logging
import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

# ========== 1. 環境設定 ==========
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "kr_stock_warehouse.db")
LIST_CSV_PATH = os.path.join(BASE_DIR, "kr_list_all.csv")

def log(msg: str):
    print(f"{pd.Timestamp.now():%H:%M:%S}: {msg}", flush=True)

logging.getLogger('yfinance').setLevel(logging.CRITICAL)

# ========== 2. 工具函式 ==========
def init_db():
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.execute('''CREATE TABLE IF NOT EXISTS stock_prices (
                            date TEXT, symbol TEXT, open REAL, high REAL, 
                            low REAL, close REAL, volume INTEGER,
                            PRIMARY KEY (date, symbol))''')
        conn.execute('''CREATE TABLE IF NOT EXISTS stock_info (
                            symbol TEXT PRIMARY KEY, name TEXT, sector TEXT, market TEXT, updated_at TEXT)''')
    finally:
        conn.close()

def get_last_date(symbol, conn):
    try:
        res = conn.execute("SELECT MAX(date) FROM stock_prices WHERE symbol = ?", (symbol,)).fetchone()
        return res[0] if res[0] else None
    except:
        return None

# ========== 3. 獲取韓股清單 (四重保險) ==========
def get_kr_stock_list():
    items = []
    if os.path.exists(LIST_CSV_PATH):
        try:
            log(f"📁 [保險 0] 讀取本地清單 {LIST_CSV_PATH}...")
            df_list = pd.read_csv(LIST_CSV_PATH)
            for _, row in df_list.iterrows():
                code = str(row['code']).zfill(6)
                board = str(row['board']).upper()
                symbol = f"{code}.KS" if board == "KS" else f"{code}.KQ"
                items.append((symbol, row['name'], "Stock", "KOSPI" if board == "KS" else "KOSDAQ"))
            if items: return items
        except: pass

    try:
        from pykrx import stock as krx
        today = datetime.now().strftime("%Y%m%d")
        for mk, suffix in [("KOSPI", ".KS"), ("KOSDAQ", ".KQ")]:
            tickers = krx.get_market_ticker_list(today, market=mk)
            for t in tickers:
                items.append((f"{t}{suffix}", krx.get_market_ticker_name(t), "Stock", mk))
        return items
    except:
        return [("005930.KS", "Samsung Electronics", "Stock", "KOSPI")]

# ========== 4. 下載單元 ==========
def download_single_kr(item, start_date, end_date):
    symbol, name, sector, market = item
    conn = sqlite3.connect(DB_PATH, timeout=30)
    last_date = get_last_date(symbol, conn)
    conn.close()
    
    actual_start = start_date
    if last_date:
        if last_date >= end_date: return "skipped", None
        actual_start = (pd.to_datetime(last_date) + timedelta(days=1)).strftime('%Y-%m-%d')

    time.sleep(random.uniform(0.1, 0.3))
    try:
        df = yf.download(symbol, start=actual_start, end=end_date, progress=False, auto_adjust=True, threads=False)
        if df is None or df.empty: return "no_data", None
        if isinstance(df.columns, pd.MultiIndex): df.columns = df.columns.get_level_values(0)
        df.reset_index(inplace=True)
        df.columns = [c.lower() for c in df.columns]
        date_col = 'date' if 'date' in df.columns else df.columns[0]
        df['date_str'] = pd.to_datetime(df[date_col]).dt.tz_localize(None).dt.strftime('%Y-%m-%d')
        df_final = df[['date_str', 'open', 'high', 'low', 'close', 'volume']].copy()
        df_final.columns = ['date', 'open', 'high', 'low', 'close', 'volume']
        df_final['symbol'] = symbol
        return "success", df_final
    except:
        return "error", None

# ========== 5. 核心執行函式 (必須叫 run_sync) ==========
def run_sync(start_date="2024-01-01", end_date="2026-01-04", max_workers=5):
    """
    這是 main.py 調用的入口點
    """
    start_time = time.time()
    init_db()
    
    items = get_kr_stock_list()
    if not items:
        log("❌ 無法獲取韓股清單")
        return {"success": 0, "total": 0}

    log(f"🚀 開始 KR 同步 | 線程: {max_workers} | 目標: {len(items)} 檔")

    success_count = 0
    skip_count = 0
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(download_single_kr, item, start_date, end_date): item for item in items}
        conn = sqlite3.connect(DB_PATH, timeout=60)
        
        for future in tqdm(as_completed(futures), total=len(items), desc="KR同步"):
            status, df_res = future.result()
            item_info = futures[future]
            
            if status == "skipped":
                skip_count += 1
            elif status == "success" and df_res is not None:
                df_res.to_sql('stock_prices', conn, if_exists='append', index=False, 
                              method=lambda table, conn, keys, data_iter: 
                              conn.executemany(f"INSERT OR REPLACE INTO {table.name} ({', '.join(keys)}) VALUES ({', '.join(['?']*len(keys))})", data_iter))
                conn.execute("INSERT OR REPLACE INTO stock_info VALUES (?, ?, ?, ?, ?)", 
                             (item_info[0], item_info[1], item_info[2], item_info[3], datetime.now().strftime("%Y-%m-%d")))
                success_count += 1
            
            if (success_count + skip_count) % 100 == 0:
                conn.commit()

        conn.commit()
        log("🧹 資料庫 VACUUM...")
        conn.execute("VACUUM")
        conn.close()

    log(f"📊 KR 完成！成功: {success_count} | 跳過: {skip_count} | 耗時: {(time.time()-start_time)/60:.1f} 分")
    return {"success": success_count, "total": len(items)}

if __name__ == "__main__":
    run_sync()
