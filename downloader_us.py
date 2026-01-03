# -*- coding: utf-8 -*-
"""
downloader_us.py
----------------
美股資料下載器（穩定單執行緒版）

✔ 支援外部日期參數：可由 main.py 指定下載區間
✔ 精準過濾：自動剔除 Warrant, ETF, Preferred 等衍生品
✔ 結構對齊：完全支援全局自動化連動機制
"""

import os, io, time, random, sqlite3, requests, re
import pandas as pd
import yfinance as yf
from io import StringIO
from datetime import datetime
from tqdm import tqdm

# ========== 1. 環境判斷與參數設定 ==========
MARKET_CODE = "us-share"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "us_stock_warehouse.db")

def log(msg: str):
    print(f"{pd.Timestamp.now():%H:%M:%S}: {msg}", flush=True)

# ========== 2. 資料庫初始化 ==========
def init_db():
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.execute('''CREATE TABLE IF NOT EXISTS stock_prices (
                            date TEXT, symbol TEXT, open REAL, high REAL, 
                            low REAL, close REAL, volume INTEGER,
                            PRIMARY KEY (date, symbol))''')
        conn.execute('''CREATE TABLE IF NOT EXISTS stock_info (
                            symbol TEXT PRIMARY KEY, name TEXT, sector TEXT, market TEXT, updated_at TEXT)''')
        
        cursor = conn.execute("PRAGMA table_info(stock_info)")
        columns = [column[1] for column in cursor.fetchall()]
        if 'market' not in columns:
            log("🔧 正在升級 US 資料庫結構：新增 'market' 欄位...")
            conn.execute("ALTER TABLE stock_info ADD COLUMN market TEXT")
            conn.commit()
    finally:
        conn.close()

# ========== 3. 獲取美股名單 (Nasdaq 官方 API) ==========
def get_us_stock_list_official():
    log("📡 正在從 Nasdaq 官方同步美股名單...")
    
    url = "https://api.nasdaq.com/api/screener/stocks?tableonly=true&limit=15000&download=true"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'application/json, text/plain, */*',
        'Referer': 'https://www.nasdaq.com/market-activity/stocks/screener'
    }

    try:
        r = requests.get(url, headers=headers, timeout=30)
        rows = r.json()['data']['rows']
        
        conn = sqlite3.connect(DB_PATH)
        stock_list = []
        exclude_kw = re.compile(r"Warrant|Right|Preferred|Unit|ETF|Index|Index-linked", re.I)

        for row in rows:
            symbol = str(row.get('symbol', '')).strip().upper()
            
            # 💡 核心過濾：排除衍生品
            if not symbol or not symbol.isalnum(): continue
            if len(symbol) > 4 and (symbol.endswith('R') or symbol.endswith('W') or symbol.endswith('U')):
                continue
            
            name = str(row.get('name', 'Unknown')).strip()
            if exclude_kw.search(name): continue
            
            sector = str(row.get('sector', 'Unknown')).strip()
            market = str(row.get('exchange', 'Unknown')).strip()
            
            if not sector or sector.lower() in ['nan', 'n/a', '']: sector = "Unknown"

            conn.execute("""
                INSERT OR REPLACE INTO stock_info (symbol, name, sector, market, updated_at) 
                VALUES (?, ?, ?, ?, ?)
            """, (symbol, name, sector, market, datetime.now().strftime("%Y-%m-%d")))
            stock_list.append((symbol, name))
            
        conn.commit()
        conn.close()
        log(f"✅ 美股清單導入成功: {len(stock_list)} 檔")
        return stock_list
    except Exception as e:
        log(f"❌ 獲取名單失敗: {e}")
        return []

# ========== 4. 下載核心 (支援傳入日期) ==========
def download_one_us(symbol, start_date, end_date):
    """
    從 Yahoo Finance 下載特定區間的資料
    """
    max_retries = 1
    
    for attempt in range(max_retries + 1):
        try:
            # 💡 使用從 run_sync 傳來的 start_date 與 end_date
            df = yf.download(symbol, start=start_date, end=end_date, progress=False, 
                             auto_adjust=True, threads=False, timeout=30)
            
            if df is None or df.empty:
                return None
            
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
        except Exception:
            if attempt < max_retries:
                time.sleep(3)
                continue
            return None

# ========== 5. 主流程 (對齊 main.py 的呼叫介面) ==========
def run_sync(start_date="2024-01-01", end_date="2025-12-31"):
    """
    接收 main.py 傳送來的日期參數
    """
    start_time = time.time()
    init_db()
    
    items = get_us_stock_list_official()
    if not items:
        return {"success": 0, "has_changed": False}

    log(f"🚀 開始美股同步 | 區間: {start_date} ~ {end_date} | 目標: {len(items)} 檔")

    success_count = 0
    conn = sqlite3.connect(DB_PATH, timeout=60)
    
    # 採用單執行緒循環下載
    pbar = tqdm(items, desc="US同步")
    for symbol, name in pbar:
        # 將日期參數傳遞給下載核心
        df_res = download_one_us(symbol, start_date, end_date)
        
        if df_res is not None:
            df_res.to_sql('stock_prices', conn, if_exists='append', index=False, 
                          method=lambda table, conn, keys, data_iter: 
                          conn.executemany(f"INSERT OR REPLACE INTO {table.name} ({', '.join(keys)}) VALUES ({', '.join(['?']*len(keys))})", data_iter))
            success_count += 1
            
        # 極小延遲，避免 API 頻率限制
        time.sleep(0.01)
    
    conn.commit()
    
    log("🧹 執行資料庫 VACUUM...")
    conn.execute("VACUUM")
    db_info_count = conn.execute("SELECT COUNT(DISTINCT symbol) FROM stock_info").fetchone()[0]
    conn.close()

    duration = (time.time() - start_time) / 60
    log(f"📊 同步完成！費時: {duration:.1f} 分鐘")
    log(f"✅ 更新成功: {success_count} / {len(items)}")
    
    return {
        "success": success_count,
        "total": db_info_count,
        "has_changed": success_count > 0
    }

if __name__ == "__main__":
    # 手動執行時預設下載近期資料
    run_sync(start_date="2024-01-01", end_date=datetime.now().strftime("%Y-%m-%d"))
