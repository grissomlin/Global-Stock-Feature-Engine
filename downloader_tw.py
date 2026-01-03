# -*- coding: utf-8 -*-
import os, io, time, random, sqlite3, requests
import pandas as pd
import yfinance as yf
from io import StringIO
from datetime import datetime, timedelta
from tqdm import tqdm

# ========== 1. 環境設定 ==========
MARKET_CODE = "tw-share"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "tw_stock_warehouse.db")

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
    finally:
        conn.close()

# 💡 新增：檢查資料庫中該標的最後一筆日期
def get_last_date(symbol, conn):
    try:
        query = "SELECT MAX(date) FROM stock_prices WHERE symbol = ?"
        res = conn.execute(query, (symbol,)).fetchone()
        return res[0] if res[0] else None
    except:
        return None

# ========== 3. 獲取台股清單 ==========
def get_tw_stock_list():
    url_configs = [
        {'name': 'listed', 'url': 'https://isin.twse.com.tw/isin/class_main.jsp?market=1&issuetype=1&Page=1&chklike=Y', 'suffix': '.TW'},
        {'name': 'dr', 'url': 'https://isin.twse.com.tw/isin/class_main.jsp?owncode=&stockname=&isincode=&market=1&issuetype=J&industry_code=&Page=1&chklike=Y', 'suffix': '.TW'},
        {'name': 'otc', 'url': 'https://isin.twse.com.tw/isin/class_main.jsp?market=2&issuetype=4&Page=1&chklike=Y', 'suffix': '.TWO'},
        {'name': 'etf', 'url': 'https://isin.twse.com.tw/isin/class_main.jsp?owncode=&stockname=&isincode=&market=1&issuetype=I&industry_code=&Page=1&chklike=Y', 'suffix': '.TW'},
        {'name': 'rotc', 'url': 'https://isin.twse.com.tw/isin/class_main.jsp?owncode=&stockname=&isincode=&market=E&issuetype=R&industry_code=&Page=1&chklike=Y', 'suffix': '.TWO'},
        {'name': 'tw_innovation', 'url': 'https://isin.twse.com.tw/isin/class_main.jsp?owncode=&stockname=&isincode=&market=C&issuetype=C&industry_code=&Page=1&chklike=Y', 'suffix': '.TW'},
        {'name': 'otc_innovation', 'url': 'https://isin.twse.com.tw/isin/class_main.jsp?owncode=&stockname=&isincode=&market=A&issuetype=C&industry_code=&Page=1&chklike=Y', 'suffix': '.TWO'},
    ]
    
    log(f"📡 獲取台股清單 (自動跳過權證分類)...")
    conn = sqlite3.connect(DB_PATH)
    stock_list = []
    
    for cfg in url_configs:
        try:
            resp = requests.get(cfg['url'], timeout=15)
            dfs = pd.read_html(StringIO(resp.text), header=0)
            if not dfs: continue
            df = dfs[0]
            
            for _, row in df.iterrows():
                code = str(row['有價證券代號']).strip()
                name = str(row['有價證券名稱']).strip()
                sector = str(row.get('產業別', 'Unknown')).strip()
                
                if code.isalnum() and len(code) >= 4:
                    symbol = f"{code}{cfg['suffix']}"
                    conn.execute("""
                        INSERT OR REPLACE INTO stock_info (symbol, name, sector, market, updated_at) 
                        VALUES (?, ?, ?, ?, ?)
                    """, (symbol, name, sector, cfg['name'], datetime.now().strftime("%Y-%m-%d")))
                    stock_list.append((symbol, name))
        except Exception as e:
            log(f"⚠️ {cfg['name']} 獲取失敗: {e}")
            
    conn.commit()
    conn.close()
    return list(set(stock_list))

# ========== 4. 下載邏輯 (支援增量更新) ==========
def download_one_stable(symbol, start_date, end_date):
    try:
        # yfinance 的 end_date 是不包含的，所以若要抓到今天，end_date 建議設為明天
        df = yf.download(symbol, start=start_date, end=end_date, progress=False, timeout=20, 
                         auto_adjust=True, threads=False)
        if df is None or df.empty: return None
        
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        
        df.reset_index(inplace=True)
        df.columns = [c.lower() for c in df.columns]
        df['date'] = pd.to_datetime(df['date']).dt.tz_localize(None).dt.strftime('%Y-%m-%d')
        
        df_final = df[['date', 'open', 'high', 'low', 'close', 'volume']].copy()
        df_final['symbol'] = symbol
        return df_final
    except:
        return None

# ========== 5. 主流程 (具備快取檢查機制) ==========
def run_sync(start_date="2024-01-01", end_date="2025-12-31"):
    start_time = time.time()
    init_db()
    
    items = get_tw_stock_list()
    if not items:
        log("❌ 無法獲取股票清單")
        return {"success": 0, "total": 0}

    log(f"🚀 開始同步 TW | 區間: {start_date} ~ {end_date}")

    success_count = 0
    skip_count = 0
    conn = sqlite3.connect(DB_PATH, timeout=60)
    
    pbar = tqdm(items, desc="TW增量同步")
    for symbol, name in pbar:
        # 💡 核心快取檢查邏輯
        last_date_in_db = get_last_date(symbol, conn)
        
        actual_start = start_date
        if last_date_in_db:
            # 如果資料庫已有資料，計算下一天
            next_day = (pd.to_datetime(last_date_in_db) + timedelta(days=1)).strftime('%Y-%m-%d')
            
            # 如果下一天已經超過了我們要抓的 end_date，就直接跳過
            if last_date_in_db >= end_date:
                skip_count += 1
                continue
            actual_start = next_day

        df_res = download_one_stable(symbol, actual_start, end_date)
        
        if df_res is not None and not df_res.empty:
            df_res.to_sql('stock_prices', conn, if_exists='append', index=False, 
                          method=lambda table, conn, keys, data_iter: 
                          conn.executemany(f"INSERT OR REPLACE INTO {table.name} ({', '.join(keys)}) VALUES ({', '.join(['?']*len(keys))})", data_iter))
            success_count += 1
        
        # 稍微等待避免 Yahoo 封鎖
        time.sleep(0.05)
    
    conn.commit()
    log(f"🧹 優化資料庫 (VACUUM)...")
    conn.execute("VACUUM")
    conn.close()

    duration = (time.time() - start_time) / 60
    log(f"📊 同步完成！更新: {success_count} 檔 | 跳過: {skip_count} 檔 | 耗時: {duration:.1f} 分鐘")
    
    return {"success": success_count, "total": len(items)}

if __name__ == "__main__":
    run_sync()
