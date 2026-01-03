# -*- coding: utf-8 -*-
import os, io, time, random, sqlite3, requests, logging
import pandas as pd
import yfinance as yf
from datetime import datetime
from tqdm import tqdm

# ========== 1. 環境設定 ==========
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "kr_stock_warehouse.db")

def log(msg: str):
    print(f"{pd.Timestamp.now():%H:%M:%S}: {msg}", flush=True)

# 降噪：避免 yfinance 在下載時印出過多不必要的錯誤資訊
logging.getLogger('yfinance').setLevel(logging.CRITICAL)

# ========== 2. KIND 產業資料抓取 (選配) ==========
def fetch_kind_industry_map():
    url = "http://kind.krx.co.kr/corpgeneral/corpList.do?method=download&searchType=13"
    log("📡 正在從 KIND 下載韓股權威產業對照表...")
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }
    try:
        r = requests.get(url, headers=headers, timeout=20)
        # 韓國 KIND 網站有時會阻擋特定 IP，這裡加入 try-except
        dfs = pd.read_html(io.BytesIO(r.content), flavor='bs4')
        if not dfs: return {}
        df = dfs[0]
        industry_map = {str(row['종목코드']).strip().zfill(6): str(row['업종']).strip() for _, row in df.iterrows()}
        return industry_map
    except Exception as e:
        log(f"⚠️ KIND 抓取跳過 (將使用預設分類): {e}")
        return {}

# ========== 3. 獲取韓股清單 (採用 pykrx 作為核心) ==========
def get_kr_stock_list():
    """
    結合 pykrx 與 KIND 獲取最完整的清單。
    如果失敗，會嘗試從現有資料庫獲取舊名單。
    """
    log("📡 正在透過 pykrx 獲取最新韓股清單...")
    items = []
    try:
        from pykrx import stock as krx
        today = datetime.now().strftime("%Y%m%d")
        
        # 獲取 KOSPI 與 KOSDAQ 的代碼
        kind_map = fetch_kind_industry_map()
        
        for mk, suffix in [("KOSPI", ".KS"), ("KOSDAQ", ".KQ")]:
            tickers = krx.get_market_ticker_list(today, market=mk)
            for t in tickers:
                code = str(t).strip().zfill(6)
                name = krx.get_market_ticker_name(t)
                symbol = f"{code}{suffix}"
                sector = kind_map.get(code, "Other/Unknown")
                items.append((symbol, name, sector, mk))
        
        # 將清單更新到資料庫的 info 表
        conn = sqlite3.connect(DB_PATH)
        for sym, nm, sec, mk in items:
            conn.execute("""
                INSERT OR REPLACE INTO stock_info (symbol, name, sector, market, updated_at) 
                VALUES (?, ?, ?, ?, ?)
            """, (sym, nm, sec, mk, datetime.now().strftime("%Y-%m-%d")))
        conn.commit()
        conn.close()
        log(f"✅ 韓股清單整合成功: 共 {len(items)} 檔")
        
    except Exception as e:
        log(f"❌ pykrx 獲取清單失敗: {e}")
        # 備援：從本地資料庫提取
        if os.path.exists(DB_PATH):
            log("🔄 嘗試從本地資料庫提取既有名單進行更新...")
            try:
                conn = sqlite3.connect(DB_PATH)
                items = conn.execute("SELECT symbol, name, sector, market FROM stock_info").fetchall()
                conn.close()
                log(f"✅ 從本地提取了 {len(items)} 檔標的")
            except: pass
            
    return items

# ========== 4. 下載核心 (單執行緒穩定版) ==========
def download_one_kr(symbol, start_date, end_date):
    max_retries = 1
    for attempt in range(max_retries + 1):
        try:
            # interval="1d" 並關閉 threads 避免記憶體衝突
            df = yf.download(symbol, start=start_date, end=end_date, progress=False, 
                             auto_adjust=True, threads=False, timeout=20)
            
            if df is None or df.empty: return None
            
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)
                
            df.reset_index(inplace=True)
            df.columns = [c.lower() for c in df.columns]
            
            # 取得日期並統一格式
            date_col = 'date' if 'date' in df.columns else df.columns[0]
            df['date_str'] = pd.to_datetime(df[date_col]).dt.tz_localize(None).dt.strftime('%Y-%m-%d')
            
            # 過濾量為 0 的日子 (代表停牌或無交易)
            df = df[df['volume'] > 0]
            
            df_final = df[['date_str', 'open', 'high', 'low', 'close', 'volume']].copy()
            df_final.columns = ['date', 'open', 'high', 'low', 'close', 'volume']
            df_final['symbol'] = symbol
            return df_final
        except Exception:
            if attempt < max_retries: time.sleep(random.uniform(1, 3))
    return None

# ========== 5. 主流程 (對齊 main.py) ==========
def run_sync(start_date="2024-01-01", end_date="2025-12-31"):
    start_time = time.time()
    
    # 初始化資料庫
    conn = sqlite3.connect(DB_PATH)
    conn.execute('''CREATE TABLE IF NOT EXISTS stock_prices (
                        date TEXT, symbol TEXT, open REAL, high REAL, 
                        low REAL, close REAL, volume INTEGER,
                        PRIMARY KEY (date, symbol))''')
    conn.execute('''CREATE TABLE IF NOT EXISTS stock_info (
                        symbol TEXT PRIMARY KEY, name TEXT, sector TEXT, market TEXT, updated_at TEXT)''')
    conn.close()
    
    items = get_kr_stock_list()
    if not items:
        log("⚠️ 無法獲取名單且資料庫無舊檔，跳過本次同步。")
        return {"success": 0, "has_changed": False}

    log(f"🚀 開始韓股同步 | 區間: {start_date} ~ {end_date} | 目標: {len(items)} 檔")

    success_count = 0
    conn = sqlite3.connect(DB_PATH, timeout=60)
    
    # 執行下載
    for item in tqdm(items, desc="KR同步"):
        # 由於 item 可能是 tuple (來自 DB) 或 list，統一處理
        symbol = item[0]
        name = item[1]
        
        df_res = download_one_kr(symbol, start_date, end_date)
        if df_res is not None:
            # 執行 Upsert (Insert or Replace)
            df_res.to_sql('stock_prices', conn, if_exists='append', index=False, 
                          method=lambda table, conn, keys, data_iter: 
                          conn.executemany(f"INSERT OR REPLACE INTO {table.name} ({', '.join(keys)}) VALUES ({', '.join(['?']*len(keys))})", data_iter))
            success_count += 1
        
        # 韓股下載稍微加一點點延遲，避免 yf 被節流
        time.sleep(random.uniform(0.01, 0.05))

    conn.commit()
    log("🧹 執行資料庫 VACUUM...")
    conn.execute("VACUUM")
    conn.close()
    
    duration = (time.time() - start_time) / 60
    log(f"📊 韓股同步完成 | 更新成功: {success_count} / {len(items)} | 耗時: {duration:.1f} 分鐘")
    
    return {"success": success_count, "total": len(items), "has_changed": success_count > 0}

if __name__ == "__main__":
    run_sync()
