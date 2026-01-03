# -*- coding: utf-8 -*-
import os, io, time, random, sqlite3, requests
import pandas as pd
import yfinance as yf
import FinanceDataReader as fdr
from datetime import datetime
from tqdm import tqdm

# ========== 1. 環境設定 ==========
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "kr_stock_warehouse.db")

def log(msg: str):
    print(f"{pd.Timestamp.now():%H:%M:%S}: {msg}", flush=True)

# ========== 2. KIND 產業資料抓取 ==========
def fetch_kind_industry_map():
    url = "http://kind.krx.co.kr/corpgeneral/corpList.do?method=download&searchType=13"
    log("📡 正在從 KIND 下載韓股權威產業對照表...")
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }

    try:
        r = requests.get(url, headers=headers, timeout=30)
        # 修正：read_html 有時需要指定解析器
        dfs = pd.read_html(io.BytesIO(r.content), flavor='bs4')
        if not dfs: return {}
        
        df = dfs[0]
        industry_map = {}
        for _, row in df.iterrows():
            code = str(row['종목코드']).strip().zfill(6)
            sector = str(row['업종']).strip()
            industry_map[code] = sector
        return industry_map
    except Exception as e:
        log(f"⚠️ KIND 抓取跳過 (不影響主流程): {e}")
        return {}

# ========== 3. 獲取韓股清單 (增加備援邏輯) ==========
def get_kr_stock_list():
    log("📡 正在獲取韓股清單...")
    items = []
    try:
        # 嘗試 A 計畫: FinanceDataReader
        df_fdr = fdr.StockListing('KRX')
        kind_map = fetch_kind_industry_map()

        conn = sqlite3.connect(DB_PATH)
        for _, row in df_fdr.iterrows():
            code = str(row['Code']).strip().zfill(6)
            market = str(row.get('Market', 'Unknown'))
            suffix = ".KS" if market == "KOSPI" else ".KQ"
            symbol = f"{code}{suffix}"
            name = str(row['Name']).strip()
            sector = kind_map.get(code, str(row.get('Sector', 'Other/Unknown')))

            conn.execute("""
                INSERT OR REPLACE INTO stock_info (symbol, name, sector, market, updated_at) 
                VALUES (?, ?, ?, ?, ?)
            """, (symbol, name, sector, market, datetime.now().strftime("%Y-%m-%d")))
            items.append((symbol, name))

        conn.commit()
        conn.close()
        log(f"✅ 韓股清單獲取成功: {len(items)} 檔")
    except Exception as e:
        log(f"❌ 清單獲取失敗: {e}")
        
        # 💡 備援計畫: 如果清單抓不到，嘗試從資料庫讀取現有的標的進行更新
        if os.path.exists(DB_PATH):
            log("🔄 嘗試從本地資料庫提取既有名單進行更新...")
            try:
                conn = sqlite3.connect(DB_PATH)
                existing = conn.execute("SELECT symbol, name FROM stock_info").fetchall()
                conn.close()
                items = existing
                log(f"✅ 從本地提取了 {len(items)} 檔標的")
            except:
                pass
    return items

# ========== 4. 下載核心 (保持原樣) ==========
def download_one_kr(symbol, start_date, end_date):
    max_retries = 1
    for attempt in range(max_retries + 1):
        try:
            df = yf.download(symbol, start=start_date, end=end_date, progress=False, 
                             auto_adjust=True, threads=False, timeout=20)
            if df is None or df.empty: return None
            if isinstance(df.columns, pd.MultiIndex): df.columns = df.columns.get_level_values(0)
            df.reset_index(inplace=True)
            df.columns = [c.lower() for c in df.columns]
            date_col = 'date' if 'date' in df.columns else df.columns[0]
            df['date_str'] = pd.to_datetime(df[date_col]).dt.tz_localize(None).dt.strftime('%Y-%m-%d')
            df_final = df[['date_str', 'open', 'high', 'low', 'close', 'volume']].copy()
            df_final.columns = ['date', 'open', 'high', 'low', 'close', 'volume']
            df_final['symbol'] = symbol
            return df_final
        except:
            time.sleep(2)
    return None

# ========== 5. 主流程 ==========
def run_sync(start_date="2024-01-01", end_date="2025-12-31"):
    start_time = time.time()
    if not os.path.exists(DB_PATH):
        # 僅在資料庫不存在時執行初始化
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
    
    pbar = tqdm(items, desc="KR同步")
    for symbol, name in pbar:
        df_res = download_one_kr(symbol, start_date, end_date)
        if df_res is not None:
            df_res.to_sql('stock_prices', conn, if_exists='append', index=False, 
                          method=lambda table, conn, keys, data_iter: 
                          conn.executemany(f"INSERT OR REPLACE INTO {table.name} ({', '.join(keys)}) VALUES ({', '.join(['?']*len(keys))})", data_iter))
            success_count += 1
        time.sleep(0.05)

    conn.commit()
    log("🧹 執行資料庫 VACUUM...")
    conn.execute("VACUUM")
    conn.close()
    
    log(f"📊 韓股同步完成 | 更新成功: {success_count} / {len(items)}")
    return {"success": success_count, "total": len(items), "has_changed": success_count > 0}

if __name__ == "__main__":
    run_sync(start_date="2024-01-01", end_date=datetime.now().strftime("%Y-%m-%d"))
