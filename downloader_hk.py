# -*- coding: utf-8 -*-
"""
downloader_hk.py
----------------
港股資料下載器（穩定單執行緒版）

✔ 支援日期連動：由 main.py 統一傳遞下載區間
✔ 強化判定邏輯：自動處理 4 位或 5 位代碼與 Yahoo Finance 格式
✔ 結構對齊：完全支援全局自動化連動機制
"""

import os, io, re, time, random, sqlite3, requests, urllib3
import pandas as pd
import yfinance as yf
from io import StringIO
from datetime import datetime
from tqdm import tqdm

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ========== 1. 環境設定 ==========
MARKET_CODE = "hk-share"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "hk_stock_warehouse.db")

def log(msg: str):
    print(f"{pd.Timestamp.now():%H:%M:%S}: {msg}", flush=True)

# ========== 2. 資料庫初始化 ==========
def init_db():
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS stock_prices (
                date TEXT, symbol TEXT, open REAL, high REAL, 
                low REAL, close REAL, volume INTEGER,
                PRIMARY KEY (date, symbol)
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS stock_info (
                symbol TEXT PRIMARY KEY, name TEXT, sector TEXT, 
                market TEXT, updated_at TEXT
            )
        """)
    finally:
        conn.close()

# ========== 3. HKEX 清單解析 ==========
def normalize_code_5d(val) -> str:
    digits = re.sub(r"\D", "", str(val))
    if digits.isdigit() and 1 <= int(digits) <= 99999:
        return digits.zfill(5)
    return ""

def get_hk_stock_list():
    url = (
        "https://www.hkex.com.hk/-/media/HKEX-Market/Services/Trading/"
        "Securities/Securities-Lists/"
        "Securities-Using-Standard-Transfer-Form-(including-GEM)-"
        "By-Stock-Code-Order/secstkorder.xls"
    )
    log("📡 正在從港交所下載最新股票清單...")

    try:
        r = requests.get(url, timeout=30, verify=False)
        r.raise_for_status()
        df_raw = pd.read_excel(io.BytesIO(r.content), header=None)
    except Exception as e:
        log(f"❌ 無法獲取 HKEX 清單: {e}")
        return []

    # 找表頭索引
    header_row = None
    for i in range(min(20, len(df_raw))):
        row_vals = [str(x).replace("\xa0", " ").strip() for x in df_raw.iloc[i].values]
        if any("Stock Code" in v for v in row_vals) and any("Short Name" in v for v in row_vals):
            header_row = i
            break

    if header_row is None:
        log("❌ 無法辨識 HKEX Excel 結構")
        return []

    df = df_raw.iloc[header_row + 1:].copy()
    df.columns = [str(x).replace("\xa0", " ").strip() for x in df_raw.iloc[header_row].values]

    code_col = next(c for c in df.columns if "Stock Code" in c)
    name_col = next(c for c in df.columns if "Short Name" in c)

    conn = sqlite3.connect(DB_PATH)
    stock_list = []

    for _, row in df.iterrows():
        code_5d = normalize_code_5d(row[code_col])
        if not code_5d: continue

        name = str(row[name_col]).strip()
        conn.execute("""
            INSERT OR REPLACE INTO stock_info (symbol, name, sector, market, updated_at)
            VALUES (?, ?, ?, ?, ?)
        """, (code_5d, name, "HK-Share", "HKEX", datetime.now().strftime("%Y-%m-%d")))
        stock_list.append((code_5d, name))

    conn.commit()
    conn.close()
    return stock_list

# ========== 4. 下載核心邏輯 (支援外部日期) ==========
def download_one_hk(code_5d, start_date, end_date):
    """
    下載特定港股，並支援 yfinance 不同代碼格式嘗試
    """
    # 港股代碼嘗試：yfinance 可能接受 0005.HK 或 5.HK
    possible_syms = [f"{code_
