# -*- coding: utf-8 -*-
import sqlite3
import pandas as pd
import pandas_ta as ta
import numpy as np
from datetime import datetime

def process_market_data(db_path):
    """
    量化特徵工程核心：包含異常清洗、市場感知、指標計算與未來報酬標籤
    """
    conn = sqlite3.connect(db_path)
    
    # 1. 讀取價格數據並關聯市場分類 (確保從 stock_info 取得 market 欄位)
    query = """
        SELECT p.*, i.market, i.name
        FROM stock_prices p
        LEFT JOIN stock_info i ON p.symbol = i.symbol
    """
    df = pd.read_sql(query, conn)
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values(['symbol', 'date'])

    # --- 2. 數據清洗：市場感知漲跌幅過濾 ---
    print(f"📡 正在對 {db_path} 執行市場感知數據清洗...")
    df['daily_return'] = df.groupby('symbol')['close'].pct_change()

    def check_anomaly(row):
        # 如果是空值則跳過
        if pd.isna(row['daily_return']): return False
        ret = abs(row['daily_return'])
        
        # 上市櫃 (listed/otc/dr) 嚴格執行 11% 過濾 (考量除息微調)
        if row['market'] in ['listed', 'otc', 'dr', 'tw_innovation']:
            return ret > 0.11
        # ETF 槓桿波動較大，設為 20%
        if row['market'] == 'etf':
            return ret > 0.20
        # 興櫃 (rotc) 無限制，但超過 100% 仍視為異常或需核實數據
        if row['market'] == 'rotc':
            return ret > 1.00
        return False

    df['is_anomaly'] = df.apply(check_anomaly, axis=1)
    bad_symbols = df[df['is_anomaly'] == True]['symbol'].unique()
    
    if len(bad_symbols) > 0:
        print(f"🛑 剔除異常變動標的 (共 {len(bad_symbols)} 檔): {list(bad_symbols)}")
        df = df[~df['symbol'].isin(bad_symbols)]

    # --- 3. 數據驗證：年度價格對帳 ---
    print("🕵️ 執行年度價格對帳 (年度報酬合理性檢查)...")
    df['year'] = df['date'].dt.year
    for yr in df['year'].unique():
        year_subset = df[df['year'] == yr]
        if year_subset.empty: continue
        
        # 檢查該年度漲幅是否超過 500% (防範如轉板、減資未還原之錯誤)
        yr_check = year_subset.groupby('symbol').agg(
            first_p=('close', 'first'),
            last_p=('close', 'last')
        )
        yr_check['yr_ret'] = (yr_check['last_p'] - yr_check['first_p']) / yr_check['first_p']
        crazy_stocks = yr_check[yr_check['yr_ret'] > 5.0].index.tolist()
        if crazy_stocks:
            print(f"⚠️ {yr} 年偵測到超常年漲幅 (>500%): {crazy_stocks}，已從分析中移除。")
            df = df[~df['symbol'].isin(crazy_stocks)]

    # --- 4. 特徵工程：技術指標與背離計算 ---
    print("🧪 計算技術指標 (MA, KD, MACD) 與背離訊號...")
    processed_list = []
    
    for symbol, group in df.groupby('symbol'):
        group = group.copy().sort_values('date')
        
        # 均線體系
        group['ma5'] = ta.sma(group['close'], length=5)
        group['ma20'] = ta.sma(group['close'], length=20)
        group['ma60'] = ta.sma(group['close'], length=60)
        
        # KD 指標 (9, 3, 3)
        kd = ta.stoch(group['high'], group['low'], group['close'], k=9, d=3)
        group['k'], group['d'] = kd['STOCHk_9_3_3'], kd['STOCHd_9_3_3']
        
        # MACD 指標
        macd = ta.macd(group['close'])
        group['macd'], group['macds'] = macd['MACD_12_26_9'], macd['MACDS_12_26_9']
        
        # 黃金交叉判定
        group['kd_gold'] = (group['k'] > group['d']) & (group['k'].shift(1) <= group['d'].shift(1))
        group['macd_gold'] = (group['macd'] > group['macds']) & (group['macd'].shift(1) <= group['macds'].shift(1))
        
        # 低檔背離 (價格創低但指標未創低 - 簡化邏輯)
        group['low_divergence'] = (group['close'] < group['close'].shift(3)) & (group['macd'] > group['macd'].shift(3))

        # --- 5. 標籤工程：未來區間最大漲跌幅 (Demo 核心) ---
        windows = {'1-5': (1, 5), '6-10': (6, 10), '11-20': (11, 20), '21-30': (21, 30)}
        for label, (s, e) in windows.items():
            # 取得未來視窗內的極值
            # 例如 1-5 代表今天之後的第 1 到第 5 天
            f_high = group['high'].shift(-s).rolling(window=(e-s+1)).max()
            f_low = group['low'].shift(-s).rolling(window=(e-s+1)).min()
            
            group[f'up_{label}'] = (f_high / group['close'] - 1).round(4)
            group[f'down_{label}'] = (f_low / group['close'] - 1).round(4)

        processed_list.append(group)

    # --- 6. 寫回資料庫 ---
    df_final = pd.concat(processed_list)
    # 刪除輔助用的欄位以節省空間
    df_final = df_final.drop(columns=['is_anomaly', 'daily_return', 'year'])
    
    print(f"💾 正在將加工後的數據寫入 stock_analysis 表...")
    df_final.to_sql('stock_analysis', conn, if_exists='replace', index=False)
    
    # 強制建立索引：這對 Streamlit 查詢極其重要
    conn.execute("CREATE INDEX IF NOT EXISTS idx_analysis_sym_date ON stock_analysis (symbol, date)")
    conn.close()
    print(f"✨ {db_path} 處理完成！")

if __name__ == "__main__":
    # 測試用
    process_market_data("tw_stock_warehouse.db")
