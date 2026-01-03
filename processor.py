# -*- coding: utf-8 -*-
import sqlite3
import pandas as pd
import numpy as np

def process_market_data(db_path):
    conn = sqlite3.connect(db_path)
    # 1. 讀取數據
    query = """
        SELECT p.*, i.market 
        FROM stock_prices p
        LEFT JOIN stock_info i ON p.symbol = i.symbol
    """
    df = pd.read_sql(query, conn)
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values(['symbol', 'date'])

    processed_list = []
    
    # 2. 分組計算指標
    for symbol, group in df.groupby('symbol'):
        group = group.copy().sort_values('date')
        if len(group) < 60: continue # 數據太短跳過

        # --- A. 均線與斜率 ---
        group['ma20'] = group['close'].rolling(window=20).mean()
        group['ma60'] = group['close'].rolling(window=60).mean()
        # 斜率 (MA20 變動量)
        group['ma20_slope'] = group['ma20'].diff(3) / 3

        # --- B. MACD 實作 ---
        ema12 = group['close'].ewm(span=12, adjust=False).mean()
        ema26 = group['close'].ewm(span=26, adjust=False).mean()
        group['macd'] = ema12 - ema26
        group['macds'] = group['macd'].ewm(span=9, adjust=False).mean()
        group['macdh'] = group['macd'] - group['macds']
        group['macdh_slope'] = group['macdh'].diff(1)

        # --- C. KD 實作 ---
        low_min = group['low'].rolling(window=9).min()
        high_max = group['high'].rolling(window=9).max()
        rsv = 100 * (group['close'] - low_min) / (high_max - low_min)
        group['k'] = rsv.ewm(com=2, adjust=False).mean() # com=2 等同於 alpha=1/3
        group['d'] = group['k'].ewm(com=2, adjust=False).mean()

        # --- D. 底部背離偵測 ---
        lookback = 10
        # 價格創新低
        group['price_low_new'] = group['close'] < group['close'].shift(1).rolling(window=lookback).min()
        # MACD 底部背離 (價跌指標升)
        group['macd_bottom_div'] = (group['price_low_new']) & (group['macdh'] > group['macdh'].shift(1).rolling(window=lookback).min())

        # --- E. 未來報酬標籤 ---
        windows = {'1-5': (1, 5), '6-10': (6, 10)}
        for label, (s, e) in windows.items():
            f_high = group['high'].shift(-s).rolling(window=(e-s+1)).max()
            group[f'up_{label}'] = (f_high / group['close'] - 1).round(4)

        processed_list.append(group)

    # 3. 寫回資料庫
    df_final = pd.concat(processed_list)
    df_final.to_sql('stock_analysis', conn, if_exists='replace', index=False)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_analysis ON stock_analysis (symbol, date)")
    conn.close()
    print(f"✅ {db_path} 處理完成 (無依賴穩定版)")
