# -*- coding: utf-8 -*-
import sqlite3
import pandas as pd
import numpy as np

def process_market_data(db_path):
    conn = sqlite3.connect(db_path)
    df = pd.read_sql("SELECT p.*, i.market FROM stock_prices p LEFT JOIN stock_info i ON p.symbol = i.symbol", conn)
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values(['symbol', 'date'])

    processed_list = []
    for symbol, group in df.groupby('symbol'):
        group = group.copy().sort_values('date')

        # --- 1. 均線 (MA) ---
        group['ma20'] = group['close'].rolling(window=20).mean()
        group['ma60'] = group['close'].rolling(window=60).mean()

        # --- 2. MACD 計算 ---
        ema12 = group['close'].ewm(span=12, adjust=False).mean()
        ema26 = group['close'].ewm(span=26, adjust=False).mean()
        group['macd'] = ema12 - ema26
        group['macds'] = group['macd'].ewm(span=9, adjust=False).mean()
        group['macdh'] = group['macd'] - group['macds']

        # --- 3. KD 計算 (9, 3, 3) ---
        low_min = group['low'].rolling(window=9).min()
        high_max = group['high'].rolling(window=9).max()
        rsv = 100 * (group['close'] - low_min) / (high_max - low_min)
        
        # 初始值設為 50
        k = [50.0]
        d = [50.0]
        for val in rsv.fillna(50).values[1:]:
            current_k = (1/3) * val + (2/3) * k[-1]
            current_d = (1/3) * current_k + (2/3) * d[-1]
            k.append(current_k)
            d.append(current_d)
        group['k'], group['d'] = k, d

        # --- 4. 背離偵測與未來報酬 (維持原邏輯) ---
        # ... (之前的 up_1-5, up_6-10 邏輯)
        
        processed_list.append(group)

    df_final = pd.concat(processed_list)
    df_final.to_sql('stock_analysis', conn, if_exists='replace', index=False)
    conn.close()
    print(f"✅ {db_path} 處理完成 (無依賴版本)")
