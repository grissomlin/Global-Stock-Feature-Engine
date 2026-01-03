# -*- coding: utf-8 -*-
import sqlite3
import pandas as pd
import pandas_ta as ta
import numpy as np

def process_market_data(db_path):
    conn = sqlite3.connect(db_path)
    # 1. 讀取數據 (關聯市場別以利後續清洗)
    query = "SELECT p.*, i.market FROM stock_prices p LEFT JOIN stock_info i ON p.symbol = i.symbol"
    df = pd.read_sql(query, conn)
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values(['symbol', 'date'])

    processed_list = []
    
    for symbol, group in df.groupby('symbol'):
        group = group.copy().sort_values('date')

        # --- A. 基礎指標計算 ---
        # KD (9, 3, 3)
        kd = ta.stoch(group['high'], group['low'], group['close'], k=9, d=3)
        group['k'], group['d'] = kd['STOCHk_9_3_3'], kd['STOCHd_9_3_3']
        
        # MACD (12, 26, 9)
        macd = ta.macd(group['close'])
        group['macd'], group['macds'], group['macdh'] = macd['MACD_12_26_9'], macd['MACDS_12_26_9'], macd['MACDH_12_26_9']

        # --- B. 底部背離偵測 (Bullish Divergence) ---
        # 定義觀察期 (例如回溯 10 天前的低點)
        lookback = 10
        
        # 1. 股價創新低：今天的收盤價低於 N 天前的最低收盤價
        group['price_low_new'] = group['close'] < group['close'].shift(1).rolling(window=lookback).min()
        
        # 2. KD 底部背離：價格創新低，但 K 值卻高於 N 天前的最低 K 值
        group['kd_bottom_div'] = (group['price_low_new']) & (group['k'] > group['k'].shift(1).rolling(window=lookback).min())
        
        # 3. MACD 底部背離：價格創新低，但 MACD 柱狀圖 (macdh) 高於 N 天前的最低值
        group['macd_bottom_div'] = (group['price_low_new']) & (group['macdh'] > group['macdh'].shift(1).rolling(window=lookback).min())

        # --- C. 複合訊號：背離 + 黃金交叉 (強勢買入訊號) ---
        group['kd_gold'] = (group['k'] > group['d']) & (group['k'].shift(1) <= group['d'].shift(1))
        # 當出現底部背離，且當天剛好黃金交叉，這就是你的 Demo 亮點
        group['bullish_resonance'] = (group['kd_bottom_div'] | group['macd_bottom_div']) & group['kd_gold']

        # --- D. 未來區間標籤 (用於驗證背離是否有效) ---
        windows = {'1-5': (1, 5), '6-10': (6, 10)}
        for label, (s, e) in windows.items():
            f_high = group['high'].shift(-s).rolling(window=(e-s+1)).max()
            group[f'up_{label}'] = (f_high / group['close'] - 1).round(4)

        processed_list.append(group)

    # 4. 寫回資料庫
    df_final = pd.concat(processed_list)
    df_final.to_sql('stock_analysis', conn, if_exists='replace', index=False)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_analysis ON stock_analysis (symbol, date)")
    conn.close()
