# -*- coding: utf-8 -*-
import sqlite3
import pandas as pd
import numpy as np

def process_market_data(db_path):
    conn = sqlite3.connect(db_path)
    
    # 1. 讀取數據 (左連接 stock_info 以獲得市場分類)
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
        if len(group) < 60: continue 

        # --- A. 均線與斜率 ---
        group['ma20'] = group['close'].rolling(window=20).mean()
        group['ma60'] = group['close'].rolling(window=60).mean()
        # 斜率 (MA20 近三日變動量)
        group['ma20_slope'] = (group['ma20'].diff(3) / 3).round(4)

        # --- B. MACD 實作 (純 Pandas) ---
        ema12 = group['close'].ewm(span=12, adjust=False).mean()
        ema26 = group['close'].ewm(span=26, adjust=False).mean()
        group['macd'] = (ema12 - ema26).round(4)
        group['macds'] = group['macd'].ewm(span=9, adjust=False).mean().round(4)
        group['macdh'] = (group['macd'] - group['macds']).round(4)
        group['macdh_slope'] = group['macdh'].diff(1).round(4)

        # --- C. KD 實作 (9, 3, 3) ---
        low_min = group['low'].rolling(window=9).min()
        high_max = group['high'].rolling(window=9).max()
        # 💡 優化：處理分母為 0 的情況
        denominator = high_max - low_min
        rsv = 100 * (group['close'] - low_min) / denominator
        rsv = rsv.replace([np.inf, -np.inf], np.nan).fillna(50) # 平盤時給 50
        
        group['k'] = rsv.ewm(com=2, adjust=False).mean().round(4)
        group['d'] = group['k'].ewm(com=2, adjust=False).mean().round(4)
        
        # 💡 新增：KD 黃金交叉訊號
        group['kd_gold'] = ((group['k'] > group['d']) & (group['k'].shift(1) <= group['d'].shift(1))).astype(int)

        # --- D. 底部背離偵測 ---
        lookback = 10
        price_low_new = group['close'] < group['close'].shift(1).rolling(window=lookback).min()
        # MACD 底部背離 (價格創新低，但柱狀圖低點抬高)
        group['macd_bottom_div'] = ((price_low_new) & (group['macdh'] > group['macdh'].shift(1).rolling(window=lookback).min())).astype(int)

        # --- E. 未來報酬標籤 (預測目標) ---
        # 💡 增加 11-20 天，範圍更廣
        windows = {'1-5': (1, 5), '6-10': (6, 10), '11-20': (11, 20)}
        for label, (s, e) in windows.items():
            f_high = group['high'].shift(-s).rolling(window=(e-s+1)).max()
            group[f'up_{label}'] = (f_high / group['close'] - 1).round(4)

        processed_list.append(group)

    # 3. 寫回資料庫
    df_final = pd.concat(processed_list)
    
    # 💡 移除不必要的原始價格欄位(可選)，或只保留分析需要的
    # df_final = df_final.drop(columns=['daily_return']) # 如果有的話

    df_final.to_sql('stock_analysis', conn, if_exists='replace', index=False)
    
    # 建立索引優化查詢
    conn.execute("CREATE INDEX IF NOT EXISTS idx_analysis_sym_date ON stock_analysis (symbol, date)")
    conn.close()
    print(f"✅ {db_path} 特徵工程完成 (無依賴穩定版)")
