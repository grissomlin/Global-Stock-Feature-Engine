# -- coding utf-8 --
import sqlite3
import pandas as pd
import pandas_ta as ta
import numpy as np

def process_market_data(db_path)
    conn = sqlite3.connect(db_path)
    # 1. 讀取原始數據
    df = pd.read_sql(SELECT  FROM stock_prices ORDER BY symbol, date, conn)
    
    processed_list = []
    
    # 2. 按股票分組處理
    for symbol, group in df.groupby('symbol')
        group = group.copy().sort_values('date')
        
        # --- A. 技術指標計算 ---
        # 均線
        group['ma5'] = ta.sma(group['close'], length=5)
        group['ma20'] = ta.sma(group['close'], length=20)
        group['ma60'] = ta.sma(group['close'], length=60)
        
        # KD (9, 3, 3)
        kd = ta.stoch(group['high'], group['low'], group['close'], k=9, d=3, smooth_k=3)
        group['k'], group['d'] = kd['STOCHk_9_3_3'], kd['STOCHd_9_3_3']
        
        # MACD (12, 26, 9)
        macd = ta.macd(group['close'], fast=12, slow=26, signal=9)
        group['macd'], group['macdh'], group['macds'] = macd['MACD_12_26_9'], macd['MACDH_12_26_9'], macd['MACDS_12_26_9']

        # --- B. 背離判定 (簡化邏輯：指標與價格走勢背向) ---
        # 低檔背離：價格創新低，但指標底部抬高
        # 黃金交叉：K 穿過 D 或 MACD 穿過 Signal
        group['kd_gold'] = (group['k']  group['d']) & (group['k'].shift(1) = group['d'].shift(1))
        group['macd_gold'] = (group['macd']  group['macds']) & (group['macd'].shift(1) = group['macds'].shift(1))

        # --- C. 未來區間最大漲跌幅 ---
        # 定義區間清單 (開始天數, 結束天數)
        windows = {'1-5' (1, 5), '6-10' (6, 10), '10-20' (10, 20), '20-30' (20, 30)}
        
        for label, (s, e) in windows.items()
            # 使用 rolling 配合 shift 抓取未來區間
            # 抓取未來 e 天內的最高價，並位移回來
            future_high = group['high'].shift(-e).rolling(window=(e-s+1)).max()
            future_low = group['low'].shift(-e).rolling(window=(e-s+1)).min()
            
            group[f'max_up_{label}'] = (future_high  group['close'] - 1).round(4)
            group[f'max_down_{label}'] = (future_low  group['close'] - 1).round(4)

        # 整理欄位 (去除 NaN 以省空間)
        group = group.dropna(subset=['ma60']) 
        processed_list.append(group)

    # 3. 寫回資料庫
    df_final = pd.concat(processed_list)
    df_final.to_sql('stock_analysis', conn, if_exists='replace', index=False)
    
    # 建立索引優化儀表板查詢速度
    conn.execute(CREATE INDEX IF NOT EXISTS idx_analysis ON stock_analysis (symbol, date))
    conn.close()
    print(f✅ 特徵工程完成 {db_path})
