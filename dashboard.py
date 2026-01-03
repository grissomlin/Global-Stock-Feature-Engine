import streamlit as st
import sqlite3
import pandas as pd
import os
import io
from googleapiclient.http import MediaIoBaseDownload

# --- 核心下載函式 ---
def download_file(service, file_id, file_name):
    request = service.files().get_media(fileId=file_id)
    fh = io.FileIO(file_name, 'wb')
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    with st.spinner(f'正在從雲端同步 {file_name}...'):
        while done is False:
            status, done = downloader.next_chunk()
    return True

# --- 讀取欄位結構 ---
def get_table_schema(db_path):
    conn = sqlite3.connect(db_path)
    # 優先找加工過的分析表，找不到才找原始價格表
    tables = pd.read_sql("SELECT name FROM sqlite_master WHERE type='table'", conn)['name'].tolist()
    target = 'stock_analysis' if 'stock_analysis' in tables else 'stock_prices'
    
    # 抓取前 5 筆資料與欄位清單
    df_sample = pd.read_sql(f"SELECT * FROM {target} LIMIT 5", conn)
    columns = df_sample.columns.tolist()
    conn.close()
    return target, columns, df_sample

# --- 主程式介面 ---
st.title("🇹🇼 台灣市場數據掃描 (預設)")

if online_db_list: # 延續你之前的診斷結果
    # 預設目標：台灣資料庫
    TARGET_DB = "tw_stock_warehouse.db"
    
    # 1. 檢查檔案是否存在，不存在則自動下載
    if not os.path.exists(TARGET_DB):
        # 從 online_db_list 找到對應的 file_id
        tw_file = next((f for f in online_db_list if f['name'] == TARGET_DB), None)
        if tw_file:
            download_file(service, tw_file['id'], TARGET_DB)
            st.success(f"✅ {TARGET_DB} 已成功同步至本地環境")
        else:
            st.error("❌ 雲端找不到台灣資料庫檔案")

    # 2. 顯示結構分析
    if os.path.exists(TARGET_DB):
        table_name, cols, df_sample = get_table_schema(TARGET_DB)
        
        st.header(f"📊 資料表結構：`{table_name}`")
        
        # 使用 Columns 呈現資訊
        c1, c2 = st.columns([1, 2])
        with c1:
            st.subheader("📌 偵測到的特徵欄位")
            st.write(cols)
        
        with c2:
            st.subheader("💡 數據內容預覽")
            st.dataframe(df_sample, use_container_width=True)

        # 3. 欄位用途初步分類 (自動識別)
        st.divider()
        st.subheader("🛠️ 特徵工程狀態檢查")
        
        # 檢查關鍵指標是否存在
        indicators = {
            "均線/斜率": ["ma20", "ma20_slope"],
            "MACD 指標": ["macd", "macdh", "macdh_slope"],
            "KD 指標": ["k", "d", "kd_gold"],
            "背離訊號": ["macd_bottom_div", "kd_bottom_div"],
            "未來報酬(標籤)": ["up_1-5", "up_6-10"]
        }
        
        check_cols = st.columns(len(indicators))
        for i, (name, fields) in enumerate(indicators.items()):
            found = [f for f in fields if f in cols]
            if len(found) == len(fields):
                check_cols[i].metric(name, "已就緒", delta="✅")
            elif len(found) > 0:
                check_cols[i].metric(name, "部分遺漏", delta="⚠️", delta_color="off")
            else:
                check_cols[i].metric(name, "未計算", delta="❌", delta_color="inverse")
