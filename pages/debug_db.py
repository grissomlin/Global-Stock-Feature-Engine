import streamlit as st
import sqlite3
import pandas as pd
import os

st.set_page_config(page_title="DB 除錯工具", layout="wide")

st.title("🔍 SQLite 資料庫底層診斷工具")

# 定義要檢查的資料庫名稱
DB_NAME = "tw_stock_warehouse.db"

if not os.path.exists(DB_NAME):
    st.error(f"❌ 找不到檔案: {DB_NAME}")
    st.info("請確認 GitHub Actions 是否已成功將檔案同步至雲端，且下載邏輯正常執行。")
else:
    st.success(f"✅ 偵測到資料庫檔案: {DB_NAME}")
    
    # 建立連線
    conn = sqlite3.connect(DB_NAME)
    
    # --- 1. 檢查所有表格 ---
    st.header("1. 資料表清單 (Tables)")
    tables = pd.read_sql("SELECT name FROM sqlite_master WHERE type='table'", conn)
    st.table(tables)
    
    if not tables.empty:
        # 讓使用者選擇要檢查的表格 (預設為 stock_analysis)
        target_table = st.selectbox("選擇要診斷的表格", tables['name'].tolist(), 
                                     index=tables['name'].tolist().index('stock_analysis') if 'stock_analysis' in tables['name'].tolist() else 0)
        
        st.divider()
        
        # --- 2. 檢查欄位結構 (Schema) ---
        st.header(f"2. `{target_table}` 欄位結構 (Schema)")
        # PRAGMA table_info 是 SQLite 查看欄位定義最直接的方式
        schema_df = pd.read_sql(f"PRAGMA table_info({target_table})", conn)
        
        # 標色顯示：如果欄位包含 slope，特別標註
        def highlight_slope(s):
            return ['background-color: #ffffb3' if 'slope' in str(val) else '' for val in s]
        
        st.dataframe(schema_df.style.apply(highlight_slope, axis=1), use_container_width=True)
        
        # --- 3. 數據完整性檢查 ---
        st.header(f"3. `{target_table}` 數據完整性統計")
        col1, col2, col3 = st.columns(3)
        
        try:
            total_rows = pd.read_sql(f"SELECT COUNT(*) as count FROM {target_table}", conn).iloc[0]['count']
            col1.metric("總列數 (Rows)", f"{total_rows:,}")
            
            date_range = pd.read_sql(f"SELECT MIN(date) as start, MAX(date) as end FROM {target_table}", conn)
            col2.metric("資料起點", str(date_range.iloc[0]['start']))
            col3.metric("資料終點 (最新日期)", str(date_range.iloc[0]['end']))
        except:
            st.warning("無法讀取數據統計，請確認欄位名稱是否包含 'date'")

        st.divider()

        # --- 4. 原始數據預覽 ---
        st.header(f"4. `{target_table}` 原始數據預覽 (最後 50 筆)")
        # 抓取最後 50 筆，方便看最新的資料有沒有斜率
        try:
            preview_df = pd.read_sql(f"SELECT * FROM {target_table} ORDER BY date DESC, symbol ASC LIMIT 50", conn)
            st.dataframe(preview_df, use_container_width=True)
        except Exception as e:
            st.error(f"讀取預覽失敗: {e}")

    conn.close()

st.sidebar.info("""
**除錯 SOP:**
1. 如果 **2. 欄位結構** 沒看到 `ma60_slope`，代表 `processor.py` 沒跑成功。
2. 如果 **3. 資料終點** 太舊，代表 `main.py` 的下載器沒更新。
3. 如果 **4. 數據預覽** 裡斜率全是 `NaN`，代表該股票資料長度不足計算指標。
""")
