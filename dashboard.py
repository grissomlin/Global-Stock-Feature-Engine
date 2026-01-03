import streamlit as st
import sqlite3
import pandas as pd
import os
from datetime import datetime

st.set_page_config(page_title="Global Stock Feature Engine", layout="wide")

def get_all_db_files():
    """尋找目錄下所有的資料庫檔案"""
    return [f for f in os.listdir('.') if f.endswith('_stock_warehouse.db')]

def get_db_metadata(db_name):
    """取得資料庫的統計資訊"""
    try:
        conn = sqlite3.connect(db_name)
        # 檢查是否有加工後的表格
        tables = pd.read_sql("SELECT name FROM sqlite_master WHERE type='table'", conn)
        has_analysis = 'stock_analysis' in tables['name'].values
        target_table = 'stock_analysis' if has_analysis else 'stock_prices'
        
        df_stats = pd.read_sql(f"""
            SELECT 
                COUNT(DISTINCT symbol) as total_symbols,
                MIN(date) as start_date,
                MAX(date) as end_date,
                COUNT(*) as total_rows
            FROM {target_table}
        """, conn)
        
        # 取得欄位名稱以供參考
        columns = pd.read_sql(f"PRAGMA table_info({target_table})", conn)['name'].tolist()
        conn.close()
        
        return {
            "db": db_name,
            "table": target_table,
            "symbols": df_stats['total_symbols'][0],
            "start": df_stats['start_date'][0],
            "end": df_stats['end_date'][0],
            "rows": df_stats['total_rows'][0],
            "columns": columns
        }
    except Exception as e:
        return {"db": db_name, "error": str(e)}

# --- UI 介面 ---
st.title("🌐 全球股市特徵引擎 - 資料庫檢查儀表板")

db_files = get_all_db_files()

if not db_files:
    st.warning("❌ 找不到任何 *_stock_warehouse.db 檔案，請確認檔案已下載至本地。")
else:
    # 1. 總覽區
    st.header("📊 資料庫健康度掃描")
    meta_data = []
    for db in db_files:
        meta_data.append(get_db_metadata(db))
    
    df_meta = pd.DataFrame(meta_data)
    st.table(df_meta[['db', 'table', 'symbols', 'start', 'end', 'rows']])

    # 2. 詳細欄位與數據預覽
    st.divider()
    selected_db = st.selectbox("選擇要檢視的資料庫", db_files)
    
    if selected_db:
        curr_meta = next(item for item in meta_data if item["db"] == selected_db)
        
        col1, col2 = st.columns([1, 3])
        
        with col1:
            st.subheader("📋 欄位清單 (Features)")
            st.write(curr_meta['columns'])
        
        with col2:
            st.subheader("🔍 數據抽樣 (Top 100)")
            conn = sqlite3.connect(selected_db)
            # 優先展示具有特徵的數據
            df_preview = pd.read_sql(f"SELECT * FROM {curr_meta['table']} LIMIT 100", conn)
            st.dataframe(df_preview, use_container_width=True)
            
            # 特徵分佈快速檢查
            if 'macdh_slope' in df_preview.columns:
                st.subheader("📈 指標變動檢查 (示例：MACD 斜率)")
                st.line_chart(df_preview.set_index('date')['macdh_slope'].head(50))
            
            conn.close()

    # 3. 異常檢索 (選配)
    with st.expander("🛠️ 進階檢查：搜尋特定標的"):
        search_symbol = st.text_input("輸入標的代號 (例如: 2330.TW)", "")
        if search_symbol and selected_db:
            conn = sqlite3.connect(selected_db)
            res = pd.read_sql(f"SELECT * FROM {curr_meta['table']} WHERE symbol = '{search_symbol}' ORDER BY date DESC LIMIT 20", conn)
            st.write(res)
            conn.close()
