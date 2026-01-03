import streamlit as st
import os, json, sqlite3, io
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from scipy.stats import skew, kurtosis
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

st.set_page_config(page_title="全球股市特徵引擎", layout="wide")

# --- 1. 固定變數定義 ---
TARGET_DB = "tw_stock_warehouse.db"

# --- 2. Google Drive 服務初始化 ---
def get_gdrive_service():
    if "GDRIVE_SERVICE_ACCOUNT" not in st.secrets:
        st.error("❌ Secrets 中缺少 GDRIVE_SERVICE_ACCOUNT")
        return None
    try:
        info = json.loads(st.secrets["GDRIVE_SERVICE_ACCOUNT"])
        creds = service_account.Credentials.from_service_account_info(info, scopes=['https://www.googleapis.com/auth/drive.readonly'])
        return build('drive', 'v3', credentials=creds)
    except Exception as e:
        st.error(f"❌ 服務初始化失敗: {e}"); return None

def download_file(service, file_id, file_name):
    request = service.files().get_media(fileId=file_id)
    fh = io.FileIO(file_name, 'wb')
    downloader = MediaIoBaseDownload(fh, request); done = False
    with st.spinner(f'🚀 正在同步 {file_name}...'):
        while done is False: _, done = downloader.next_chunk()
    return True

# --- 3. 側邊欄：策略篩選條件 ---
st.sidebar.header("📊 選股策略條件")
year = st.sidebar.selectbox("選擇年份", [2024, 2025], index=1)
month = st.sidebar.selectbox("選擇月份", list(range(1, 13)), index=0)
strategy_type = st.sidebar.selectbox("技術指標策略", ["無", "KD 黃金交叉", "MACD 柱狀圖轉正", "均線多頭排列(MA20>MA60)"])
reward_period = st.sidebar.selectbox("評估未來報酬區間", ["1-5", "6-10", "11-20"])
up_col = f"up_{reward_period}"
down_col = f"down_{reward_period}"

# --- 4. 主程式邏輯 ---
st.title("🌐 全球股市特徵引擎 - 策略篩選中心")
service = get_gdrive_service()

if service:
    if not os.path.exists(TARGET_DB):
        folder_id = st.secrets["GDRIVE_FOLDER_ID"]
        query = f"'{folder_id}' in parents and name = '{TARGET_DB}' and trashed = false"
        results = service.files().list(q=query, fields="files(id, name)").execute()
        files = results.get('files', [])
        if files: download_file(service, files[0]['id'], TARGET_DB)

    if os.path.exists(TARGET_DB):
        try:
            conn = sqlite3.connect(TARGET_DB)
            start_date = f"{year}-{month:02d}-01"; end_date = f"{year}-{month:02d}-31"
            df = pd.read_sql(f"SELECT * FROM stock_analysis WHERE date BETWEEN '{start_date}' AND '{end_date}'", conn)
            conn.close()

            # 💡 核心修正：在過濾前先定義好 existing_features
            all_potential_features = ['ma20_slope', 'ma60_slope', 'macdh_slope']
            existing_features = [f for f in all_potential_features if f in df.columns]
            
            if not existing_features:
                st.warning("⚠️ 警告：資料庫中找不到任何斜率特徵欄位，請檢查 processor.py 是否執行成功。")

            # 策略過濾
            if strategy_type == "KD 黃金交叉": df = df[df['kd_gold'] == 1]
            elif strategy_type == "MACD 柱狀圖轉正": 
                if 'macdh_slope' in df.columns:
                    df = df[df['macdh_slope'] > 0]
            elif strategy_type == "均線多頭排列(MA20>MA60)": df = df[df['ma20'] > df['ma60']]

            st.subheader(f"🚀 {year}年{month}月 符合訊號標的 (共 {len(df)} 筆)")
            
            if not df.empty:
                def make_wantgoo_link(symbol): return f"https://www.wantgoo.com/stock/{str(symbol).split('.')[0]}/technical-chart"

                core_cols = ['date', 'symbol', 'close', 'ytd_ret', up_col, down_col]
                # 確保只顯示存在的欄位
                show_cols = [c for c in core_cols if c in df.columns] + existing_features
                
                res_df = df[show_cols].copy()
                res_df['分析'] = res_df['symbol'].apply(make_wantgoo_link)

                st.data_editor(
                    res_df,
                    column_config={
                        "date": "訊號日期", "symbol": "股票代號", 
                        "close": st.column_config.NumberColumn("收盤價", format="%.2f"),
                        "ytd_ret": st.column_config.NumberColumn("年初至今(%)", format="%.2f%%"),
                        up_col: st.column_config.NumberColumn("未來最大漲幅(%)", format="%.2f%%"),
                        down_col: st.column_config.NumberColumn("未來最大跌幅(%)", format="%.2f%%"),
                        "ma20_slope": st.column_config.NumberColumn("MA20斜率", format="%.4f"),
                        "ma60_slope": st.column_config.NumberColumn("MA60斜率", format="%.4f"),
                        "macdh_slope": st.column_config.NumberColumn("MACD動能速度", format="%.4f"),
                        "分析": st.column_config.LinkColumn("玩股網", display_text="開圖"),
                    },
                    hide_index=True, use_container_width=True
                )


                  # --- 5. 漲跌幅分佈柱狀圖 (視覺化) ---
                if not res_df.empty:
                    st.divider()
                    st.header("📊 策略報酬分佈視覺化")
                    
                    # 準備數據
                    bins_total = [-100, -20, -10, -5, 0, 5, 10, 20, 50, 100, 500]
                    labels_total = ["<-20%", "-20~-10%", "-10~-5%", "-5~0%", "0~5%", "5~10%", "10~20%", "20~50%", "50~100%", ">100%"]
                    
                    # 計算家數與比例
                    res_df['total_bin'] = pd.cut(res_df[up_col if strategy_type != "無" else 'ytd_ret'], bins=bins_total, labels=labels_total)
                    counts = res_df['total_bin'].value_counts().sort_index()
                    percents = (counts / len(res_df) * 100).round(2)
                    
                    # 設定顏色：負值紅色，正值藍色 (符合台灣視覺習慣)
                    colors = ['#e74c3c' if "~-" in label or "<-" in label else '#3498db' for label in labels_total]
                    
                    # 使用 Plotly 繪製柱狀圖
                    fig = go.Figure(data=[go.Bar(
                        x=labels_total,
                        y=counts,
                        text=[f"{c}家 ({p}%)" for c, p in zip(counts, percents)], # 在柱子上顯示家數與比例
                        textposition='auto',
                        marker_color=colors
                    )])
                    
                    fig.update_layout(
                        title=f"未來 {reward_period} 天漲跌幅分佈圖 (樣本數: {len(res_df)} 家)",
                        xaxis_title="漲跌幅區間",
                        yaxis_title="家數",
                        height=500,
                        margin=dict(l=20, r=20, t=50, b=20)
                    )
                    st.plotly_chart(fig, use_container_width=True)
                
                # --- 6. 特徵統計矩陣 (原有的矩陣邏輯放在圖表下方) ---
                # ... [保留原本的 create_stat_matrix 邏輯] ...
                
                # --- 7. 通俗版解釋區 ---
                st.divider()
                st.header("📖 投資小學堂：什麼是「特徵欄位」？")
                
                with st.expander("💡 深入淺出：特徵欄位分析是什麼？ (點擊展開)"):
                    st.markdown("""
                    ### 🧬 什麼是「特徵欄位」 (Feature Fields)？
                    
                    如果把「股價」比喻成一個人的**長相**，那麼「特徵欄位」就是這個人的**基因與體檢數據**。
                    
                    * **傳統分析**：看著照片（股價圖）說：「這個人看起來紅光滿面，應該會長壽（漲）。」這比較主觀。
                    * **特徵分析**：測量血壓（MA20斜率）、心跳（MACD速度）、體脂率（MA60斜率）。我們不看長相，我們看**數據指標**。
                    
                    **特徵分析的威力**在於：我們可以透過歷史數據發現，「血壓 120、心跳 70（特定斜率組合）」的人，有 80% 的機率能跑完馬拉松（大漲 20%）。
                    
                    ---
                
                    ### 🔎 本系統的三大「核心基因」
                    
                    
                    1. **MA20 斜率 (短期動能)**
                        * **像什麼**：車子的**時速表**。
                        * **怎麼看**：斜率大代表衝很快，但如果太高（如斜率 > 1），代表車速過快，轉彎容易翻車（重摔）。
                    
                    2. **MA60 斜率 (長期趨勢)**
                        * **像什麼**：跑道的**坡度**。
                        * **怎麼看**：斜率是正的，代表你在跑下坡（順風），就算腳痠（回檔）也容易繼續滑行；斜率是負的，代表你在爬好漢坡（逆風），非常吃力。
                        
                    3. **MACD 動能速度 (加速度)**
                        * **像什麼**：你的**油門深度**。
                        * **怎麼看**：加速度由負轉正，代表你開始踩油門了！這通常發生在價格還沒噴發前，是量化交易員最愛的「轉折特徵」。
                
                    ---
                
                    ### 📈 為什麼要看偏度與峰度？ (大白的解釋)
                    
                
                    * **偏度 (Skewness) —— 「發財機會」**
                        * **正偏 (大於0)**：代表這堆股票裡藏著幾隻「超級飆股」，雖然平均漲 5%，但那幾隻飆股可能漲了 50%！這是有機會「中大獎」的特徵。
                    
                    * **峰度 (Kurtosis) —— 「複製成功」**
                        * **高峰度**：代表這群股票的表現「整齊劃一」。如果一個策略峰度很高且平均獲利，代表你可以很放心地**重複操作**，因為標的表現都很穩定。
                    """)

            else:
                st.info("💡 此條件下查無資料，請放寬篩選標準。")

        except Exception as e:
            st.error(f"❌ 數據處理失敗: {e}")
