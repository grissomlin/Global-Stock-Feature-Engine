import streamlit as st
import os, json, sqlite3, io
import pandas as pd
import numpy as np
from scipy.stats import skew, kurtosis
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

st.set_page_config(page_title="全球股市特徵引擎", layout="wide")

# --- 1. 固定變數定義 ---
TARGET_DB = "tw_stock_warehouse.db"

# --- 2. Google Drive 服務初始化 (省略重複程式碼以節省篇幅，保持原樣) ---
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

            # 策略過濾
            if strategy_type == "KD 黃金交叉": df = df[df['kd_gold'] == 1]
            elif strategy_type == "MACD 柱狀圖轉正": df = df[df['macdh_slope'] > 0]
            elif strategy_type == "均線多頭排列(MA20>MA60)": df = df[df['ma20'] > df['ma60']]

            st.subheader(f"🚀 {year}年{month}月 符合訊號標的 (共 {len(df)} 筆)")
            
            if not df.empty:
                def make_wantgoo_link(symbol): return f"https://www.wantgoo.com/stock/{str(symbol).split('.')[0]}/technical-chart"

                # 包含所有斜率欄位
                feature_cols = ['ma20_slope', 'ma60_slope', 'macdh_slope']
                core_cols = ['date', 'symbol', 'close', 'ytd_ret', up_col, down_col]
                available_cols = [c for c in core_cols + feature_cols if c in df.columns]
                
                res_df = df[available_cols].copy()
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

                # --- 5. 分箱統計與統計特徵矩陣 ---
                st.divider()
                st.header(f"📊 特徵統計矩陣 (報酬分箱 vs 技術斜率)")
                
                # 定義分箱
                bins = [-100, 0, 5, 10, 20, 50, 100, float('inf')]
                labels = ["下行/不漲", "0-5%", "5-10%", "10-20%", "20-50%", "50-100%", ">100%"]
                res_df['bin'] = pd.cut(res_df[up_col], bins=bins, labels=labels)

                # 計算每個分箱的斜率統計
                stats_list = []
                for b_label, group in res_df.groupby('bin', observed=True):
                    row = {"報酬分箱": b_label, "樣本數": len(group)}
                    for f in feature_cols:
                        if f in group.columns:
                            row[f"{f}_平均"] = group[f].mean()
                            row[f"{f}_中位數"] = group[f].median()
                            row[f"{f}_偏度(Skew)"] = skew(group[f]) if len(group) > 2 else 0
                            row[f"{f}_峰度(Kurt)"] = kurtosis(group[f]) if len(group) > 2 else 0
                    stats_list.append(row)
                
                full_stats_df = pd.DataFrame(stats_list)
                st.dataframe(full_stats_df, use_container_width=True)

                # --- 6. AI 提示詞複製區 ---
                st.divider()
                st.subheader("🤖 AI 量化分析助手提示詞")
                st.info("複製下方文字貼給 ChatGPT/Claude，進行深度特徵分析：")
                
                # 建立 AI 摘要文字
                ai_summary = full_stats_df.to_csv(index=False)
                prompt_text = f"""
你現在是一位資深量化交易員。請分析以下這份『全球股市特徵引擎』的統計數據。
這是 {year}年{month}月，策略為『{strategy_type}』，觀察未來 {reward_period} 天的表現。

數據包含不同『未來報酬分箱』對應的技術特徵（MA20斜率、MA60斜率、MACD動能速度）的平均值、中位數、偏度與峰度。

統計數據清單：
{ai_summary}

請幫我分析：
1. 哪些特徵數值與『高回報酬 (>20%)』有強烈關聯？
2. 偏度 (Skew) 與 峰度 (Kurtosis) 顯示了哪些數據異常值或極端風險？
3. 請給出一個基於這些特徵的優化選股建議。
"""
                st.code(prompt_text, language="markdown")
                
                # 說明與定義
                with st.expander("📝 統計學特徵名詞解釋"):
                    st.write("""
                    * **平均值/中位數**：代表該特徵的中心趨勢。若平均值遠大於中位數，表示該區間存在極端強勢股。
                    * **偏度 (Skewness)**：衡量數據分布是否對稱。
                        - 正偏 (Positive Skew)：長尾在右側，代表該特徵有少數標的高得驚人（例如爆發力強的飆股）。
                        - 負偏 (Negative Skew)：長尾在左側，代表數據分布偏低。
                    * **峰度 (Kurtosis)**：衡量數據的「尖銳程度」與「胖尾現象」。
                        - 峰度高：代表數據非常集中，或是有明顯的「極端離群值」。
                    """)

        except Exception as e:
            st.error(f"❌ 數據處理失敗: {e}")
