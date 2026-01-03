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

                # --- 5. 報酬分布分箱統計 (漲幅與跌幅雙矩陣) ---
                if not res_df.empty and len(existing_features) > 0:
                    st.divider()
                    st.header(f"📊 特徵統計矩陣 (分箱分析 vs 技術特徵)")
                
                    # 定義統計函式
                    def create_stat_matrix(data, bin_col, feat_cols):
                        stats_list = []
                        for b_label, group in data.groupby(bin_col, observed=True):
                            row = {"分箱區間": b_label, "樣本數": len(group)}
                            for f in feat_cols:
                                row[f"{f}_平均"] = group[f].mean()
                                row[f"{f}_中位數"] = group[f].median()
                                row[f"{f}_偏度(爆發力)"] = skew(group[f]) if len(group) > 3 else 0
                                row[f"{f}_峰度(穩定度)"] = kurtosis(group[f]) if len(group) > 3 else 0
                            stats_list.append(row)
                        return pd.DataFrame(stats_list)
                
                    # 1. 最大漲幅分箱
                    st.subheader("📈 最大漲幅分箱特徵 (看哪種斜率最容易大賺)")
                    bins_up = [-100, 0, 5, 10, 20, 50, float('inf')]
                    labels_up = ["下行", "0-5%", "5-10%", "10-20%", "20-50%", ">50%"]
                    res_df['bin_up'] = pd.cut(res_df[up_col], bins=bins_up, labels=labels_up)
                    
                    up_matrix = create_stat_matrix(res_df, 'bin_up', existing_features)
                    st.dataframe(up_matrix, use_container_width=True)
                
                    # 2. 最大跌幅分箱 (新增加)
                    st.subheader("📉 最大跌幅分箱特徵 (看哪種斜率最容易大跌/避險)")
                    # 跌幅通常是負數，我們定義區間：0~-5, -5~-10...
                    bins_down = [float('-inf'), -20, -10, -5, 0, 100]
                    labels_down = ["重摔(<-20%)", "大跌(-20%~-10%)", "中跌(-10%~-5%)", "小跌(-5%~0%)", "抗跌(>0%)"]
                    res_df['bin_down'] = pd.cut(res_df[down_col], bins=bins_down, labels=labels_down)
                    
                    down_matrix = create_stat_matrix(res_df, 'bin_down', existing_features)
                    st.dataframe(down_matrix, use_container_width=True)
                
                    # --- 6. AI 提示詞 (自動包含漲跌雙矩陣數據) ---
                    st.divider()
                    st.subheader("🤖 AI 量化大師提示詞")
                    prompt = f"""
                你是一位量化投資專家。請分析以下兩份數據：
                漲幅特徵矩陣：{up_matrix.to_csv(index=False)}
                跌幅特徵矩陣：{down_matrix.to_csv(index=False)}
                
                請幫我找出：
                1. 大漲標的與大跌標在『MA20斜率』與『MACD速度』上的數值差異。
                2. 怎樣的斜率組合可以過濾掉『重摔』的分箱標的？
                """
                    st.code(prompt, language="markdown")
                
                # --- 7. 通俗版解釋區 ---
                st.divider()
                with st.expander("📝 為什麼這些指標能預測漲跌？ (通俗版解釋)"):
                    st.markdown("""
                    ### 🔎 技術指標與漲跌的「模式」
                    
                    * **MA20 斜率 (短期動能)**
                        * **大漲模式**：通常斜率 > 0.1 且持續增加。這像是一台正在「加速」的跑車。
                        * **大跌模式**：如果股價在高檔，MA20 斜率開始「轉平」甚至變負數，就像跑車沒油了，通常是大跌的前兆。
                    
                    * **MA60 斜率 (長期趨勢/地基)**
                        * **大漲模式**：MA60 斜率最好是正的。就像在順風跑，即便短線拉回，也會有支撐。
                        * **大跌模式**：若 MA60 斜率是負的，代表「大勢已去」，這時任何反彈都是逃命波。
                        
                    * **MACD 動能速度 (加速度)**
                        * **大漲模式**：這是在看「力道的轉折」。當速度從負轉正，代表空頭力竭、多頭接手。
                        * **大跌模式**：速度如果在高檔開始劇烈下滑，代表多頭力道正在消失，往往會伴隨急跌。
                
                    ---
                
                    ### 📊 統計學特徵是在看什麼？
                    
                    * **平均值 (Mean) / 中位數 (Median)**
                        * **解釋**：這組標的的「平均表現」。如果「漲 > 20%」分箱的 MA20 平均斜率是 0.3，代表這就是你要找的「飆股特徵」。
                    
                    * **偏度 (Skewness) —— 「爆發力偵測」**
                        * **解釋**：如果偏度是**正值 (正偏)**，代表這區間裡混著幾隻「超級大黑馬」拉高了數據，這組策略有中大獎的潛力！
                        
                    * **峰度 (Kurtosis) —— 「穩定度偵測」**
                        * **解釋**：數值越高，代表這群股票的特性「長得越像」。如果峰度很高，代表這組指標很「準」，選出來的標的表現都很統一，不會有的漲、有的跌。
                    """)

        except Exception as e:
            st.error(f"❌ 數據處理失敗: {e}")
