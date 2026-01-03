import streamlit as st
import os, json, sqlite3, io, pyperclip
import pandas as pd
import numpy as np
import plotly.graph_objects as go 
from scipy.stats import skew, kurtosis
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from datetime import datetime

# --- 0. 頁面基本設定 ---
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
        creds = service_account.Credentials.from_service_account_info(
            info, scopes=['https://www.googleapis.com/auth/drive.readonly']
        )
        return build('drive', 'v3', credentials=creds)
    except Exception as e:
        st.error(f"❌ 服務初始化失敗: {e}")
        return None

def download_file(service, file_id, file_name):
    request = service.files().get_media(fileId=file_id)
    fh = io.FileIO(file_name, 'wb')
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    with st.spinner(f'🚀 正在同步 {file_name}...'):
        while done is False:
            _, done = downloader.next_chunk()
    return True

# --- 3. 側邊欄：策略篩選條件 ---
st.sidebar.header("📊 選股策略條件")
year = st.sidebar.selectbox("選擇年份", [2024, 2025], index=1)
month = st.sidebar.selectbox("選擇月份", list(range(1, 13)), index=0)
strategy_type = st.sidebar.selectbox(
    "技術指標策略", 
    ["無", "KD 黃金交叉", "MACD 柱狀圖轉正", "均線多頭排列(MA20>MA60)"]
)
reward_period = st.sidebar.selectbox("評估未來報酬區間", ["1-5", "6-10", "11-20"])
up_col = f"up_{reward_period}"
down_col = f"down_{reward_period}"

# --- 4. 主標題與全球即時戰報 ---
st.title("🌐 全球股市特徵引擎 - 策略篩選中心")

def show_global_battlefield():
    if os.path.exists("global_summary.json"):
        with open("global_summary.json", "r", encoding="utf-8") as f:
            summary_data = json.load(f)
        
        st.header("🌍 全球市場戰況報")
        cols = st.columns(len(summary_data))
        
        for i, m in enumerate(summary_data):
            with cols[i]:
                color = "normal" if "✅" in m['status'] else "inverse"
                st.metric(
                    label=f"{m['market']} 市場",
                    value=f"{m['success']} 家",
                    delta=f"{m['coverage']} 涵蓋",
                    delta_color=color
                )
                st.caption(f"📅 最後更新: {m['end_date']}")
    else:
        st.info("ℹ️ 尚未偵測到全球摘要數據 (global_summary.json)，請確認後台同步流程。")

show_global_battlefield()

# --- 5. 數據核心：讀取與過濾 ---
service = get_gdrive_service()
res_df = pd.DataFrame()  # 初始化避免錯誤
existing_features = []

if service:
    # 下載資料庫 (如果本地不存在)
    if not os.path.exists(TARGET_DB):
        folder_id = st.secrets["GDRIVE_FOLDER_ID"]
        query = f"'{folder_id}' in parents and name = '{TARGET_DB}' and trashed = false"
        results = service.files().list(q=query, fields="files(id, name)").execute()
        files = results.get('files', [])
        if files: 
            download_file(service, files[0]['id'], TARGET_DB)

    if os.path.exists(TARGET_DB):
        try:
            conn = sqlite3.connect(TARGET_DB)
            start_date = f"{year}-{month:02d}-01"
            end_date = f"{year}-{month:02d}-31"
            df = pd.read_sql(f"SELECT * FROM stock_analysis WHERE date BETWEEN '{start_date}' AND '{end_date}'", conn)
            conn.close()

            if not df.empty:
                # 偵測特徵欄位是否存在
                all_potential_features = ['ma20_slope', 'ma60_slope', 'macdh_slope']
                existing_features = [f for f in all_potential_features if f in df.columns]

                # 執行過濾
                if strategy_type == "KD 黃金交叉": 
                    df = df[df['kd_gold'] == 1]
                elif strategy_type == "MACD 柱狀圖轉正": 
                    if 'macdh_slope' in df.columns: df = df[df['macdh_slope'] > 0]
                elif strategy_type == "均線多頭排列(MA20>MA60)": 
                    df = df[df['ma20'] > df['ma60']]

                # 準備顯示用 DataFrame
                def make_wantgoo_link(s): return f"https://www.wantgoo.com/stock/{str(s).split('.')[0]}/technical-chart"
                
                core_cols = ['date', 'symbol', 'close', 'ytd_ret', up_col, down_col]
                available_show = [c for c in core_cols if c in df.columns] + existing_features
                res_df = df[available_show].copy()
                res_df['分析'] = res_df['symbol'].apply(make_wantgoo_link)

                # 顯示表格
                st.subheader(f"🚀 {year}年{month}月 符合訊號標的 (共 {len(df)} 筆)")
                st.data_editor(
                    res_df,
                    column_config={
                        "ytd_ret": st.column_config.NumberColumn("YTD(%)", format="%.2f%%"),
                        up_col: st.column_config.NumberColumn("未來漲幅", format="%.2f%%"),
                        down_col: st.column_config.NumberColumn("未來跌幅", format="%.2f%%"),
                        "分析": st.column_config.LinkColumn("玩股網", display_text="開圖"),
                    },
                    hide_index=True, use_container_width=True
                )
            else:
                st.info("💡 該時段內無資料，請更換年份或月份。")

        except Exception as e:
            st.error(f"❌ 數據讀取失敗: {e}")

# --- 6. 視覺化分析區 (Plotly 圖表與統計矩陣) ---
if not res_df.empty:
    st.divider()
    st.header("📊 策略報酬分佈視覺化")
    
    plot_col = up_col if strategy_type != "無" else 'ytd_ret'
    bins_total = [-100, -20, -10, -5, 0, 5, 10, 20, 50, 100, 500]
    labels_total = ["<-20%", "-20~-10%", "-10~-5%", "-5~0%", "0~5%", "5~10%", "10~20%", "20~50%", "50~100%", ">100%"]
    
    res_df['total_bin'] = pd.cut(res_df[plot_col], bins=bins_total, labels=labels_total)
    counts = res_df['total_bin'].value_counts().sort_index()
    percents = (counts / len(res_df) * 100).round(2)
    colors = ['#e74c3c' if "~-" in str(label) or "<-" in str(label) else '#3498db' for label in labels_total]

    fig = go.Figure(data=[go.Bar(
        x=labels_total, y=counts,
        text=[f"{c}家 ({p}%)" for c, p in zip(counts, percents)],
        textposition='auto', marker_color=colors
    )])
    fig.update_layout(title="報酬率區間分佈圖 (藍色:正報酬 / 紅色:負報酬)", xaxis_title="報酬區間", yaxis_title="標的數量")
    st.plotly_chart(fig, use_container_width=True)

    # 統計矩陣
    if len(existing_features) > 0:
        st.divider()
        st.header("🔬 特徵統計矩陣 (深入研究標的基因)")
        
        def create_stat_matrix(data, bin_col, feat_cols):
            stats_list = []
            total_samples = len(data)
            for b_label, group in data.groupby(bin_col, observed=True):
                scount = len(group)
                row = {"分箱區間": b_label, "樣本數": scount, "比例(%)": f"{(scount/total_samples*100):.2f}%"}
                for f in feat_cols:
                    row[f"{f}_平均"] = group[f].mean()
                    row[f"{f}_中位數"] = group[f].median()
                    row[f"{f}_偏度(爆發力)"] = skew(group[f]) if scount > 3 else 0
                    row[f"{f}_峰度(穩定度)"] = kurtosis(group[f]) if scount > 3 else 0
                stats_list.append(row)
            return pd.DataFrame(stats_list)

        # 漲幅矩陣
        st.subheader("📈 最大漲幅 vs 技術特徵")
        bins_up = [-100, 0, 5, 10, 20, 50, float('inf')]
        res_df['bin_up'] = pd.cut(res_df[up_col], bins=bins_up, labels=["下行", "0-5%", "5-10%", "10-20%", "20-50%", ">50%"])
        up_matrix = create_stat_matrix(res_df, 'bin_up', existing_features)
        st.dataframe(up_matrix, use_container_width=True)

        # 跌幅矩陣
        st.subheader("📉 最大跌幅 vs 技術特徵")
        bins_down = [float('-inf'), -20, -10, -5, 0, 100]
        res_df['bin_down'] = pd.cut(res_df[down_col], bins=bins_down, labels=["重摔(<-20%)", "大跌(-20%~-10%)", "中跌(-10%~-5%)", "小跌(-5%~0%)", "抗跌(>0%)"])
        down_matrix = create_stat_matrix(res_df, 'bin_down', existing_features)
        st.dataframe(down_matrix, use_container_width=True)

        # AI 提示詞 + 複製按鈕
        st.divider()
        st.subheader("🤖 AI 量化大師提示詞")
        
        # 建立提示詞
        csv_data = up_matrix.to_csv(index=False)
        prompt_text = f"""請分析這份漲幅特徵矩陣，找出高報酬分箱的斜率規律：

{csv_data}

請提供以下分析：
1. 找出哪個特徵在高報酬分箱中有明顯差異
2. 建議具體的量化交易策略
3. 預測此策略的風險與回報特性
4. 提供可能的改進方向"""

        # 顯示提示詞框和複製按鈕
        cols = st.columns([4, 1])
        with cols[0]:
            st.code(prompt_text, language="markdown")
        
        with cols[1]:
            st.write("")  # 空白行對齊
            st.write("")
            if st.button("📋 一鍵複製到剪貼板", use_container_width=True):
                try:
                    # 嘗試使用 pyperclip
                    import pyperclip
                    pyperclip.copy(prompt_text)
                    st.success("✅ 已複製到剪貼板！")
                except:
                    # 如果 pyperclip 不可用，使用 streamlit 的複製功能
                    st.info("📋 請手動複製上方程式碼")

# --- 7. 教學解釋區 ---
st.divider()
st.header("📖 量化特徵小知識")
with st.expander("💡 什麼是「特徵欄位分析」？"):
    st.markdown("""
    ### 🧬 為什麼看斜率而不只看價格？
    * **MA20 斜率**：車子的「瞬時時速」。斜率越高，衝刺力越強。
    * **MA60 斜率**：跑道的「長緩坡」。正值代表你在跑下坡（順風），勝率天生較高。
    * **MACD 加速度**：油門踩下去的「深度」。轉正代表動能正在爆發。
    
    ### 📊 如何解讀統計數據？
    * **偏度 (Skewness)**：衡量「暴發戶」的存在。正偏代表這區間裡混有大漲的飆股。
    * **峰度 (Kurtosis)**：衡量「規律性」。峰度越高，代表選出來的標的表現越整齊，容易複製成功。
    """)

# --- 8. 頁尾連結區 (新增打賞按鈕) ---
st.divider()
st.markdown("""
<div style="text-align: center;">
    <table style="margin: 0 auto; border-collapse: separate; border-spacing: 20px 0;">
        <tr>
            <td style="text-align: center; vertical-align: top;">
                <div style="font-size: 1.5em;">🛠️</div>
                <a href="https://vocus.cc/article/695636c3fd89780001d873bd" target="_blank" style="text-decoration: none;">
                    <b>⚙️ 環境與 AI 設定教學</b>
                </a>
            </td>
            <td style="text-align: center; vertical-align: top;">
                <div style="font-size: 1.5em;">📊</div>
                <a href="https://vocus.cc/salon/grissomlin/room/695636ee0c0c0689d1e2aa9f" target="_blank" style="text-decoration: none;">
                    <b>📖 儀表板功能詳解</b>
                </a>
            </td>
            <td style="text-align: center; vertical-align: top;">
                <div style="font-size: 1.5em;">🐙</div>
                <a href="https://github.com/grissomlin/StockRevenueLab" target="_blank" style="text-decoration: none;">
                    <b>💻 GitHub 專案原始碼</b>
                </a>
            </td>
            <td style="text-align: center; vertical-align: top;">
                <div style="font-size: 1.5em;">❤️</div>
                <a href="https://vocus.cc/pay/donate/606146a3fd89780001ba32e9?donateSourceType=article&donateSourceRefID=69107512fd89780001396f10" 
                   target="_blank" style="text-decoration: none; color: #ff6b6b;">
                    <b>💝 打賞支持作者</b>
                </a>
                <div style="font-size: 0.8em; margin-top: 5px; color: #666;">
                    喜歡這個儀表板嗎？<br>歡迎支持繼續開發！
                </div>
            </td>
        </tr>
    </table>
</div>
""", unsafe_allow_html=True)
