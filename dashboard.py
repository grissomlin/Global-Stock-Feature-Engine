import streamlit as st
import os, json, sqlite3, io, urllib.parse
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

# 市場選擇
market_options = {
    "台股 (TW)": "tw",
    "美股 (US)": "us", 
    "陸股 (CN)": "cn",
    "港股 (HK)": "hk",
    "日股 (JP)": "jp",
    "韓股 (KR)": "kr"
}
selected_market_label = st.sidebar.selectbox("選擇市場", list(market_options.keys()))
market_code = market_options[selected_market_label]

# 動態設定資料庫名稱
TARGET_DB = f"{market_code}_stock_warehouse.db"

# 基本條件
year = st.sidebar.selectbox("選擇年份", [2024, 2025], index=1)
month = st.sidebar.selectbox("選擇月份", list(range(1, 13)), index=0)

# 技術指標策略
strategy_type = st.sidebar.selectbox(
    "1. 技術指標策略", 
    ["無", "KD 黃金交叉", "MACD 柱狀圖轉正", "均線多頭排列(MA20>MA60)"]
)

# 💡 B. 獨立的背離選單 (從資料庫欄位對應)
# 2. 疊加背離條件
divergence_type = st.sidebar.selectbox(
    "2. 疊加背離條件 (必備特徵)",
    ["不限", "MACD 底部背離", "KD 底部背離", "雙重背離 (MACD+KD)"]
)

# 💡 優化：背離有效窗口選擇 (預設當天，並加入 4 天選項)
lookback_days = st.sidebar.selectbox(
    "∟ 背離發生在最近幾天內？",
    options=[0, 1, 2, 3, 4, 5],
    index=0,  # 預設為當天
    format_func=lambda x: "僅限當天 (共振)" if x == 0 else f"最近 {x} 天內",
    help="由於背離常早於訊號發生。選擇『最近 3 天』代表：只要過去 3 個交易日內曾出現過背離，且『今天』符合第一階段技術策略，標的就會被選出。"
)

# 3. 評估期間 (加上註解與說明)
period_options = {
    "1-5 天 (極短線展望)": "1-5",
    "6-10 天 (波段啟動期)": "6-10",
    "11-20 天 (中期趨勢驗證)": "11-20"
}
selected_period_label = st.sidebar.selectbox(
    "3. 評估未來報酬區間", 
    list(period_options.keys()),
    help="選擇訊號發生後『未來第幾天到第幾天』之間出現的最大漲跌幅。例如 6-10 天代表觀察訊號日後第 6 個交易日到第 10 個交易日的表現。"
)
reward_period = period_options[selected_period_label]
up_col = f"up_{reward_period}"
down_col = f"down_{reward_period}"

# --- 4. 主標題 ---
st.title("🌐 全球股市特徵引擎 - 策略篩選中心")

# 顯示當前篩選條件
strategy_desc = "無" if strategy_type == "無" else strategy_type
divergence_desc = "無" if divergence_type == "不限" else divergence_type
st.markdown(f"""
**當前選擇市場:** {selected_market_label} | **分析時段:** {year}年{month}月  
**技術策略:** {strategy_desc} | **背離條件:** {divergence_desc} | **評估期間:** {reward_period}天
""")

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

                # 🛠️ 執行技術指標過濾 (這是當天的訊號)
                if strategy_type == "KD 黃金交叉": 
                    df = df[df['kd_gold'] == 1]
                elif strategy_type == "MACD 柱狀圖轉正": 
                    if 'macdh_slope' in df.columns: df = df[df['macdh_slope'] > 0]
                elif strategy_type == "均線多頭排列(MA20>MA60)": 
                    df = df[df['ma20'] > df['ma60']]
                
                # 🛠️ 執行「背離追蹤天數」過濾 (滾動檢查)
                if divergence_type != "不限":
                    # 根據選擇的類型決定檢查哪些欄位
                    check_cols = []
                    if divergence_type in ["MACD 底部背離", "雙重背離 (MACD+KD)"]:
                        check_cols.append('macd_bottom_div')
                    if divergence_type in ["KD 底部背離", "雙重背離 (MACD+KD)"]:
                        check_cols.append('kd_bottom_div')
                
                    if all(col in df.columns for col in check_cols):
                        # 簡化版邏輯：直接過濾當前 df 中符合條件的
                        def check_recent_div(row, full_df, lookback, cols):
                            target_symbol = row['symbol']
                            current_date = row['date']
                            
                            # 獲取該股歷史窗口資料
                            history = full_df[full_df['symbol'] == target_symbol]
                            recent_history = history[history['date'] <= current_date].tail(lookback + 1)
                            
                            if divergence_type == "雙重背離 (MACD+KD)":
                                # 雙重背離要求分開看：MACD 在窗口內有過，且 KD 在窗口內也有過
                                has_macd = recent_history['macd_bottom_div'].max() == 1
                                has_kd = recent_history['kd_bottom_div'].max() == 1
                                return has_macd and has_kd
                            else:
                                # 單一背離：窗口內任何一列有 1 即可
                                return recent_history[cols].max().max() == 1
                
                        # 執行過濾
                        with st.spinner("🔍 正在追蹤背離歷史窗口..."):
                            mask = df.apply(check_recent_div, axis=1, args=(df, lookback_days, check_cols))
                            df = df[mask]

                # 準備顯示用 DataFrame
                def make_wantgoo_link(s): return f"https://www.wantgoo.com/stock/{str(s).split('.')[0]}/technical-chart"
                
                core_cols = ['date', 'symbol', 'close', 'ytd_ret', up_col, down_col]
                available_show = [c for c in core_cols if c in df.columns] + existing_features
                
                # 如果選擇了背離條件，也顯示背離欄位
                if divergence_type != "不限":
                    if 'macd_bottom_div' in df.columns and 'kd_bottom_div' in df.columns:
                        available_show += ['macd_bottom_div', 'kd_bottom_div']
                
                res_df = df[available_show].copy()
                res_df['分析'] = res_df['symbol'].apply(make_wantgoo_link)

                # 顯示表格
                st.subheader(f"🚀 {year}年{month}月 符合訊號標的 (共 {len(df)} 筆)")
                
                # 設定欄位格式
                column_config = {
                    "ytd_ret": st.column_config.NumberColumn("YTD(%)", format="%.2f%%"),
                    up_col: st.column_config.NumberColumn("未來漲幅", format="%.2f%%"),
                    down_col: st.column_config.NumberColumn("未來跌幅", format="%.2f%%"),
                    "分析": st.column_config.LinkColumn("玩股網", display_text="開圖"),
                }
                
                # 如果有背離欄位，設定布林值顯示
                if 'macd_bottom_div' in res_df.columns:
                    column_config["macd_bottom_div"] = st.column_config.CheckboxColumn("MACD背離")
                if 'kd_bottom_div' in res_df.columns:
                    column_config["kd_bottom_div"] = st.column_config.CheckboxColumn("KD背離")
                
                st.data_editor(
                    res_df,
                    column_config=column_config,
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

        # AI 提示詞 + ChatGPT 按鈕
        st.divider()
        st.subheader("🤖 AI 量化大師提示詞")
        
        # 建立提示詞（加入背離條件信息）
        csv_data = up_matrix.to_csv(index=False)
        strategy_desc = "無" if strategy_type == "無" else strategy_type
        divergence_desc = "無" if divergence_type == "不限" else divergence_type
        
        prompt_text = f"""請分析這份漲幅特徵矩陣，找出高報酬分箱的斜率規律：

{csv_data}

分析背景：
- 市場：{selected_market_label}
- 技術策略：{strategy_desc}
- 背離條件：{divergence_desc}
- 評估期間：{reward_period}天
- 樣本數：{len(res_df)}筆

請提供以下分析：
1. 找出哪個特徵在高報酬分箱中有明顯差異
2. 結合技術策略({strategy_desc})和背離條件({divergence_desc})，建議具體的量化交易策略
3. 預測此策略的風險與回報特性
4. 提供可能的改進方向
5. 分析背離條件是否對策略效果有顯著影響"""

        # 顯示提示詞框
        st.code(prompt_text, language="markdown")
        
        # 按鈕區域
        col1, col2 = st.columns([1, 1])
        
        with col1:
            # 手動複製提示詞
            if st.button("📋 複製提示詞到剪貼簿", use_container_width=True):
                try:
                    import pyperclip
                    pyperclip.copy(prompt_text)
                    st.success("✅ 已複製到剪貼簿！")
                except:
                    st.info("⚠️ 請手動選取並複製上方的提示詞")
        
        with col2:
            # ChatGPT 連結按鈕
            encoded_prompt = urllib.parse.quote(prompt_text)
            st.link_button(
                "🔥 ChatGPT 分析", 
                f"https://chatgpt.com/?q={encoded_prompt}",
                help="自動帶入完整分析指令",
                use_container_width=True,
                type="primary"
            )

# --- 7. 教學解釋區 ---
st.divider()
st.header("📖 量化特徵小知識")
with st.expander("💡 什麼是「特徵欄位分析」？"):
    st.markdown("""
    ### 🎯 特徵欄位分析：數據的「基因解碼」
    
    在量化交易中，「特徵」就像是股票的基因代碼。我們將原始價格、成交量等數據轉換成**更具預測力的數學特徵**，幫助機器學習模型辨識模式。
    
    #### 🔍 為何要進行特徵工程？
    1. **降維**：將複雜的股價走勢簡化成幾個關鍵指標
    2. **去雜訊**：過濾市場雜音，聚焦真正趨勢
    3. **標準化**：讓不同股票之間可以公平比較
    4. **可預測性**：找出與未來報酬高度相關的信號
    
    #### 📈 特徵的三大類型：
    **1. 趨勢型特徵**
    ```
    MA20斜率 = (今日MA20 - 昨日MA20) / 昨日MA20
    ```
    * 目的：捕捉中期動能方向
    * 應用：判斷趨勢是否加速或減速
    
    **2. 動能型特徵**
    ```
    MACD柱狀圖斜率 = 最近N根K線MACD柱狀圖的線性回歸斜率
    ```
    * 目的：衡量動能變化率
    * 應用：預測技術指標是否即將轉向
    
    **3. 擺盪型特徵**
    ```
    KD位置 = (今日K值 - 20) / (80 - 20) × 100%
    ```
    * 目的：識別超買超賣極端值
    * 應用：反轉點位預測
    
    **4. 背離型特徵**
    ```
    MACD底部背離 = 價格創新低但MACD未創新低
    KD底部背離 = 價格創新低但KD未創新低
    ```
    * 目的：識別潛在的反轉信號
    * 應用：尋找買入時機
    
    ---
    ### 🧬 為什麼看斜率而不只看價格？
    | 指標 | 比喻 | 關鍵洞察 |
    |------|------|----------|
    | **MA20斜率** | 車子的「瞬時時速」 | 斜率越高，衝刺力越強，短期動能越充足 |
    | **MA60斜率** | 跑道的「長緩坡」 | 正值代表順風（多頭環境），勝率天生較高 |
    | **MACD加速度** | 油門踩下去的「深度」 | 轉正代表買盤動能正在爆發，非短暫反彈 |
    | **背離信號** | 雷達的「異常警示」 | 價格與指標不同步，預示潛在轉折點 |
    
    ---  
    ### 📊 統計數據的解碼藝術  
    **偏度 (Skewness) - 「暴發戶指數」**
    ```
    正偏度 > 0：右尾較長 → 這組股票可能藏有飆股
    負偏度 < 0：左尾較長 → 這組股票可能有地雷股
    ```
    * **實戰意義**：正偏度越高的策略，代表有機會抓到「十倍股」
    
    **峰度 (Kurtosis) - 「一致性指數」**
    ```
    高峰度 > 3：分布集中 → 選股結果穩定可預測
    低峰度 < 3：分布分散 → 選股結果像買樂透
    ```
    * **實戰意義**：高峰度的策略代表每次執行結果相似，適合資金配置    
    ---
    ### 🚀 實戰應用：三層過濾法則
    1. **第一層：趨勢篩選**
    ```
    IF MA20斜率 > 0.5 AND MA60斜率 > 0.2 THEN 進入觀察名單
    ```
    2. **第二層：背離確認**
    ```
    IF MACD底部背離 = 1 OR KD底部背離 = 1 THEN 列為候選標的
    ```
    3. **第三層：動態調整**
    ```
    根據市場狀態，調整特徵權重（牛市重斜率，熊市重背離）
    ```    
    --- 
    ### 💡 進階思考：特徵交互作用
    真正賺錢的秘密往往不在單一特徵，而在**特徵之間的交互作用**：
    ```
    黃金組合 = MA20斜率↑ + MACD柱狀圖斜率轉正 + MACD底部背離
    死亡組合 = MA20斜率↓ + MACD柱狀圖斜率轉負 + 量價背離
    ```
    本儀表板的統計矩陣功能，正是幫助您挖掘這些隱藏的「特徵化學反應」！
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
