# 🌍 Global Stock Feature Engine
**全球股市特徵工程資料庫 | Global Stock Feature Engineering Pipeline**

---
<img width="1860" height="777" alt="image" src="https://github.com/user-attachments/assets/6f24528d-43cd-4677-baa5-af5463570a76" />

## 📌 專案簡介（Project Overview）

**Global Stock Feature Engine** 是一個以「**資料工程與量化研究**」為核心的開源專案，  
目標是將**原始股價資料轉換為結構化、可研究、可回測的特徵資料庫**。

本專案**不是交易系統，也不是選股工具**，而是一個：

- 📦 股市資料清洗與標準化工具  
- 🧪 技術指標與衍生特徵的「資料層」產生器  
- 🔬 用於研究市場行為、事件反應與統計關係的基礎設施  

---

**Global Stock Feature Engine** is an open-source project focused on  
**data engineering and quantitative research infrastructure**.

Its purpose is to transform **raw stock price data** into a  
**clean, structured, and research-ready feature database**.

This project is **NOT**:
- a trading bot  
- a stock recommendation system  
- an investment advisory tool  

It is designed as:
- 📦 a market data cleaning & normalization pipeline  
- 🧪 a feature / indicator generation layer  
- 🔬 a foundation for market behavior and statistical research  

---

## 🧠 核心設計理念（Core Philosophy）

### 中文
> **先把資料處理好，才有資格談分析與策略**

- 專注在「**特徵工程（Feature Engineering）**」
- 不追求預測、不內建策略、不輸出買賣點
- 所有欄位都是 **研究用變數（Research Variables）**

### English
> **Clean data first. Analysis comes later.**

- Focus on **feature engineering**, not prediction
- No built-in strategies, signals, or trade execution
- All outputs are **research variables**, not recommendations

---

## ⚠️ 重要聲明（Important Disclaimer）

### 中文聲明
> **本專案中出現的所有技術指標（MA、MACD、KD 等）僅為資料工程示範用途。**  
>
> - ❌ 不構成任何投資建議  
> - ❌ 不代表作者的交易策略  
> - ❌ 作者本人實務交易中「完全沒有使用」這些技術指標  
>
> 它們的存在目的僅是：
> - 驗證資料處理流程是否正確  
> - 作為特徵工程的教學與範例  
> - 提供研究者自行延伸與替換的模板  

### English Disclaimer
> **All technical indicators (MA, MACD, KD, etc.) included in this project are for demonstration purposes only.**  
>
> - ❌ They are NOT investment advice  
> - ❌ They do NOT represent the author’s trading strategy  
> - ❌ The author does NOT use these indicators in real trading  
>
> Their purpose is solely to:
> - Validate data pipelines  
> - Demonstrate feature engineering examples  
> - Serve as editable templates for researchers  

---

## 🧩 專案功能概覽（Key Features）

### 🔹 資料清洗（Data Cleaning）
- 異常漲跌偵測與平滑處理
- 支援極端行情與資料斷點修正

### 🔹 技術特徵工程（Feature Engineering）
- Moving Averages & Slopes (MA)
- MACD / Histogram / Slope
- KD / Golden Cross / Bottom Divergence
- Year-to-Date Return (YTD)
- Forward Max Up / Down Returns

### 🔹 自動化流程（Automation）
- GitHub Actions scheduling
- Google Drive database sync
- Multi-market support (TW / US / HK)
<img width="1886" height="836" alt="image" src="https://github.com/user-attachments/assets/fee9d5b2-d4b7-4438-835c-249d86615bcc" />
<img width="1905" height="876" alt="image" src="https://github.com/user-attachments/assets/88c27239-716e-4320-bbbc-88664d6b715c" />
<img width="1905" height="910" alt="image" src="https://github.com/user-attachments/assets/0fed5dbb-4087-445e-84f7-043d78d714a6" />
<img width="1919" height="846" alt="image" src="https://github.com/user-attachments/assets/fd672e2b-9326-4792-b02c-e69a387b1251" />

---

## 🧪 Only Feature Engineering 模式說明

### 中文
GitHub Actions 中的 **Only Feature Engineering** 任務：

- 只負責：
  - 下載既有資料庫  
  - 產生 / 更新技術指標與特徵欄位  
  - 回傳資料庫  
- **不抓新行情、不涉及任何交易邏輯**

若你有自己偏好的研究特徵，可直接修改：

processor.py

### English
The **Only Feature Engineering** workflow in GitHub Actions:

- Handles only:
  - Downloading the existing database
  - Generating / updating feature columns
  - Uploading the processed database
- **No price fetching, no trading logic**

You are encouraged to customize features in:

processor.py

---

## 🔐 Secrets 與環境變數（Secrets & Environment Variables）

Only required for **GitHub Actions / Streamlit Cloud**:

GDRIVE_FOLDER_ID
GDRIVE_SERVICE_ACCOUNT

📌 **Not required for local execution**

---

## 🔗 延伸閱讀與資源（Resources）

- 🛠️ [環境與 AI 設定教學](https://vocus.cc/article/6959a592fd89780001295ad1)
- 📊 [儀表板功能詳解](https://vocus.cc/article/6959a091fd8978000128b592)
- 🐙 [GitHub 專案原始碼](https://github.com/grissomlin/Global-Stock-Feature-Engine)
- ❤️ [打賞支持作者](https://vocus.cc/pay/donate/606146a3fd89780001ba32e9?donateSourceType=article&donateSourceRefID=69107512fd89780001396f10)

---

## 📄 License

This project is open-sourced for **research and educational purposes only**.
