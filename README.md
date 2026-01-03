# 🌐 Global Stock Feature Engine  
## 全球股市特徵引擎

A multi-market quantitative pipeline for technical indicator backtesting and future return analysis.  
一個支援多國市場的量化資料流水線，專注於技術指標回測與未來報酬特徵分析。

---

## 📌 Overview / 專案簡介

This project provides an end-to-end framework to:
- Download historical stock data from **Taiwan (TW), US (US), China (CN), Hong Kong (HK), Japan (JP), and Korea (KR)**
- Compute technical indicators (MA, MACD, KD) and advanced features (divergence, slope, future max return)
- Backtest signal performance based on customizable conditions
- Export statistical matrices for AI-assisted strategy optimization

本專案提供完整流程，可：
- 下載 **台股、美股、陸股、港股、日股、韓股** 的歷史股價資料
- 計算技術指標（均線、MACD、KD）與進階特徵（背離、斜率、未來最大漲跌幅）
- 根據自訂條件回測訊號表現
- 匯出統計矩陣，供 AI 分析策略優化方向

> ⚠️ **Disclaimer / 免責聲明**  
> All technical indicators shown are for **demonstration and educational purposes only**.  
> The author **does not use these indicators for personal trading**, and this tool is **not investment advice**.  
>   
> 所有技術指標僅供**示範與教學用途**。  
> 作者**本人已不再使用這些指標進行交易**，本工具**不構成任何投資建議**。

---

## 🧩 Key Features / 核心功能

| Feature | Description |
|--------|-------------|
| **Multi-Market Support**<br>多市場支援 | TW, US, CN, HK, JP, KR — with market-specific downloaders |
| **Robust Data Pipeline**<br>穩健資料流程 | Incremental sync, anomaly cleaning, Google Drive caching |
| **Feature Engineering**<br>特徵工程 | MA slope, MACD histogram acceleration, bottom divergence, YTD return, future max drawdown/upside |
| **AI-Ready Output**<br>AI 友好輸出 | CSV-style statistical matrix for prompt-based LLM analysis (e.g., ChatGPT) |
| **Modular Design**<br>模組化架構 | Easily extend with your own indicators in `processor.py` |

---

## 🚀 Quick Start / 快速上手

1. Clone the repository  
   ```bash
   git clone https://github.com/grissomlin/Global-Stock-Feature-Engine.git
   cd Global-Stock-Feature-Engine
