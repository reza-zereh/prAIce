<p align="center">
  <img src="https://iili.io/iZeZkF.png" alt="prAIce" width="250" height="250">
</p>

<p align="center">
  <em>Advanced AI-powered framework designed to forecast long-term financial asset prices.</em>
</p>

---

## Introduction

_prAIce_ is an innovative AI framework developed to make informed long-term price forecasts on financial assets, with a current focus on US stocks. By emulating the comprehensive approach of a financial analyst, _prAIce_ integrates multiple analysis techniques to provide a holistic view of an asset's potential future performance.

Key features of _prAIce_ include:

- **News Sentiment Analysis:** Evaluates the impact of current events on asset prices.
- **Technical Analysis:** Utilizes numerous technical indicators to identify trends and patterns.
- **Candlestick Pattern Recognition:** Recognizes and interprets various candlestick patterns.
- **Fundamental Analysis:** Assesses the financial health and intrinsic value of companies.
- **Hedge Fund Holdings Analysis:** Tracks and analyzes institutional investor positions.
- **Options Market Analysis:** Examines options data for insights into market sentiment and potential price movements.
- **Macroeconomic Indicators Analysis:** Considers broader economic factors that may impact asset prices.
- **Peer and Sector Analysis:** Compares performance and metrics within industry groups
- **Data Collection and Management:** Efficiently collects and stores relevant financial data.
- **Customizable Analysis:** Allows for flexible configuration of data collection and analysis parameters.

_prAIce_ is designed to be a robust tool for investors, analysts, and researchers seeking to make data-driven decisions in the financial markets. By combining traditional financial analysis methods with cutting-edge AI technologies, _prAIce_ aims to provide valuable insights for long-term investment strategies.

## Installation
To set up _prAIce_ on your local machine, follow these steps:

1. Clone the repository
   ```bash
   git clone https://github.com/reza-zereh/prAIce.git
   cd prAIce
   ```
2. Set up a PostgreSQL database for the project.
3. Create and activate a virtual environment with Python version 3.10 or higher.
4. Rename the `.env.sample` file to `.env` and fill in the required values.
5. Install the TA-Lib library:
   - Linux:
     ```bash
     make install-talib
     ```
   - MacOS/Windows: TBD
6. Install the project requirements:
   - Linux:
     ```bash
     make install-requirements
     ```
   - MacOS/Windows: TBD

## Disclaimer

_prAIce_ is an experimental tool designed for research and educational purposes only. The predictions and analyses provided by _prAIce_ should not be considered as financial advice or a guarantee of future market performance. Users of _prAIce_ should always conduct their research and consult with qualified financial advisors before making any investment decisions. The creators and maintainers of _prAIce_ are not responsible for any financial losses incurred based on the use of this tool.
