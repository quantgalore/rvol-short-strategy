# High Realized Volatility Short Strategy

This repository contains a strategy that sorts stocks by realized volatility and returns the top-N candidates to short over the next day. It includes infrastructure for both backtesting and daily production use.

Original Source - [A Junior Quant's Guide to Chasing Vol and Shorting Stocks](https://quantgalore.substack.com/p/a-junior-quants-guide-to-chasing)


## Setup Instructions

1. Run `pit-stocks.py`  
   - Generates point-in-time universes of stocks with at least `$N` million in average daily volume  
   - Requires your MySQL credentials

2. Run `daily-db-build.py`  
   - Builds local OHLCV data for all qualified tickers for use in backtesting  
   - Saves data as Parquet files

3. Run `mtum-rvol-public.py`  
   - Runs a backtest on the strategy using the historical data  
   - Sorts by realized volatility and returns top-N daily short picks  
   - Evaluates returns over the following day

4. Run `mtum-rvol-prod-public.py`  
   - Generates live daily short candidates from the most recent data  
   - Used for production/live signal generation

5. In a separate terminal, run `daily-db-live.py`  
   - Continuously updates the latest market data  
   - This feed is used by the live production script

## Notes

- Top-N parameter can be adjusted inside the scripts
- Output files are saved locally in relevant folders (`/data/`, `/signals/`, etc.)
- Make sure all file paths and credentials are set properly
