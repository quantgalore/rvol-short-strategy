# -*- coding: utf-8 -*-
"""
Created in 2025

@author: Quant Galore
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import requests
import sqlalchemy
import mysql.connector

from datetime import datetime, timedelta
from pandas_market_calendars import get_calendar
from sklearn.linear_model import LinearRegression

polygon_api_key = "KkfCQ7fsZnx0yK4bhX9fD81QplTh0Pf3"
calendar = get_calendar("NYSE")

engine = sqlalchemy.create_engine('mysql+mysqlconnector://user:pass@localhost:3306/my_database')
universe = pd.read_sql("historical_active_stocks", con = engine)

# =============================================================================
# Current Universe Calcs.
# =============================================================================

data_length = 20

dates = calendar.schedule(start_date = (datetime.today() - timedelta(days=20)), end_date = (datetime.today())).index.strftime("%Y-%m-%d").values
date = dates[-1]

point_in_time_dates = np.sort(universe[universe["date"] < date]["date"].drop_duplicates().values)
point_in_time_universe = universe[universe["date"] == point_in_time_dates[-1]].drop_duplicates(subset=["ticker"], keep = "last")

tickers = point_in_time_universe["ticker"].drop_duplicates().values

full_data_list = []
times = []
    
# ticker = tickers[np.random.randint(0, len(tickers))] # ticker = "JNVR"
for ticker in tickers:
    
    try:
        
        start_time = datetime.now()
        
        underlying_data = pd.read_parquet(f"Daily Data/{ticker}/{ticker}.parquet").set_index("t").sort_index()
        
        newest_data = pd.read_parquet(f"Daily Data - Real Time/{ticker}/{ticker}.parquet").set_index("t").sort_index()
        
        updated_data = pd.concat([underlying_data, newest_data], axis = 0).drop_duplicates(subset=["date"], keep="last").sort_index()
        
        historical_underlying_data = updated_data.copy()
        historical_underlying_data["pct_chg"] = round(historical_underlying_data["c"].pct_change()*100, 2)
        historical_underlying_data["rvol"] = abs(historical_underlying_data["pct_chg"])
        historical_underlying_data["rvol_chg"] = historical_underlying_data["rvol"].diff()
                
        historical_underlying_data = historical_underlying_data.copy().tail(data_length)
        
        last_date = historical_underlying_data["date"].iloc[-1]
        
        time_between = (pd.to_datetime(date) - pd.to_datetime(last_date)).days
        
        if time_between >= 5:
            continue   
        
        ticker_return_over_period = historical_underlying_data["rvol_chg"].sum()
        std_of_returns = historical_underlying_data["rvol_chg"].std() * np.sqrt(252)
        
        sharpe = ticker_return_over_period / std_of_returns
        
        lookback_price_return = round(((historical_underlying_data["c"].iloc[-1] - historical_underlying_data["c"].iloc[0]) / historical_underlying_data["c"].iloc[0]) * 100, 2)    
        last_day_price_return = round(((historical_underlying_data["c"].iloc[-1] - historical_underlying_data["c"].iloc[-2]) / historical_underlying_data["c"].iloc[-2]) * 100, 2)    
        
        lookback_vol = abs(lookback_price_return)
        last_day_vol = abs(last_day_price_return)
        
        ticker_data = pd.DataFrame([{"date": date, "ticker": ticker,  "sharpe": sharpe,
                                     "lookback_return": lookback_price_return, "vol_return": ticker_return_over_period,
                                     "1d_lookback_px_return": last_day_price_return, "1d_lookback_vol": last_day_vol,
                                     "time_between": time_between, "latest_t": newest_data["timestamp"].iloc[0]}])
        
        full_data_list.append(ticker_data)
        
        end_time = datetime.now()
        seconds_to_complete = (end_time - start_time).total_seconds()
        times.append(seconds_to_complete)
        iteration = round(((np.where(tickers==ticker)[0][0]+1)/len(tickers))*100,2)
        iterations_remaining = len(tickers) - np.where(tickers==ticker)[0][0]
        average_time_to_complete = np.mean(times)
        estimated_completion_time = (datetime.now() + timedelta(seconds = int(average_time_to_complete*iterations_remaining)))
        time_remaining = estimated_completion_time - datetime.now()
        print(f"{iteration}% complete, {time_remaining} left, ETA: {estimated_completion_time}")
        
    except Exception as error:
        # print(error, ticker)
        continue
    
full_dataset = pd.concat(full_data_list).sort_values(by="1d_lookback_vol", ascending = False)

top_decile = full_dataset.head(10)