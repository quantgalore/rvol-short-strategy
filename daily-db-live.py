# -*- coding: utf-8 -*-
"""
Created in 2025

@author: Quant Galore
"""

import pandas as pd
import numpy as np
import requests
import sqlalchemy
import mysql.connector
import os

from datetime import datetime, timedelta
from pandas_market_calendars import get_calendar

mkt_open_time = pd.Timestamp("09:30").time()
mkt_close_time = pd.Timestamp("16:00").time()

def is_market_open():
    now = datetime.now().time()
    return mkt_open_time <= now <= mkt_close_time

polygon_api_key = "KkfCQ7fsZnx0yK4bhX9fD81QplTh0Pf3"
calendar = get_calendar("NYSE")

engine = sqlalchemy.create_engine('mysql+mysqlconnector://user:pass@localhost:3306/my_database')
master_universe = pd.read_sql("historical_active_stocks", con = engine)

trading_dates = calendar.schedule(start_date = (datetime.today()), end_date = (datetime.today())).index.strftime("%Y-%m-%d").values
date = trading_dates[-1]

point_in_time_dates = np.sort(master_universe[master_universe["date"] < date]["date"].drop_duplicates().values)
point_in_time_universe = master_universe[master_universe["date"] == point_in_time_dates[-1]].drop_duplicates(subset=["ticker"], keep = "last")

all_tickers = point_in_time_universe["ticker"].drop_duplicates().values

start_date = trading_dates[0]
end_date = trading_dates[-1]

while is_market_open():
    
    times = []
    
    # ticker = all_tickers[0]
    for ticker in all_tickers:
        
        try:
            
            start_time = datetime.now()
            
            ticker_request_0 = requests.get(f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/minute/{start_date}/{end_date}?adjusted=true&sort=asc&limit=50000&apiKey={polygon_api_key}").json()
            ticker_data = pd.json_normalize(ticker_request_0["results"]).set_index("t")
            ticker_data.index = pd.to_datetime(ticker_data.index, unit="ms", utc=True).tz_convert("America/New_York")
            ticker_data["timestamp"] = ticker_data.index.strftime("%Y-%m-%d %H:%M")
            ticker_data["ticker"] = ticker
            
            ticker_data = ticker_data[ticker_data.index.time <= pd.Timestamp("16:00").time()].copy()
            
            ticker_data = ticker_data.reset_index()
            
            complete_single_ticker_data = ticker_data.copy().tail(1)
            complete_single_ticker_data["date"] = complete_single_ticker_data["t"].dt.strftime("%Y-%m-%d")
                    
            if len(complete_single_ticker_data) < 1:
                continue
            
            # Create ticker-specific folder
            folder_path = f"Daily Data - Real Time/{ticker}"
            os.makedirs(folder_path, exist_ok=True)
    
            # Save entire DataFrame to single parquet file
            file_path = f"{folder_path}/{ticker}.parquet"
            complete_single_ticker_data.to_parquet(file_path, index=False)
            
            end_time = datetime.now()
            seconds_to_complete = (end_time - start_time).total_seconds()
            times.append(seconds_to_complete)
            iteration = round(((np.where(all_tickers==ticker)[0][0]+1)/len(all_tickers))*100,2)
            iterations_remaining = len(all_tickers) - np.where(all_tickers==ticker)[0][0]
            average_time_to_complete = np.mean(times)
            estimated_completion_time = (datetime.now() + timedelta(seconds = int(average_time_to_complete*iterations_remaining)))
            time_remaining = estimated_completion_time - datetime.now()
            print(f"{iteration}% complete, {time_remaining} left, ETA: {estimated_completion_time}")
            
        except Exception:
            print(f"Error with: {ticker} – {start_date} to {end_date}")
            continue