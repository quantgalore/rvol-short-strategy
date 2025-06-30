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

polygon_api_key = "KkfCQ7fsZnx0yK4bhX9fD81QplTh0Pf3"
calendar = get_calendar("NYSE")

engine = sqlalchemy.create_engine('mysql+mysqlconnector://user:pass@localhost:3306/my_database')

universe = pd.read_sql("historical_active_stocks", con = engine).drop_duplicates(subset=["ticker", "date"])
universe_dates = np.sort(universe["date"].drop_duplicates().values)

# =============================================================================
# Construction of Historical Rankings
# =============================================================================

dates = calendar.schedule(start_date = "2025-01-01", end_date = (datetime.today() - timedelta(days=1))).index.strftime("%Y-%m-%d").values

data_length = 20

full_data_list = []
times = []

# date = dates[0]
for date in dates[:-5]:
    
    try:

        start_time = datetime.now()
        
        point_in_time_dates = np.sort(universe[universe["date"] < date]["date"].drop_duplicates().values)
        point_in_time_date = point_in_time_dates[-1]
        point_in_time_universe = universe[universe["date"] == point_in_time_date].drop_duplicates(subset=["ticker"], keep = "last")
        
        tickers = point_in_time_universe["ticker"].drop_duplicates().values
        
        daily_ticker_list = []
            
        # ticker = tickers[np.random.randint(0, len(tickers))] # ticker = "YGMZ"
        for ticker in tickers:
            
            try:
                
                underlying_data = pd.read_parquet(f"Daily Data/{ticker}/{ticker}.parquet").set_index("t").sort_index()
                
                historical_underlying_data = underlying_data[underlying_data["date"] <= date].copy()#.tail(data_length)
                historical_underlying_data["pct_chg"] = round(historical_underlying_data["c"].pct_change()*100, 2)
                historical_underlying_data["rvol"] = abs(historical_underlying_data["pct_chg"])
                historical_underlying_data["rvol_chg"] = historical_underlying_data["rvol"].diff()
                        
                historical_underlying_data = historical_underlying_data.copy().tail(data_length)
                historical_underlying_data["notional_volume"] = historical_underlying_data["vw"] * historical_underlying_data["v"]
                
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
                
                next_period_underlying_data = underlying_data[(underlying_data["date"] >= date)].copy().sort_index()
                
                next_period_returns = round(((next_period_underlying_data["c"].iloc[1] - next_period_underlying_data["c"].iloc[0]) / next_period_underlying_data["c"].iloc[0])*100, 2)
                next_period_vol = abs(next_period_returns)
                
                next_period_returns_1 = round(((next_period_underlying_data["c"].iloc[5] - next_period_underlying_data["c"].iloc[0]) / next_period_underlying_data["c"].iloc[0])*100, 2)
                next_period_vol_1 = abs(next_period_returns_1)
                
                mean_notional_volume = historical_underlying_data["notional_volume"][:-1].mean()
                
                ticker_data = pd.DataFrame([{"date": date, "ticker": ticker, "sharpe": sharpe,
                                              "lookback_px_return": lookback_price_return, "lookback_vol": lookback_vol,
                                              "mean_notional_volume": mean_notional_volume,
                                              "1d_lookback_px_return": last_day_price_return, "1d_lookback_vol": last_day_vol,
                                              "vol_return": ticker_return_over_period,
                                              "forward_returns": next_period_returns, "forward_vol": next_period_vol,
                                              "forward_returns_1": next_period_returns_1, "forward_vol_1": next_period_vol_1}])
                
                
                daily_ticker_list.append(ticker_data)
                
            except Exception as error:
                # print(error, ticker)
                continue
            
        full_period_ticker_data = pd.concat(daily_ticker_list)
        full_data_list.append(full_period_ticker_data)
        
        end_time = datetime.now()    
        seconds_to_complete = (end_time - start_time).total_seconds()
        times.append(seconds_to_complete)
        iteration = round((np.where(dates==date)[0][0]/len(dates))*100,2)
        iterations_remaining = len(dates) - np.where(dates==date)[0][0]
        average_time_to_complete = np.mean(times)
        estimated_completion_time = (datetime.now() + timedelta(seconds = int(average_time_to_complete*iterations_remaining)))
        time_remaining = estimated_completion_time - datetime.now()
        print(f"{iteration}% complete, {time_remaining} left, ETA: {estimated_completion_time}")
        
    except Exception as macro_error:
        print(macro_error, date)
        continue

full_dataset = pd.concat(full_data_list)

# =============================================================================
# Backtest
# =============================================================================

covered_dates = np.sort(full_dataset["date"].drop_duplicates().values)

ranking_criteria = "1d_lookback_vol"

trade_list = []
top_decile_list = []

# covered_date = covered_dates[-1] # covered_date = "2025-06-05"
for covered_date in covered_dates:
        
    daily_unranked_data = full_dataset[full_dataset["date"] == covered_date].copy()
       
    portfolio_uni = daily_unranked_data.sort_values(by=ranking_criteria, ascending=False).head(3)    
    top_decile_dataset = portfolio_uni[["date", "ticker", ranking_criteria, "forward_vol", "forward_vol_1"]]
    
    trade_data = pd.DataFrame([{"date": covered_date, "return": portfolio_uni["forward_returns"].mean(),
                                "vol": portfolio_uni["forward_vol"].mean()}])
    
    trade_list.append(trade_data)
    top_decile_list.append(top_decile_dataset)
    
full_top_decile_dataset = pd.concat(top_decile_list)    

all_trades = pd.concat(trade_list)

all_trades["pnl"] = (all_trades["return"]*-1)

all_trades["capital"] = 10000 + (10000 * (all_trades["pnl"]/100)).cumsum()

plt.figure(dpi=200, figsize=(10, 6))
plt.title(f"Ranking Metric: {ranking_criteria}")
plt.xticks(rotation=45)
plt.plot(pd.to_datetime(all_trades["date"]), all_trades["capital"])
plt.legend(["Gross PnL (No Transaction Costs)"])
plt.show()
plt.close()