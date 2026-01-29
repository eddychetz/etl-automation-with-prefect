#!/usr/bin/env python
# coding: utf-8

# # TIME SERIES AUTOMATION PROJECT
# 
# ## Busisness Problem:
# 
#  - Forecasting **S&P500** prices with Modeltime and Prefect
# 
# ## Task:
#  - Build an automated trading system
# 
# ## Steps:
#  1. Get S&P 500 prices on a 5-minute interval
#  2. Store prices on a CSV file
#  3. Forecasting the movement
#  4. Store forecasted prices on a CSV file on every 60 seconds.
# 
# # ETL process of **S&P 500** prices data

# # GOALS
# - Add `prefect` and examine default cli logging

# ### RUN COMMAND

# Run this command `python etl_flow_with_prefect.py` on command line.*italicized text*
# 
# ## GOALS
# - Make a Deployment YAML file
# - Expose Scheduling to run the flow on an "Internet Schedule"
# - **IMPORTANT**: Interval Scheduler must be 60 seconds or greater (it must be this minimum for it to work)
# - Can also do `cron` schedule [MOST Common Automation]
# 
# 
# ## RESOURCES
#  https://docs.prefect.io/concepts/schedules/

# # LIBRARIES

import subprocess

def install(package):
    subprocess.check_call(["pip", "install", package])

# try to import package if it exist or else install package using pip
try:
    import prefect
except ImportError:
    install("prefect")

try:
    import yfinance
except ImportError:
    install("yfinance")

# Data manipulation
import pandas as pd

# Extracting stock data from yahoo finance website
import yfinance as yf

# ETL Automation process
from prefect import task, flow


# ## EXTRACT
# - Fetch S&P 500 stock prices from the `yfinance` API.
# - If fails, retry twice (3-second delay).
# 
# *We use the last 3 hours of S&P 500 stock prices to make 5 minute interval forecast using `modeltime`.*

@task(
    name="Extract SP500 Stock Prices",
    retries = 2,
    retry_delay_seconds=3
)

def extract_sp500_prices(
    tickers: str,
    period: str,
    interval: str
    ) -> pd.DataFrame:

    # Access yf API
    data = yf.download(
      tickers = tickers,
      period = period,
      interval = interval
    )
    data = pd.DataFrame(data)
    data.to_csv("./sp500_prices.csv")
    return data


# # TRANSFORM
@task
def transform(
    data: pd.DataFrame
    ) -> pd.DataFrame:
    
    return data


# # LOAD
# Store the S&P 500 stock price data in a CSV
@task
def load_sp500_prices(
    
    data: pd.DataFrame,
    path: str
) -> None:
    
    data.to_csv(
      path_or_buf=path,
      index=True
    )


# # PREFECT FLOW
# main_sp500_flow() parameters are now changeable
@flow(
    name="SP500 Stock Price Pipeline"
    )

def main_sp500_flow(
    tickers = "AAPL",
    period  = "3h",
    interval= "5m",
    path   = "./sp500_prices.csv"
    ):
    print(">>> Extracting S&P 500 Stock Prices")
    data = extract_sp500_prices(
          tickers=tickers,
          period=period,
          interval=interval
      )
    print(">>> Doing Transform")
    data = transform(data)
    print(f">>> Saving S&P 500 Stock Prices: {path}")
    load_sp500_prices(
          data=data,
          path=path
    )


# # MAIN PROGRAM
# 
# **GOALS**
# - Handle API failure (retries)
# - Move key parameters to `main_sp500_flow()`

# In[8]:


if __name__=="__main__":
    main_sp500_flow(
      tickers  = "AAPL",
      period   = "3h",
      interval = "5m",
      # WARNING: Relative paths won't work with deployments
      # Solution is to override the parameters in the
      # deployment.YAML file with the absolute path
      path     = "./sp500_prices.csv"
    )


# # TESTING
# `python etl_flow_with_prefect.py`
#
# # DEPLOYMENT STEPS & CLI COMMANDS:
###########################################

# 1. BUILD:
#     `prefect deployment build etl_flow_with_prefect.py:main_sp500_flow --name sp500_flow --interval 60`
#
# 2. PARAMETERS:
#     path: '/Users/Eddie/Desktop/etl_automation/sp500_prices.csv'
#
# 3. APPLY:
#     `prefect deployment apply main_sp500_flow-deployment.yaml`
#
# 4. LIST DEPLOYMENTS:
#     `prefect deployment ls`
#
# 5. RUN:
#     `prefect deployment run "SP500 Stock Price Pipeline/sp500_flow"`

# 6. ORION GUI:
#     `prefect orion start`
#
# 7. AGENT START: (on a new terminal)
#     `prefect agent start --work-queue "default"`
#
# 8. Ctrl + C to exit
#