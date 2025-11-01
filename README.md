# Lab 1: Building a Stock Price Prediction Analytics System using Snowflake & Airflow

![Python](https://img.shields.io/badge/Python-3.8%2B-blue)
![Snowflake](https://img.shields.io/badge/Snowflake-Cloud%20Data%20Warehouse-lightblue)
![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-Pipelines-orange)
![MIT License](https://img.shields.io/badge/License-MIT-green)

---

## Project Overview
This project implements a **Stock Price Prediction Analytics system** using **Python**, **Snowflake**, and **Apache Airflow**.  

The system extracts historical stock price data for companies like IBM, Cisco, Microsoft, Apple, NVIDIA, and Amazon using the **yfinance API**, processes the data through Airflow pipelines, and stores it in Snowflake.  

A **machine learning forecasting pipeline** predicts future stock prices. Finally, historical and forecasted data are combined into a single table for analytics and visualization.

---

## Problem Statement
Financial analysts require a system to **forecast short-term stock prices** using historical data. The challenge is to build an **automated, reliable, and scalable system** that:  
- Extracts stock price data  
- Stores data in a structured Snowflake database  
- Runs ML forecasting in-database  
- Combines historical and forecast data for analytics  

---

## System Requirements
- Python 3.8+  
- Snowflake account  
- Apache Airflow (Cloud Composer or Local setup)  
- Python packages:  
```bash
pip install apache-airflow-providers-snowflake yfinance pandas
```
## System Architecture

Workflow Diagram:
```
yfinance API → Airflow DAG 1 (ETL) → Snowflake RAW & STG tables → 
Airflow DAG 2 (ML Forecast) → ADHOC Forecast table → ANALYTICS final table
```
DAG 1: Extract & load historical stock data

DAG 2: Train ML Forecasting model and merge with ETL data

Snowflake Tables
Table	Description	Key Fields / Constraints
RAW.MARKET_DATA	Stores historical stock prices	SYMBOL (PK), DATE (PK), OPEN, HIGH, LOW, CLOSE, VOLUME
RAW.MARKET_DATA_STG	Staging for transactional merge	SYMBOL, DATE, OPEN, HIGH, LOW, CLOSE, VOLUME
ADHOC.MARKET_DATA_FORECAST	Stores 7-day forecasts	SYMBOL, DATE, PREDICTED_CLOSE
ANALYTICS.MARKET_DATA	Final analytics table (union of RAW + Forecast)	SYMBOL, DATE, ACTUAL_CLOSE, PREDICTED_CLOSE

Notes:

All ETL and Forecast pipelines use SQL transactions with try/except blocks to ensure data consistency.

Airflow Pipelines (DAGs)
1. ETL Pipeline (part1_create_and_load)

Extracts historical stock data using yfinance API

Loads data into RAW and RAW_STG tables

Implements transactional MERGE operations

2. ML Forecasting Pipeline (part2_train_predict)

Trains Snowflake ML Forecast model on historical data

Generates 7-day stock price forecasts

Merges forecast results with historical data to create ANALYTICS.MARKET_DATA

Both DAGs:

Use Airflow connections (snowflake_conn)

Use Airflow variables for symbols, lookback days, and table names

Airflow Configuration

Connections:

snowflake_conn → Add Snowflake credentials (user, password, account, warehouse, database)

Variables:
```SYMBOLS_JSON = ["IBM", "CSCO", "MSFT", "AAPL", "NVDA", "AMZN"]
LOOKBACK_DAYS = 180
HISTORICAL_TABLE = "RAW.MARKET_DATA"
```
##Code & Repository Structure
```
Lab1-Stock-Prediction/
│
├─ dags/
│   ├─ part1_create_and_load.py
│   └─ part2_train_predict.py
│
├─ README.md
├─ requirements.txt
└─ .gitignore
```
##Results & Findings
Automatically retrieves 180 days of historical stock data for all symbols
Generates 7-day forecasts using Snowflake ML Forecast
Combines historical and forecast data in ANALYTICS.MARKET_DATA
Airflow ensures automation, error handling, and transactional consistency
Snowflake provides scalable storage, fast queries, and in-database ML capabilities

##Future Enhancements
Incorporate deep learning models (LSTM) for improved prediction accuracy
Enable real-time stock streaming via WebSocket/Kafka
Add technical indicators for richer features
Integrate visualization tools (Tableau / Power BI)
Implement alerts for significant stock trends
