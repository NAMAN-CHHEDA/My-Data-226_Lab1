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
