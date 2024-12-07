from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import create_engine
from ETL_pipeline import (
    validate_symbols_finnhub,
    get_stock_data_alpaca,
    calculate_metrics,
    get_news_articles,
    get_reddit_posts
)
import sys
import logging
import os
sys.path.append(os.path.abspath('/home/project/logs')) 
from logging_setup import setup_logging

# Initialize logger
logger = setup_logging("data_engineering_pipeline_dag")

# Set SQLAlchemy logging level
logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)

# Example usage
logger.info("Data engineering pipeline DAG initialized.")

# API credentials
API_KEY_FINNHUB = 'csrpp41r01qj3u0os1a0csrpp41r01qj3u0os1ag'
API_KEY_ALPACA = 'PK8CD81HSACAEOWCME0N'
API_SECRET_ALPACA = 'xOK87P6C3nvkkLaiagR5MSGWi1jIknpWqbPA8XUN'
API_KEY_NEWS = '3060f04959c64b1a9ac96ca8cdddb6ce'
CLIENT_ID = 'ueLaC2VMtGQzkZcYZ5KbYw'
CLIENT_SECRET = '2Y5ztfTDaV3KxJq66s9Tw1qjyUQb0Q'
USER_AGENT = 'Living_Figure_4023'

# Company list
companies = [
    {"symbol": "AAPL", "name": "Apple Inc."},
    {"symbol": "MSFT", "name": "Microsoft Corporation"},
    {"symbol": "NVDA", "name": "NVIDIA Corporation"},
    {"symbol": "GOOGL", "name": "Alphabet Inc."},
    {"symbol": "AMZN", "name": "Amazon.com Inc."},
    {"symbol": "TSLA", "name": "Tesla, Inc."},
    {"symbol": "META", "name": "Meta Platforms, Inc."},
    {"symbol": "BRK.B", "name": "Berkshire Hathaway Inc."},
    {"symbol": "TSM", "name": "Taiwan Semiconductor Manufacturing Company"},
    {"symbol": "LLY", "name": "Eli Lilly and Company"},
    {"symbol": "AVGO", "name": "Broadcom Inc."},
    {"symbol": "JPM", "name": "JPMorgan Chase & Co."},
    {"symbol": "NVO", "name": "Novo Nordisk A/S"},
    {"symbol": "WMT", "name": "Walmart Inc."},
    {"symbol": "UNH", "name": "UnitedHealth Group Incorporated"},
    {"symbol": "XOM", "name": "Exxon Mobil Corporation"},
    {"symbol": "V", "name": "Visa Inc."},
    {"symbol": "MA", "name": "Mastercard Incorporated"},
    {"symbol": "PG", "name": "Procter & Gamble Co."},
    {"symbol": "ORCL", "name": "Oracle Corporation"},
    {"symbol": "ASML", "name": "ASML Holding N.V."},
    {"symbol": "SHEL", "name": "Shell plc"},
    {"symbol": "KO", "name": "The Coca-Cola Company"},
    {"symbol": "PEP", "name": "PepsiCo, Inc."},
    {"symbol": "CSCO", "name": "Cisco Systems, Inc."},
]
symbols = [company['symbol'] for company in companies]
names = [company['name'] for company in companies]

# Default args for DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# PostgreSQL connection
DATABASE_URI = "postgresql+psycopg2://project:jhu@postgres:5432/project_db?options=-csearch_path=analytics"
engine = create_engine(DATABASE_URI)

# DAG definition
dag = DAG(
    'data_engineering_pipeline',
    default_args=default_args,
    description='ETL pipeline for stocks, news, Reddit, and company data',
    schedule_interval='@hourly',  # Run once every hour
    start_date=datetime(2024, 11, 16),
    catchup=False,
)


def validate_symbols():
    try:
        logging.info("Starting validate_symbols task")
        validated_symbols, ticker_table = validate_symbols_finnhub(symbols, API_KEY_FINNHUB)

        # Insert company tickers into the database
        insert_query = """
        INSERT INTO company_tickers (symbol, name)
        VALUES (%s, %s)
        ON CONFLICT (symbol) DO NOTHING
        """
        data = [tuple(row) for row in ticker_table.itertuples(index=False, name=None)]
        with engine.connect() as conn:
            conn.execute(insert_query, data)

        logging.info("Validated symbols and updated company_tickers table")
        return validated_symbols
    except Exception as e:
        logging.error(f"Error in validate_symbols: {e}")
        raise


def fetch_stock_data(**kwargs):
    try:
        ti = kwargs['ti']

        # Pull validated symbols
        validated_symbols = ti.xcom_pull(task_ids='validate_symbols')

        # Fetch news and Reddit data to determine date range
        with engine.connect() as conn:
            news_data = pd.read_sql('SELECT * FROM news_articles', con=conn.connection)
            reddit_data = pd.read_sql('SELECT * FROM reddit_posts', con=conn.connection)

        # Calculate the dynamic date range
        start_date, end_date = get_date_range_from_news_and_reddit(news_data, reddit_data)

        # Fetch stock data using Alpaca for the calculated date range
        stock_data_list = [
            get_stock_data_alpaca(symbol, API_KEY_ALPACA, API_SECRET_ALPACA, start_date, end_date)
            for symbol in validated_symbols
        ]
        stock_data = pd.concat(stock_data_list, ignore_index=True) if stock_data_list else pd.DataFrame()

        if stock_data.empty:
            logging.warning("No stock data fetched. Skipping database insertion.")
            return

        # Ensure the data matches the updated schema
        required_columns = [
            'stock_symbol', 'date', 'open_price', 'high_price', 'low_price', 
            'close_price', 'volume'
        ]
        stock_data = stock_data[required_columns]

        # Insert stock data into the database
        insert_query = """
        INSERT INTO stock_data (
            stock_symbol, date, open_price, high_price, low_price, close_price, volume
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (stock_symbol, date) DO NOTHING
        """
        data = [tuple(row) for row in stock_data.itertuples(index=False, name=None)]

        with engine.connect() as conn:
            conn.connection.cursor().executemany(insert_query, data)
            conn.connection.commit()

        logging.info("Stock data fetched and written to stock_data table")
    except Exception as e:
        logging.error(f"Error in fetch_stock_data: {e}")
        raise


def calculate_metrics_task():
    try:
        with engine.connect() as conn:
            # Fetch stock data
            stock_data = pd.read_sql('SELECT * FROM stock_data', con=conn.connection)

            # Calculate metrics
            metrics_data = calculate_metrics(stock_data)

            # Verify the structure of the metrics_data DataFrame
            logging.info(f"Metrics DataFrame columns: {metrics_data.columns}")
            logging.info(f"Metrics DataFrame sample: {metrics_data.head()}")

            # Prepare the data for insertion
            data = [tuple(row) for row in metrics_data.itertuples(index=False, name=None)]

            # Log sample data for debugging
            logging.info(f"Sample data for insertion: {data[:5]}")

            # Insert metrics into the database using executemany
            insert_query = """
            INSERT INTO stock_metrics (stock_symbol, date, moving_average_5, moving_average_10, daily_return, daily_price_change, volatility)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (stock_symbol, date) DO NOTHING
            """
            with conn.connection.cursor() as cursor:
                cursor.executemany(insert_query, data)
                conn.connection.commit()

        logging.info("Metrics calculated and written to stock_metrics table")
    except Exception as e:
        logging.error(f"Error in calculate_metrics_task: {e}")
        raise

def fetch_news():
    try:
        # Fetch the news data
        news_data = get_news_articles(symbols, API_KEY_NEWS)

        if news_data.empty:
            logging.warning("No news data fetched. Skipping news insertion.")
            return

        # Ensure columns match the expected schema
        required_columns = ['symbol', 'published_at', 'title', 'description', 'source_name', 'url']
        news_data = news_data[required_columns]

        # Insert data into the database
        data = [tuple(row) for row in news_data.itertuples(index=False, name=None)]

        insert_query = """
        INSERT INTO news_articles (
            symbol, published_at, title, description, source_name, url
        )
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING;
        """
        with engine.connect() as conn:
            conn.execute(insert_query, data)

        logging.info("News articles successfully written to the database.")
    except Exception as e:
        logging.error(f"Error in fetch_news: {e}")
        raise


def fetch_reddit():
    try:
        reddit_data = get_reddit_posts(symbols, names, CLIENT_ID, CLIENT_SECRET, USER_AGENT)

        # Insert Reddit posts into the database
        insert_query = """
        INSERT INTO reddit_posts (
            post_id, created_utc, title, selftext, subreddit, author, score, 
            num_comments, company_name, ticker, sentiment_score, positive, 
            negative, neutral
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (post_id) DO NOTHING;
        """
        data = [tuple(row) for row in reddit_data.itertuples(index=False, name=None)]
        with engine.connect() as conn:
            conn.execute(insert_query, data)

        logging.info("Reddit posts fetched and written to reddit_posts table")
    except Exception as e:
        logging.error(f"Error in fetch_reddit: {e}")
        raise


def get_date_range_from_news_and_reddit(news_df, reddit_df):
    all_dates = pd.concat([
        news_df['published_at'],
        reddit_df['created_utc']
    ])
    all_dates = all_dates.dropna()  # Drop invalid dates
    start_date = all_dates.min().strftime('%Y-%m-%d')
    end_date = all_dates.max().strftime('%Y-%m-%d')
    return start_date, end_date


# Define tasks
validate_symbols_task = PythonOperator(
    task_id='validate_symbols',
    python_callable=validate_symbols,
    dag=dag,
)

fetch_stock_data_task = PythonOperator(
    task_id='fetch_stock_data',
    python_callable=fetch_stock_data,
    provide_context=True,
    dag=dag,
)

calculate_metrics_operator = PythonOperator(
    task_id='calculate_metrics',
    python_callable=calculate_metrics_task,
    dag=dag,
)

fetch_news_task = PythonOperator(
    task_id='fetch_news',
    python_callable=fetch_news,
    dag=dag,
)

fetch_reddit_task = PythonOperator(
    task_id='fetch_reddit',
    python_callable=fetch_reddit,
    dag=dag,
)

# Define dependencies
validate_symbols_task >> [fetch_news_task, fetch_reddit_task]
[fetch_news_task, fetch_reddit_task] >> fetch_stock_data_task
fetch_stock_data_task >> calculate_metrics_operator