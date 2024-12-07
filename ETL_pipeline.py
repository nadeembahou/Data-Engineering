import requests
import pandas as pd
import time
import logging
import traceback
from datetime import datetime, timedelta
import praw
from nltk.sentiment import SentimentIntensityAnalyzer
import nltk
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame

import os
import sys
sys.path.append(os.path.abspath('/home/project/logs')) 
from logging_setup import setup_logging

# Initialize logger
logger = setup_logging("ETL_pipeline")

# Initialize NLTK Sentiment Analyzer
nltk.download('vader_lexicon', quiet=True)
sia = SentimentIntensityAnalyzer()
logging.info("SentimentIntensityAnalyzer initialized.")

# Global throttle control
GLOBAL_THROTTLE = 30  # Global wait time for API rate limits

# Finnhub: Validate stock symbols
def validate_symbols_finnhub(symbols, api_key):
    validated_symbols = []
    ticker_table = []
    for symbol in symbols:
        url = f'https://finnhub.io/api/v1/stock/profile2?symbol={symbol}&token={api_key}'
        retries = 0
        while retries < 3:
            try:
                response = requests.get(url)
                if response.status_code == 200:
                    profile = response.json()
                    if 'name' in profile and profile['name']:  # Ensure the symbol has a valid name
                        validated_symbols.append(symbol)
                        ticker_table.append({"symbol": symbol, "name": profile['name']})
                        logging.info(f"Validated symbol: {symbol}")
                    break
                elif response.status_code == 429:
                    retries += 1
                    wait_time = GLOBAL_THROTTLE * (2 ** retries)
                    logging.warning(f"Rate limit hit for {symbol}. Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                else:
                    logging.error(f"Error validating symbol {symbol}: {response.status_code}")
                    break
            except Exception as e:
                logging.error(f"Error validating symbol {symbol}: {traceback.format_exc()}")
                break
        time.sleep(1)
    return validated_symbols, pd.DataFrame(ticker_table)

def get_date_range(news_df, reddit_df):
    # Combine all dates into a single series
    all_dates = pd.concat([
        news_df['published_at'],
        reddit_df['created_utc']
    ])
    # Drop invalid dates
    all_dates = all_dates.dropna()

    # Get the earliest and latest dates
    start_date = all_dates.min().strftime('%Y-%m-%d')
    end_date = all_dates.max().strftime('%Y-%m-%d')
    return start_date, end_date

def get_stock_data_alpaca(symbol, api_key, api_secret, start_date, end_date):
    try:
        # Initialize Alpaca client
        client = StockHistoricalDataClient(api_key, api_secret)

        # Request stock data
        request_params = StockBarsRequest(
            symbol_or_symbols=symbol,
            timeframe=TimeFrame.Day,
            start=start_date,
            end=end_date
        )
        bars = client.get_stock_bars(request_params).df

        if not bars.empty:
            bars['stock_symbol'] = symbol
            bars.reset_index(inplace=True)
            bars.rename(columns={
                'timestamp': 'date',
                'open': 'open_price',
                'high': 'high_price',
                'low': 'low_price',
                'close': 'close_price',
                'volume': 'volume'
            }, inplace=True)
            logging.info(f"Fetched stock data for {symbol}")
            return bars
        else:
            logging.warning(f"No stock data found for {symbol}.")
            return pd.DataFrame()

    except Exception as e:
        logging.error(f"Error fetching stock data for {symbol}: {traceback.format_exc()}")
        return pd.DataFrame()

# Metrics Calculation
def calculate_metrics(stock_data):
    if stock_data.empty:
        logging.warning("Stock data is empty. Metrics calculation skipped.")
        return pd.DataFrame()

    metrics_df = stock_data.copy()
    
    # Calculate moving averages
    metrics_df['moving_average_5'] = metrics_df.groupby('stock_symbol')['close_price'].transform(lambda x: x.rolling(5).mean())
    metrics_df['moving_average_10'] = metrics_df.groupby('stock_symbol')['close_price'].transform(lambda x: x.rolling(10).mean())
    
    # Calculate daily return
    metrics_df['daily_return'] = metrics_df.groupby('stock_symbol')['close_price'].transform(lambda x: x.pct_change() * 100)
    
    # Calculate daily price change
    metrics_df['daily_price_change'] = metrics_df.groupby('stock_symbol')['close_price'].transform(lambda x: x.diff())
    
    # Calculate volatility
    metrics_df['volatility'] = metrics_df.groupby('stock_symbol')['daily_return'].transform(lambda x: x.rolling(5).std())
    
    logging.info("Metrics calculated successfully.")
    
    return metrics_df[['stock_symbol', 'date', 'moving_average_5', 'moving_average_10', 'daily_return', 'daily_price_change', 'volatility']].dropna()


# Fetch News Articles
def get_news_articles(symbols, api_key, max_retries=3):
    news_data = []
    for symbol in symbols:
        url = f'https://newsapi.org/v2/everything'
        params = {'q': symbol, 'language': 'en', 'pageSize': 5, 'apiKey': api_key}
        retries = 0
        while retries <= max_retries:
            try:
                response = requests.get(url, params=params)
                if response.status_code == 200:
                    articles = response.json().get('articles', [])
                    for article in articles:
                        # Skip articles containing '[Removed]'
                        if '[Removed]' in (article.get('title'), article.get('description'), article.get('url'), article.get('publishedAt')):
                            continue
                        news_data.append({
                            'symbol': symbol,
                            'title': article.get('title'),
                            'description': article.get('description'),
                            'url': article.get('url'),
                            'published_at': article.get('publishedAt'),
                            'source_name': article.get('source', {}).get('name')
                        })
                    logging.info(f"Fetched news for {symbol}.")
                    break
                elif response.status_code == 429:
                    retries += 1
                    wait_time = 2 ** retries  # Exponential backoff
                    logging.warning(f"Rate limit hit for {symbol}. Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                else:
                    logging.warning(f"Error fetching news for {symbol}: {response.status_code}")
                    break
            except Exception as e:
                logging.error(f"Error fetching news for {symbol}: {traceback.format_exc()}")
                break
        time.sleep(1)  # Add a delay between symbols

    # Convert to DataFrame
    news_df = pd.DataFrame(news_data)

    # Convert 'published_at' to datetime
    if not news_df.empty:
        news_df['published_at'] = pd.to_datetime(news_df['published_at'], errors='coerce')
        news_df = news_df.dropna(subset=['published_at'])

    return news_df


# Fetch Reddit Posts
def get_reddit_posts(symbols, names, client_id, client_secret, user_agent):
    reddit = praw.Reddit(client_id=client_id, client_secret=client_secret, user_agent=user_agent)
    posts_data = []

    for symbol, name in zip(symbols, names):
        queries = [symbol, name]
        for query in queries:
            try:
                logging.info(f"Fetching Reddit posts for query: {query}")
                subreddit = reddit.subreddit('stocks')
                for post in subreddit.search(query, limit=5):
                    sentiment = sia.polarity_scores(post.selftext or "")
                    # Skip articles containing '[Removed]'
                    if '[Removed]' in (post.title, post.selftext):
                        continue
                    posts_data.append({
                        'post_id': post.id,
                        'created_utc': datetime.utcfromtimestamp(post.created_utc),
                        'title': post.title,
                        'selftext': post.selftext,
                        'subreddit': post.subreddit.display_name,
                        'author': post.author.name if post.author else None,
                        'score': post.score,  # Combined upvotes/downvotes
                        'num_comments': post.num_comments,
                        'company_name': name,
                        'ticker': symbol,
                        'sentiment_score': sentiment['compound'],
                        'positive': sentiment['pos'],
                        'negative': sentiment['neg'],
                        'neutral': sentiment['neu']
                    })
                if posts_data:
                    break
            except Exception as e:
                logging.error(f"Error fetching Reddit posts for query '{query}': {traceback.format_exc()}")
            time.sleep(2)
    return pd.DataFrame(posts_data)

# Main Execution
if __name__ == '__main__':
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

    # Validate symbols
    validated_symbols, ticker_table = validate_symbols_finnhub(symbols, API_KEY_FINNHUB)

    # Fetch news articles
    news_data = get_news_articles(validated_symbols, API_KEY_NEWS)

    # Fetch Reddit posts
    reddit_data = get_reddit_posts(symbols, names, CLIENT_ID, CLIENT_SECRET, USER_AGENT)

    # Determine date range based on news and Reddit data
    if not news_data.empty or not reddit_data.empty:
        start_date, end_date = get_date_range(news_data, reddit_data)
        logging.info(f"Date range for stock data: {start_date} to {end_date}")
    else:
        logging.warning("No news or Reddit data available to determine date range.")
        start_date = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
        end_date = datetime.now().strftime('%Y-%m-%d')

    # Fetch stock data using Alpaca
    stock_data_list = [
        get_stock_data_alpaca(symbol, API_KEY_ALPACA, API_SECRET_ALPACA, start_date, end_date)
        for symbol in validated_symbols
    ]
    stock_data = pd.concat(stock_data_list, ignore_index=True) if stock_data_list else pd.DataFrame()

    # Calculate metrics
    metrics_data = calculate_metrics(stock_data)

    # Print Ticker Table
    print("\nTicker Data (First 12 Rows):")
    if not ticker_table.empty:
        print(ticker_table.head(12).to_string(index=False))
    else:
        print("No ticker data available.")

    # Print Stock Data
    print("\nStock Data (First 12 Rows):")
    if not stock_data.empty:
        print(stock_data.head(12).to_string(index=False))
    else:
        print("No stock data available.")

    # Print Metrics Data
    print("\nMetrics Data (First 12 Rows):")
    if not metrics_data.empty:
        print(metrics_data.head(12).to_string(index=False))
    else:
        print("No metrics data available.")

    # Print News Articles
    print("\nNews Articles (First 12 Rows):")
    if not news_data.empty:
        print(news_data[['symbol', 'title', 'description', 'published_at']].head(12).to_string(index=False))
    else:
        print("No news articles available.")

    if not reddit_data.empty:
        print("\nReddit Posts with Detailed Sentiment (First 12 Rows):")
        print(reddit_data[['symbol', 'title', 'selftext', 'sentiment', 'positive', 'negative', 'neutral']].head(12).to_string(index=False))
    else:
        print("No Reddit posts available.")

