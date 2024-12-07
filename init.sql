-- SQL script to initialize the database with tables

-- Create a schema for the tables
CREATE SCHEMA IF NOT EXISTS analytics;

-- 1. company_tickers Table
CREATE TABLE IF NOT EXISTS analytics.company_tickers (
    symbol VARCHAR(10) PRIMARY KEY,
    name VARCHAR(255)
);

-- 2. stock_data Table
CREATE TABLE IF NOT EXISTS analytics.stock_data (
    stock_symbol VARCHAR(10) NOT NULL,
    date DATE NOT NULL,
    open_price DECIMAL(10, 2),
    high_price DECIMAL(10, 2),
    low_price DECIMAL(10, 2),
    close_price DECIMAL(10, 2),
    volume BIGINT,
    PRIMARY KEY (stock_symbol, date),
    FOREIGN KEY (stock_symbol) REFERENCES analytics.company_tickers(symbol)
);

-- 3. news_articles Table
CREATE TABLE IF NOT EXISTS analytics.news_articles (
    symbol VARCHAR(10),
    published_at TIMESTAMP,
    title TEXT,
    description TEXT,
    source_name VARCHAR(255),
    url TEXT,
    PRIMARY KEY (symbol, published_at)
);

-- 4. reddit_posts Table
CREATE TABLE IF NOT EXISTS analytics.reddit_posts (
    post_id VARCHAR(255) PRIMARY KEY,
    created_utc TIMESTAMP,
    title TEXT,
    selftext TEXT,
    subreddit VARCHAR(50),
    author VARCHAR(50),
    score INT,
    num_comments INT,
    company_name VARCHAR(255),
    ticker VARCHAR(10),
    sentiment_score FLOAT,
    positive FLOAT,
    negative FLOAT,
    neutral FLOAT,
    FOREIGN KEY (ticker) REFERENCES analytics.company_tickers(symbol)
);

-- 5. stock_metrics Table
CREATE TABLE IF NOT EXISTS analytics.stock_metrics (
    stock_symbol VARCHAR(10),
    date DATE,
    moving_average_5 FLOAT,
    moving_average_10 FLOAT,
    daily_return FLOAT,
    daily_price_change FLOAT,
    volatility FLOAT,
    PRIMARY KEY (stock_symbol, date),
    FOREIGN KEY (stock_symbol) REFERENCES analytics.company_tickers(symbol)
);
