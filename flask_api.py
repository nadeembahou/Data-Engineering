from flask import Flask, jsonify, request, render_template_string
from sqlalchemy import create_engine, text
import logging
import sys
import os
sys.path.append(os.path.abspath('/home/project/logs')) 
from logging_setup import setup_logging

app = Flask(__name__)

# Database connection
DATABASE_URI = "postgresql+psycopg2://project:jhu@postgres:5432/project_db?options=-csearch_path=analytics"
engine = create_engine(DATABASE_URI)

# Initialize logger
logger = setup_logging("flask_api")

# HTML Templates
HOME_TEMPLATE = """
<html>
    <head>
        <title>Stock Analytics API</title>
    </head>
    <body>
        <h1>Welcome to the Stock Analytics API</h1>
        <p>Explore the following endpoints:</p>
        <ul>
            <li><a href="/api/metrics/AAPL">/api/metrics/&lt;symbol&gt;</a>: Get stock metrics for the given symbol</li>
            <li><a href="/api/news/AAPL">/api/news/&lt;symbol&gt;</a>: Get news for the given symbol</li>
            <li><a href="/api/reddit/AAPL">/api/reddit/&lt;symbol&gt;</a>: Get Reddit posts for the given symbol</li>
            <li><a href="/api/summary/AAPL">/api/summary/&lt;symbol&gt;</a>: Get a summary of metrics, news, and Reddit posts for the given symbol</li>
            <li><a href="/api/sentiment_trend/AAPL">/api/sentiment_trend/&lt;symbol&gt;</a>: Get sentiment trend for the given symbol</li>
            <li><a href="/api/compare_metrics?symbols=AAPL,MSFT">/api/compare_metrics</a>: Compare stock metrics across multiple symbols (query parameter: symbols)</li>
        </ul>
        <form action="/search" method="get">
            <label for="symbol">Search for a symbol:</label>
            <input type="text" id="symbol" name="symbol" placeholder="Enter symbol (e.g., AAPL)">
            <button type="submit">Search</button>
        </form>
        <h2>Available Tickers</h2>
        <table border="1">
            <thead>
                <tr>
                    <th>Symbol</th>
                    <th>Name</th>
                </tr>
            </thead>
            <tbody>
                {% for ticker in tickers %}
                <tr>
                    <td>{{ ticker['symbol'] }}</td>
                    <td>{{ ticker['name'] }}</td>
                </tr>
                {% endfor %}
            </tbody>
        </table>
    </body>
</html>
"""

METRICS_TEMPLATE = """
<html>
    <head>
        <title>Metrics for {{ symbol }}</title>
    </head>
    <body>
        <h1>Metrics for {{ symbol }}</h1>
        <table border="1">
            <thead>
                <tr>
                    <th>Date</th>
                    <th>Moving Average (5)</th>
                    <th>Moving Average (10)</th>
                    <th>Daily Return</th>
                    <th>Price Change</th>
                    <th>Volatility</th>
                </tr>
            </thead>
            <tbody>
                {% for row in metrics %}
                <tr>
                    <td>{{ row['date'] }}</td>
                    <td>{{ row['moving_average_5']|round(2) }}</td>
                    <td>{{ row['moving_average_10']|round(2) }}</td>
                    <td>{{ row['daily_return']|round(2) }}</td>
                    <td>{{ row['daily_price_change']|round(2) }}</td>
                    <td>{{ row['volatility']|round(2) }}</td>
                </tr>
                {% endfor %}
            </tbody>
        </table>
        <a href="javascript:history.back()">Go Back</a><br>
        <a href="/">Back to Home</a>
    </body>
</html>
"""

NEWS_TEMPLATE = """
<html>
    <head>
        <title>News for {{ symbol }}</title>
    </head>
    <body>
        <h1>News for {{ symbol }}</h1>
        <table border="1">
            <thead>
                <tr>
                    <th>Published At</th>
                    <th>Title</th>
                    <th>Source</th>
                    <th>URL</th>
                </tr>
            </thead>
            <tbody>
                {% for row in news %}
                <tr>
                    <td>{{ row['published_at'] }}</td>
                    <td>{{ row['title'] }}</td>
                    <td>{{ row['source_name'] }}</td>
                    <td><a href="{{ row['url'] }}" target="_blank">Link</a></td>
                </tr>
                {% endfor %}
            </tbody>
        </table>
        <a href="javascript:history.back()">Go Back</a><br>
        <a href="/">Back to Home</a>
    </body>
</html>
"""

REDDIT_TEMPLATE = """
<html>
    <head>
        <title>Reddit Posts for {{ symbol }}</title>
    </head>
    <body>
        <h1>Reddit Posts for {{ symbol }}</h1>
        <table border="1">
            <thead>
                <tr>
                    <th>Created At</th>
                    <th>Title</th>
                    <th>Score</th>
                    <th>Comments</th>
                </tr>
            </thead>
            <tbody>
                {% for row in reddit %}
                <tr>
                    <td>{{ row['created_utc'] }}</td>
                    <td>{{ row['title'] }}</td>
                    <td>{{ row['score']|round(2) }}</td>
                    <td>{{ row['num_comments']|round(2) }}</td>
                </tr>
                {% endfor %}
            </tbody>
        </table>
        <a href="javascript:history.back()">Go Back</a><br>
        <a href="/">Back to Home</a>
    </body>
</html>
"""

SUMMARY_TEMPLATE = """
<html>
    <head>
        <title>Summary for {{ symbol }}</title>
    </head>
    <body>
        <h1>Summary for {{ symbol }}</h1>
        <h2>Metrics</h2>
        <table border="1">
            <thead>
                <tr>
                    <th>Date</th>
                    <th>Moving Average (5)</th>
                    <th>Moving Average (10)</th>
                    <th>Daily Return</th>
                    <th>Price Change</th>
                    <th>Volatility</th>
                </tr>
            </thead>
            <tbody>
                {% for row in summary["metrics"] %}
                <tr>
                    <td>{{ row['date'] }}</td>
                    <td>{{ row['moving_average_5']|round(2) }}</td>
                    <td>{{ row['moving_average_10']|round(2)}}</td>
                    <td>{{ row['daily_return']|round(2) }}</td>
                    <td>{{ row['daily_price_change']|round(2) }}</td>
                    <td>{{ row['volatility']|round(2) }}</td>
                </tr>
                {% endfor %}
            </tbody>
        </table>
        <h2>News</h2>
        <table border="1">
            <thead>
                <tr>
                    <th>Published At</th>
                    <th>Title</th>
                    <th>Source</th>
                    <th>URL</th>
                </tr>
            </thead>
            <tbody>
                {% for row in summary["news"] %}
                <tr>
                    <td>{{ row['published_at'] }}</td>
                    <td>{{ row['title'] }}</td>
                    <td>{{ row['source_name'] }}</td>
                    <td><a href="{{ row['url'] }}" target="_blank">Link</a></td>
                </tr>
                {% endfor %}
            </tbody>
        </table>
        <h2>Reddit Posts</h2>
        <table border="1">
            <thead>
                <tr>
                    <th>Created At</th>
                    <th>Title</th>
                    <th>Score</th>
                    <th>Comments</th>
                </tr>
            </thead>
            <tbody>
                {% for row in summary["reddit"] %}
                <tr>
                    <td>{{ row['created_utc'] }}</td>
                    <td>{{ row['title'] }}</td>
                    <td>{{ row['score']|round(2) }}</td>
                    <td>{{ row['num_comments']|round(2) }}</td>
                </tr>
                {% endfor %}
            </tbody>
        </table>
        <a href="javascript:history.back()">Go Back</a><br>
        <a href="/">Back to Home</a>
    </body>
</html>
"""

TREND_TEMPLATE = """
<html>
    <head>
        <title>Sentiment Trend for {{ symbol }}</title>
    </head>
    <body>
        <h1>Sentiment Trend for {{ symbol }}</h1>
        <table border="1">
            <thead>
                <tr>
                    <th>Date</th>
                    <th>Average Sentiment</th>
                    <th>Average Reddit Score</th>
                </tr>
            </thead>
            <tbody>
                {% for row in trend %}
                <tr>
                    <td>{{ row['sentiment_date'] }}</td>
                    <td>{{ row['average_sentiment']|round(2) }}</td>
                    <td>{{ row['avg_reddit_score']|round(2) }}</td>
                </tr>
                {% endfor %}
            </tbody>
        </table>
        <a href="javascript:history.back()">Go Back</a><br>
        <a href="/">Back to Home</a>
    </body>
</html>
"""

COMPARE_TEMPLATE = """
<html>
    <head>
        <title>Compare Metrics</title>
    </head>
    <body>
        <h1>Compare Metrics</h1>
        <form method="post">
            <label for="symbols">Enter stock symbols (comma-separated):</label><br>
            <input type="text" id="symbols" name="symbols" placeholder="e.g., AAPL,MSFT,NVDA">
            <button type="submit">Compare</button>
        </form>
        <h2>Comparison Results</h2>
        {% if comparison %}
        <table border="1">
            <thead>
                <tr>
                    <th>Symbol</th>
                    <th>Average Moving Average (5)</th>
                    <th>Average Volatility</th>
                </tr>
            </thead>
            <tbody>
                {% for row in comparison %}
                <tr>
                    <td>{{ row['stock_symbol'] }}</td>
                    <td>{{ row['avg_moving_average_5']|round(2) }}</td>
                    <td>{{ row['avg_volatility']|round(2) }}</td>
                </tr>
                {% endfor %}
            </tbody>
        </table>
        {% else %}
        <p>No data available for the provided symbols.</p>
        {% endif %}
        <a href="/">Back to Home</a>
    </body>
</html>
"""

@app.route('/', methods=['GET'])
def home():
    try:
        # Fetch the available tickers from the database
        query = text("SELECT symbol, name FROM company_tickers")
        with engine.connect() as conn:
            result = conn.execute(query)
            tickers = [dict(row) for row in result]
        return render_template_string(HOME_TEMPLATE, tickers=tickers)
    except Exception as e:
        logging.error(f"Error fetching available tickers: {e}")
        return """
        <html>
            <head><title>Error</title></head>
            <body>
                <h1>Error fetching available tickers</h1>
                <p>Please try again later.</p>
                <a href="/">Back to Home</a>
            </body>
        </html>
        """

@app.route('/search', methods=['GET'])
def search():
    symbol = request.args.get('symbol', '').upper()
    if symbol:
        return f"""
        <html>
            <head><title>Search Results</title></head>
            <body>
                <h1>Search Results for {symbol}</h1>
                <ul>
                    <li><a href="/api/metrics/{symbol}">Stock Metrics</a></li>
                    <li><a href="/api/news/{symbol}">News</a></li>
                    <li><a href="/api/reddit/{symbol}">Reddit Posts</a></li>
                    <li><a href="/api/summary/{symbol}">Summary</a></li>
                    <li><a href="/api/sentiment_trend/{symbol}">Sentiment Trend</a></li>
                </ul>
                <a href="/">Back to Home</a>
            </body>
        </html>
        """
    else:
        return """
        <html>
            <head><title>Search Error</title></head>
            <body>
                <h1>Error: No symbol provided</h1>
                <a href="/">Back to Home</a>
            </body>
        </html>
        """

@app.route('/api/metrics/<symbol>', methods=['GET'])
def get_metrics(symbol):
    try:
        query = text("SELECT * FROM stock_metrics WHERE stock_symbol = :symbol ORDER BY date DESC LIMIT 10")
        with engine.connect() as conn:
            result = conn.execute(query, {"symbol": symbol})
            metrics = [dict(row) for row in result]
        return render_template_string(METRICS_TEMPLATE, symbol=symbol, metrics=metrics)
    except Exception as e:
        logging.error(f"Error fetching metrics for {symbol}: {e}")
        return jsonify({"error": "Could not fetch metrics"}), 500

@app.route('/api/news/<symbol>', methods=['GET'])
def get_news(symbol):
    try:
        query = text("SELECT * FROM news_articles WHERE symbol = :symbol ORDER BY published_at DESC LIMIT 10")
        with engine.connect() as conn:
            result = conn.execute(query, {"symbol": symbol})
            news = [dict(row) for row in result]
        return render_template_string(NEWS_TEMPLATE, symbol=symbol, news=news)
    except Exception as e:
        logging.error(f"Error fetching news for {symbol}: {e}")
        return jsonify({"error": "Could not fetch news"}), 500

@app.route('/api/reddit/<symbol>', methods=['GET'])
def get_reddit(symbol):
    try:
        query = text("SELECT created_utc, title, score, num_comments, score FROM reddit_posts WHERE ticker = :symbol ORDER BY created_utc DESC LIMIT 10")
        with engine.connect() as conn:
            result = conn.execute(query, {"symbol": symbol})
            reddit = [dict(row) for row in result]
        return render_template_string(REDDIT_TEMPLATE, symbol=symbol, reddit=reddit)
    except Exception as e:
        logging.error(f"Error fetching Reddit posts for {symbol}: {e}")
        return jsonify({"error": "Could not fetch Reddit posts"}), 500

@app.route('/api/summary/<symbol>', methods=['GET'])
def get_summary(symbol):
    try:
        summary = {}
        metrics_query = text("SELECT * FROM stock_metrics WHERE stock_symbol = :symbol ORDER BY date DESC LIMIT 10")
        news_query = text("SELECT * FROM news_articles WHERE symbol = :symbol ORDER BY published_at DESC LIMIT 5")
        reddit_query = text("SELECT * FROM reddit_posts WHERE ticker = :symbol ORDER BY created_utc DESC LIMIT 5")
        
        with engine.connect() as conn:
            metrics_result = conn.execute(metrics_query, {"symbol": symbol})
            news_result = conn.execute(news_query, {"symbol": symbol})
            reddit_result = conn.execute(reddit_query, {"symbol": symbol})

            summary["metrics"] = [dict(row) for row in metrics_result]
            summary["news"] = [dict(row) for row in news_result]
            summary["reddit"] = [dict(row) for row in reddit_result]
        
        return render_template_string(SUMMARY_TEMPLATE, symbol=symbol, summary=summary)
    except Exception as e:
        logging.error(f"Error fetching summary for {symbol}: {e}")
        return """
        <html>
            <head><title>Error</title></head>
            <body>
                <h1>Error fetching summary for {{ symbol }}</h1>
                <p>Please try again later.</p>
                <a href="/">Back to Home</a>
            </body>
        </html>
        """

@app.route('/api/sentiment_trend/<symbol>', methods=['GET'])
def get_sentiment_trend(symbol):
    try:
        query = text("""
            SELECT DATE(created_utc) AS sentiment_date,
                   AVG(sentiment_score) AS average_sentiment,
                   AVG(score) AS avg_reddit_score
            FROM reddit_posts
            WHERE ticker = :symbol
            GROUP BY sentiment_date
            ORDER BY sentiment_date DESC
            LIMIT 10
        """)
        with engine.connect() as conn:
            result = conn.execute(query, {"symbol": symbol})
            trend = [dict(row) for row in result]
        return render_template_string(TREND_TEMPLATE, symbol=symbol, trend=trend)
    except Exception as e:
        logging.error(f"Error fetching sentiment trend for {symbol}: {e}")
        return jsonify({"error": "Could not fetch sentiment trend"}), 500

@app.route('/api/compare_metrics', methods=['GET', 'POST'])
def compare_metrics():
    try:
        if request.method == 'POST':
            symbols = request.form.get('symbols', '').split(',')
        else:
            symbols = request.args.get('symbols', '').split(',')
        
        # Ensure symbols are cleaned and uppercase
        symbols = [symbol.strip().upper() for symbol in symbols if symbol.strip()]
        
        query = text("""
            SELECT stock_symbol, 
                   AVG(moving_average_5) AS avg_moving_average_5, 
                   AVG(volatility) AS avg_volatility
            FROM stock_metrics
            WHERE stock_symbol = ANY(:symbols)
            GROUP BY stock_symbol
        """)
        
        with engine.connect() as conn:
            result = conn.execute(query, {"symbols": symbols})
            comparison = [dict(row) for row in result]
        
        # If no symbols are provided, return an empty comparison
        if not symbols or not comparison:
            comparison = []
        return render_template_string(COMPARE_TEMPLATE, comparison=comparison)
    except Exception as e:
        logging.error(f"Error comparing metrics: {e}")
        return """
        <html>
            <head><title>Error</title></head>
            <body>
                <h1>Error comparing metrics</h1>
                <p>Please try again later.</p>
                <a href="/">Back to Home</a>
            </body>
        </html>
        """
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001)
