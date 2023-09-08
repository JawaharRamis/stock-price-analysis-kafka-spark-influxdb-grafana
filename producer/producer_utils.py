import sys
# append the path of the parent directory
sys.path.append("./")

import yfinance as yf
import os
import json
from dotenv import load_dotenv
load_dotenv()
import time
from logs.logger import setup_logger
from datetime import datetime, timedelta

def send_to_kafka(producer, topic, message):
    producer.produce(topic, json.dumps(message).encode("utf-8"))
    producer.flush()

def retrieve_historical_data(producer):
    # Define the stock symbol you want to retrieve historical data for
    stock_symbol = 'AMZN'  # Replace with your desired stock symbol

    # Define the date range for historical data (last 1 year)
    end_date = time.strftime('%Y-%m-%d')
    start_date = (yf.Ticker(stock_symbol).history(period="1y").index[0]).strftime('%Y-%m-%d')

    # Fetch historical data
    historical_data = yf.download(stock_symbol, start=start_date, end=end_date, interval="1h")

    # Convert and send historical data to Kafka
    for index, row in historical_data.iterrows():
        historical_data_point = {
            'date': index.strftime('%Y-%m-%d %H:%M:%S'),
            'open': row['Open'],
            'high': row['High'],
            'low': row['Low'],
            'close': row['Close'],
            'volume': row['Volume']
        }
        send_to_kafka(producer, 'historical-stock-prices', historical_data_point)
        # time.sleep(1)  # Sleep for 1 second between messages

def retrieve_real_time_data(producer, kafka_topic, logger):
    # Define the stock symbol for real-time data
    stock_symbol = 'AMZN'  # Replace with your desired stock symbol

    # First, retrieve historical data
    # retrieve_historical_data(producer)

    while True:
        # Fetch real-time data for the last 1 minute
        end_time = datetime.now()
        start_time = end_time - timedelta(days=1)
        real_time_data = yf.download(stock_symbol, start=start_time, end=end_time, interval="1m")

        if not real_time_data.empty:
            # Convert and send the latest real-time data point to Kafka
            latest_data_point = real_time_data.iloc[-1]
            real_time_data_point = {
                'date': latest_data_point.name.strftime('%Y-%m-%d %H:%M:%S'),
                'open': latest_data_point['Open'],
                'high': latest_data_point['High'],
                'low': latest_data_point['Low'],
                'close': latest_data_point['Close'],
                'volume': latest_data_point['Volume']
            }
            send_to_kafka(producer, kafka_topic, real_time_data_point)
            logger.info(f"Stock value retrieved and pushed to kafka topic {kafka_topic}")
        
        time.sleep(5)  # Sleep for 60 seconds (1 minute) between messages

def get_stock_details(producer, stock_symbol, kafka_topic, logger):
    try:
        # Create a Ticker object for the specified stock symbol
        ticker = yf.Ticker(stock_symbol)

        # Retrieve general stock information
        stock_info = {
            'Symbol': stock_symbol,
            'ShortName': ticker.info['shortName'],
            'LongName': ticker.info['longName'],
            'Industry': ticker.info['industry'],
            'Sector': ticker.info['sector'],
            'MarketCap': ticker.info['marketCap'],
            'ForwardPE': ticker.info['forwardPE'],
            'TrailingPE': ticker.info['trailingPE'],
            'Currency': ticker.info['currency'],
            'FiftyTwoWeekHigh': ticker.info['fiftyTwoWeekHigh'],
            'FiftyTwoWeekLow': ticker.info['fiftyTwoWeekLow'],
            'FiftyDayAverage': ticker.info['fiftyDayAverage'],
            'Exchange': ticker.info['exchange'],
            'ShortRatio': ticker.info['shortRatio']
        }
        print(stock_info)
        send_to_kafka(producer, kafka_topic, stock_info)
        logger.info(f"Stock information retrieved and pushed to kafka topic {kafka_topic}")
        return stock_info

    except Exception as e:
        return f"Error fetching stock details for {stock_symbol}: {str(e)}"

