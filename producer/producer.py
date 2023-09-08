import sys
from pathlib import Path

path_to_utils = Path(__file__).parent.parent
sys.path.insert(0, str(path_to_utils))

from confluent_kafka import Producer
import yfinance as yf
import os
import json
from dotenv import load_dotenv
load_dotenv()
import time
from logs.logger import setup_logger
from datetime import datetime, timedelta

from producer_utils import retrieve_real_time_data, get_stock_details

kafka_bootstrap_servers = "localhost:9094"
kafka_config = {
    "bootstrap.servers": kafka_bootstrap_servers,
}
producer = Producer(kafka_config)

if __name__ == '__main__':
    logger = setup_logger(__name__, 'producer.log')
    stock_details = get_stock_details(producer, 'AMZN', 'stock-general-information', logger)
    retrieve_real_time_data(producer,'real-time-stock-prices', logger)


