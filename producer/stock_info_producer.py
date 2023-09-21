import psycopg2
import logging
from producer_utils import get_stock_details
from logs.logger import setup_logger
import os
import time
import schedule
from datetime import datetime

def main(logger):
    logger.info(f"Script executed at: {datetime.now()}")

    postgresql_properties  = {
        "user": os.environ.get("POSTGRES_USER"),
        "password": os.environ.get("POSTGRES_PASSWORD"),
        "host":os.environ.get("POSTGRES_HOST"),
        "port":os.environ.get("POSTGRES_PORT"),
        "dbname":os.environ.get("POSTGRES_DBNAME"),
    }

    try:
        time.sleep(10)
        # Add a delay before attempting to connect
        connection = psycopg2.connect(**postgresql_properties)
        
        # connection = psycopg2.connect(
        #     host='postgresql',
        #     port=5432,
        #     dbname='stock-info',
        #     user='admin',
        #     password='admin'
        # )
        cursor = connection.cursor()
        # Fetch stock details
        stock_infos = get_stock_details(os.environ.get("STOCKS"), logger)
        # Insert data into PostgreSQL
        insert_query = """
            INSERT INTO public.stock_info (
                Entry_Date, Symbol, ShortName, LongName, Industry, Sector,
                MarketCap, ForwardPE, TrailingPE, Currency,
                FiftyTwoWeekHigh, FiftyTwoWeekLow, FiftyDayAverage,
                Exchange, ShortRatio
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,  %s, %s,  %s, %s)
        """
        for stock_info in stock_infos:
            cursor.execute(
                insert_query,
                (
                    stock_info['Date'],
                    stock_info['Symbol'],
                    stock_info['ShortName'],
                    stock_info['LongName'],
                    stock_info['Industry'],
                    stock_info['Sector'],
                    stock_info['MarketCap'],
                    stock_info['ForwardPE'],
                    stock_info['TrailingPE'],
                    stock_info['Currency'],
                    stock_info['FiftyTwoWeekHigh'],
                    stock_info['FiftyTwoWeekLow'],
                    stock_info['FiftyDayAverage'],
                    stock_info['Exchange'],
                    stock_info['ShortRatio']
                )
            )

        # Commit changes
        cursor.close()
        connection.commit()
        connection.close()
        # Log success
        logger.info(f"Stock information retrieved and inserted into the database")

    except Exception as e:
        logger.error(f"Error inserting stock details into PostgreSQL: {e}")

if __name__ == "__main__":
    logger = setup_logger(__name__, 'producer.log')
    main(logger)
    schedule.every().day.at("16:00").do(main,logger)
    while True:
        schedule.run_pending()
        time.sleep(1)

