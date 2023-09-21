import sys
# append the path of the parent directory
sys.path.append("/app")


from pyspark.sql import SparkSession, Row
from pyspark.sql.window import Window
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from datetime import datetime, timedelta
import os


from logs.logger import setup_logger
from InfluxDBWriter import InfluxDBWriter
import findspark
findspark.init()

KAFKA_TOPIC_NAME = "real-time-stock-prices"
KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
postgresql_properties  = {
    "user": "admin",
    "password": "admin",
    "driver": "org.postgresql.Driver"
}

scala_version = '2.12'
spark_version = '3.3.3'
packages = [
    f'org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}',
    'org.apache.kafka:kafka-clients:2.8.1'
]

if __name__ == "__main__":
    logger = setup_logger(__name__, 'consumer.log')

    spark = (
        SparkSession.builder.appName("KafkaInfluxDBStreaming")
        .master("spark://spark-master:7077")
        .config("spark.jars.packages", ",".join(packages))
        .config("spark.executor.extraClassPath", "/app/packages/postgresql-42.2.18.jar")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    stockDataframe = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC_NAME) \
        .load()
    
    stockDataframe = stockDataframe.select(col("value").cast("string").alias("data"))

    inputStream =  stockDataframe.selectExpr("CAST(data as STRING)")

    stock_price_schema = StructType([
        StructField("stock", StringType(), True),
        StructField("date", TimestampType(), True),
        StructField("open", DoubleType(), True),
        StructField("high", DoubleType(), True),
        StructField("low", DoubleType(), True),
        StructField("close", DoubleType(), True),
        StructField("volume", DoubleType(), True)
    ])

    # Parse JSON data and select columns
    stockDataframe = inputStream.select(from_json(col("data"), stock_price_schema).alias("stock_price"))
    expandedDf = stockDataframe.select("stock_price.*")

    # influxdb_writer = InfluxDBWriter('stock-prices-bucket', 'stock-price-v1')
    influxdb_writer = InfluxDBWriter(os.environ.get("INFLUXDB_BUCKET"), os.environ.get("INFLUXDB_MEASUREMENT"))

    def process_batch(batch_df, batch_id):
        logger.info(f"Processing batch {batch_id}")
        realtimeStockPrices = batch_df.select("stock_price.*")
        for realtimeStockPrice in realtimeStockPrices.collect():
            timestamp = realtimeStockPrice["date"]
            tags = {"stock": realtimeStockPrice["stock"]}
            fields = {
                "open": realtimeStockPrice['open'],
                "high": realtimeStockPrice['high'],
                "low": realtimeStockPrice['low'],
                "close": realtimeStockPrice['close'],
                "volume": realtimeStockPrice['volume']
            }
            influxdb_writer.process(timestamp, tags, fields)

    query = stockDataframe \
        .writeStream \
        .foreachBatch(process_batch) \
        .outputMode("append") \
        .start()

    query.awaitTermination()





   