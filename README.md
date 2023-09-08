# Reddit Data Pipeline Project

This project is designed to collect data from Reddit using the Reddit API, process it using Apache Kafka and Apache Spark, and store the results in a PostgreSQL database. It involves a data pipeline that consists of a Kafka producer, Spark streaming consumer, and PostgreSQL sink.

## Table of Contents

- [Prerequisites](#prerequisites)
- [Project Structure](#project-structure)
- [Getting Started](#getting-started)
- [Configuration](#configuration)
- [Project Components and Interaction](#project-components-and-interaction)
- [Workflow Overview](#workflow-overview)
- [Usage](#usage)


## Prerequisites

Before running the project, ensure you have the following installed:

- Python (3.x)
- Docker
- Apache Kafka
- Apache Spark
- PostgreSQL

## Project Structure

The project is structured as follows:

```
reddit-data-pipeline/
├── producer/
│   ├── producer.py             # Reddit data collection using PRAW
├── consumer/
│   ├── spark_consumer.py       # Spark streaming application
├── logs/
│   ├── logger.py               # Logging configuration
├── docker-compose.yml          # Docker Compose configuration
└── README.md
```

## Getting Started

1. Clone the repository:

   ```bash
   git clone https://github.com/yourusername/reddit-data-pipeline.git
   ```

2. Navigate to the project directory:

   ```bash
   cd reddit-data-pipeline
   ```

3. Create a `.env` file with necessary environment variables.

## Configuration

- Configure your Reddit API credentials in `.env`.
- Update Kafka and Spark configurations in `docker-compose.yml`.
- Update PostgreSQL connection properties in `spark_consumer.py`.
- Update subreddit to explore in `.env`.

## Project Components and Interaction

1. **Producer:**
   - Responsible for collecting data (post details) from Reddit using the PRAW library.
   - Formats the data into JSON messages and produces them to the Kafka topics ("reddit-submissions").
   - Interacts with the Kafka broker to publish messages for downstream processing.
   - By design choice, publisher is executed outside the docker container.

2. **Kafka:**
   - Acts as a message broker that handles the communication between the Producer and Consumer.
   - Receives messages from the Producer and stores them in topics ("reddit-submissions").
   - Distributes messages to interested Consumers for processing.

3. **Spark Consumer:**
   - Reads data from Kafka topics using structured streaming.
   - Parses JSON messages into structured data using defined schemas.
   - Performs data transformations and analysis on the streaming data using PySpark.
   - Writes the processed results to PostgreSQL.
   - Utilizes the PostgreSQL JDBC driver to establish a connection to the PostgreSQL database and write data.

4. **PostgreSQL:**
   - Serves as the data storage for the analyzed Reddit data.
   - Receives processed data from the Spark Consumer and inserts it into the "reddit_submission_data" table.
   - Stores data in a structured format for querying and analysis.

### Workflow Overview

1. The Producer component uses PRAW to collect data from Reddit and sends it as JSON messages to Kafka topics.
2. Kafka, acting as a broker, stores the messages and forwards them to the Spark Consumer.
3. The Spark Consumer reads messages from Kafka, parses JSON, and processes the data using PySpark.
4. Processed results are written to the PostgreSQL database using a JDBC connection.
5. PostgreSQL stores the analyzed data in a structured manner for querying and further analysis.

Overall, this architecture allows data to flow seamlessly from Reddit to Kafka, processed by Spark, and finally stored in PostgreSQL for future reference and analysis. Each component plays a crucial role in the data pipeline, facilitating efficient data processing and storage.

## Usage

1. Start the Kafka, Spark, and PostgreSQL containers:

   ```bash
   docker-compose up -d
   ```

2. Run the Kafka producer to collect Reddit data:

   ```bash
   python producer/producer.py
   ```


## Contributing

Contributions are welcome! Please fork the repository and create a pull request.


