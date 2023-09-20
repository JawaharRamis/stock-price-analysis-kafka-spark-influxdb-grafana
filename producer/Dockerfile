FROM python:3.8-slim-buster

WORKDIR /app

COPY . .

RUN python3 -m pip install -r requirements.txt


CMD [ "python3", "stock_info_producer.py"]
