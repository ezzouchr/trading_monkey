import pandas as pd
import psycopg2
import requests
from psycopg2 import sql
import datetime
import numpy as np

class DataHandler:

    def __init__(self, exchange_api, api_key=None, api_secret=None, db_config=None):
        self.exchange_api = exchange_api
        self.api_key = api_key
        self.api_secret = api_secret
        self.db_config = db_config

    def get_historical_data(self, symbol, start_date, end_date, timeframe):
        # Set up the API URL and parameters
        url = self.exchange_api
        params = {
            "symbol": symbol,
            "interval": timeframe,
            "startTime": int(start_date.timestamp() * 1000),
            "endTime": int(end_date.timestamp() * 1000),
        }

        # Send the API request
        response = requests.get(url, params=params)

        # Check if the request was successful
        if response.status_code == 200:
            raw_data = response.json()

            # Process the raw data into a pandas DataFrame
            columns = ["open_time", "open", "high", "low", "close", "volume", "close_time", "quote_asset_volume", "num_trades", "taker_buy_base_volume", "taker_buy_quote_volume", "ignore"]
            df = pd.DataFrame(raw_data, columns=columns)

            # Convert timestamp columns to human-readable datetime
            df["open_time"] = pd.to_datetime(df["open_time"], unit="ms")
            df["close_time"] = pd.to_datetime(df["close_time"], unit="ms")

            return df
        else:
            raise Exception(f"Error {response.status_code}: {response.text}")


    # ... other methods ...

    def save_data_to_db(self, data, table_name):
        connection = psycopg2.connect(**self.db_config)
        cursor = connection.cursor()

        # Save the data to the specified table
        for index, row in data.iterrows():
            # Assuming your data has columns 'timestamp', 'open', 'high', 'low', 'close', 'volume'
            values = (row['timestamp'], row['open'], row['high'], row['low'], row['close'], row['volume'])
            insert_query = sql.SQL(
                "INSERT INTO {} (timestamp, open, high, low, close, volume) VALUES (%s, %s, %s, %s, %s, %s)"
            ).format(sql.Identifier(table_name))
            cursor.execute(insert_query, values)

        connection.commit()
        cursor.close()
        connection.close()

    def load_data_from_db(self, table_name):
        connection = psycopg2.connect(**self.db_config)
        cursor = connection.cursor()

        # Load the data from the specified table
        select_query = sql.SQL("SELECT * FROM {}").format(sql.Identifier(table_name))
        cursor.execute(select_query)
        data = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]

        cursor.close()
        connection.close()

        # Convert the fetched data into a pandas DataFrame
        df = pd.DataFrame(data, columns=columns)
        return df