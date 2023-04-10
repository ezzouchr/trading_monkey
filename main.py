import datetime
from modules.DataHandler import DataHandler

data_handler = DataHandler(exchange_api="https://api.binance.com/api/v3/klines")

start_date = datetime.datetime(2020, 1, 1)
end_date = datetime.datetime(2020, 12, 31)
timeframe = "1d"

historical_data = data_handler.get_historical_data("XLMUSDT", start_date, end_date, timeframe)
historical_data