from binance import ThreadedWebsocketManager
from datetime import datetime
from binance_config import APIKEY
from binance_config import APISECRET
import csv
import os
import time

last_time = time.time() - 60 

def convert_to_UTC(x):
    ts = int(x)
    ts /= 1000
    return datetime.utcfromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')

def write_to_csv(data):

    filename = "ETH_prices.csv"
    if os.path.exists(filename):
        with open(filename, "a+", newline='') as write_obj:
            csv_writer = csv.DictWriter(write_obj, data.keys())
            csv_writer.writerow(data)

    else:
        # Write header for file creation
         with open(filename, "a+", newline='') as write_obj:
            csv_writer = csv.DictWriter(write_obj, data.keys())
            csv_writer.writeheader()
            csv_writer.writerow(data)

def handle_ticker_message(msg):
    global last_time
    curr_time = time.time()
    if not curr_time - last_time >= 60:
        return
        
    last_time = curr_time

    data = {}
    data['Ticker'] = msg['s']
    data['UTC Time'] = convert_to_UTC(msg['E'])
    data['Price'] = msg['c']
    
    write_to_csv(data)
    


def main():

    twm = ThreadedWebsocketManager(api_key=APIKEY, api_secret=APISECRET)
    twm.start()

    symbol = 'ETHUSDT'
    twm.start_symbol_miniticker_socket(callback=handle_ticker_message, symbol=symbol)

    twm.join()


if __name__ == "__main__":
   main()
