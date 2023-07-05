from web_socket_app import WebSocketApp
from ticker import Ticker
from poly_rest_api import PolyRestApi as pra
import multiprocessing as mp
import time
import threading

class MultiProcessing:

    @staticmethod
    def run_read_ws_queue(queue,stocks):
        keys = stocks.keys()
        while True:
            if not queue.empty():
                msg = queue.get()
                if msg.symbol in keys:
                    stocks[msg.symbol].update_metrics(msg.close,msg.accumulated_volume,msg.vwap)
    
    @staticmethod
    def run_minutely_updates():
        while True:
            st = time.time()
            Ticker.compute_current_average_volumes()
            Ticker.compute_relative_volumes()
            Ticker.update_last_prices()
            Ticker.update_vwap_deltas()
            et = time.time()
            print('Update Time',et-st)
            time.sleep(30)

    @staticmethod
    def handle_input_msg(msg):
        tickers = {k:v for k,v in Ticker.RVOLS.items() if v > msg and Ticker.ACCUMULATED_VOLUMES[k]>1_000_000}
        for k,v in sorted(tickers.items(),key=lambda x: -x[1]):
            print(k,v)

    @staticmethod
    def run_filter_input(queue):
        while True:
            if not queue.empty():
                msg = queue.get()
                MultiProcessing.handle_input_msg(msg)

    @staticmethod
    def run_threading(queue,input_q):
        stocks = {e:Ticker(e) for e in pra.get_all_tickers()}
        run_ws_response = threading.Thread(target=MultiProcessing.run_read_ws_queue,args=(queue,stocks,))
        run_minutely_updates = threading.Thread(target=MultiProcessing.run_minutely_updates)
        run_filter_input = threading.Thread(target=MultiProcessing.run_filter_input,args=(input_q,))

        run_ws_response.start()
        run_minutely_updates.start()
        run_filter_input.start()

    @staticmethod
    def run_ws(queue):
        app = WebSocketApp(queue)
        app.run_ws()

    @staticmethod
    def run_input_main(queue):
        while True:
            filter = float(input('RVOL: '))
            if filter:
                queue.put(filter)

    @staticmethod
    def start_app():
        if __name__ == '__main__':
            queue = mp.Queue()
            input_queue = mp.Queue()
            ws_process = mp.Process(target=MultiProcessing.run_ws, args=(queue,))
            threading_process = mp.Process(target=MultiProcessing.run_threading, args=(queue,input_queue,))

            ws_process.start()
            threading_process.start()
    
            MultiProcessing.run_input_main(input_queue)

            ws_process.join()
            threading_process.join()

            
MultiProcessing.start_app()

