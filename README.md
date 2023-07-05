# PublicPolygonScanner
The app works with the polygon stock api. Allows for scanning the New York Stock Exchange for stocks trading with unusual volume. 

start by running MultiProcessing.start_app() in multiprocessing.py.

ws_process is responsible for listening to the websocket and putting the received data in a queue.

