from polygon import WebSocketClient
from polygon.websocket.models import WebSocketMessage
from typing import List
from ticker import Ticker
from poly_rest_api import PolyRestApi as pra

class WebSocketApp:
    def __init__(self,queue) -> None:    
        self.ws = WebSocketClient(
            api_key="API KEY",
            feed='delayed.polygon.io',
            market='stocks',
            subscriptions=["A.*"]
        )
        self.queue = queue

    def handle_msg(self,msg: List[WebSocketMessage]):
        for m in msg:
            self.queue.put(m)

    def run_ws(self):
        self.ws.run(handle_msg=self.handle_msg)

