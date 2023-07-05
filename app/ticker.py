import pandas as pd
import datetime
from poly_rest_api import PolyRestApi as pra
import pytz,time

class Ticker:

    AVERAGE_TRUE_RANGES = pd.read_parquet('../average_true_ranges.parquet').to_dict(orient='records')[0]
    TICKERS = {}
    AVERAGE_VOLUMESS = {}
    CURRENT_AVERAGE_VOLUMES = {}
    ACCUMULATED_VOLUMES = {}
    RVOLS = {}
    YDAY_CLOSES = {}
    OPENS_TODAY = {}
    LAST_PRICES = {}
    VWAP_DELTAS = {}
    VWAPS = {}
    MASSIVE_DUMP = {}

    def __init__(self,name) -> None:
        self.name = name
        self.last = 0
        self.percentage_change = 0
        self.moving_average = 0
        self.massive_dump = {}
        # self.daily_data = self.create_daily_data_df() HAVE DO ADD OPENS,HIGHS,LOWS,CLOSES to class variables.

    def update_metrics(self,c,acc_vol,vwap):
        self.last = c
        # self.massive_dump[t] = {o,h,l,c}
        Ticker.ACCUMULATED_VOLUMES[self.name] = acc_vol
        self.VWAPS[self.name] = vwap

    def update_percentage_change(self):
        y_close = self.YDAY_CLOSES[self.name]
        last_price = self.LAST_PRICES[self.name]
        self.percentage_change = (last_price - y_close) / y_close * 100

    def create_daily_data_df(self):
        closes = self.CLOSES[['timestamp','datetime',self.name]].rename(columns={self.name:'close'})
        highs = self.HIGHS[['timestamp',self.name]].rename(columns={self.name:'high'})
        opens = self.OPENS[['timestamp',self.name]].rename(columns={self.name:'open'})
        volumes = self.VOLUMES[['timestamp',self.name]].rename(columns={self.name:'volume'})
        lows = self.LOWS[['timestamp',self.name]].rename(columns={self.name:'low'})
        for df in [highs,opens,lows,volumes]:
            closes = pd.concat([closes.set_index('timestamp'),df.set_index('timestamp')],axis=1,join='outer').reset_index()
        return closes

    @staticmethod
    def compute_relative_volumes():
        valid_keys = set(Ticker.ACCUMULATED_VOLUMES.keys()).intersection(set(Ticker.CURRENT_AVERAGE_VOLUMES.keys()))
        Ticker.RVOLS = {k:Ticker.ACCUMULATED_VOLUMES[k]/Ticker.CURRENT_AVERAGE_VOLUMES[k] for k in valid_keys if Ticker.CURRENT_AVERAGE_VOLUMES[k] != 0}

    @staticmethod
    def compute_current_average_volumes():
        now = datetime.datetime.now(tz=pytz.timezone('US/Eastern'))
        df = pd.read_parquet('../intraday_volumes.parquet')
        df = pra.add_datetime_by_timestamp(df)
        df_filtered = df[
            ((df['datetime'].dt.hour < now.hour) |
            ((df['datetime'].dt.hour == now.hour) & (df['datetime'].dt.minute <= now.minute)))
        ]
        unique_days = df_filtered['datetime'].dt.date.unique()
        #have to handle tickers that have less days than unique_days length
        Ticker.CURRENT_AVERAGE_VOLUMES = (df_filtered.drop(columns=['timestamp','datetime']).sum()/len(unique_days)).to_dict()

    @staticmethod
    def compute_average_volumes():
        volumes = pd.read_parquet('../market_volumes.parquet')
        Ticker.AVERAGE_VOLUMESS = volumes.iloc[-20:].mean().to_dict()

    @staticmethod
    def update_yday_closes():
        Ticker.YDAY_CLOSES = pd.read_parquet('../market_closes.parquet').iloc[-1].to_dict()

    @staticmethod
    def update_last_prices():
        Ticker.LAST_PRICES = pra.get_last_prices()

    @staticmethod
    def update_vwap_deltas():
        dict1 = Ticker.AVERAGE_TRUE_RANGES
        dict2 = Ticker.VWAPS
        dict3 = Ticker.LAST_PRICES
        valid_tickers = set(dict1.keys()).intersection(set(dict2.keys())).intersection(set(dict3.keys()))
        Ticker.VWAP_DELTAS = {key: (Ticker.LAST_PRICES[key]-Ticker.VWAPS[key])/Ticker.AVERAGE_TRUE_RANGES[key] for key in valid_tickers if Ticker.AVERAGE_TRUE_RANGES[key] != 0}

    @staticmethod
    def filter_by_moving_average(period):
        #pd.mean().iloc[last_row].to_dict().keys() or something like that to get all stocks above/below some moving average.
        pass

    def settle_previous_minutely_data(self):
        pass
