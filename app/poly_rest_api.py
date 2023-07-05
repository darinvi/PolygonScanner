from polygon import RESTClient
import numpy as np
import pandas as pd
from datetime import datetime,timedelta
import time,asyncio,pytz
from env import API_KEY


class PolyRestApi:
    #this class is responsible for maintaining my datasets at the beggining of date or initializing data. It does not have a dinamic use in the app.

    @staticmethod
    def client():
        return RESTClient(api_key="API KEY")

    @staticmethod
    def get_past_dates_market_open(days):
        date = pd.read_parquet('../market_opens.parquet',columns=['timestamp'])['timestamp'].unique()
        return list(map(PolyRestApi.epoch_to_Y_M_D_string,date))[-days:]

    @staticmethod
    def string_to_epoch(str_datetime):
        ny_timezone = pytz.timezone('America/New_York')
        datetime_object = datetime.strptime(str_datetime, '%Y-%m-%d %H:%M:%S')
        datetime_object = ny_timezone.localize(datetime_object).astimezone(ny_timezone)
        return int(datetime_object.timestamp()*1000)

    @staticmethod
    def get_all_tickers():
        return list(filter(lambda x: x not in ['timestamp','datetime',None],pd.read_parquet('../market_closes.parquet')))

    @staticmethod
    def get_aggregate_by_day(date):
        return PolyRestApi.add_datetime_by_timestamp(pd.DataFrame(PolyRestApi.client().get_grouped_daily_aggs(date)))
    
    @staticmethod
    def get_intraday_by_date_range(start,end,ticker):
        return PolyRestApi.add_datetime_by_timestamp(pd.DataFrame(PolyRestApi.client().get_aggs(ticker=ticker, multiplier=1, timespan="minute", from_=start, to=end, limit=50000)).drop(columns=['transactions','otc']))

    @staticmethod
    def add_datetime_by_timestamp(df):
        df['datetime'] = pd.to_datetime(df['timestamp'], unit='ms').dt.tz_localize('UTC').dt.tz_convert('America/New_York')
        return df

    @staticmethod
    def get_date_timeseries(time_delta=200):
        past_dates = pd.bdate_range(start=datetime.today() - timedelta(days=time_delta), end=datetime.today()).to_pydatetime().tolist()
        past_dates = list(map(lambda x: f'{x.year}-{x.month:02d}-{x.day:02d}',past_dates))
        return past_dates[-100:]
    
    @staticmethod
    def get_last_prices():
        closes = PolyRestApi.get_aggregate_by_day(PolyRestApi.epoch_to_Y_M_D_string(time.time()*1000))[['close','ticker']].transpose()
        closes.columns = closes.loc['ticker']
        return closes.drop('ticker').iloc[-1].to_dict()

    @staticmethod
    def epoch_to_Y_M_D_string(epoch):
        day = datetime.fromtimestamp(epoch/1000)
        return f'{day.year}-{day.month:02d}-{day.day:02d}'

    @staticmethod
    def concat_list_of_dataframes(df_list):
        benchmark = None
        for df in df_list:
            benchmark = pd.concat([benchmark,df],join='outer',ignore_index=True)
        return benchmark

    @staticmethod
    def init_raw_daily_dataframe(window):
        dataframes = []
        dates = PolyRestApi.get_past_dates_market_open(window)
        for date in dates:
            if len(dataframes) < window:
                try:
                    dataframes.append(PolyRestApi.get_aggregate_by_day(date).drop(columns=['transactions','otc','vwap']))
                    print('SUCCESS', date)
                except:
                    print('ERROR', date)
        PolyRestApi.concat_list_of_dataframes(dataframes).to_parquet('../raw_market_data.parquet')

    @staticmethod
    def init_intraday_volumes():
        tickers = list(filter(lambda x: x!='SPY',PolyRestApi.get_all_tickers('datasets/market_values.parquet')))
        dates = PolyRestApi.get_past_dates_market_open(20)
        start = dates[0]
        end = dates[-1]
        benchmark = PolyRestApi.get_intraday_by_date_range(start,end,'SPY')[['timestamp','volume']].rename(columns={'volume':'SPY'})
        for ticker in tickers:
            try:
                curr_df = PolyRestApi.get_intraday_by_date_range(start,end,ticker)[['timestamp','volume']].rename(columns={'volume':ticker})
                benchmark = pd.concat([benchmark.set_index('timestamp'),curr_df.set_index('timestamp')],axis=1,join='outer').reset_index()
            except:
                print(ticker,'BAD RESPONSE')
        benchmark.to_parquet('../intraday_volumes.parquet')

    @staticmethod
    def chop_market_values_columns(column,df):
        tickers = list(filter(lambda x: x!='SPY',df['ticker'].unique()))
        benchmark = df[df['ticker']=='SPY'].drop(columns=['ticker']).rename(columns={column:'SPY'})
        count = 0
        dataframes = [benchmark]
        for ticker in tickers:
            count += 1
            print(count,ticker)
            curr_df = df[df['ticker']==ticker].drop(columns=['ticker','datetime']).rename(columns={column:ticker})
            dataframes.append(curr_df)
        return benchmark

    @staticmethod
    def create_market_values_columnar():
        for col in ['open','high','low','close','volume']:
            df = pd.read_parquet('market_values.parquet', columns=['timestamp','datetime','ticker',f'{col}'])
            df = PolyRestApi.chop_market_values_columns(col,df)
            df.to_parquet(f'market_{col}.parquet')
            
    @staticmethod
    def daily_df_management():
        workdays = PolyRestApi.get_date_timeseries()
        epoch = pd.read_parquet('../market_closes.parquet',columns=['timestamp'])['timestamp'].unique()
        epoch_to_date = list(map(PolyRestApi.epoch_to_Y_M_D_string,epoch))
        day_today = PolyRestApi.epoch_to_Y_M_D_string(time.time()*1000)
        for day in workdays:
            if day not in epoch_to_date and day != day_today:
                PolyRestApi.extend_csv_todays_data(day)
        PolyRestApi.cut_csv()

    @staticmethod
    def cut_csv():
        for file in ['opens','highs','lows','closes','volumes']:
            df = pd.read_parquet(f'../market_{file}.parquet')
            timestamps = df['timestamp'].unique()
            if len(timestamps)>100:
                df = df[df['timestamp'] >= timestamps[-100]]
                df.to_parquet(f'../market_{file}.parquet')
                print(f'Rolling windom cut from {datetime.fromtimestamp(timestamps[0]/1000)} to {datetime.fromtimestamp(df["timestamp"].unique()[0]/1000)}')
            else:
                print('CSV has less than 100 days of data')

    @staticmethod
    def extend_csv_todays_data(date):
        try:
            df = PolyRestApi.get_aggregate_by_day(date)
            for col in ['open','high','low','close','volume']:
                parquet_file = pd.read_parquet(f'../market_{col}s.parquet')
                curr_df = df[['timestamp','datetime',col,'ticker']]
                timestmap,date = curr_df.values[-1][:2]
                curr_df = curr_df.drop(columns=['timestamp','datetime']).transpose()
                curr_df.columns = curr_df.iloc[1]
                curr_df = curr_df.drop(index='ticker').reset_index(drop=True)
                curr_df['timestamp'] = timestmap
                curr_df['datetime'] = date
                merged = pd.concat([parquet_file, curr_df], axis=0, sort=False, ignore_index=True).to_parquet(f'../market_{col}s.parquet')
        except:
            print('ERROR', date)

    @staticmethod
    def compute_epoch_df():
        start,end = PolyRestApi.get_yday_epoch_range()
        epoch_range = []
        epoch_now = start
        while epoch_now < end:
            epoch_range.append(epoch_now)
            epoch_now += 60_000
        return pd.DataFrame({'timestamp':epoch_range})

    @staticmethod
    def get_yday_epoch_range():
        yday = PolyRestApi.get_past_dates_market_open(1)[0]
        start_epoch = PolyRestApi.string_to_epoch('2023-04-28 4:00:00')
        end_epoch = PolyRestApi.string_to_epoch('2023-04-28 20:00:00')
        # start_epoch = PolyRestApi.string_to_epoch(f'{yday} 4:00:00')
        # end_epoch = PolyRestApi.string_to_epoch(f'{yday} 20:00:00')
        return start_epoch,end_epoch
    
    @staticmethod
    async def async_get_intraday_data(client, stock, date):
        try:
            data = pd.DataFrame(client.get_aggs(ticker=stock, multiplier=1, timespan='minute', from_=date, to=date))
            data['ticker'] = stock
            return [data,stock]
        except:
            print('BAD RESPONSE',stock)
    
    @staticmethod
    async def async_main():
        client = PolyRestApi.client()
        # y_date = PolyRestApi.get_past_dates_market_open(1)[0]
        y_date = '2023-04-28'
        print(y_date)
        tasks = [asyncio.create_task(PolyRestApi.async_get_intraday_data(client, stock, y_date)) for stock in PolyRestApi.get_all_tickers()]
        results = list(map(lambda x: PolyRestApi.transform_intraday_data(x), await asyncio.gather(*tasks)))
        volumes_df = PolyRestApi.intraday_response_as_dataframe([res for res in results if len(res) > 0])
        pd.concat([pd.read_parquet('../intraday_volumes.parquet'), volumes_df], ignore_index=True).to_parquet('../intraday_volumes_safe.parquet')
        #have to add a cut 

    @staticmethod
    def transform_intraday_data(res):
        return res[0][['timestamp','volume']].rename(columns={'volume':f'{res[1]}'}) if res else pd.DataFrame({})

    @staticmethod
    def run_async_intraday_data():
        asyncio.run(PolyRestApi.async_main())
    
    @staticmethod
    def intraday_response_as_dataframe(results):
        benchmark = PolyRestApi.compute_epoch_df()
        for res in results:
            benchmark = pd.concat([benchmark.set_index('timestamp'),res.set_index('timestamp')],axis=1,join='outer').reset_index()
        benchmark.to_parquet('../yesterday.parquet')
        return benchmark

    @staticmethod
    def test_intraday_data(day,month):
        intraday = pd.read_parquet('../intraday_volumes_safe.parquet')
        intraday = PolyRestApi.add_datetime_by_timestamp(intraday)
        # y_day = PolyRestApi.get_past_dates_market_open(1)[0]
        y_day = '2023-04-28'
        stock = PolyRestApi.get_intraday_by_date_range(y_day,y_day,'SPY')
        intraday = intraday[(intraday['datetime'].dt.day == day) & (intraday['datetime'].dt.month == month)]
        print(intraday)
        print(stock)
        if input('Rewrite intraday_volumes?') == 'Y':
            pd.read_parquet('../intraday_volumes_safe.parquet').to_parquet('../intraday_volumes.parquet')
            print('successfully written')

    @staticmethod
    def validate_dates():
        df = pd.read_parquet('../intraday_volumes.parquet')
        dates = df['datetime'].dt.date.unique()

    @staticmethod
    def calculate_atr_raw_dataframe(df):
        grr = {}
        tickers = df['ticker'].unique()
        count = 0
        for ticker in tickers:
            count += 1
            print(ticker,count)
            curr_df = PolyRestApi.add_true_ranges(df[df['ticker']==ticker])
            grr[ticker] = curr_df['TR'].mean()
        pd.DataFrame(grr,index=[0]).to_parquet('../average_true_ranges.parquet')

    @staticmethod
    def add_true_ranges(df):
        df_tr = df.copy()
        df_tr['yclose'] = df_tr['close'].shift(1)
        max_sum = np.maximum(df_tr['high'] - df_tr['low'], abs(df_tr['high'] - df_tr['yclose']), abs(df_tr['low'] - df_tr['yclose']))
        df_tr.loc[:, 'TR'] = max_sum
        return df_tr.drop(columns=['yclose'])    

# PolyRestApi.daily_df_management()
# PolyRestApi.init_raw_daily_dataframe(20)
# PolyRestApi.calculate_atr_raw_dataframe(pd.read_parquet('../raw_market_data.parquet'))
# PolyRestApi.run_async_intraday_data()
# PolyRestApi.test_intraday_data(28,4)
