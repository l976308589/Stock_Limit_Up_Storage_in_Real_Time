from time import sleep

import arrow as ar
import pandas as pd
from path import Path

from Include.Except import Except
from Include.Header import get_last
from Include.Log import log
from Include.Parsing import parsing
from Include.Trding_Time import is_trading_time


class StockLimitUp:
    def __init__(self):
        self.data = pd.DataFrame()
        self.file_path = Path(f'Bin\\Data\\{ar.now().format("YYYYMMDD")}.csv')
        self.pre_date = ar.now()

    def init_pre(self):
        if ar.now().day != self.pre_date.day:
            self.pre_date = ar.now()
            self.file_path = Path(f'Bin\\Data\\{ar.now().format("YYYYMMDD")}.csv')
            if self.file_path.exists():
                self.data = pd.read_csv(self.file_path, encoding='gbk')

    @staticmethod
    def waiting():
        sleep_time = is_trading_time()
        if sleep_time > 0:
            log(f'距离开盘仍需{sleep_time}s')
            sleep(sleep_time)
            return True
        else:
            return False

    def get_last(self):
        data = parsing(get_last())
        pre_data = self.data.copy()
        if not data.empty:
            self.data = self.data.append(data)
            self.data = self.data.drop_duplicates(subset=['code', 'openTime', 'time'], keep='last')
            newly_added = self.data.append(pre_data)
            newly_added = newly_added.drop_duplicates(subset=['code', 'openTime', 'time'], keep=False)
            return newly_added
        return pd.DataFrame()

    @staticmethod
    def format_time(x):
        tzinfo = ar.now().tzinfo
        if isinstance(x['openTime'], float) and x['openTime'] > 1000:
            x['openTime'] = ar.get(x['openTime'] / 1000).to(tz=tzinfo).format('YYYY-MM-DD HH:mm:ss')
        x['time'] = ar.get(float(x['time']) / 1000).to(tz=tzinfo).format('YYYY-MM-DD HH:mm:ss')
        x['updatedTime'] = ar.get(float(x['updatedTime']) / 1000).to(tz=tzinfo).format('YYYY-MM-DD HH:mm:ss')
        return x

    def storage(self, newly_added):
        newly_added = newly_added.apply(self.format_time, axis=1)
        '''涨停时间	涨停打开时间	持续时间	首次封单量	占实际流通比	占成交量比	最高封单量	占实际流通比	占成交量比'''
        print(newly_added)
        self.data.apply(self.format_time, axis=1).to_csv(self.file_path, index=False, encoding='gbk')

    def run(self):
        self.waiting()
        self.init_pre()
        newly_added = self.get_last()
        if not newly_added.empty:
            self.storage(newly_added)

    def loop(self):
        while 1:
            try:
                self.run()
            except:
                Except()
            sleep(10)
