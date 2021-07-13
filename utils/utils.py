import datetime
import errno
import json
import logging
import os
import re
import requests
import sys
import yaml

import praw
from sqlalchemy import create_engine
import yfinance_ez as yf


logging_map = {
        'DEBUG': logging.DEBUG,
        'INFO': logging.INFO,
        'WARNING': logging.WARNING,
        'ERROR': logging.ERROR
    }
paths = {
    "local": "/Users/maximerichard/dev/wsbscraper"
}
blacklist_words = [
      "YOLO", "TOS", "CEO", "CFO", "CTO", "DD", "BTFD", "WSB", "OK", "RH",
      "KYS", "FD", "TYS", "US", "USA", "IT", "ATH", "RIP", "BMW", "GDP",
      "OTM", "ATM", "ITM", "IMO", "LOL", "DOJ", "BE", "PR", "PC", "ICE",
      "TYS", "ISIS", "PRAY", "PT", "FBI", "SEC", "GOD", "NOT", "POS", "COD",
      "AYYMD", "FOMO", "TL;DR", "EDIT", "STILL", "LGMA", "WTF", "RAW", "PM",
      "LMAO", "LMFAO", "ROFL", "EZ", "RED", "BEZOS", "TICK", "IS", "DOW"
      "AM", "PM", "LPT", "GOAT", "FL", "CA", "IL", "PDFUA", "MACD", "HQ",
      "OP", "DJIA", "PS", "AH", "TL", "DR", "JAN", "FEB", "JUL", "AUG",
      "SEP", "SEPT", "OCT", "NOV", "DEC", "FDA", "IV", "ER", "IPO", "RISE"
      "IPA", "URL", "MILF", "BUT", "SSN", "FIFA", "USD", "CPU", "AT",
      "GG", "ELON", "BEAR", "BULL", "BNGO", "APE", "PORN", "NUTS", "DEEZ",
      "NOPE", "TLDR", "ETF", "AKA", "TO", "FAQ", "CNBC", "LIKE", "ON", "HELP",
      "THAT", "DM", "THE", "ARE", "TLDR", "MOON"
   ]

class ConfigReader(object):
    """Class for reading configs"""

    def __init__(self, env):
        self.env = env
        self.logger = logging.getLogger(__name__)
        self.configs = None

    def read_configs(self):
        directory = "{}/configs/config.yml".format(paths[self.env])
        with open(directory) as f:
            self.configs = yaml.load(f, Loader=yaml.FullLoader)

    @property
    def reddit_config(self):
        return self.configs['reddit']

    @property
    def discord_config(self):
        return self.configs['discord']

    @property
    def alpha_vantage_config(self):
        return self.configs['alpha_vantage']


class SQLiteExecutor(object):
    """Class for executing SQL statements"""
    def __init__(self, env):
        self.env = env
        self.logger = logging.getLogger(__name__)

    @property
    def engine(self, echo=False):
        path_to_db = "{}/data/reddit.db".format(paths[self.env])
        self.logger.info("Path to db: {}".format(path_to_db))
        sqlite_loc = 'sqlite:///{path}'.format(path=path_to_db)
        engine = create_engine(sqlite_loc, echo=echo)
        return engine

    def execute_query(self, query):
        with self.engine.begin() as con:
            return con.execute(query).fetchall()

    def insert_query(self, query):
        with self.engine.begin() as con:
            return con.execute(query)


class TickerExtractor(object):
    """Class for extracting tickers from text."""
    def __init__(self, env):
        self.env = env
        self.tickers = {}
        self.logger = logging.getLogger(__name__)

    def extract_tickers_from_text(self, text_list):
        """"""
        for text in text_list:
            word_list = re.sub("[^\w]", " ",  text).split()
            for word in word_list:
                if word.isupper() and len(word) != 1 and (word.upper() not in blacklist_words) and len(word) <= 5 and word.isalpha():
                    _ticker = Ticker(word, self.env)
                    if word not in self.tickers:
                        _ticker.verify()
                        if _ticker.is_ticker:
                            self.tickers[word] = self.tickers.get(word, 0) + 1

        return json.dumps(self.tickers)


class Ticker(ConfigReader, SQLiteExecutor):
    """Class for managing Tickers"""
    def __init__(self, ticker, env):
        super().__init__(env)
        self.logger = logging.getLogger(__name__)
        self.read_configs()
        self.ticker = ticker
        self.is_ticker = False
        self.count = 0
        self.url = "https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={}&interval=5min&apikey={}"
        self.logger = logging.getLogger(__name__)

    def increment_count(self):
        self.count+=1

    @property
    def all_tickers(self):
        query = ("SELECT ticker "
                 "from tickers;")
        return flatten_list(self.execute_query(query))

    def verify(self):
        """Verify that the ticker is actually a ticker"""
        self.logger.info("Verifying: {}".format(self.ticker))
        if self.ticker in self.all_tickers:
            self.is_ticker = True
        else:
            data = self.get_info_data()
            if 'shortName' not in data.keys():
                self.logger.info("{} not a ticker".format(self.ticker))
                self.is_ticker = False
            else:
                self.logger.info("{} is a ticker".format(self.ticker))
                self.is_ticker = True

    def get_info_data(self):
        _ticker = yf.Ticker(self.ticker)
        return _ticker.info


    def get_timeseries_data(self):
        endpoint = self.url.format(self.ticker, self.alpha_vantage_config['api_key'])
        self.logger.info(endpoint)
        try:
            r = requests.get(endpoint)
            data = r.json()
        except Exception as e:
            self.logger.error(e)
            self.logger.error("Error reaching alphavantage api")
            sys.exit()
        self.logger.info(data)
        return data


def create_log_dir(name):
    date = datetime.datetime.now()
    log_directory = date.strftime('/Users/maximerichard/dev/wsbscraper/logs/%Y_%m_%d')
    try:
        os.makedirs(log_directory)
    except OSError as exception:
        if exception.errno != errno.EEXIST:
            raise
    log_file_name = date.strftime('{}_%d_%m_%Y_%H_%M.log'.format(name))
    return log_directory, log_file_name


def get_date_from_timestamp(created):
    return datetime.datetime.fromtimestamp(created)


def get_sqlite_engine(path="", echo=False):
    sqlite_loc = 'sqlite:///{path}data/reddit.db'.format(path=path)
    engine = create_engine(sqlite_loc, echo=echo)
    return engine


def read_configs(path="", object='reddit'):
    p = '{}configs/config.yml'.format(path)
    with open(p) as f:
        config = yaml.load(f, Loader=yaml.FullLoader)
        return config[object]


def get_reddit_client(config):
    reddit = praw.Reddit(
        client_id=config['client_id'],
        client_secret=config['client_secret'],
        user_agent=config['user_agent'],
        username=config['username'],
        password=config['password']
    )
    return reddit

def flatten_list(a_list):
    return [element[0] for element in a_list]
