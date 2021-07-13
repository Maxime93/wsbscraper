import datetime
import json
import logging

from utils.utils import (
    flatten_list,
    SQLiteExecutor
)


class TickerSaver(SQLiteExecutor):
    """Class saving unique tickers from Redit Posts to SQLite"""

    def __init__(self, env):
        super().__init__(env)
        self.logger = logging.getLogger(__name__)
        self.logger.info("Initiating TickerSaver.")
        self.env = env
        self.day = datetime.datetime.now().strftime("%Y-%m-%d")

    def run(self):
        self.logger.info("Getting posts from DB")
        self.get_daily_post_tickers()

        # self.logger.info("Getting comments from DB")
        # get_daily_comment_tickers(args.day, args.path)

        self.logger.info("Saving new tickers in DB")
        self.save_tickers()

    def get_daily_post_tickers(self):
        query = ("SELECT tickers, strftime('%Y-%m-%d', `timestamp`) as day "
                 "FROM posts "
                 "WHERE tickers != '{blank}' and day = '{day}';".format(blank="{}", day=self.day))
        self.tickers_from_posts = flatten_list(self.execute_query(query))

    def save_tickers(self):
        ticker_list = []
        self.logger.info(self.tickers_from_posts)
        for ticker_dict in self.tickers_from_posts:
            ticker_dict = json.loads(ticker_dict)
            ticker_list = ticker_list + list(ticker_dict.keys())
        self.ticker_list = list(set(ticker_list))
        self.insert_tickers()

    def insert_tickers(self):
        self.logger.info(self.ticker_list)
        for ticker in self.ticker_list:
            query = "INSERT OR REPLACE INTO tickers(ticker) VALUES('{ticker}');".format(
                ticker=ticker
            )
            self.insert_query(query)

