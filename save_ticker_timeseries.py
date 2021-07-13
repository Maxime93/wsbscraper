import datetime
import json
import logging

from collections import Counter
import pandas as pd

from utils.utils import (
    SQLiteExecutor
)
from utils.upsert import upsert

sqlite_table = "tickers_timeseries"
sqlite_temp_table = "temp_tickers_timeseries"

class TickerTimeSeriesSaver(SQLiteExecutor):
    """Class saving daily ticker counts from Redit Posts to SQLite"""

    def __init__(self, subreddit, source, env):
        super().__init__(env)
        self.logger = logging.getLogger(__name__)
        self.logger.info("Initiating TickerTimeSeriesSaver.")
        self.subreddit = subreddit
        self.env = env
        self.source = source
        self.day = datetime.datetime.now().strftime("%Y-%m-%d")
        self.tickers = None
        self.counts = None
        self.tickers_count = None
        self.final_count_dict = None

    def run(self):
        self.logger.info("Getting tickers from posts")
        self.get_tickers()

        self.logger.info("Count tickers from posts")
        self.count_tickers()

        self.logger.info(self.final_count_dict)

        self.save()


    def get_tickers(self):
        """Get tickers from posts DB.

        eg. [('2021-07-10', '{"GNUS": 1, "OG": 1, "AMC": 1, "GME": 1}'),
             ('2021-07-10', '{"GNUS": 1, "OG": 1, "AMC": 1, "GME": 1, "TA": 1, "ZLAB": 1}')]
        """
        query = ("SELECT strftime('%Y-%m-%d', `timestamp`) as day, tickers "
                "FROM {source} "
                "WHERE tickers != '{blank}' and day = '{day}' "
                "and subreddit = '{subreddit}';")

        self.tickers = self.execute_query(query.format(
            source=self.source,
            blank='{}',
            day=self.day,
            subreddit=self.subreddit
        ))

    def count_tickers(self):
        """Counting the tickers in the different blobs.

        eg.
        [Counter({'GNUS': 1, 'OG': 1, 'AMC': 1, 'GME': 1}),
         Counter({'GNUS': 1, 'OG': 1, 'AMC': 1, 'GME': 1, 'TA': 1, 'ZLAB': 1}),
         Counter({'TPST': 1}),
         Counter({'TPST': 1, 'PSFE': 1, 'OG': 1, 'PIPE': 1, 'OS': 1, 'FWIW': 1, 'NOTE': 1})]
        """
        self.counts = []
        for tuple in self.tickers:
            self.counts.append(self.count_blob(tuple))

        _final_count_dict = Counter()
        for _counter in self.counts:
            _final_count_dict += _counter

        self.final_count_dict = dict(_final_count_dict)


    def count_blob(self, tuple):
        counter = {}
        blob = json.loads(tuple[1])
        for ticker in blob:
            counter[ticker] = counter.get(ticker, 0) + blob[ticker]
        return Counter(counter)


    def save(self):
        data = [
            {"id": "{}_post".format(self.day),
            "day": self.day,
            "source": "post",
            "blob": json.dumps(self.final_count_dict),
            "subreddit": self.subreddit}
        ]
        df = pd.DataFrame.from_records(data)
        columns = ['blob']
        upsert(self.env, sqlite_temp_table, sqlite_table, df, columns)
