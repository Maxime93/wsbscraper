import argparse
import datetime
import logging

from utils.utils import (
    logging_map,
    create_log_dir
)
from save_reddit_posts import RedditPostSaver
from save_tickers import TickerSaver
from save_ticker_timeseries import TickerTimeSeriesSaver

class RedditScraperRunner(object):
    """Class for running Reddit Scrapers"""

    def __init__(self, number_posts, timespan, subreddit, env, logger):
        self.number_posts = number_posts
        self.timespan = timespan
        self.subreddit = subreddit
        self.env = env
        self.logger = logger
        self.day = datetime.datetime.now().strftime("%Y-%m-%d")

    def run(self):

        self.logger.info("[Runner]: Started run")

        self.logger.info("[Runner]: Starting reddit scrapper:")
        self.logger.info("[Runner]: --> Number of posts: {}".format(self.number_posts))
        self.logger.info("[Runner]: --> Subreddit: {}".format(self.subreddit))

        _save_posts = RedditPostSaver(
            self.number_posts, self.timespan,
            self.subreddit, self.env, self.logger
        )
        _save_posts.run()

        _save_tickers = TickerSaver(self.env, self.logger)
        _save_tickers.run()

        _save_ticker_timeseries = TickerTimeSeriesSaver(self.subreddit, 'posts', self.env, self.logger)
        _save_ticker_timeseries.run()


if __name__ == "__main__":
    # Setup argument parser
    parser = argparse.ArgumentParser('Gather configs for fetching ticker data')
    parser.add_argument('-log-level', '--log-level',
                        help='Set the level for logging',
                        choices=('DEBUG', 'INFO', 'WARNING', 'ERROR'),
                        default='DEBUG')
    parser.add_argument("-s", "--subreddit",
                        help="eg. wallstreetbets",
                        type=str, required=True)
    parser.add_argument("-n", "--number-posts",
                        help="Number of posts to scrape",
                        type=int, default=5)
    parser.add_argument("-t", "--timespan",
                        help="Over how much time",
                        type=str, default='day')
    parser.add_argument("-e", "--env",
                        help="Environement the code is running on",
                        type=str, default='local')

    args = parser.parse_args()

    logger = logging.getLogger(__name__)
    logger.setLevel(logging_map[args.log_level])

    log_directory, log_file_name = create_log_dir('RedditScrapper', args.env)
    log_format = logging.Formatter("%(asctime)s:%(levelname)s:%(message)s",datefmt="%H:%M:%S")

    file_handler = logging.FileHandler(log_directory + "/" + log_file_name)
    file_handler.setLevel(logging_map[args.log_level])
    file_handler.setFormatter(log_format)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(log_format)

    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)

    rsr = RedditScraperRunner(args.number_posts, args.timespan, args.subreddit, args.env, logger)
    rsr.run()
