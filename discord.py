import argparse
import datetime
import json
import logging
import requests
import sys

from utils.utils import (
    logging_map,
    create_log_dir,
    ConfigReader,
    SQLiteExecutor
)

class DiscordNotifier(ConfigReader, SQLiteExecutor):
    """Class for managing Tickers"""
    def __init__(self, day, subreddit, env, logger):
        super().__init__(env, logger)
        self.read_configs()
        self.day = day
        self.subreddit = subreddit
        self.env = env
        self.logger = logger

    def run(self):
        self.logger.info("[DiscordNotifier] Getting latest tickers from DB")
        blob = self.get_day_tickers()
        self.logger.info("[DiscordNotifier] {}".format(blob))

        self.logger.info("[DiscordNotifier] Posting to discord")
        self.post_tickers()

    def get_day_tickers(self):
        query = ("SELECT blob "
                "from tickers_timeseries "
                "where day = '{day}' "
                "and subreddit = '{subreddit}' "
                "and source = 'post';".format(
                    day=self.day, subreddit=self.subreddit
                    )
                )
        self.logger.info("[DiscordNotifier] {}".format(query))
        self.blob = self.execute_query(query)

    def post_tickers(self):
        payload = {
        'content':"Daily ticker count {}: {}\n{}".format(
                self.day, self.subreddit, json.loads(self.blob[0][0])
            )
        }
        self.post_discord(payload)

    def post_discord(self, payload):
        try:
            r = requests.post(
                self.discord_config['endpoint'],
                data=payload,
                headers=self.discord_config['headers']
            )
            self.logger.info("[DiscordNotifier] {}".format(r.text))
        except Exception as e:
            self.logger.error("[DiscordNotifier] Failed posting to Discord")
            self.logger.error("[DiscordNotifier] {}".format(e))
            sys.exit()


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
    parser.add_argument("-e", "--env",
                        help="Environement the code is running on",
                        type=str, default='local')

    args = parser.parse_args()

    logger = logging.getLogger(__name__)
    logger.setLevel(logging_map[args.log_level])

    log_directory, log_file_name = create_log_dir('DiscordNotifier', args.env)
    log_format = logging.Formatter("%(asctime)s:%(levelname)s:%(message)s",datefmt="%H:%M:%S")

    file_handler = logging.FileHandler(log_directory + "/" + log_file_name)
    file_handler.setLevel(logging_map[args.log_level])
    file_handler.setFormatter(log_format)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(log_format)

    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)

    day = datetime.datetime.now().strftime("%Y-%m-%d")
    _discord_notifier = DiscordNotifier(day, args.subreddit, args.env, logger)
    _discord_notifier.run()