import datetime
import json
import logging

import pandas as pd
import praw

from utils.utils import (
    ConfigReader,
    SQLiteExecutor,
    TickerExtractor,
    get_date_from_timestamp,
    logging_map
)
from utils.upsert import upsert

sqlite_table = "posts"
sqlite_temp_table = "temp_posts"
posts_dict = {
    "id": [],
    "title": [],
    "score": [],
    "url": [],
    "comms_num": [],
    "created": [],
    "body": [],
    "upvote_ratio": [],
    "author": [],
    "is_original_content": [],
    "subreddit": [],
}
COLUMNS = ["id", "title", "score", "url", "comms_num",
               "created", "timestamp", "body", "upvote_ratio",
               "author", "is_original_content", "tickers", "subreddit"]


class RedditPostSaver(ConfigReader, SQLiteExecutor):
    """Class for saving Redit Posts to SQLite"""

    def __init__(self, number_posts, timespan, subreddit, env, logger):
        super().__init__(env)
        self.logger = logger
        # self.logger.getLogger(__name__)
        self.read_configs()
        self.logger.info("Initiating RedditPostSaver.")
        self.timespan = timespan
        self.number_posts = number_posts
        self.subreddit = subreddit
        self.env = env

        self.logger.info("Starting RedditPostSaver.")

    def run(self):
        # Get config and reddit client
        self.logger.info("Getting reddit client..")
        self.get_reddit_client()

        self.logger.info("Getting pre-existing tickers..")
        self.get_tickers()
        self.logger.info(self.pre_existing_tickers)

        # Get recent top submissions & save
        self.logger.info("Getting reddit posts..")
        self.get_reddit_posts()


        self.logger.info("Collected {} posts".format(len(self.posts)))
        self.logger.info("Saving reddit posts..")
        self.save_new_posts()


    def get_reddit_client(self):
        self.reddit = praw.Reddit(
            client_id=self.reddit_config['client_id'],
            client_secret=self.reddit_config['client_secret'],
            user_agent=self.reddit_config['user_agent'],
            username=self.reddit_config['username'],
            password=self.reddit_config['password']
        )

    def get_tickers(self):
        query = ("SELECT ticker "
                 "from tickers;")
        self.pre_existing_tickers = self.execute_query(query)

    def get_reddit_posts(self):
        praw_subreddit = self.reddit.subreddit(self.subreddit)
        top_subreddit = praw_subreddit.top(self.timespan, limit=self.number_posts)
        hot_subreddit = praw_subreddit.hot(limit=self.number_posts)
        new_subreddit = praw_subreddit.new(limit=self.number_posts)

        top_subreddit_df = self.loop_through_posts(top_subreddit)
        hot_subreddit_df = self.loop_through_posts(hot_subreddit)
        new_subreddit_df = self.loop_through_posts(new_subreddit)

        self.posts = pd.concat([top_subreddit_df, hot_subreddit_df, new_subreddit_df])
        self.posts.drop_duplicates(subset=['id'], inplace=True)

    def loop_through_posts(self, posts):
        for submission in posts:
            posts_dict["title"].append(submission.title)
            posts_dict["score"].append(submission.score)
            posts_dict["upvote_ratio"].append(submission.upvote_ratio)
            posts_dict["author"].append(submission.author.__str__())
            posts_dict["id"].append(submission.id)
            posts_dict["url"].append(submission.url)
            posts_dict["is_original_content"].append(submission.is_original_content)
            posts_dict["comms_num"].append(submission.num_comments)
            posts_dict["created"].append(submission.created)
            posts_dict["body"].append(submission.selftext)
            posts_dict["subreddit"].append(self.subreddit)

        posts_df = pd.DataFrame(posts_dict)
        _timestamp = posts_df["created"].apply(get_date_from_timestamp)
        posts_df = posts_df.assign(timestamp=_timestamp)
        _ticker_extractor = TickerExtractor(self.env)
        posts_df['tickers'] = posts_df.apply(lambda x: _ticker_extractor.extract_tickers_from_text([x.title, x.body]), axis=1)

        return posts_df[COLUMNS]

    def save_new_posts(self):
        columns = ['score', 'comms_num', 'upvote_ratio']
        upsert(self.env, sqlite_temp_table, sqlite_table, self.posts, columns)
