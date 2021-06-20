from os import path
import pandas as pd

from utils.utils import (
    extract_tickers_from_text,
    get_sqlite_engine,
    read_configs,
    get_reddit_client,
    get_date
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


def get_reddit_posts(subreddit_string, limit, timespan):
    columns = ["id", "title", "score", "url", "comms_num",
               "created", "timestamp", "body", "upvote_ratio",
               "author", "is_original_content", "tickers", "subreddit"]
    subreddit = reddit.subreddit(subreddit_string)
    top_subreddit = subreddit.top(timespan, limit=limit)
    for submission in top_subreddit:
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
        posts_dict["subreddit"].append(subreddit_string)

    posts_df = pd.DataFrame(posts_dict)
    _timestamp = posts_df["created"].apply(get_date)
    posts_df = posts_df.assign(timestamp=_timestamp)
    posts_df['tickers'] = posts_df.apply(lambda x: extract_tickers_from_text([x.title, x.body]), axis=1)
    return posts_df[columns]


def save_new_posts(path, df):
    columns = ['score', 'comms_num', 'upvote_ratio']
    upsert(path, sqlite_temp_table, sqlite_table, df, columns)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Save Posts")
    parser.add_argument("-s", "--subreddit",
                        help="Subreddit to scrape",
                        type=str, default='wallstreetbets')
    parser.add_argument("-n", "--number-posts",
                        help="Number of posts to scrape",
                        type=int, default=5)
    parser.add_argument("-t", "--timespan",
                        help="Over how much time",
                        type=str, default='day')
    parser.add_argument("-p", "--path",
                        help="Path to your DB file",
                        default="", type=str)
    parser.add_argument(
        "-l",
        "--log-level",
        help="Set the level for logging",
        choices=("DEBUG", "INFO", "WARNING", "ERROR"),
        default="INFO",
    )
    args = parser.parse_args()
    # Get config and reddit client
    config = read_configs(path=args.path)
    reddit = get_reddit_client(config)

    # Get recent top submissions & save
    posts_df = get_reddit_posts(
        args.subreddit,
        args.number_posts,
        args.timespan)
    save_new_posts(args.path, posts_df)
