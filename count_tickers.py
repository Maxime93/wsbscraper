import json
import logging

import pandas as pd

from utils.utils import (
    get_sqlite_engine,
    define_log_file
)
from utils.upsert import upsert

sqlite_table = "tickers_timeseries"
sqlite_temp_table = "temp_tickers_timeseries"


def get_tickers(source, day, path):
    engine = get_sqlite_engine(path=path)
    query = ("SELECT strftime('%Y-%m-%d', `timestamp`) as day, tickers "
             "FROM {source} "
             "WHERE tickers != '{blank}' and day = '{day}';".format(
                 blank="{}", day=day, source=source
                ))
    with engine.begin() as con:
        tickers = con.execute(query)
        return tickers.fetchall()


def count_blob(blobs):
    counter = {}
    for day, blob in blobs:
        blob = json.loads(blob)
        for ticker in blob:
            counter[ticker] = counter.get(ticker, 0) + blob[ticker]
    return counter


def merge_dict(dict1, dict2):
    ''' Merge dictionaries and keep values of common keys in list'''
    dict3 = {**dict1, **dict2}
    for key, value in dict3.items():
        if key in dict1 and key in dict2:
            dict3[key] = value + dict1[key]
    return dict3


def count_tickers(post_blobs, comment_blobs):
    post_count = count_blob(post_blobs)
    comment_count = count_blob(comment_blobs)

    all_count = merge_dict(post_count, comment_count)

    return post_count, comment_count, all_count


def save(post_count, comment_count, all_count, day, path):
    data = [
        {"id": "{}_post".format(day),
         "day": day,
         "source": "post",
         "blob": json.dumps(post_count)},
        {"id": "{}_comment".format(day),
         "day": day,
         "source": "comment",
         "blob": json.dumps(comment_count)},
        {"id": "{}_all".format(day),
         "day": day,
         "source": "all",
         "blob": json.dumps(all_count)}
    ]
    df = pd.DataFrame.from_records(data)
    columns = ['blob']
    upsert(path, sqlite_temp_table, sqlite_table, df, columns)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Save Posts")
    parser.add_argument("-d", "--day",
                        help="yyyy-mm-dd",
                        type=str, required=True)
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

    group = parser.add_mutually_exclusive_group()
    group.add_argument("--logfile", dest="logfile", action="store_true")
    group.add_argument("--no-logfile", dest="logfile", action="store_false")
    parser.set_defaults(logfile=False)

    args = parser.parse_args()

    # Set up logger
    logger = logging.getLogger(__name__)

    logger.info("Getting tickers from posts")
    post_blobs = get_tickers('posts', args.day, args.path)
    logger.info("Getting tickers from comments")
    comment_blobs = get_tickers('comments', args.day, args.path)

    logger.info("Counting tickers")
    post_count, comment_count, all_count = count_tickers(post_blobs, comment_blobs)
    logger.info("Save ticker counts")
    save(post_count, comment_count, all_count, args.day, args.path)
