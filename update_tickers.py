import json

from utils.utils import (
    get_sqlite_engine
)


def get_daily_post_tickers(day, path):
    engine = get_sqlite_engine(path=path)
    query = ("SELECT strftime('%Y-%m-%d', `timestamp`) as day, tickers "
             "FROM posts "
             "WHERE tickers != '{blank}' and day = '{day}';".format(blank="{}", day=day))
    with engine.begin() as con:
        tickers = con.execute(query)
        return tickers.fetchall()


def get_daily_comment_tickers(day, path):
    engine = get_sqlite_engine(path=path)
    query = ("SELECT strftime('%Y-%m-%d', `timestamp`) as day, tickers "
             "FROM comments "
             "WHERE tickers != '{blank}' and day = '{day}';".format(blank="{}", day=day))
    with engine.begin() as con:
        tickers = con.execute(query)
        return tickers.fetchall()


def insert_tickers(ticker_list, path):
    query = "INSERT OR REPLACE INTO tickers(ticker) VALUES('{ticker}');"
    engine = get_sqlite_engine(path=path)
    with engine.begin() as con:
        for ticker in ticker_list:
            q = query.format(ticker=ticker)
            print(q)
            con.execute(q)


def save_tickers(tickers, path):
    ticker_list = []
    for day, ticker_dict in tickers:
        ticker_dict = json.loads(ticker_dict)
        ticker_list = ticker_list + list(ticker_dict.keys())
    ticker_list = list(set(ticker_list))
    insert_tickers(ticker_list, path)


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
    args = parser.parse_args()
    post_tickers = get_daily_post_tickers(args.day, args.path)
    comment_tickers = get_daily_comment_tickers(args.day, args.path)
    save_tickers(post_tickers + comment_tickers, args.path)
