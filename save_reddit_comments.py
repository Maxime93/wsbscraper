import pandas as pd

from utils.utils import (
    extract_tickers_from_text,
    get_sqlite_engine,
    read_configs,
    get_reddit_client,
    get_date
)
from utils.upsert import upsert

sqlite_table = "comments"
sqlite_temp_table = "temp_comments"
columns = [
    "id",
    "score",
    "created",
    "body",
    "author",
    "upvote",
    "downvote",
    "comment_type",
    "name",
    "parent_id",
    "post_id",
    "timestamp",
    "tickers"
]


def get_posts(day, path):
    query = ("SELECT strftime('%Y-%m-%d', `timestamp`) as day, id "
             "FROM posts "
             "WHERE day = '{day}';".format(blank="{}", day=day))
    engine = get_sqlite_engine(path=path)
    with engine.begin() as con:
        posts = con.execute(query).fetchall()
    return posts


def save_comments(df, path):
    columns = ['score', 'upvote', 'downvote']
    upsert(path, sqlite_temp_table, sqlite_table, df, columns)


def process_post(post, path):
    """post is a tuple. Eg: ('2021-06-09', 'nw7ug5')
    """
    comments_records = []

    post_id = post[1]
    submission = reddit.submission(id=post_id)
    submission.comments.replace_more(limit=None)
    for comment in submission.comments.list():
        comments_records.append(
            {"id": comment.id,
             "score": comment.score,
             "created": comment.created,
             # Saving every comment is too much for the DB for now
             "body": "",  # comment.body
             "author": comment.author.__str__(),
             "upvote": comment.ups,
             "downvote": comment.downs,
             "comment_type": comment.comment_type,
             "name": comment.name,
             "parent_id": comment.parent_id,
             "post_id": post_id},
        )
    comments_df = pd.DataFrame.from_records(comments_records)
    _timestamp = comments_df["created"].apply(get_date)
    comments_df = comments_df.assign(timestamp=_timestamp)
    comments_df['tickers'] = comments_df.apply(lambda x: extract_tickers_from_text([x.body]), axis=1)
    save_comments(comments_df, path)


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
    # Get config and reddit client
    config = read_configs(path=args.path, object="reddit")
    reddit = get_reddit_client(config)

    # Get reddit posts for a day
    posts = get_posts(args.day, args.path)

    # Process posts
    for post in posts:
        process_post(post, args.path)
