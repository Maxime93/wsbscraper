import json
import sys

import pandas as pd

from utils.utils import (
    extract_tickers_from_text,
    get_sqlite_engine,
    read_configs,
    get_reddit_client,
    get_date
)

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


def get_posts(day):
    query = ("SELECT strftime('%Y-%m-%d', `timestamp`) as day, id "
             "FROM posts "
             "WHERE day = '{day}';".format(blank="{}", day=day))
    engine = get_sqlite_engine()
    with engine.begin() as con:
        posts = con.execute(query).fetchall()
    return posts


def save_comments(comments_df):
    engine = get_sqlite_engine()
    with engine.begin() as con:
        # DELETE temp table
        query = 'DROP TABLE IF EXISTS `{temp}`;'.format(
            temp=sqlite_temp_table
        )
        con.execute(query)

        # Create temp table like target table to stage data for upsert
        query = "CREATE TABLE `{temp}` AS SELECT * FROM `{prod}` WHERE false;".format(
            temp=sqlite_temp_table, prod=sqlite_table
        )
        con.execute(query)

        # Insert dataframe into temp table
        comments_df[columns].to_sql(
            sqlite_temp_table,
            con,
            if_exists='append',
            index=False,
            method='multi'
        )

        # INSERT where the key doesn't match (new rows)
        query = "INSERT INTO `{prod}` SELECT * FROM `{temp}` WHERE `id` NOT IN (SELECT `id` FROM `{prod}`);".format(
            temp=sqlite_temp_table, prod=sqlite_table
        )
        con.execute(query)

        # Do an UPDATE ... JOIN to set all non-key columns of target to equal source
        query = """UPDATE
                        {prod}
                    SET score = (SELECT score
                                FROM {temp}
                                WHERE id = {prod}.id),
                        upvote = (SELECT upvote
                                FROM {temp}
                                WHERE id = {prod}.id),
                        downvote = (SELECT downvote
                                FROM {temp}
                                WHERE id = {prod}.id)
                    where EXISTS (SELECT score, upvote, downvote
                                FROM {temp}
                                WHERE id = {prod}.id)
                    ;""".format(
                        temp=sqlite_temp_table, prod=sqlite_table
                    )
        con.execute(query)

        # DELETE temp table
        query = 'DROP TABLE `{temp}`;'.format(
            temp=sqlite_temp_table
        )
        con.execute(query)


def process_post(post):
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
    save_comments(comments_df)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Save Posts")
    parser.add_argument("-d", "--day",
                        help="yyyy-mm-dd",
                        type=str, required=True)
    parser.add_argument(
        "-l",
        "--log-level",
        help="Set the level for logging",
        choices=("DEBUG", "INFO", "WARNING", "ERROR"),
        default="INFO",
    )
    args = parser.parse_args()
    # Get config and reddit client
    config = read_configs()
    reddit = get_reddit_client(config)

    # Get reddit posts for a day
    posts = get_posts(args.day)

    # Process posts
    for post in posts:
        print(post)
        process_post(post)
