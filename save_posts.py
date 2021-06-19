from os import path
import pandas as pd

from utils.utils import (
    extract_tickers_from_text,
    get_sqlite_engine,
    read_configs,
    get_reddit_client,
    get_date
)

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


def save_new_posts(path):
    engine = get_sqlite_engine(path=path)
    with engine.begin() as con:
        # DELETE temp table
        query = 'DROP TABLE IF EXISTS `{temp_posts}`;'.format(
            temp_posts=sqlite_temp_table
        )
        con.execute(query)

        # Create temp table like target table to stage data for upsert
        query = "CREATE TABLE `{temp_posts}` AS SELECT * FROM `{posts}` WHERE false;".format(
            temp_posts=sqlite_temp_table, posts=sqlite_table
        )
        con.execute(query)

        # Insert dataframe into temp table
        posts_df.to_sql(
            sqlite_temp_table,
            con,
            if_exists='append',
            index=False,
            method='multi'
        )

        # INSERT where the key doesn't match (new rows)
        query = "INSERT INTO `{posts}` SELECT * FROM `{temp_posts}` WHERE `id` NOT IN (SELECT `id` FROM `{posts}`);".format(
            temp_posts=sqlite_temp_table, posts=sqlite_table
        )
        con.execute(query)

        # Do an UPDATE ... JOIN to set all non-key columns of target to equal source
        query = """UPDATE
                        posts
                    SET score = (SELECT score
                                FROM temp_posts
                                WHERE id = posts.id),
                        comms_num = (SELECT comms_num
                                FROM temp_posts
                                WHERE id = posts.id),
                        upvote_ratio = (SELECT upvote_ratio
                                FROM temp_posts
                                WHERE id = posts.id)
                    where EXISTS (SELECT score, comms_num, upvote_ratio
                                FROM temp_posts
                                WHERE id = posts.id)
                    ;"""
        con.execute(query)

        # DELETE temp table
        query = 'DROP TABLE `{temp_posts}`;'.format(
            temp_posts=sqlite_temp_table
        )
        con.execute(query)


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
    config = read_configs()
    reddit = get_reddit_client(config)

    # Get recent top submissions & save
    posts_df = get_reddit_posts(
        args.subreddit,
        args.number_posts,
        args.timespan)
    save_new_posts(args.path)
