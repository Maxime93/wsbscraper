import json

import pandas as pd

from utils.utils import (
    get_sqlite_engine
)

sqlite_table = "tickers_timeseries"
sqlite_temp_table = "temp_tickers_timeseries"

def get_tickers(source, day):
    engine = get_sqlite_engine()
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


def save(post_count, comment_count, all_count, day):
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
        df.to_sql(
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
                    SET blob = (SELECT blob
                                FROM {temp}
                                WHERE id = {prod}.id)
                    where EXISTS (SELECT blob
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
    post_blobs = get_tickers('posts', args.day)
    comment_blobs = get_tickers('comments', args.day)

    post_count, comment_count, all_count = count_tickers(post_blobs, comment_blobs)
    save(post_count, comment_count, all_count, args.day)
