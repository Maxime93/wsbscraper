import datetime
import json
import logging
import os
import re
import sys
import yaml

import praw
from sqlalchemy import create_engine


def define_log_file(path):
    # Create log directory
    directory = "{path}/logs/{day}".format(
        path=path,
        day=datetime.datetime.now().strftime("%Y_%m_%d")
    )
    try:
        os.makedirs(directory, exist_ok=True)
    except OSError as error:
        sys.exit("Directory '{}' can not be created. Error:\n{}".format(
            directory, error
        ))
    # Save log in correct directory
    logging.basicConfig(
        filename="{path}/logs/{day}/{hour}.log".format(
            path=path,
            day=datetime.datetime.now().strftime("%Y_%m_%d"),
            hour=datetime.datetime.now().strftime("%H")
        )
    )


def get_date(created):
    return datetime.datetime.fromtimestamp(created)


def get_sqlite_engine(path="", echo=False):
    sqlite_loc = 'sqlite:///{path}data/reddit.db'.format(path=path)
    engine = create_engine(sqlite_loc, echo=echo)
    return engine


def read_configs(path=""):
    p = '{}configs/config.yml'.format(path)
    with open(p) as f:
        config = yaml.load(f, Loader=yaml.FullLoader)
        return config


def get_reddit_client(config):
    reddit = praw.Reddit(
        client_id=config['client_id'],
        client_secret=config['client_secret'],
        user_agent=config['user_agent'],
        username=config['username'],
        password=config['password']
    )
    return reddit


blacklist_words = [
      "YOLO", "TOS", "CEO", "CFO", "CTO", "DD", "BTFD", "WSB", "OK", "RH",
      "KYS", "FD", "TYS", "US", "USA", "IT", "ATH", "RIP", "BMW", "GDP",
      "OTM", "ATM", "ITM", "IMO", "LOL", "DOJ", "BE", "PR", "PC", "ICE",
      "TYS", "ISIS", "PRAY", "PT", "FBI", "SEC", "GOD", "NOT", "POS", "COD",
      "AYYMD", "FOMO", "TL;DR", "EDIT", "STILL", "LGMA", "WTF", "RAW", "PM",
      "LMAO", "LMFAO", "ROFL", "EZ", "RED", "BEZOS", "TICK", "IS", "DOW"
      "AM", "PM", "LPT", "GOAT", "FL", "CA", "IL", "PDFUA", "MACD", "HQ",
      "OP", "DJIA", "PS", "AH", "TL", "DR", "JAN", "FEB", "JUL", "AUG",
      "SEP", "SEPT", "OCT", "NOV", "DEC", "FDA", "IV", "ER", "IPO", "RISE"
      "IPA", "URL", "MILF", "BUT", "SSN", "FIFA", "USD", "CPU", "AT",
      "GG", "ELON", "BEAR", "BULL", "BNGO", "APE", "PORN"
   ]


def extract_tickers_from_text(texts):
    """texts is a list of strings.
    """
    ticker_dict = {}
    for text in texts:
        word_list = re.sub("[^\w]", " ",  text).split()
        for word in word_list:
            if word.isupper() and len(word) != 1 and (word.upper() not in blacklist_words) and len(word) <= 5 and word.isalpha():
                ticker_dict[word] = ticker_dict.get(word, 0) + 1
    return json.dumps(ticker_dict)
