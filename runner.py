import argparse
import datetime
import logging
import subprocess
import time

if __name__ == "__main__":
    # Setup argument parser
    parser = argparse.ArgumentParser('Gather configs for fetching ticker data')
    parser.add_argument('-log-level', '--log-level',
                        help='Set the level for logging',
                        choices=('DEBUG', 'INFO', 'WARNING', 'ERROR'),
                        default='DEBUG')
    parser.add_argument("-p", "--path",
                        help="Path to your DB file",
                        default="", type=str)
    args = parser.parse_args()

    # Set up logger
    logger = logging.getLogger(__name__)
    logging_map = {
        'DEBUG': logging.DEBUG,
        'INFO': logging.INFO,
        'WARNING': logging.WARNING,
        'ERROR': logging.ERROR
    }

    logger.setLevel(logging_map[args.log_level])
    ch = logging.StreamHandler()
    ch.setLevel('INFO')
    logger.addHandler(ch)
    logger.info('Logger set up')

    day = datetime.datetime.now().strftime("%Y-%m-%d")
    logger.info("Save posts..")
    subprocess.call(["python", "save_posts.py", "-n", "5", "-p", args.path])
    logger.info("Done.")
    time.sleep(2)
    logger.info("Save comments..")
    subprocess.call(["python", "save_comments.py", "-d", day, "-p", args.path])
    logger.info("Done.")
    time.sleep(2)
    logger.info("Update tickers..")
    subprocess.call(["python", "update_tickers.py", "-d", day, "-p", args.path])
    logger.info("Done.")
    time.sleep(2)
    logger.info("Count tickers..")
    subprocess.call(["python", "count_tickers.py", "-d", day, "-p", args.path])
    logger.info("Done.")
