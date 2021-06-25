import argparse
import datetime
import logging
import subprocess
import time

from utils.utils import define_log_file

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
    parser.add_argument("-n", "--number-posts",
                        help="Number of posts to scrape",
                        type=int, default=5)

    group = parser.add_mutually_exclusive_group()
    group.add_argument("--logfile", dest="logfile", action="store_true")
    group.add_argument("--no-logfile", dest="logfile", action="store_false")
    parser.set_defaults(logfile=False)

    args = parser.parse_args()

    # Set up logger
    logger = logging.getLogger(__name__)

    # Save logs to file
    if args.logfile:
        define_log_file(args.path)

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

    # ##############################
    logger.info("Save posts..")
    command = [
        "python",
        "{}save_posts.py".format(args.path),
        "-n", str(args.number_posts),
        "-p", args.path
    ]
    if args.logfile:
        command.append("--logfile")
    subprocess.call(command)
    logger.info("Done.\n\n")

    # ##############################
    time.sleep(2)
    # ##############################

    # Skipping saving comments, content sucks
    # logger.info("Save comments..")
    # subprocess.call([
    #     "python",
    #     "save_comments.py",
    #     "-d", day, "-p", args.path
    # ])
    # logger.info("Done.\n\n")

    # ##############################
    # time.sleep(2)
    # ##############################

    logger.info("Update tickers..")
    subprocess.call([
        "python",
        "{}update_tickers.py".format(args.path),
        "-d", day,
        "-p", args.path
    ])
    logger.info("Done.\n\n")

    # ##############################
    time.sleep(2)
    # ##############################

    logger.info("Count tickers..")
    subprocess.call([
        "python",
        "{}count_tickers.py".format(args.path),
        "-d", day,
        "-p", args.path
    ])
    logger.info("Done.")
