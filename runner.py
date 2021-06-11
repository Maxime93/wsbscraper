import datetime
import subprocess
import time

if __name__ == "__main__":
    day = datetime.datetime.now().strftime("%Y-%m-%d")
    subprocess.call(["python", "save_posts.py"])
    time.sleep(2)
    subprocess.call(["python", "save_comments.py", "-d", day])
    time.sleep(2)
    subprocess.call(["python", "update_tickers.py", "-d", day])
    time.sleep(2)
    subprocess.call(["python", "count_tickers.py", "-d", day])
