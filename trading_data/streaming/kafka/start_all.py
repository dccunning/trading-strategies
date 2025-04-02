import subprocess
import logging
import glob


def run_all(path):
    for run_file in glob.glob(f"{path}/**/run.py", recursive=True):
        logging.log(logging.INFO, f"Running {run_file}")
        subprocess.Popen(["python", run_file])


if __name__ == "__main__":
    run_all("producers")
    run_all("consumers")

    import time
    while True:
        time.sleep(60)
