import sys
# append the path of the parent directory
sys.path.append("..")

import os
import logging

LOGS_DIR = 'logs'
LOG_FORMAT = '%(asctime)s - %(levelname)s - %(message)s'
DATE_FORMAT = '%Y-%m-%d %H:%M:%S'

def ensure_logs_directory():
    if not os.path.exists(LOGS_DIR):
        os.makedirs(LOGS_DIR)

def setup_logger(logger_name, log_filename):
    ensure_logs_directory()

    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.INFO)

    log_file = os.path.join(LOGS_DIR, log_filename)
    formatter = logging.Formatter(LOG_FORMAT, datefmt=DATE_FORMAT)

    file_handler = logging.FileHandler(log_file, mode='a')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger
