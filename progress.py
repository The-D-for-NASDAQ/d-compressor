import datetime
import glob
import numpy as np
import os
import sys

compress_progress_file_name = 'compress_progress.txt'


def get_process_date():
    raw_csv_file_names_path = os.path.normcase('data/unprocessed/AAPL*.csv')
    raw_csv_file_names = sorted(glob.glob(raw_csv_file_names_path))

    if not raw_csv_file_names:
        sys.exit('Missing csv files to process')

    open(compress_progress_file_name, 'a').close()  # create file if not exists

    compress_progress_file = open(compress_progress_file_name, "r+")
    dates = compress_progress_file.read().split('\n')

    if not dates[0]:
        date = raw_csv_file_names[0][21:29]
    else:
        last_date = datetime.datetime.strptime(dates[-2], "%Y%m%d") + datetime.timedelta(days=1)
        date = last_date.strftime('%Y%m%d')

    filename = os.path.normcase('data/unprocessed/AAPL' + date + '.csv')
    np_raw_csv_file_names = np.array(raw_csv_file_names)
    not_processed_raw_files = np_raw_csv_file_names[np_raw_csv_file_names >= filename]

    if np.size(not_processed_raw_files) == 0:
        sys.exit('Nothing to process')

    if filename not in not_processed_raw_files:
        save_processed_date(date)
        date = get_process_date()

    return date


def save_processed_date(date):
    compress_progress_file = open(compress_progress_file_name, "r+")
    compress_progress_file.write(date + '\n')
    compress_progress_file.close()
