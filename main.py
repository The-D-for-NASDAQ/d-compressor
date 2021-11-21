import datetime
import numpy as np
import progress
import csv_to_matrices
from tqdm import tqdm

tqdm.pandas()

floor_price_round = 2 / (2 * 2)  # 0.5

side_num_layers = 5  # Price, Ordered volume, Filled volume, Canceled volume, Pending volume
side_num_price_levels = 500 * 2  # price level ($500), 50 cents per level (*2)
side_minutes_per_day = 16 * 60  # 960, 16 hours of data per day, from 4:00 to 20:00

d_num_layers = 6  # Price, Ordered volume, Filled volume, Canceled volume, Pending volume, Time index
d_num_price_levels = 10 * 2 * 2  # price level ($10) per 50 cents per level (*2) per side (*2)
d_minutes_per_day = int(6.5 * 60)  # 6 hours 30 minutes of data per trading session, from 9:30 to 16:00


def main(date):
    records = csv_to_matrices.load_csv_file('data/AAPL' + date + '.csv')

    full_d_asks = csv_to_matrices.process_side_records('ASK', floor_price_round, side_num_layers, side_num_price_levels,
                                                       side_minutes_per_day, records)[:, :, 330:720]  # working hours
    full_d_bids = csv_to_matrices.process_side_records('BID', floor_price_round, side_num_layers, side_num_price_levels,
                                                       side_minutes_per_day, records)[:, :, 330:720]

    d = csv_to_matrices.get_d(d_num_layers, d_num_price_levels, d_minutes_per_day, full_d_asks, full_d_bids)

    np.save('data/processed/AAPL' + date + '.npy', d)


if __name__ == '__main__':
    while True:
        begin_time = datetime.datetime.now()

        date_to_process = progress.get_process_date()
        print(date_to_process)

        main(date_to_process)

        progress.save_processed_date(date_to_process)

        print('processing time: ' + str(datetime.datetime.now() - begin_time))
