import os
import numpy as np
import pandas as pd
import pika
from csv_to_matrices import compressor
from csv_to_matrices import compressor_by_minutes
from datetime import datetime
from tqdm import tqdm

tqdm.pandas()

floor_price_round = 2 / (2 * 2)  # 0.5

side_num_layers = 5  # Price, Ordered volume, Filled volume, Canceled volume, Pending volume
side_num_price_levels = 500 * 2  # price level ($500), 50 cents per level (*2)
side_minutes_per_day = 16 * 60  # 960, 16 hours of data per day, from 4:00 to 20:00
side_start_trading_session_index = int(5.5 * 60)  # 330, from 4:00 to 9:30

d_num_layers = 6  # Price, Ordered volume, Filled volume, Canceled volume, Pending volume, Time index
d_num_price_levels = 10 * 2 * 2  # price level ($10) per 50 cents per level (*2) per side (*2)
d_minutes_per_day = int(6.5 * 60)  # 390, 6 hours 30 minutes of data per trading session, from 9:30 to 16:00

previous_add_records = pd.DataFrame({
    'OrderNumber': pd.Series(dtype=np.int64),
    'EventType': pd.Series(dtype=np.object),
    'Price': pd.Series(dtype=np.float64),
    'Quantity': pd.Series(dtype=np.int64),
    'time_index': pd.Series(dtype=np.int32),
    'price_level': pd.Series(dtype=np.int32)
})

asks_matrices = compressor.create_zeros_array(side_num_layers, side_num_price_levels, side_minutes_per_day)
asks_matrices = compressor.fill_matrices_price_level(asks_matrices, side_num_price_levels, side_minutes_per_day,
                                                     floor_price_round)
bids_matrices = compressor.create_zeros_array(side_num_layers, side_num_price_levels, side_minutes_per_day)
bids_matrices = compressor.fill_matrices_price_level(bids_matrices, side_num_price_levels, side_minutes_per_day,
                                                     floor_price_round)


def process_minute(date):
    global previous_add_records

    csv_data_path = os.path.join('..', 'd-converter', 'data', date.strftime('%Y%m%d'), 'AAPL', date.strftime('%H%M') + '.csv')
    if not os.path.exists(csv_data_path):
        return

    records = compressor.load_csv_file(csv_data_path)
    records_t_index = records['time_index'].iloc[0]  # time_index same for all records

    full_d_asks = compressor_by_minutes.process_side_records('ASK', side_num_price_levels,
                                                             records, previous_add_records, asks_matrices, records_t_index)
    full_d_bids = compressor_by_minutes.process_side_records('BID', side_num_price_levels,
                                                             records, previous_add_records, bids_matrices, records_t_index)

    # TODO: exclude add records by delete records

    previous_add_records = pd.concat((
        previous_add_records,
        records
            .loc[records['EventType'] == 'ADD ASK']
            .loc[records['price_level'] < side_num_price_levels],
        records
            .loc[records['EventType'] == 'ADD BID']
            .loc[records['price_level'] < side_num_price_levels]
    ))

    d = compressor_by_minutes.get_t_index_from_d(d_num_layers, d_num_price_levels, full_d_asks, full_d_bids,
                                                 records_t_index, side_start_trading_session_index)

    return d


def send_processed_date_to_predictor(processed_until):
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()
    channel.queue_declare(queue='predictor')
    channel.basic_publish(exchange='', routing_key='predictor', body=processed_until.isoformat())
    connection.close()


def main(date_to_process):
    # TODO: add checking if all previous dates were processed, if not - ACHTUNG!!!!

    begin_time = datetime.now()

    d = process_minute(date_to_process)

    if d is not None:
        base_file_path = 'data/processed_minutes/' + date_to_process.strftime('%Y%m%d') + '/AAPL/'
        if not os.path.exists(base_file_path):
            os.makedirs(base_file_path)

        npy_data_path = base_file_path + date_to_process.strftime('%H%M') + '.npy'
        np.save(npy_data_path, d)

        send_processed_date_to_predictor(date_to_process)

    print('At: ' + str(datetime.now(tz=date_to_process.tzinfo)) + ' | Processed date: ' + str(date_to_process) + ' | Processing time: ' + str(datetime.now() - begin_time))
