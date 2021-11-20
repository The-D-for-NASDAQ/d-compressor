import numpy as np
import pandas as pd


def load_csv_file(filename):
    raw_columns = ['Date', 'Timestamp', 'OrderNumber', 'EventType', 'Price', 'Quantity', 'Exchange']
    records = pd.read_csv(filename, usecols=raw_columns)

    records['OrderNumber'] = records['Exchange'] + records['OrderNumber'].astype(str)

    records['DateTime'] = records['Date'].apply(str) + ' ' + records['Timestamp']
    records['DateTime'] = pd.to_datetime(records['DateTime'], format='%Y%m%d %H:%M:%S.%f')
    records['Timestamp'] = records['DateTime'].values.astype(np.int64) // 10 ** 9

    start_day_timestamp = int(records['DateTime'][0].timestamp())
    records['time_index'] = np.int32((records['Timestamp'] - start_day_timestamp) / 60)  # index per minute

    records['price_level'] = np.int32(records['Price'] * 2)  # index per minute

    return records.drop(['Date', 'DateTime', 'Timestamp', 'Exchange'], axis=1)


def create_zeros_array(d1, d2, d3):
    return np.zeros((d1, d2, d3), np.float32)


def process_add_record(r, full_d):
    full_d[1, r.price_level, r.time_index] += r.Quantity


def process_trade_records(r, full_d):
    full_d[2, r.price_level, r.time_index] += r.Quantity


def process_execute_price_records(r, full_d):
    full_d[2, r.price_level, r.time_index] += r.Quantity


def process_execute_priceless_records(r, full_d):
    full_d[2, r.price_level_add, r.time_index_execute] += r.Quantity_execute


def process_fill_records(r, full_d):
    full_d[2, r.price_level_add, r.time_index_fill] += r.Quantity_add


def process_cancel_records(r, full_d):
    full_d[3, r.price_level_add, r.time_index_cancel] += r.Quantity_cancel


def process_delete_records(r, full_d):
    quantity = r.Quantity_delete if r.Quantity_delete != 0 else r.Quantity_add
    full_d[3, r.price_level_add, r.time_index_delete] += quantity


def process_side_records(side, floor_price_round, num_layers, num_price_levels, minutes_per_day, records):
    full_d_records = records \
        .loc[records['price_level'] < num_price_levels] \
        .loc[records['time_index'] < minutes_per_day]

    side_matrices = create_zeros_array(num_layers, num_price_levels, minutes_per_day)

    incrementer = 0
    for d_price_index in range(0, num_price_levels):
        for time_index in range(0, minutes_per_day):
            side_matrices[0, d_price_index, time_index] = incrementer
        incrementer += floor_price_round

    full_d_price_records = full_d_records.loc[full_d_records['Price'] != 0]

    full_d_price_records \
        .loc[full_d_price_records['EventType'] == 'ADD ' + side] \
        .apply(process_add_record, full_d=side_matrices, axis=1)

    side_matrices[4] = side_matrices[1]

    full_d_price_records \
        .loc[full_d_price_records['EventType'] == 'TRADE ' + side] \
        .apply(process_trade_records, full_d=side_matrices, axis=1)

    full_d_price_records \
        .loc[full_d_price_records['EventType'] == 'EXECUTE ' + side] \
        .apply(process_execute_price_records, full_d=side_matrices, axis=1)


    full_d_priceless_records = full_d_records.loc[full_d_records['Price'] == 0]
    add_asks = full_d_records.loc[full_d_records['EventType'] == 'ADD ' + side].set_index('OrderNumber')

    fill_asks = full_d_priceless_records.loc[full_d_priceless_records['EventType'] == 'FILL ' + side].set_index('OrderNumber')
    full_fill_asks = fill_asks.join(add_asks, on='OrderNumber', how='left', lsuffix='_fill', rsuffix='_add')
    full_fill_asks \
        .loc[full_fill_asks['time_index_fill'] >= full_fill_asks['time_index_add']] \
        .apply(process_fill_records, full_d=side_matrices, axis=1)

    execute_asks = full_d_priceless_records.loc[full_d_priceless_records['EventType'] == 'EXECUTE ' + side].set_index(
        'OrderNumber')
    full_execute_asks = execute_asks.join(add_asks, on='OrderNumber', how='left', lsuffix='_execute', rsuffix='_add')
    full_execute_asks \
        .loc[full_execute_asks['time_index_execute'] >= full_execute_asks['time_index_add']] \
        .apply(process_execute_priceless_records, full_d=side_matrices, axis=1)

    cancel_asks = full_d_priceless_records.loc[full_d_priceless_records['EventType'] == 'CANCEL ' + side].set_index(
        'OrderNumber')
    full_cancel_asks = cancel_asks.join(add_asks, on='OrderNumber', how='left', lsuffix='_cancel', rsuffix='_add')
    full_cancel_asks \
        .loc[full_cancel_asks['time_index_cancel'] >= full_cancel_asks['time_index_add']] \
        .apply(process_cancel_records, full_d=side_matrices, axis=1)


    delete_asks = full_d_priceless_records.loc[full_d_priceless_records['EventType'] == 'DELETE ' + side].set_index(
        'OrderNumber')
    full_delete_asks = delete_asks.join(add_asks, on='OrderNumber', how='left', lsuffix='_delete', rsuffix='_add')
    full_delete_asks['price_level_add'] = np.int32(
        full_delete_asks['price_level_add'])  # after join right table has wrong column types
    full_delete_asks \
        .loc[full_delete_asks['time_index_delete'] >= full_delete_asks['time_index_add']] \
        .apply(process_delete_records, full_d=side_matrices, axis=1)


    for t_index in range(0, minutes_per_day):
        for p_index in range(0, num_price_levels):
            side_matrices[4, p_index, t_index] = \
                side_matrices[4, p_index, t_index - 1 if t_index - 1 > 0 else 0] \
                + side_matrices[1, p_index, t_index] \
                - side_matrices[2, p_index, t_index] \
                - side_matrices[3, p_index, t_index]

            if side_matrices[4, p_index, t_index] < 0:
                side_matrices[4, p_index, t_index] = 0

    return side_matrices


def get_d(num_layers, num_price_levels, minutes_per_day, full_d_asks, full_d_bids):
    d = create_zeros_array(num_layers, num_price_levels, minutes_per_day)

    for t_index in range(0, minutes_per_day):
        lowest_ask_price = min(
            np.where(full_d_asks[1, :, t_index] > 0)[0][0],
            np.where(full_d_asks[2, :, t_index] > 0)[0][0],
            np.where(full_d_asks[3, :, t_index] > 0)[0][0],
            np.where(full_d_asks[4, :, t_index] > 0)[0][0],
        )

        highest_bid_price = max(
            np.where(full_d_bids[1, :, t_index] > 0)[0][0],
            np.where(full_d_bids[2, :, t_index] > 0)[0][0],
            np.where(full_d_bids[3, :, t_index] > 0)[0][0],
            np.where(full_d_bids[4, :, t_index] > 0)[0][0],
        )

        for l_index in range(0, num_layers):
            d[l_index, 0:20, t_index] = np.flip(full_d_asks[l_index, lowest_ask_price:lowest_ask_price + 20, t_index])
            d[l_index, 20:40, t_index] = np.flip(
                full_d_bids[l_index, highest_bid_price - 20:highest_bid_price, t_index])

    return d