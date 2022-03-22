import numpy as np
import pandas as pd
from csv_to_matrices import compressor


def process_complex_side_records(side, side_matrices, full_d_records, previous_add_records):
    full_d_priceless_records = full_d_records.loc[full_d_records['Price'] == 0]
    add_records = pd.concat((
        previous_add_records,
        full_d_records.loc[full_d_records['EventType'] == 'ADD ' + side]
    )).set_index('OrderNumber')

    # FILL was not fount in ITCH file, excessive work to add them + processing time consuming

    # EXECUTE
    execute_asks = full_d_priceless_records.loc[full_d_priceless_records['EventType'] == 'EXECUTE ' + side].set_index('OrderNumber')
    full_execute_asks = execute_asks.join(add_records, on='OrderNumber', how='left', lsuffix='_execute', rsuffix='_add')
    full_execute_asks['price_level_add'] = np.int32(full_execute_asks['price_level_add'])  # after join right table has wrong column types
    full_execute_asks \
        .loc[full_execute_asks['time_index_execute'] >= full_execute_asks['time_index_add']] \
        .apply(compressor.process_execute_priceless_records, full_d=side_matrices, axis=1)

    # CANCEL
    cancel_asks = full_d_priceless_records.loc[full_d_priceless_records['EventType'] == 'CANCEL ' + side].set_index('OrderNumber')
    full_cancel_asks = cancel_asks.join(add_records, on='OrderNumber', how='left', lsuffix='_cancel', rsuffix='_add')
    full_cancel_asks['price_level_add'] = np.int32(full_cancel_asks['price_level_add'])  # after join right table has wrong column types
    full_cancel_asks \
        .loc[full_cancel_asks['time_index_cancel'] >= full_cancel_asks['time_index_add']] \
        .apply(compressor.process_cancel_records, full_d=side_matrices, axis=1)

    # DELETE
    delete_asks = full_d_priceless_records.loc[full_d_priceless_records['EventType'] == 'DELETE ' + side].set_index('OrderNumber')
    full_delete_asks = delete_asks.join(add_records, on='OrderNumber', how='left', lsuffix='_delete', rsuffix='_add')
    full_delete_asks['price_level_add'] = np.int32(full_delete_asks['price_level_add'])  # after join right table has wrong column types
    full_delete_asks \
        .loc[full_delete_asks['time_index_delete'] >= full_delete_asks['time_index_add']] \
        .apply(compressor.process_delete_records, full_d=side_matrices, axis=1)

    return side_matrices


def process_pending_matrices_layer(matrices, t_index, num_price_levels):
    matrices[4, :, t_index] = matrices[1, :, t_index]

    for p_index in range(0, num_price_levels):
        matrices[4, p_index, t_index] = \
            matrices[4, p_index, t_index - 1 if t_index - 1 > 0 else 0] \
            + matrices[1, p_index, t_index] \
            - matrices[2, p_index, t_index] \
            - matrices[3, p_index, t_index]

        if matrices[4, p_index, t_index] < 0:
            matrices[4, p_index, t_index] = 0

    return matrices


def process_side_records(side, num_price_levels, records, previous_add_records, side_matrices, t_index):
    full_d_records = records.loc[records['price_level'] < num_price_levels]

    side_matrices = compressor.process_simple_side_records(side, side_matrices, full_d_records)
    side_matrices = process_complex_side_records(side, side_matrices, full_d_records, previous_add_records)

    side_matrices = process_pending_matrices_layer(side_matrices, t_index, num_price_levels)

    return side_matrices


def get_t_index_from_d(num_layers, num_price_levels, full_d_asks, full_d_bids, t_index):
    d = compressor.create_zeros_array(num_layers, num_price_levels, 1)

    lowest_ask_prices = []
    ordered_asks = np.where(full_d_asks[1, :, t_index] > 0)
    filled_asks = np.where(full_d_asks[2, :, t_index] > 0)
    canceled_asks = np.where(full_d_asks[3, :, t_index] > 0)
    pending_asks = np.where(full_d_asks[4, :, t_index] > 0)

    if np.any(ordered_asks):
        lowest_ask_prices.append(ordered_asks[0][0])
    if np.any(filled_asks):
        lowest_ask_prices.append(filled_asks[0][0])
    if np.any(canceled_asks):
        lowest_ask_prices.append(canceled_asks[0][0])
    if np.any(pending_asks):
        lowest_ask_prices.append(pending_asks[0][0])

    lowest_ask_price = min(lowest_ask_prices)


    highest_bid_prices = []
    ordered_bids = np.where(full_d_bids[1, :, t_index] > 0)
    filled_bids = np.where(full_d_bids[2, :, t_index] > 0)
    canceled_bids = np.where(full_d_bids[3, :, t_index] > 0)
    pending_bids = np.where(full_d_bids[4, :, t_index] > 0)

    if np.any(ordered_bids):
        highest_bid_prices.append(ordered_bids[0][0])
    if np.any(filled_bids):
        highest_bid_prices.append(filled_bids[0][0])
    if np.any(canceled_bids):
        highest_bid_prices.append(canceled_bids[0][0])
    if np.any(pending_bids):
        highest_bid_prices.append(pending_bids[0][0])

    highest_bid_price = max(highest_bid_prices)

    for l_index in range(0, num_layers - 1):
        # if lowest ask price missing, price slice will be from 0 to 20
        # if highest bid price missing, price slice will be from (max) - 20 to (max)
        d[l_index, 0:20, 0] = np.flip(full_d_asks[l_index, lowest_ask_price:lowest_ask_price + 20, t_index])

        if highest_bid_price - 20 < 0:
            highest_bid_price = -1

        d[l_index, 20:40, 0] = np.flip(full_d_bids[l_index, highest_bid_price - 20:highest_bid_price, t_index])

    d[num_layers - 1, :, 0] = t_index

    return d
