import pandas as pd


def snapshot_ops(data):
    snapshot_info_cols = {
        'snapshot_id',
        'warehouse_id',
        'customer_account_id',
        'snapshot_started_at',
        'warehouses_searched',
        'snapshot_finished_at',
    }

    snapshot_info_df = pd.DataFrame([{
        i: data[i] for i in snapshot_info_cols
    }])

    snapshot_info_df.to_parquet('pq/snapshot_info.pq', engine='fastparquet')
    snapshot_info_cols.add('products')

    if not len(setx := (set(data.keys()) - snapshot_info_cols)) == 0:
        print('unprocessed columns in dataframe:', setx)
        exit()

    return list(data['products'].values())


def product_ops(data, snapshot_id):
    product_cols = {
        'sku',
        'account_id'
    }
    product_df = {
        i: data[i] for i in product_cols
    }
    product_df['snapshot_id'] = snapshot_id
    product_cols.add('vendors')
    product_cols.add('warehouse_products')

    if not len(setx := (set(data.keys()) - product_cols)) == 0:
        print('unprocessed columns in dataframe:', setx)
        exit()

    return list(data['vendors'].values()), list(data['warehouse_products'].values()), data['sku'], product_df


def vendor_ops(data, product_id):
    vendor_cols = {
        'vendor_id', 'vendor_name', 'vendor_sku'
    }

    df_list = []
    for datax in data:
        vendor_df = {
            i: datax[i] for i in vendor_cols
        }
        vendor_df['product_id'] = product_id

        if not len(setx := (set(datax.keys()) - vendor_cols)) == 0:
            print('unprocessed columns in dataframe:', setx)
            exit()

        df_list.append(vendor_df)
    return df_list


def warehouse_products_ops(data, product_id):
    df_list = []
    item_bins = []
    for datax in data:
        warehouse_products_cols = {
            'warehouse_id',
            'on_hand',
            'allocated',
            'backorder',
            'available',
            'non_sellable'
        }
        warehouse_products_df = {
            i: datax[i] for i in warehouse_products_cols
        }
        warehouse_products_df['product_id'] = product_id
        warehouse_products_cols.add('item_bins')
        if datax['item_bins']:
            item_bins.append((tuple(datax['item_bins'].values()), datax['warehouse_id']))

        if not len(setx := (set(datax.keys()) - warehouse_products_cols)) == 0:
            print('unprocessed columns in dataframe:', setx)
            exit()

        df_list.append(warehouse_products_df)
    return df_list, item_bins


def item_bins_ops(data, snapshot_id):
    item_unit = [item for sublist in data for item in sublist]
    item_bins = []
    for process_item in item_unit:
        for item in process_item[0]:
            item_cols = {
                'location_id', 'location_name', 'lot_id',
                'lot_name', 'expiration_date', 'sellable', 'quantity'
            }
            current_item = {
                i: item[i] for i in item_cols
            }
            current_item['warehouse_id'] = process_item[1]
            item_bins.append(current_item)
            item_cols.add('warehouse_id')

            if not len(setx := (set(current_item.keys()) - item_cols)) == 0:
                print('unprocessed columns in dataframe:', setx)
                exit()

    item_bins_df = pd.DataFrame(item_bins)
    item_bins_df['snapshot_id'] = snapshot_id
    item_bins_df.to_parquet('pq/item_bins_df.pq', engine='fastparquet')
