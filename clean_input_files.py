import json
import gzip
import pandas as pd
import os
from table_cleaning import \
    snapshot_ops, \
    product_ops, \
    vendor_ops, \
    warehouse_products_ops, \
    item_bins_ops

def extract_substring_from_file(content, start_pos, end_pos_from_right):
    end_pos = len(content) - end_pos_from_right + 1
    substring = content[start_pos:end_pos]
    return substring


def extract_cpgz_file(gzip_file_path):
    with gzip.open(gzip_file_path, 'rb') as f_in:
        return f_in.read()


def clean_input_files():
    if not os.path.isdir('pq'):
        os.mkdir('pq')

    zip_file_path = 'Inventory_snapshot_data.cpgz'
    content = extract_cpgz_file(zip_file_path)
    print('extracting cpgz file')
    json_data = json.loads(extract_substring_from_file(content, 100, 2108).decode())
    print('converted, cleaned and loaded json in variable')
    snapshot_id = json_data['snapshot_id']
    products = snapshot_ops(json_data)
    print('Saved table info parquet file: snapshot')

    product_ids = []
    vendors = []
    warehouse_products = []
    products_data = []
    for product in products:
        vendor, warehouse_product, product_id, product_df = product_ops(product, snapshot_id)
        vendors.append(vendor)
        warehouse_products.append(warehouse_product)
        product_ids.append(product_id)
        products_data.append(product_df)

    products_df = pd.DataFrame(products_data)
    products_df.to_parquet('pq/products_df.pq', engine='fastparquet')
    print('Saved table info parquet file: product')

    vendors_final = []
    warehouse_products_final = []
    item_bins_final = []
    for idx, product_id in enumerate(product_ids):
        vendor_dictx = vendor_ops(vendors[idx], product_id)
        vendors_final.append(vendor_dictx)

        warehouse_products_dictx, item_bin = warehouse_products_ops(
            warehouse_products[idx],
            product_id
        )
        item_bins_final.append(item_bin)
        warehouse_products_final.append(warehouse_products_dictx)

    vendors_df = pd.DataFrame([item for sublist in vendors_final for item in sublist])
    vendors_df['snapshot_id']=snapshot_id
    vendors_df.to_parquet('pq/vendors_df.pq', engine='fastparquet')
    print('Saved table info parquet file: vendor')

    warehouse_products_df = pd.DataFrame([item for sublist in warehouse_products_final for item in sublist])
    warehouse_products_df['snapshot_id']=snapshot_id
    warehouse_products_df.to_parquet('pq/warehouse_products_df.pq', engine='fastparquet')
    print('Saved table info parquet file: warehouse_product')

    item_bins_ops(item_bins_final,snapshot_id)
    print('Saved table info parquet file: item_bins')
