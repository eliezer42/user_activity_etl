import pandas as pd
import json
import logging


# Extract column mapping
with open('column_mapping.json') as f:
    cols = json.load(f)

def main(source, chunksize:int=None):
    logging.info('Starting data extraction', extra={'step': 'Extract'})
    try:
        # Reads the csv file and extracts a subset of the columns
        raw_df = pd.read_csv(source, encoding='utf-8', usecols=cols.keys(), chunksize=chunksize, on_bad_lines='skip', index_col='id')

        logging.info(f'Data extraction finished for {source}.', extra={'step': 'Extract'})

    except Exception as e:
        logging.error(f'Error: {e}', extra={'step': 'Extract'})
        raise

    return raw_df
