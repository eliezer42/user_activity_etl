import pandas as pd
import json
import common.base as base
import logging
import requests
from sqlalchemy import text
from time import sleep
import datetime

with open('column_mapping.json') as f:
    cols = json.load(f)

# Defines the API endpoint and parameters
api_endpoint = 'http://ip-api.com/batch'
api_fields = ['query', 'status', 'lat', 'lon', 'country', 'regionName', 'city', 'timezone', 'isp']

# Defines a function to get geolocation info for a list of IP addresses
def get_geolocation_info(unique_ip_addresses:list):

    # Splits the list of IP addresses into batches of 100 for API bulk limit compliance
    batches = [unique_ip_addresses[i:i+100] for i in range(0, len(unique_ip_addresses), 100)]
    # Query the API for each batch
    results = []
    params = {'fields': ','.join(api_fields)}
    for batch in batches:

        response = requests.post(api_endpoint, params=params, json=batch)
        geolocation_info = response.json()

        new_geo_info_list = [{'ip_address':r['query'],
                            'latitude':r['lat'],
                            'longitude':r['lon'],
                            'country':r['country'],
                            'region':r['regionName'],
                            'city':r['city'],
                            'timezone':r['timezone'],
                            'isp_name':r['isp']}
                            for r in geolocation_info if r['status'] == 'success']
        
        results.extend(new_geo_info_list)

        print(f"{len(new_geo_info_list)} ip addresses succesfully fetched from ip-api.com")

        sleep(4)    #Blocking sleep just for API free usage limit compliance.
    
    new_ip_entries = pd.DataFrame(results)
    return new_ip_entries

def get_existing_emails(unique_emails:pd.Series) -> pd.Series:

        query = f"SELECT email FROM users WHERE email IN {tuple(unique_emails)}"
        # Execute query and load results into pd dataframe
        existing_emails_df = pd.read_sql_query(text(query), base.engine.connect())

        return existing_emails_df.iloc[:,0]


def main(extracted_df:pd.DataFrame) -> pd.DataFrame:

    logging.info('Starting data transformation', extra={'step': 'Transform'})

    try:
        # Ranames the columns according to mapping
        df = extracted_df.rename(columns=cols)

        # Converts dates into datetime objects
        df['created_at'] = pd.to_datetime(df['created_at'])
        df['updated_at'] = pd.to_datetime(df['created_at'])

        # This makes sure that we only keep the most recent entry for every user being processed
        df = df.sort_values(['email', 'updated_at'], ascending=[True, False])
        df = df.drop_duplicates(subset=['email'], keep='first')
        df = df.reset_index(drop=True)

        # Create new column in original dataframe to flag existing records
        df['user_exists'] = df['email'].isin(get_existing_emails(df['email']))

        # Retrieves geolocation info for every unique IP address
        ip_geolocation_df = get_geolocation_info(list(df.ip_address.unique()))

        # Performs a JOIN operation to enrich the dataset
        enriched_df = pd.merge(df,ip_geolocation_df, how='left', on='ip_address')

        # Finally we set the updated_at field with current datetime, indicating when was the last time it was updated in the database
        enriched_df['migrated_at'] = datetime.datetime.utcnow()

        logging.info('Data transformation finished', extra={'step': 'Transform'})

    except Exception as e:
        logging.error(f'Error: {e}', extra={'step': 'Transform'})
        raise

    return enriched_df