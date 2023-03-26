import pandas as pd
from common.base import engine
from sqlalchemy import text
import logging
from common.base import session
from common.tables import User

def main(processed_df:pd.DataFrame) -> None:
    logging.info('Starting data load.', extra={'step': 'Load'})
    try:
        # first insert new users
        new_users_df = processed_df[~processed_df['user_exists']]
        inserted_rows = new_users_df.rename(columns={'user_exists':'updated'}).to_sql('users', con=engine, if_exists='append', index = False)
        logging.info(f'{inserted_rows} rows were inserted.', extra={'step': 'Load'}) 
        print(f'[LOAD] {inserted_rows} rows were inserted.') 

        #then update existing ones
        updated_rows = 0
        existing_users_df = processed_df[processed_df['user_exists']]
        for index, row in existing_users_df.iterrows():
            user = session.query(User).filter_by(email=row['email']).first()
            if user is not None:
                updated_rows += 1
                user.ip_address = row['ip_address']
                user.password = row['password']
                user.status = row['status']
                user.latitude = row['latitude']
                user.longitude = row['longitude']
                user.country = row['country']
                user.region = row['region']
                user.city = row['city']
                user.timezone = row['timezone']
                user.isp_name = row['isp_name']
                user.updated = True
                session.commit()

        logging.info(f'{updated_rows} rows were updated.', extra={'step': 'Load'})
        print(f'[LOAD] {updated_rows} rows were updated.')   

        logging.info('Data load finished.', extra={'step': 'Load'})
    except Exception as e:
        logging.error(f'Error: {e}', extra={'step': 'Load'})
        raise
