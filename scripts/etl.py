import extract
import transform
import load
import argparse
import logging
from logging_utilities.log_record import LogRecordIgnoreMissing
import os
import boto3
from botocore import UNSIGNED

def etl(source, chunksize):
    with extract.main(source=source, chunksize=chunksize) as reader:
        for chunk in reader:
            processed_users = transform.main(chunk)
            logging.info(f'Dimensions of processed chunk: {processed_users.shape}', extra={'step': 'ETL'})  
            load.main(processed_users)

if __name__ == '__main__':
    uploaded_rows = 0
    parser = argparse.ArgumentParser(description='Process different data sources')
    parser.add_argument('--source', type=str, required=True, help='path to the source directory or S3 prefix')
    parser.add_argument('--logfile', type=str, default='./etl.log', help='Path to logfile')
    parser.add_argument('--chunksize', type=int, default=1000, help='Chunksize to read csv files with')
    parser.add_argument('--aws', dest='aws', action='store_true', help='Use it if the source is an AWS S3 bucket')
    parser.add_argument('--bucket', type=str, help='S3 Bucket Name')

    args = parser.parse_args()

    logging.setLogRecordFactory(LogRecordIgnoreMissing)
    logging.basicConfig(filename=args.logfile, level=logging.INFO,
                        format='[%(asctime)s] [%(levelname)s] [%(step)s] %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S',
    )

    logging.info('ETL pipeline started.', extra={'step': 'ETL'})
    print('ETL pipeline started.')

    try:
        if args.aws:
            s3 = boto3.client('s3', config=boto3.session.Config(signature_version=UNSIGNED))
            # Get list of all objects in bucket with the specified prefix
            objects = s3.list_objects_v2(Bucket=args.bucket, Prefix=args.source)
            # Filter the objects list to only include CSV files
            csv_files = [obj['Key'] for obj in objects['Contents'] if obj['Key'].lower().endswith('.csv')]
            for file_key in csv_files:
                # Download the file from S3
                file_obj = s3.get_object(Bucket=args.bucket, Key=file_key)
                print(f"Parsing {file_key}")
                etl(source=file_obj['Body'], chunksize=args.chunksize)
        else:
            # Get all files in source directory
            for filename in os.listdir(args.source):
                # Keep CSV files only
                if filename.endswith('.csv'):
                    file_path = os.path.join(args.source, filename)
                    print(f"Parsing {file_path}")
                    etl(source=file_path, chunksize=args.chunksize)

        logging.info('ETL pipeline finished succesfully.', extra={'step': 'ETL'})
        print('ETL pipeline finished succesfully.')

    except Exception as e:
        logging.error(f'ETL pipeline failed with error: {e}', extra={'step': 'ETL'})
        print(f'ETL pipeline failed with error: {e}')

