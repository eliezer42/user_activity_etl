# user_activity_etl

User activity ETL written in python. Takes data from CSV files, process them and dumps the result into a PostgresSQL Database.

## Pre-requisites

To run this project you will have to:
1. set up a postgresql server
2. have python3.9 or higher installed
3. clone or download this repo
4. set database connection parameters as environment variables
5. (optional) set up a python virtual environment

## Usage

These ETL project is enabled for handling csv files either stored locally or remotely in an AWS S3 bucket. The following examples shows both use cases. The chunksize parameter set the amount of rows to be parsed at a time by the ETL, which makes the project able to handle large csv files. 
In order to test S3 data retrieving functionality, I have set up a bucket with publicly available files.

```
python ./scripts/etl.py --source ./input/ --chunksize 1000
python ./scripts/etl.py --source user_activity --aws --bucket em-datasets --chunksize 1000
```
Depending on your operating system you may need to use `python3` instead of `python`, to run these commands with the right version of python.

When running the ETL, all logs are stored in a file called `etl.log` located in the root folder of the project.

Assuming you have the project folder in your local disk and a postgresql server was properly set up either in your localhost or in a remote host, go to the root directory of the project and open a terminal in that location.

Set the following environment variables as shown.

Windows:

````
$env:DB_USER='YOUR_USERNAME_HERE'
$env:DB_PASS='YOUR_PASSWORD_HERE'
$env:DB_HOST='YOUR_HOSTNAME_HERE'
$env:DB_PORT='YOUR_TCP_PORT_HERE'
````

Linux:

````
export DB_USER='YOUR_USERNAME_HERE'
export DB_PASS='YOUR_PASSWORD_HERE'
export DB_HOST='YOUR_HOSTNAME_HERE'
export DB_PORT='YOUR_TCP_PORT_HERE'
````

Take into account that the projects defines default values for DB_USER, DB_HOST and DB_PORT as 'postgres', 'localhost' and 5432 respectively, so if these values matches your configuration, you only require to set up DB_PASS. Additionally, you have to create a database called `user_activity` manually. You must also run the `common/create_tables.py` script to create the table `users` for the first time.

Finally, make sure you have the dependencies installed in your environment. You can do so by runnning the following line `pip install -r requirements.txt` in Windows. Alternatively, if you are using a linux distro, execute `python3 -m pip install -r requirements_linux.txt` (or just `pip install -r requirements_linux.txt` in case you are using a virtual environment, which I highly encourage when using linux).

## Project Structure

    .
    ├── input                   # All input files could be locally stored here
    │   └── data_01.csv         # Sample CSV file
    │   ...
    │   ...
    ├── scripts                 # Source files for ETL
    │   ├── common              # Common functionality folder
    │   │   ├── base.py         # Contains database connection definitions
    │   │   └── tables.py       # Defines the data model
    │   ├── create_tables.py    # Creates the `users` table when run
    │   ├── extract.py          # Retrieves the data into a proper dataframe
    │   ├── transform.py        # Holds the tranformations logic
    │   ├── load.py             # Stores the processed dataframe into the database
    │   └── etl.py              # Main script
    ├── column_mappings.json    # JSON files that helps to map original field names to the final ones
    └── ...

## Approach

This ETL project follows a straightforward file structure, where the  Extract, Transform and Load functions are defined in their corresponding source files. Every one of these python modules has a `main` function, that is responsible for executing the internal logic. In the same way, there is one script that orchestrates the three phases called `etl.py`. This source file handles cli interaction as well, so it has to be executed with proper arguments and options from a shell in order to run this ETL pipeline.

The main libraries of the project are `pandas` and `sqlalchemy`. The first was choosen due to its user-friendliness and efficiency since it is built on top of `numpy`. The latter makes it easy to upload data to the databases without sacrificing performance. In the same way `boto3` was choosen to intereact with S3 service of AWS.

In order to enrich the data with geolocation info, ip-api.com is used, since it allows an unlimited amount of requests per day. It allows to perform bulk requests wich makes the whole process of data enrichment faster. The only drawback is the limit of 15 requests per minute in its free tier, which makes it mandatory to add a delay to avoid getting blocked by the API. Another thing to take into consideration is that there are some IP addresses in the dataset that belong to a multicast IP range (224.0.0.0 through 239.255.255.255) so they do not correspond to any geographic region. As a result, some entries have no geolocation info.

The schema of the final table is defined through a User model that leverages SQLAlchemy ORM for datatype and constraint definitions that will be applied to the table.

```python
class User(base.Base):
    __tablename__ = "users"

    id = mapped_column(Integer, primary_key=True)
    first_name = mapped_column(String(80))
    last_name = mapped_column(String(80))
    email = mapped_column(String(320))
    gender = mapped_column(String(16))
    ip_address = mapped_column(String(40))  # It takes into account IPv4 and IPv6 address length
    created_at = mapped_column(Date)
    updated_at = mapped_column(Date)
    password = mapped_column(String(64))    # Suitable for MD5 and SHA-256 hash length
    status = mapped_column(Boolean)
    latitude = mapped_column(Float)
    longitude = mapped_column(Float)
    country = mapped_column(String(64))
    region = mapped_column(String(64))
    city = mapped_column(String(64))
    timezone =mapped_column(String(32))
    isp_name = mapped_column(String(128))
    updated = mapped_column(Boolean)        # If set to True, the record was updated
```

In order to check for existing entries, the `email` field is used to query the database during the Transform phase.

### Scalability

This ETL is able to handle large csv files since it takes advantage of pandas ability to load data in chunks. As a result the script could process csv files of several GBs.

```python
def etl(source, chunksize):
    with extract.main(source=source, chunksize=chunksize) as reader:
        for chunk in reader:
            processed_users = transform.main(chunk)
            ...
            load.main(processed_users)
```

It is also possible to enable this ETL to run on a cluster by using pandas API for PySpark, or to leverage multi-core execution with polars library, which requires little code refactoring.

