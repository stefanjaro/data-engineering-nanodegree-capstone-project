# import libraries
import logging
import datetime
from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from helpers import sql_create_tables

################
# CONFIGURATIONS
################

# name of s3 bucket with scripts
s3_bucket = "dendcapstoneproject"

# database config
db_schema = "public"
copy_options = ["FORMAT AS CSV", "IGNOREHEADER 1", "DELIMITER ','"]

# initialize dag
dag = DAG(
    "load-data-into-redshift",
    start_date=datetime.datetime.now()-datetime.timedelta(days=1),
    schedule_interval="@once"
)

################
# CREATE TABLES
################

create_tables_on_redshift = PostgresOperator(
    task_id="create_tables_on_redshift",
    dag=dag,
    postgres_conn_id="pg_redshift",
    sql=sql_create_tables.CREATE_ALL_TABLES
)

#######################
# COPY DATA OPERATIONS
#######################

# copy fact immigration (select columns to exclude partition columns)
copy_fact_immigration = S3ToRedshiftOperator(
    task_id="copy_fact_immigration",
    dag=dag,
    schema=db_schema,
    table="fact_immigration",
    column_list=[
        "record_id", "state_id", "arrival_date", "profile_id", 
        "all_travellers", "land_travellers", "sea_travellers",
        "air_travellers", "business_travellers", "pleasure_travellers",
        "student_travellers"
        ],
    s3_bucket=s3_bucket,
    s3_key="output_files/fact_immigration/",
    redshift_conn_id="aws_redshift",
    aws_conn_id="aws_credentials",
    copy_options=copy_options
)

# copy fact demographics
copy_fact_demographics = S3ToRedshiftOperator(
    task_id="copy_fact_demographics",
    dag=dag,
    schema=db_schema,
    table="fact_demographics",
    column_list=[
        "state_id", "total_pop", "male_pop", "female_pop",
        "veteran_pop", "foreign_pop", "hispanic_pop", "white_pop",
        "asian_pop", "black_pop", "native_pop", "avg_hh_size", "total_hh"
    ],
    s3_bucket=s3_bucket,
    s3_key="output_files/fact_demographics/",
    redshift_conn_id="aws_redshift",
    aws_conn_id="aws_credentials",
    copy_options=copy_options
)

# copy fact ports
copy_fact_ports = S3ToRedshiftOperator(
    task_id="copy_fact_ports",
    dag=dag,
    schema=db_schema,
    table="fact_ports",
    column_list=[
        "record_id", "state_id",
        "port_type", "num_of_ports"
    ],
    s3_bucket=s3_bucket,
    s3_key="output_files/fact_ports/",
    redshift_conn_id="aws_redshift",
    aws_conn_id="aws_credentials",
    copy_options=copy_options
)

# copy fact temperature
copy_fact_temperature = S3ToRedshiftOperator(
    task_id="copy_fact_temperature",
    dag=dag,
    schema=db_schema,
    table="fact_temperature",
    column_list=[
        "record_id", "state_id", "year", "observation_count",
        "num_of_cities", "min_temp", "max_temp", "avg_temp", "median_temp"
    ],
    s3_bucket=s3_bucket,
    s3_key="output_files/fact_temperature/",
    redshift_conn_id="aws_redshift",
    aws_conn_id="aws_credentials",
    copy_options=copy_options
)

# copy dim state
copy_dim_state = S3ToRedshiftOperator(
    task_id="copy_dim_state",
    dag=dag,
    schema=db_schema,
    table="dim_state",
    column_list=["state_id", "state_name"],
    s3_bucket=s3_bucket,
    s3_key="output_files/dim_state/",
    redshift_conn_id="aws_redshift",
    aws_conn_id="aws_credentials",
    copy_options=copy_options
)

# copy dim state
copy_dim_datetime = S3ToRedshiftOperator(
    task_id="copy_dim_datetime",
    dag=dag,
    schema=db_schema,
    table="dim_datetime",
    column_list=[
        "timestamp", "day", "month",
        "year", "month_year", "day_of_week"
    ],
    s3_bucket=s3_bucket,
    s3_key="output_files/dim_time/",
    redshift_conn_id="aws_redshift",
    aws_conn_id="aws_credentials",
    copy_options=copy_options
)

# copy dim traveller profile to a staging table (we need to deduplicate the data)
copy_staging_dim_traveller_profile = S3ToRedshiftOperator(
    task_id="copy_dim_traveller_profile",
    dag=dag,
    schema=db_schema,
    table="staging_dim_traveller_profile",
    column_list=[
        "profile_id", "gender", "age_category",
        "citizen_region", "citizen_global_region"
    ],
    s3_bucket=s3_bucket,
    s3_key="output_files/dim_traveller_profile/",
    redshift_conn_id="aws_redshift",
    aws_conn_id="aws_credentials",
    copy_options=copy_options
)

#######################################
# DEDUPE OPERATION ON TRAVELLER PROFILE
#######################################

# dedupe
traveller_profile_dedupe = PostgresOperator(
    task_id="traveller_profile_dedupe",
    dag=dag,
    postgres_conn_id="pg_redshift",
    sql="""
        INSERT INTO dim_traveller_profile
            SELECT DISTINCT *
            FROM staging_dim_traveller_profile;
    """
)

# drop staging table
drop_staging_table = PostgresOperator(
    task_id="drop_staging_table",
    dag=dag,
    postgres_conn_id="pg_redshift",
    sql="""
        DROP TABLE IF EXISTS staging_dim_traveller_profile;
    """
)

#######################
# DATA QUALITY CHECKS
#######################

def non_empty_check(*args, **kwargs):
    """
    Makes sure the provided tables on Redshift have data in it
    by simply getting their number of records (running a count) and
    raising a value error if they have less than a single record
    """
    # get list of tables
    tables = kwargs["params"]["tables"]

    # form connection to redshift
    redshift = PostgresHook(postgres_conn_id="pg_redshift")

    # run query and check results for each table
    for table in tables:
        results = redshift.get_records(f"SELECT COUNT(*) FROM {table}")

        if results[0][0] < 1 or results[0][0] == "0":
            raise ValueError(f"Data quality check failed. {table} is empty!")

        logging.info(f"Data quality check passed. {table} has {results[0][0]} records.")

def unique_values_check(*args, **kwargs):
    """
    Makes sure the provided tables on Redshift only have unique values in the
    provided columns by simply comparing their total size with the number of distinct
    values in the provided columns
    """
    # get tables and columns
    tables_and_columns = kwargs["params"]["tables_and_columns"]

    # form connection to redshift
    redshift = PostgresHook(postgres_conn_id="pg_redshift")

    # run checks on each table and column combination
    for table, column in tables_and_columns.items():
        total_count = redshift.get_records(f"SELECT COUNT(*) FROM {table}")
        distinct_count = redshift.get_records(f"SELECT COUNT(DISTINCT {column}) FROM {table}")

        if total_count[0][0] != distinct_count[0][0]:
            raise ValueError(
                f"Check failed for {table}. Total count is {total_count[0][0]}. Distinct count is {distinct_count[0][0]}."
            )
        
        logging.info(
            f"Data quality check passed. Total and distinct counts are the same for the {column} column in {table}"
        )

# make sure every table has records
check_for_records = PythonOperator(
    task_id="check_for_records",
    dag=dag,
    python_callable=non_empty_check,
    provide_context=True,
    params={"tables": [
        "fact_immigration", "fact_demographics", "fact_temperature",
        "fact_ports", "dim_datetime", "dim_traveller_profile", "dim_state"
        ]
    }
)

# make sure all primary key columns are truly unique
check_for_uniques = PythonOperator(
    task_id="check_for_uniques",
    dag=dag,
    python_callable=unique_values_check,
    provide_context=True,
    params={"tables_and_columns": {
        "fact_immigration": "record_id",
        "fact_demographics": "state_id",
        "fact_temperature": "record_id",
        "fact_ports": "record_id",
        "dim_datetime": "timestamp",
        "dim_traveller_profile": "profile_id",
        "dim_state": "state_id"
    }}
)

###########
# JOB FLOW
###########

create_tables_on_redshift >> [
    copy_fact_immigration, copy_fact_demographics, copy_fact_ports, copy_fact_temperature,
    copy_dim_state, copy_dim_datetime, copy_staging_dim_traveller_profile
]

copy_staging_dim_traveller_profile >> traveller_profile_dedupe >> drop_staging_table

[
    copy_fact_immigration, copy_fact_demographics, copy_fact_ports, 
    copy_fact_temperature, copy_dim_state, copy_dim_datetime, drop_staging_table] >> check_for_records

check_for_records >> check_for_uniques