# import libraries
import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

def initialize_spark():
    """
    Initializes a spark instance
    """
    # initialize spark
    spark = SparkSession\
        .builder\
        .appName("airport-data-processing")\
        .getOrCreate()

    return spark

def preprocess_ports_data(spark, input_fp):
    """
    Preprocesses the airport codes data to be used by other functions

    Params
    ------
    spark: spark session
        An initialized spark session
    input_fp: str
        The location of the airport codes raw data file
    """

    # load raw data
    ports = spark.read.csv(input_fp, header=True, inferSchema=True)

    # filter out the non us airports
    ports = ports.where(ports["iso_country"] == "US")

    # extract the US states from the iso_region
    extract_state = F.udf(lambda x: x.split("-")[-1].strip(), StringType())
    ports = ports.withColumn("state_id", extract_state(ports["iso_region"]))

    # drop any records with state codes that aren't 2 characters long
    ports = ports.where(F.length(ports["state_id"]) == 2)

    return ports

def create_ports_fact(ports, output_fp):
    """
    Creates the ports fact table

    Params
    ------
    ports: spark dataframe
        The preprocessed airport codes data
    output_fp: str
        The location where the final fact table should be stored
    """
    # create ports fact table
    fact_trans_ports = ports.groupby("state_id", "type").count()

    # create record id
    fact_trans_ports = fact_trans_ports.withColumn("record_id", F.monotonically_increasing_id())

    # rename columns
    fact_trans_col_names = {
        "type": "port_type",
        "count": "num_of_ports"
    }

    for k,v in fact_trans_col_names.items():
        fact_trans_ports = fact_trans_ports.withColumnRenamed(k, v)

    fact_trans_ports.write.parquet(output_fp + "fact_ports/", "append")

def create_state_city_lookup(ports, preprocessed_fp):
    """
    Creates a state city lookup table that we can use for the temperature data (to assign state codes)
    """
    # get state id and municipality and drop duplicates
    state_city_lookup = ports.select("state_id", "municipality").dropDuplicates()

    # write to proprocessed folder
    state_city_lookup.write.parquet(preprocessed_fp + "state_city_lookup_ports/", "append")

def main():
    """
    The main function that runs the airport codes spark job
    """
    # get file locations
    from shared_spark_vars import (
        ac_input_fp as input_fp,
        output_fp,
        preprocessed_fp
    )

    # run spark job
    spark = initialize_spark()
    ports = preprocess_ports_data(spark, input_fp)
    create_ports_fact(ports, output_fp)
    create_state_city_lookup(ports, preprocessed_fp)
    spark.stop()

if __name__ == "__main__":
    main()