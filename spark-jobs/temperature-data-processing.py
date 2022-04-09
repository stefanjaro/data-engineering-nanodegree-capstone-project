# import libraries
import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType

def initialize_spark():
    """
    Initializes a spark instance
    """
    # initialize spark
    spark = SparkSession\
        .builder\
        .appName("temperature-data-processing")\
        .getOrCreate()

    return spark

def load_and_filter_data(spark, input_fp):
    """
    Loads in the temperature data 
    and filters out non-US records and nulls

    Params
    ------
    spark: spark session
        An initialized spark session
    input_fp: str
        The location of the temperature data file
    """
    # load data
    temp_schema = StructType([
        StructField("dt", DateType()),
        StructField("AverageTemperature", DoubleType()),
        StructField("AverageTemperatureUncertainty", DoubleType()),
        StructField("City", StringType()),
        StructField("Country", StringType()),
        StructField("Latitude", StringType()),
        StructField("Longitude", StringType())
    ])

    temp = spark.read.csv(input_fp, header=True, schema=temp_schema)

    # filter out all other countries except for the US
    temp = temp.where(temp["Country"] == "United States")

    # drop all nulls in the average temperature column
    temp = temp.dropna(how="any", subset=["AverageTemperature"])

    return temp

def map_state_id(spark, temp, demo_lookup_fp, port_lookup_fp):
    """
    Combines the state city lookup tables created using the
    demographic data and airport codes data and maps the state ids
    to the cities in the temperature dataset

    Params
    ------
    spark: spark session
        An initialized spark session
    temp: spark dataframe
        The temperature dataset
    demo_lookup_fp: str
        The location of the lookup table created using the demographic data
    port_lookup_fp: str
        The location of the lookup table created using the airport codes data
    """

    # import files needed for mapping state codes
    state_city_lookup_demo = spark.read.csv(demo_lookup_fp, header=True)
    state_city_lookup_port = spark.read.csv(port_lookup_fp, header=True)

    # clean up the files before appending them together
    state_city_lookup_demo = state_city_lookup_demo.select("state_id", "City")
    state_city_lookup_demo = state_city_lookup_demo.dropna(how="any")
    state_city_lookup_demo = state_city_lookup_demo.where(
        (state_city_lookup_demo["City"] != "None") &
        (state_city_lookup_demo["state_id"] != "None")
    )

    state_city_lookup_port = state_city_lookup_port.withColumnRenamed("municipality", "City")
    state_city_lookup_port = state_city_lookup_port.dropna(how="any")
    state_city_lookup_port = state_city_lookup_port.where(
        (state_city_lookup_port["City"] != "None") &
        (state_city_lookup_port["state_id"] != "None")
    )

    # append the files together and remove duplicate records
    state_lookup = state_city_lookup_demo.union(state_city_lookup_port).distinct()

    # there are undoubtedly cities with the same name that have been mapped to multiple states
    # we can't tell which of those in the temperature dataset it belongs to
    # so we're going to do a clean de-duplicate
    state_lookup = state_lookup.dropDuplicates(["City"])

    # merge the state_id into the temperature dataset
    # of the 661524 records, only 5407 get dropped
    # these are the observations for only 2 cities
    temp = temp.join(
        F.broadcast(state_lookup),
        on="City",
        how="inner"
    )

    return temp

def create_temp_fact(temp, output_fp):
    """
    Create the temperature fact table

    Params
    ------
    temp: spark dataframe
        The temperature data
    output_fp: str
        The location to store the fact table
    """
    # extract the year from the dt
    temp = temp.withColumn("year", F.year(temp["dt"]))

    # create the starter table for the fact table
    fact_temp = temp.select("state_id", "year").dropDuplicates()

    # number of cities
    fact_temp = fact_temp.join(
        temp.groupBy("state_id", "year").agg(F.countDistinct("City")),
        on=["state_id", "year"],
        how="left"
    )

    # summary statistics on average temperature
    for stat in ["count", "mean", "max", "min"]:
        fact_temp = fact_temp.join(
            temp.groupBy("state_id", "year").agg({"AverageTemperature": stat}),
            on=["state_id", "year"],
            how="left"
        )

    # rename columns
    fact_temp_col_names = {
        "count(City)": "num_of_cities",
        "count(AverageTemperature)": "observation_count",
        "avg(AverageTemperature)": "avg_temp",
        "max(AverageTemperature)": "max_temp",
        "min(AverageTemperature)": "min_temp"
    }

    for k,v in fact_temp_col_names.items():
        fact_temp = fact_temp.withColumnRenamed(k,v)

    # calculate medians (approximate, not precise)
    fact_temp = fact_temp.join(
        temp.groupBy("state_id", "year").agg(
            F.percentile_approx("AverageTemperature", 0.5).alias("median_temp")
        ),
        on=["state_id", "year"],
        how="left"
    )

    # add a record id column
    fact_temp = fact_temp.withColumn("record_id", F.monotonically_increasing_id())

    # rearrange columns
    fact_temp = fact_temp.select(
        "record_id",
        "state_id",
        "year",
        "observation_count",
        "num_of_cities",
        "min_temp",
        "max_temp",
        "avg_temp",
        "median_temp"
    )

    # duplicate the year column since when partitioned, it'll be dropped
    # and redshift won't be able to read it from the partitioned folder names
    fact_temp = fact_temp.withColumn("year_", fact_temp["year"])

    fact_temp.write.partitionBy("year_").csv(output_fp + "fact_temperature/", "append", header=True)

def main():
    """
    Runs the spark job
    """
    # get file locations
    from shared_spark_vars import (
        temp_input_fp as input_fp,
        output_fp,
        demo_lookup_fp,
        port_lookup_fp
    )

    # run the spark job
    spark = initialize_spark()
    temp = load_and_filter_data(spark, input_fp)
    temp = map_state_id(spark, temp, demo_lookup_fp, port_lookup_fp)
    create_temp_fact(temp, output_fp)

    spark.stop()

if __name__ == "__main__":
    main()