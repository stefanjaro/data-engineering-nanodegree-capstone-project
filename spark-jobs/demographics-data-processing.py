# import libraries
import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType

def initialize_spark():
    """
    Initializes a spark instance
    """
    # initialize spark
    spark = SparkSession\
        .builder\
        .appName("demographics-data-processing")\
        .getOrCreate()

    return spark

def load_demo_data(spark, input_fp):
    """
    This function simply loads in the demographic data

    Params
    ------
    spark: spark session
        An initialized spark session
    input_fp: str
        The location of the demographics file
    """
    # specify schema
    demo_file_schema = StructType([
        StructField("City", StringType()),
        StructField("State", StringType()),
        StructField("Median Age", DoubleType()),
        StructField("Male Population", IntegerType()),
        StructField("Female Population", IntegerType()),
        StructField("Total Population", IntegerType()),
        StructField("Number of Veterans", IntegerType()),
        StructField("Foreign-born", IntegerType()),
        StructField("Average Household Size", DoubleType()),
        StructField("State Code", StringType()),
        StructField("Race", StringType()),
        StructField("Count", IntegerType())
    ])

    # load data
    demo = spark.read.csv(
        input_fp, 
        header=True, 
        sep=";",
        schema=demo_file_schema
    )

    return demo

def create_city_state_lookup(demo, preprocessed_fp):
    """
    Creates a lookup table containing cities and state codes

    Params
    ------
    demo: spark dataframe
        The demographics data
    preprocessed_fp: str
        The location to store the lookup table
    """
    # create a city, state, and state code database that we can use to map state codes to the temperature data
    state_city_lookup = demo.select("City", "State", "State Code").dropDuplicates()

    # rename state code to avoid running into an error
    state_city_lookup = state_city_lookup.withColumnRenamed("State Code", "state_id")

    # write to folder
    state_city_lookup.write.parquet(preprocessed_fp + "state_city_lookup_demo/", "append")

def create_state_dim(demo, output_fp):
    """
    Creates the state dimension table

    Params
    ------
    demo: spark dataframe
        The demographics data
    output_fp: str
        The location to store the dimension table in
    """
    # create our state dimension table
    state_dim = demo.select("State", "State Code").dropDuplicates()

    # rename columns
    state_dim_new_cols = {
        "State Code": "state_id",
        "State": "state_name"
    }

    for k,v in state_dim_new_cols.items():
        state_dim = state_dim.withColumnRenamed(k,v)

    # write into folder
    state_dim.write.parquet(output_fp + "dim_state/", "append")

def create_fact_demographics(demo, output_fp):
    """
    Creates the fact demographics table

    Params
    ------
    demo: spark dataframe
        The demographics data
    output_fp: str
        The location to store the dimension table in
    """
    # remove the race data from the table and store it separately
    race_demo = demo.select("City", "State Code", "Race", "Count")

    # drop the race data and deduplicate
    demo = demo.drop("Race", "Count")
    demo = demo.dropDuplicates()

    # calculate total number of households
    demo = demo.withColumn("total_number_of_households", demo["Total Population"] / demo["Average Household Size"])

    # roll everything we need up to a state code level to begin creating the fact table
    exprs = {x:"sum" for x in demo.columns if x not in ["City", "State", "State Code", 
                                                        "Median Age", "Average Household Size"]}

    fact_demo = demo.groupBy("State Code").agg(exprs)

    # now for the race demo table, aggregate to a state level with a column for each race
    fact_race_demo = race_demo.groupBy("State Code").pivot("Race").agg({"Count": "sum"})

    # merge the main demo table and the race-wise table
    fact_demo = fact_demo.join(
        fact_race_demo,
        on="State Code",
        how="left"
    )

    # rename all columns
    fact_demo_col_names = {
        "State Code": "state_id",
        "sum(Total Population)": "total_pop",
        "sum(Female Population)": "female_pop",
        "sum(Number of Veterans)": "veteran_pop",
        "sum(Foreign-born)": "foreign_pop",
        "sum(Male Population)": "male_pop",
        "sum(total_number_of_households)": "total_hh",
        "American Indian and Alaska Native": "native_pop",
        "Asian": "asian_pop",
        "Black or African-American": "black_pop",
        "Hispanic or Latino": "hispanic_pop",
        "White": "white_pop"
    }

    for k,v in fact_demo_col_names.items():
        fact_demo = fact_demo.withColumnRenamed(k,v)

    # calculate average household size at a state level
    fact_demo = fact_demo.withColumn("avg_hh_size", fact_demo["total_pop"] / fact_demo["total_hh"])

    # write to output folder
    fact_demo.write.parquet(output_fp + "fact_demographics/", "append")

def main():
    """
    The main function that orchestrates the spark job
    """
    # get file locations
    from shared_spark_vars import (
        demo_input_fp as input_fp,
        preprocessed_fp,
        output_fp
    )
    
    # run the spark job
    spark = initialize_spark()
    demo = load_demo_data(spark, input_fp)
    create_city_state_lookup(demo, preprocessed_fp)
    create_state_dim(demo, output_fp)
    create_fact_demographics(demo, output_fp)

    spark.stop()

if __name__ == "__main__":
    main()