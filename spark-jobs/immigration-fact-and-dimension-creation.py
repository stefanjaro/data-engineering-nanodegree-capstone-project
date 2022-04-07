# import libraries
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def initialize_spark():
    """
    Initializes a spark instance
    """
    # initialize spark
    spark = SparkSession\
        .builder\
        .appName("immigration-fact-and-dimension-creation")\
        .getOrCreate()

    return spark

def create_immigration_fact(spark, preprocessed_file_path, output_file_path):
    """
    Creates the immigration fact table

    Params
    ------
    spark: spark instance
        A spark instance that's been initialized
    preprocessed_file_path: str
        The location of the preprocessed immigration data
    output_file_path: str
        The location to store the final fact/dimension data
    """
    # read in the immigration data
    imm = spark.read.parquet(preprocessed_file_path)

    # create the skeleton table that will become fact_immigration table
    fact_imm = imm.select("i94port_state", "profile_id", "arrdateclean").dropDuplicates()

    # index columns
    index_cols = ["i94port_state", "profile_id", "arrdateclean"]

    # add total number of travellers to the above
    fact_imm = fact_imm.join(
        imm.groupby(index_cols).count(),
        on=index_cols,
        how="left"
    )

    # add number of travellers by transportation mode
    fact_imm = fact_imm.join(
        imm.groupby(index_cols).pivot("i94mode_label").count(),
        on=index_cols,
        how="left"
    )

    # add number of travellers by purpose of visit
    fact_imm = fact_imm.join(
        imm.groupby(index_cols).pivot("i94visa_label").count(),
        on=index_cols,
        how="left"
    )

    # drop the not reported column in the fact table
    fact_imm = fact_imm.drop("Not Reported")

    # rename columns in the fact immigration table
    fact_imm_col_names = {
        "i94port_state": "state_id",
        "arrdateclean": "arrival_date",
        "count": "all_travellers",
        "Air": "air_travellers",
        "Land": "land_travellers",
        "Sea": "sea_travellers",
        "Business": "business_travellers",
        "Pleasure": "pleasure_travellers",
        "Student": "student_travellers"
    }

    for k,v in fact_imm_col_names.items():
        fact_imm = fact_imm.withColumnRenamed(k, v)

    # fill nulls with 0
    fact_imm = fact_imm.fillna(0)

    # add a record id column
    fact_imm = fact_imm.withColumn("record_id", F.monotonically_increasing_id())

    # add a month and year for partitioning
    fact_imm = fact_imm.withColumn("month", F.month(fact_imm["arrival_date"]))
    fact_imm = fact_imm.withColumn("year", F.year(fact_imm["arrival_date"]))

    # write to parquet files
    fact_imm.write.partitionBy("month", "year").parquet(output_file_path + "fact_immigration/", "append")

def create_traveller_profile_dimension(spark, preprocessed_file_path, output_file_path):
    """
    Creates the traveller profile dimension table

    Params
    ------
    spark: spark instance
        A spark instance that's been initialized
    preprocessed_file_path: str
        The location of the preprocessed immigration data
    output_file_path: str
        The location to store the final fact/dimension data
    """

    # read in the immigration data
    imm = spark.read.parquet(preprocessed_file_path)

    # create the traveller profile dimension table
    dim_traveller_profile = imm.select(
        "profile_id", "genderclean", "agecategory", 
        "i94cit_continent", "i94cit_global_region"
    ).dropDuplicates()

    # rename columns
    profile_col_names = {
        "genderclean": "gender",
        "agecategory": "age_category",
        "i94cit_continent": "citizen_region",
        "i94cit_global_region": "citizen_global_region"
    }

    for k,v in profile_col_names.items():
        dim_traveller_profile = dim_traveller_profile.withColumnRenamed(k, v)

    # write to output file path
    dim_traveller_profile.write.parquet(output_file_path + "dim_traveller_profile/", "append")

def create_time_dimension(spark, preprocessed_file_path, output_file_path):
    """
    Creates the time dimension table

    Params
    ------
    spark: spark instance
        A spark instance that's been initialized
    preprocessed_file_path: str
        The location of the preprocessed immigration data
    output_file_path: str
        The location to store the final fact/dimension data
    """
    # read in the immigration data
    imm = spark.read.parquet(preprocessed_file_path)

    # create the time dimension table skeleton
    dim_time = imm.select("arrdateclean").drop_duplicates()

    # add the date components
    dim_time = dim_time.withColumn("day", F.dayofmonth(dim_time["arrdateclean"]))
    dim_time = dim_time.withColumn("month", F.month(dim_time["arrdateclean"]))
    dim_time = dim_time.withColumn("year", F.year(dim_time["arrdateclean"]))
    dim_time = dim_time.withColumn("day_of_week", F.dayofweek(dim_time["arrdateclean"]))
    dim_time = dim_time.withColumn("month_year", F.concat(dim_time["month"], F.lit("-"), dim_time["year"]))

    # rename the arrdateclean column
    dim_time = dim_time.withColumnRenamed("arrdateclean", "timestamp")

    # write to parquet files
    dim_time.write.partitionBy("month", "year").parquet(output_file_path + "dim_time/", "append")

def fact_and_dimension_creation_main():
    """
    The main function that runs the Spark job
    to create the fact and dimension tables off the immigration data
    """
    # get file locations
    from shared_spark_vars import (
        preprocessed_fp as preprocessed_file_path,
        output_fp as output_file_path
    )

    # run the spark job
    spark = initialize_spark()
    create_traveller_profile_dimension(spark, preprocessed_file_path, output_file_path)
    create_immigration_fact(spark, preprocessed_file_path, output_file_path)
    create_time_dimension(spark, preprocessed_file_path, output_file_path)

    spark.stop()

if __name__ == "__main__":
    fact_and_dimension_creation_main()