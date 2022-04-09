# MUST INSTALL BOTO3 ON THE EMR CLUSTER FOR THIS TO WORK
# MUST ALSO PASS IN A CONFIG FILE CONTAINING THE AWS ACCESS KEYS
# MUST BE RUN WITH:
# /usr/bin/spark-submit --master yarn --packages "saurfang:spark-sas7bdat:3.0.0-s_2.12" immigration-data-preprocessing.py

# import libraries
import datetime
import configparser
# import boto3
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, StringType

def initialize_spark():
    """
    Initializes a spark instance
    """
    # initialize spark
    spark = SparkSession\
        .builder\
        .appName("immigration-data-preprocessing")\
        .config("spark.jars.repositories", "https://repos.spark-packages.org/")\
        .config("spark.jars.packages", "saurfang:spark-sas7bdat:3.0.0-s_2.12")\
        .getOrCreate()

    return spark

# def get_file_list_from_s3(s3_link_prefix, file_prefix):
#     """
#     Connects to s3, gets the list of files with the prefix stated and
#     dynamically generates the list of SAS files that to be read in by spark

#     Params
#     ------
#     s3_link_prefix: str
#         The link to the bucket
#     file_prefix: str
#         The prefix of the files we're looking for
#     """
#     # get our aws keys from the locally stored config file
#     config = configparser.ConfigParser()
#     config.read('dl.cfg')
#     AWS_ACCESS_KEY_ID = config['DEFAULT']['AWS_ACCESS_KEY_ID']
#     AWS_SECRET_KEY_ID = config['DEFAULT']['AWS_SECRET_ACCESS_KEY']

#     # create a boto3 s3 instance
#     s3 = boto3.resource(
#         "s3",
#         region_name="us-west-2",
#         aws_access_key_id=AWS_ACCESS_KEY_ID,
#         aws_secret_access_key=AWS_SECRET_KEY_ID
#     )

#     # connect to our bucket
#     bucket = s3.Bucket(s3_link_prefix.split("/")[-2].strip())

#     # get a list of all objects with the file_prefix
#     all_objects = [obj for obj in bucket.objects.filter(Prefix=file_prefix)]

#     # create the list of files to be read
#     list_of_files = []
#     for obj in all_objects:
#         list_of_files.append(s3_link_prefix + obj.key)

#     return list_of_files

def generate_sas_file_list(s3_link_prefix):
    """
    Generates the list of SAS files that need to be read

    Params
    ------
    s3_link_prefix: str
        The prefix of the s3 bucket
    """
    # the list of sas files in the raw data folder
    sas_files = [
        "raw_data/18-83510-I94-Data-2016/i94_jan16_sub.sas7bdat",
        "raw_data/18-83510-I94-Data-2016/i94_feb16_sub.sas7bdat",
        "raw_data/18-83510-I94-Data-2016/i94_mar16_sub.sas7bdat",
        "raw_data/18-83510-I94-Data-2016/i94_apr16_sub.sas7bdat",
        "raw_data/18-83510-I94-Data-2016/i94_may16_sub.sas7bdat",
        "raw_data/18-83510-I94-Data-2016/i94_jun16_sub.sas7bdat",
        "raw_data/18-83510-I94-Data-2016/i94_jul16_sub.sas7bdat",
        "raw_data/18-83510-I94-Data-2016/i94_aug16_sub.sas7bdat",
        "raw_data/18-83510-I94-Data-2016/i94_sep16_sub.sas7bdat",
        "raw_data/18-83510-I94-Data-2016/i94_oct16_sub.sas7bdat",
        "raw_data/18-83510-I94-Data-2016/i94_nov16_sub.sas7bdat",
        "raw_data/18-83510-I94-Data-2016/i94_dec16_sub.sas7bdat"
    ]

    # add the prefix
    imm_fps = [s3_link_prefix + x for x in sas_files]

    return imm_fps

def import_immigration_and_add_labels(spark, imm_fp, mode_labels_fp, port_labels_fp, 
                                      visa_labels_fp, cit_labels_fp, cols_required):
    """
    Imports all of the data required for immigration pre-processing
    Filters the immigration data down to only the columns we need
    And adds all of the labels to the codified values we're interested in

    Params
    ------
    spark: spark instance
        A spark instance that's been initialized
    imm_fp: str
        The location of the immigration data files
    mode_labels_fp: str
        The location of the labels file for i94mode
    port_labels_fp: str
        The location of the labels file for i94port
    visa_labels_fp: str
        The location of the labels file for i94visa
    cit_labels_fp: str
        The location of the labels file for i94cit
    cols_required: list
        The list of columns we require from the immigration data file
    """
    # import the sas files
    imm = spark.read.format('com.github.saurfang.sas.spark').load(imm_fp)

    # import label files
    mode_labels = spark.read.csv(mode_labels_fp, header=True) # for mode of travel
    port_labels = spark.read.csv(port_labels_fp, header=True) # for port of entry
    visa_labels = spark.read.csv(visa_labels_fp, header=True) # for type of visa
    citz_labels = spark.read.csv(cit_labels_fp, header=True) # for citizenship country

    # select only the columns we need
    imm = imm.select(cols_required)

    # add labels to codified values
    for df in [mode_labels, port_labels, visa_labels, citz_labels]:
        imm = imm.join(
            F.broadcast(df),
            on=df.columns[0],
            how="left"
        )

    return imm

def preprocess_immigration_data(imm, preprocessed_output_fp):
    """
    Proprocess the immigration data by:
        1. Removing records with irrelevant and invalid values
        2. Removing records without an arrival date
        3. Adjusting the format of the arrival date
        4. Adjusting the gender column so everything apart from M and F is O for Other
        5. Bucketing the ages into distinct categories

    Params
    ------
    imm: spark dataframe
        The immigration dataset
    preprocessed_output_fp: str
        The location to save the preprocessed file
    """
    # hardcoded variables
    SAS_START_DATE = datetime.datetime(1960, 1, 1)

    # remove the irrelevant ports and invalid citizenship countries from the dataset
    imm = imm.where(
        (imm["i94port_relevant"] == "RELEVANT") &
        (imm["i94cit_continent"] != "INVALID")
    )
    
    # clean up arrival date which is the number of days after January 1, 1960
    imm = imm.where(imm["arrdate"].isNotNull())    
    adjust_date = F.udf(lambda x: SAS_START_DATE + datetime.timedelta(days=x), DateType())
    imm = imm.withColumn("arrdateclean", adjust_date(imm["arrdate"]))

    # clean up the gender column
    adjust_gender = F.udf(lambda x: x if x in ["M", "F"] else "O")
    imm = imm.withColumn("genderclean", adjust_gender(imm["gender"]))

    # bucket the age groups into distinct categories
    @F.udf(StringType())
    def age_categorizer(age):
        """
        Takes an age value and buckets it into a distinct category
        Returns "Unknown" if the age is a null value, is negative, or is greater than 120

        Params
        ------
        age: integer or float
            the age of a person
        """
        if age == None:
            return "Unknown"
        elif 0 <= age < 18:
            return "Below 18"
        elif 18 <= age < 30:
            return "18 to 29"
        elif 30 <= age < 40:
            return "30 to 39"
        elif 40 <= age < 50:
            return "40 to 49"
        elif 50 <= age < 60:
            return "50 to 59"
        elif 60 <= age <= 120:
            return "60 and Above"
        else:
            return "Unknown"

    imm = imm.withColumn("agecategory", age_categorizer(imm["i94bir"]))

    # create a unique identifier for traveller profiles
    imm = imm.withColumn(
        "profile_id", 
        F.lower(F.concat(
            imm["genderclean"], 
            F.substring(imm["i94cit_continent"], 1, 3), 
            F.substring(imm["agecategory"], 1, 2),
            F.substring(imm["i94cit_global_region"], 1, 1)
            )
        )
    )

    # add month and year for partitioning
    imm = imm.withColumn("month", F.month(imm["arrdateclean"]))
    imm = imm.withColumn("year", F.year(imm["arrdateclean"]))

    imm.write.partitionBy("month", "year").parquet(preprocessed_output_fp + "immigration_data", "append")

def preprocessing_main():
    """
    The main function that executes the Spark job that preprocessing the immigration data
    """
    # get file locations
    from shared_spark_vars import (
        s3_link_prefix,
        # file_prefix,
        mode_labels_fp,
        port_labels_fp,
        visa_labels_fp,
        cit_labels_fp,
        preprocessed_fp as preprocessed_output_fp
    )

    # cols required for fact and dimension tables
    cols_required = [
        "i94cit", "i94port", "arrdate",
        "i94mode", "i94visa", "i94bir", "gender"
    ]

    spark = initialize_spark()
    # imm_fps = get_file_list_from_s3(s3_link_prefix, file_prefix)
    imm_fps = generate_sas_file_list(s3_link_prefix)

    for imm_fp in imm_fps:
        imm = import_immigration_and_add_labels(
            spark, imm_fp, mode_labels_fp, port_labels_fp, 
            visa_labels_fp, cit_labels_fp, cols_required
        )
        imm = preprocess_immigration_data(imm, preprocessed_output_fp)

    spark.stop()

if __name__ == "__main__":
    preprocessing_main()