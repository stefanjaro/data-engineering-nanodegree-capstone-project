# variables used across all spark jobs

# bucket name
s3_bucket_name = "dendcapstoneproject"
s3_link_prefix = f"s3://{s3_bucket_name}/"

# i94 data prefix and required files
# file_prefix = "raw_data/18-83510-I94-Data-2016/i94" # no longer needed, no longer using boto3 method
mode_labels_fp =  f"{s3_link_prefix}raw_data/i94mode_labels.csv"
port_labels_fp = f"{s3_link_prefix}raw_data/i94port_labels.csv"
visa_labels_fp = f"{s3_link_prefix}raw_data/i94visa_labels.csv"
cit_labels_fp = f"{s3_link_prefix}raw_data/i94cit_labels.csv"

# airport codes location
ac_input_fp = f"{s3_link_prefix}raw_data/airport-codes_csv.csv"

# demographics location
demo_input_fp = f"{s3_link_prefix}raw_data/us-cities-demographics.csv"

# temperature data location and required lookup files
temp_input_fp = f"{s3_link_prefix}raw_data/GlobalLandTemperaturesByCity.csv"
demo_lookup_fp = f"{s3_link_prefix}preprocessed_files/state_city_lookup_demo/"
port_lookup_fp = f"{s3_link_prefix}preprocessed_files/state_city_lookup_ports/"

# pre- and processed file storage locations
preprocessed_fp = f"{s3_link_prefix}preprocessed_files/"
output_fp = f"{s3_link_prefix}output_files/"