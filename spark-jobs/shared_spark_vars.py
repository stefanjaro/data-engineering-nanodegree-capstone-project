# variables used across all spark jobs

# bucket link
s3_link_prefix = "s3://dendcapstoneproject/"

# i94 data prefix and required files
file_prefix = "raw_data/18-83510-I94-Data-2016/i94"
mode_labels_fp = "s3://dendcapstoneproject/raw_data/i94mode_labels.csv"
port_labels_fp = "s3://dendcapstoneproject/raw_data/i94port_labels.csv"
visa_labels_fp = "s3://dendcapstoneproject/raw_data/i94visa_labels.csv"
cit_labels_fp = "s3://dendcapstoneproject/raw_data/i94cit_labels.csv"

# airport codes location
ac_input_fp = "s3://dendcapstoneproject/raw_data/airport-codes_csv.csv"

# demographics location
demo_input_fp = "s3://dendcapstoneproject/raw_data/us-cities-demographics.csv"

# temperature data location and required lookup files
temp_input_fp = "s3://dendcapstoneproject/raw_data/GlobalLandTemperaturesByCity.csv"
demo_lookup_fp = "s3://dendcapstoneproject/preprocessed_files/state_city_lookup_demo/"
port_lookup_fp = "s3://dendcapstoneproject/preprocessed_files/state_city_lookup_ports/"

# pre- and processed file storage locations
preprocessed_fp = "s3://dendcapstoneproject/preprocessed_files/"
output_fp = "s3://dendcapstoneproject/output_files/"