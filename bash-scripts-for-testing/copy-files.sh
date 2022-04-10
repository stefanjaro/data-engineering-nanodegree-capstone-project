#!/bin/bash
# For moving the spark jobs to the emr cluster for testing
# Make sure this is in the folder that contains the scripts and cfg file

# emr master node
MASTERNODE="hadoop@ec2-34-223-65-79.us-west-2.compute.amazonaws.com:/home/hadoop"

# pem file
PEMLOC="<PEM_FILE_LOCATION>"

# location of scripts
PREFIXLOC=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

# scripts to copy
SHARED="/shared_spark_vars.py"
AIR="/airport-codes-processing.py"
DEMO="/demographics-data-processing.py"
IMMONE="/immigration-data-preprocessing.py"
IMMTWO="/immigration-fact-and-dimension-creation.py"
TEMP="/temperature-data-processing.py"

# config file to copy
CONFF="/dl.cfg"

# copy files
for targetfile in $SHARED $AIR $DEMO $IMMONE $IMMTWO $TEMP $CONFF
do
    scp -i $PEMLOC $PREFIXLOC$targetfile $MASTERNODE
    echo 
done