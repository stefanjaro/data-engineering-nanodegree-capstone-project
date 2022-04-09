# import libraries
import datetime
from airflow import DAG
from airflow.contrib.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from airflow.contrib.operators.emr_terminate_job_flow_operator import EmrTerminateJobFlowOperator

################
# CONFIGURATIONS
################

# name of s3 bucket with scripts
s3_bucket = "s3://dendcapstoneproject/"

# initialize dag
dag = DAG(
    "prepare-data-for-redshift",
    start_date=datetime.datetime.now()-datetime.timedelta(days=1),
    schedule_interval="@once"
)

####################
# CREATE EMR CLUSTER
####################

JOB_FLOW_OVERRIDES = {
    "Name": "capstone-emr",
    "LogUri": "s3://aws-logs-576946247943-us-west-2/elasticmapreduce/",
    "ReleaseLabel": "emr-6.5.0",
    "Applications": [{"Name": "Hadoop"}, {"Name": "Spark"}],
    "Configurations": [
        {
            "Classification": "spark-env",
            "Configurations": [
                {
                    "Classification": "export",
                    "Properties": {"PYSPARK_PYTHON": "/usr/bin/python3"},
                }
            ],
        }
    ],
    "Instances": {
        "InstanceGroups": [
            {
                "Name": "Master node",
                "Market": "ON_DEMAND",
                "InstanceRole": "MASTER",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 1,
            },
            {
                "Name": "Core - 2",
                "Market": "ON_DEMAND",
                "InstanceRole": "CORE",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 2,
            },
        ],
        "KeepJobFlowAliveWhenNoSteps": True,
        "TerminationProtected": False,
    },
    "JobFlowRole": "EMR_EC2_DefaultRole",
    "ServiceRole": "EMR_DefaultRole",
}

create_emr_cluster = EmrCreateJobFlowOperator(
    task_id="create_emr_cluster",
    job_flow_overrides=JOB_FLOW_OVERRIDES,
    aws_conn_id="aws_credentials",
    emr_conn_id="emr_default",
    dag=dag
)

############################
# IMMIGRATION DATA HANDLING
############################

# preprocess the immigration data prior to create fact and dimension tables
preprocess_immigration_data = EmrAddStepsOperator(
    task_id="preprocess_immigration_data",
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    aws_conn_id="aws_credentials",
    steps=[{
        "Name": "preprocess_immigration_data",
        "ActionOnFailure": "TERMINATE_CLUSTER",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--master",
                "yarn",
                "--packages",
                "saurfang:spark-sas7bdat:3.0.0-s_2.12",
                "--py-files",
                f"{s3_bucket}scripts/shared_spark_vars.py",
                f"{s3_bucket}scripts/immigration-data-preprocessing.py"
            ]
        }
    }],
    dag=dag
)

# create the fact and dimension tables
create_immigration_fact_dims = EmrAddStepsOperator(
    task_id="create_immigration_fact_dims",
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    aws_conn_id="aws_credentials",
    steps=[{
        "Name": "create_immigration_fact_dims",
        "ActionOnFailure": "TERMINATE_CLUSTER",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--master",
                "yarn",
                "--py-files",
                f"{s3_bucket}scripts/shared_spark_vars.py",
                f"{s3_bucket}scripts/immigration-fact-and-dimension-creation.py"
            ]
        }
    }],
    dag=dag
)

# watch the immigration data handling process
watch_immigration_data_handling = EmrStepSensor(
    task_id="watch_immigration_data_handling",
    job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='create_immigration_fact_dims', key='return_value')[0] }}",
    aws_conn_id="aws_credentials",
    dag=dag
)

############################
# DEMOGRAPHIC DATA HANDLING
############################

# preprocess the demographic data and create fact and dimension tables using it
process_demographic_data = EmrAddStepsOperator(
    task_id="process_demographic_data",
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    aws_conn_id="aws_credentials",
    steps=[{
        "Name": "process_demographic_data",
        "ActionOnFailure": "TERMINATE_CLUSTER",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--master",
                "yarn",
                "--py-files",
                f"{s3_bucket}scripts/shared_spark_vars.py",
                f"{s3_bucket}scripts/demographics-data-processing.py"
            ]
        }
    }],
    dag=dag
)

# watch the demographic data handling process
watch_demographic_data_handling = EmrStepSensor(
    task_id="watch_demographic_data_handling",
    job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='process_demographic_data', key='return_value')[0] }}",
    aws_conn_id="aws_credentials",
    dag=dag
)

#############################
# AIRPORT CODES DATA HANDLING
#############################

# preprocess the airport data and create fact and dimension tables using it
process_airport_data = EmrAddStepsOperator(
    task_id="process_airport_data",
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    aws_conn_id="aws_credentials",
    steps=[{
        "Name": "process_airport_data",
        "ActionOnFailure": "TERMINATE_CLUSTER",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--master",
                "yarn",
                "--py-files",
                f"{s3_bucket}scripts/shared_spark_vars.py",
                f"{s3_bucket}scripts/airport-codes-processing.py"
            ]
        }
    }],
    dag=dag
)

# watch the airport data handling process
watch_airport_data_handling = EmrStepSensor(
    task_id="watch_airport_data_handling",
    job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='process_airport_data', key='return_value')[0] }}",
    aws_conn_id="aws_credentials",
    dag=dag
)

###########################
# TEMPERATURE DATA HANDLING
###########################

# preprocess the temperature data and create fact and dimension tables using it
process_temperature_data = EmrAddStepsOperator(
    task_id="process_temperature_data",
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    aws_conn_id="aws_credentials",
    steps=[{
        "Name": "process_temperature_data",
        "ActionOnFailure": "TERMINATE_CLUSTER",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--master",
                "yarn",
                "--py-files",
                f"{s3_bucket}scripts/shared_spark_vars.py",
                f"{s3_bucket}scripts/temperature-data-processing.py"
            ]
        }
    }],
    dag=dag
)

# watch the temperature data handling process
watch_temperature_data_handling = EmrStepSensor(
    task_id="watch_temperature_data_handling",
    job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='process_temperature_data', key='return_value')[0] }}",
    aws_conn_id="aws_credentials",
    dag=dag
)

#####################
# TERMINATE CLUSTER
####################

# terminate the EMR cluster
terminate_emr_cluster = EmrTerminateJobFlowOperator(
    task_id="terminate_emr_cluster",
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    aws_conn_id="aws_default",
    dag=dag,
)

###########
# JOB FLOW
###########

create_emr_cluster >> preprocess_immigration_data >> create_immigration_fact_dims >> watch_immigration_data_handling
create_emr_cluster >> process_airport_data >> watch_airport_data_handling
create_emr_cluster >> process_demographic_data >> watch_demographic_data_handling
[watch_airport_data_handling, watch_demographic_data_handling] >> process_temperature_data >> watch_temperature_data_handling
[watch_immigration_data_handling, watch_temperature_data_handling] >> terminate_emr_cluster