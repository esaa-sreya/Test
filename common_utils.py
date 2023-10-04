import io
import json
import zipfile
from datetime import datetime, timedelta, date
from typing import Any, Dict
from collections import OrderedDict

import boto3
import pytz
from airflow.contrib.operators.sns_publish_operator import SnsPublishOperator
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.sensors.base import BaseSensorOperator
from airflow.sensors.python import PythonSensor
from airflow.models import Variable
from airflow.models.baseoperator import BaseOperatorP
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.utils.timezone import convert_to_utc
from dateutil.relativedelta import relativedelta
from dateutil.tz import gettz
from pytz import timezone
import logging
from utils.airflow_log_formatter import GenericLoggerFormatterUtility
import gzip
import csv
import os
import pandas as pd
import re
import calendar
import operator
import time
from jira_utils import jira_utility_v1 as jira_util
import base64
import psycopg2
from sqlalchemy import create_engine
import requests
from operator import itemgetter
from utils.status_manager import configure_publisher, StatusPublisher
#Change-37 Start
import smtplib
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
#Change-37 End
#Change-35
#################
from utils.version_control import get_glue_operator
AwsGlueJobOperator = get_glue_operator()

from utils.version_control import get_traceback
traceback = get_traceback()
##################
from dateutil.relativedelta import relativedelta

#Change-36
#################
from utils import rds_audit_runner as audit_runner

region = Variable.get("cbs_udh_region_name")
glue_client = boto3.client('glue', region_name=region)
s3_resource = boto3.resource("s3")
s3 = boto3.client('s3', region_name=region)

## Change- 39
from airflow.providers.amazon.aws.transfers.s3_to_sftp import S3ToSFTPOperator


#######################################################################################
# Job : This file contains generic utility methods
#
# Date : 15-Mar-2022
# version : V5
# Change Request : Added below new methods
# 1. change_case_of_string
# 2. save_feed_date_formatted
# 3. get_postgres_conn_cursor
# 4. get_next_item_to_run
# 5. get_filename_to_process
# 6. check_next_task_run
# 7. check_for_update_feeddate
# 8. is_skip_alert_from_config_table
#######################################################################################
# Change-1
# Modified by : Parth Khambhayta
# Date : 15-Mar-2022
# Details : 1) Added get_filename_to_process function, get_next_item_to_run. It will get 
#           next filename from last completed dag name and save data in xcom variable.
#           2) Added save_feed_date_formatted, change_case_of_string function, save feed_date 
#           in xcom variable based on passing date format and given case of feed_Date
#           3) Added check_next_task_run, check_for_update_feeddate functions, are used
#           for branch operator
#           4) Added is_skip_alert_from_config_table function,common method to check the 
#           enrty for skip alter from config table
# Modified by : Sandeep
# Date : 24-Mar-2022
# Details : 1) renaming files based on input file format ,prefix and suffix
#######################################################################################
# Change-2
# Modified by : Deepak Jindal
# Date : 05-April-2022
# Details : Update is_to_skip_alert method to skip alert only for s3keysensor operator
########################################################################################
# Change-3
# Modified by : Shravan Kumar
# Date : 5-May-2022
# Details : 1) Replaced print statement with logger.info in get_dependency function
#############################################################################################
#############################################################################################
# Change-4
# Modified by : Rajat Pandya
# Date : 28-April-2022
# Details : Implementing Jira integration call in send_email() method to create JIRA for DevOps
# whenever there is a failure
#############################################################################################
#############################################################################################
# Change-5
# Modified by : Parth Khambhayta
# Date : 31-May-2022
# Details : 1) Added get_dependency_by_operator for checking dependency by using operator as per teradata
# 2) Added get_operator return operator
# 3) Added get_dag_status it will check dag status should not be started for given loadname
# 4) Update empty_file_or_zero_record_check and is_object_exists implemented logic to read file using pandas
#    and it will skip blank lines from file and added header_count as optional parameter for skipping header.
##################################################################################################
# Change-6
# Modified by : Ibney Hasan
# Date : 08-Jun-2022
# Details : 1)  inter_bucket_file_pattern_transfer method added to copy/move files pattern based
#############################################################################################
#############################################################################################
# Change-7
# Modified by : Shravan Kumar
# Date : 15-Jun-2022
# Details : Created custom sftp file sensor.
#         Below two function has been added - 
#           1-custom_sftp_file_sensor()(Line -> 2104-2198)
#           2-update_custom_sftp_file_sensor_status(Line -> 2200-2232)
#         Imported packages -
#           1-from airflow.providers.sftp.hooks.sftp import SFTPHook
#           2-from airflow.utils.timezone import convert_to_utc
#############################################################################################
#############################################################################################
# Change-8
# Modified by : Amit Garg
# Date : 19-July-2022
# Details : Below functions have been added - 
#           1-get_pg_connection
#           2-get_redshift_connection
#           3-rds_conditional_check
#           4-redshift_conditional_check 
#           5-get_rds_connection
#           6-get_branching_task_details
#           Import modules-
#           base64 , psycopg2 and sqlalchemy
#############################################################################################
#############################################################################################
# Change-9
# Modified by : Amit Garg
# Date : 18-July-2022
# Details : Below function have been modified with some one file related condition to avoid counter in the file name - 
#           custom_file_renaming
#############################################################################################
# Change-10
# Modified by : Karan Mohadikar
# Date : 12-July-2022
# Details:
# 1) Updated get_dag_status, it will check the status of load with exact value of loadname
# with optional parameter 'wildcard_match' set to 'N'.
#############################################################################################
# Change-11
# Modified by : Rahul Dashore
# Date : 25-July-2022
# Details : Below function have been modified with reading of the file from pd.read_csv to pd.read_table - 
#           is_object_exists
#############################################################################################
# Change-12
# Modified by : Karan Mohadikar
# Date : 8-August-2022
# Details : check_empty_file_branching
# Task will be returned based on the emptiness of the file
#############################################################################################
# Change-13
# Modified by : Karan Mohadikar
# Date : 8-August-2022
# Details : Function added
# check_empty_file_branching: Task will be returned based on the emptiness of the file
# get_branching_task: utility method to check for a key in a dictionary and returns value
#############################################################################################
#############################################################################################
# Change-14
# Modified by : Priyanka Panchakshri
# Date : 25-August-2022
# Details: Updated get_dag_status, it will check the multiple status of load for latest previous run
# If optional parameter 'last_run_status' set to 'True' then function will check latest entry for given status 
# and if 'last_run_status' parameter is not given by user ,it will check any entry for given status.
#############################################################################################
#############################################################################################
# Change-15
# Modified by : Manish Kumar Gupta
# Date : 06-September-2022
# Details: Updated get_feed_date, it will check for the last_day_of_month for provided feed_date
# If optional parameter 'need_last_day_of_month' set to 'True' then function will check
# if feed_date present in table is 'last_day_of_month',it will provide last day of next month.
#############################################################################################
#############################################################################################
# Change-16
# Modified by : Pavan Kumar Ramanna
# Date : 07-Septembet-2022
# Details: Bug fix in custom_sftp_file_sensor - changed less than equal to condition to less than only for comparing newer_than vs mod_time
#############################################################################################
#############################################################################################
# Change-17
# Modified by : Gourav Thakur
# Date : 15-Septembet-2022
# Details: Adding custom_sns_send_email method for sending email with custom message and subject
###############################################################################################
## Change-18
# Modified by : Ibney Hasan
# Date : 15-Septembet-2022
# Details: Add empty file creation method with timestamp format suffix and bug fix for gz uncompress 
##################################################################################################
## Change-19
# Modified by : Pavan Kumar R
# Date : 11-October-2022
# Details: Add insert_job_exec_control method used to maintain start_time of the DAG, required by next run as feed_start_time.
#          This method is simillar to upsert_start_date, but start_date_value is received from DAG.
#          It will insert the record if not exist else it will update the record.
###################################################################################################
## Change-20
# Modified by : Madhukar Rao
# Date : 10-October-2022
# Details: Added milliseconds as well in timestamp format in method set_start_timestamp_in_xcom 
###################################################################################################
## Change-21
# Modified by : Ibney Hasan
# Date : 25-October-2022
# Details: Foundry sync execution methods added
#          trigger_foundry_sync
#          check_foundry_sync_status  
###################################################################################################
## Change- 22
# Modified by : Gourav Thakur/ Kaustubh Porwal
# Date : 11-Nov-2022
# Details: Sending Co-realtion message sent to SNS 
#          Updation two methods on_failure, on_sucess 
#          Creating two new methods get_corelation_id, send_message_with_correlation_id
###################################################################################################
###################################################################################################
## Change- 23
# Modified by : Neeraj Kumar Srivastava
# Date : 04-Jan-2023
# Details: Added ssl for rds connection
###################################################################################################
###################################################################################################
## Change- 24
# Modified by : Masoom Pophaley
# Date : 24-Jan-2023
# Details: Added new functions:
#	   fail_on_specific_object_count()- for failing process on specific number of objects in a folder
#	   get_objects_in_bucket_count()- for finding count of objects in any bucket folder.
#	   get_traceback()- traceback function for proper exception handling
#          Function Names - fail_on_specific_object_count(), get_objects_in_bucket_count(), get_traceback()
###################################################################################################
## Change- 25
# Modified by : Gourav Thakur
# Date : 30-Jan-2023
# Details: Sending Co-realtion id to AwsGlueJobOperator
#          Adding Openlineage parameter
###################################################################################################
## Change- 26
# Modified by : Gourav Thakur
# Date : 14-Feb-2023
# Details: Modified below methods to avoid sql injection issues by passing parameters during runtime.
            # get_run_date_for_catchup
            # get_feed_date_to_process
            # get_max_feed_date_by_load_name
            # get_dependency
###################################################################################################
## Change- 27
# Modified by : Aravind S Unni
# Date : 07-March-2023
# Details: Added back the add_openlineage_to_args() function which sends openlineage parameters to 
#          glue jobs and used this function in create_glue_task().
###################################################################################################
## Change - 28
# Modified by : Tanuj Rohilla
# Date : 18-April-2023
# Details : Added get_secret_values() method for retrieving secret value from secret manager
#           Added trigger_foundry_dataset_build() method for trigger the build of foundry dataset
########################################################################################################
## Change- 29
# Modified by : AK
# Date : 05-May-2023
# Details: Updated dynamic import of aws glue job operator depending on mwaa.airflow_version being used .
#########################################################################################################
## Change- 30
# Modified by : Spandan Mohanty
# Date : 17-May-2023
# Details: Added function task_decider_for_ICON_process This function will check 'file_content' which has 
#          information if historical data and source data are same. This will help us to decide if ICON process 
#            is required (new data available) or skip ICON process if there are no changes in data..
#          # task_decider_for_ICON_process   
###################################################################################################
## Change- 31
# Modified by : Rajeev pandey
# Date : 07-June-2023
# Details: Added check_file_time_change function in custom_sftp_file_sensor code to check if remote
#          file modification time is changing on sftp then will keep waiting until complete file
#          has arrived on sftp then sftp sensor will be succeded          
###################################################################################################
## Change- 32
# Modified by : Boppudi Sudheera, KV Palani
# Date : 20-June-2023
# Details: Added condition check for FILE_EXTENSION arguement in rename_file function
###################################################################################################
## Change - 33
# Modified by : Pintu Prajapati
# Date : 10-july-2023
# Details: Added method to schedule a Dag every 30th month, and for february, it should be schedule on march 1st
###################################################################################################
###################################################################################################
## Change - 34
# Modified by : Rohit More, Manish Gupta
# Date : 11-July-2023
# Details: Updated rename_file and rename_s3_file functions to rename export files with specific requirements.
###################################################################################################
## Change- 35
# Modified by : Anik Sinha
# Date : 11-July-2023
# Details: Updated dynamic import of AwsGlueJobOperator depending on mwaa.airflow_version being used
#          and handled traceback format exception method issue.
#          Additionally inclusion of version_control.py file where the conflicting mpdules will be
#          handled in case of conflicts during Airflow version upgrade
###################################################################################################
## Change- 36
# Modified by : Rajat Pandya
# Date : 21-July-2023
# Details: Changed update_audit_operator_info function to call Aiflow module instead of Glue.
###################################################################################################
## Change- 37
# Modified by : Vinuprasad O M
# Date : 21-July-2023
# Details: Added send_load_completion_email to send email to a set of provided receipients post load completion
###################################################################################################
## Change- 38
# Modified by : Vinuprasad O M
# Date : 07-Aug-2023
# Details: Added attachment functionality and configurable parameters from dag instead of static
# for send_load_completion_email function
# Mandatory parameters:
#  environment      - The environment where load executes (Dev, Stg, Prod)
#  domain_name      - Domain name for the load (cargo, loyalty etc.)
#  load_name        - Load name
# Optional parameters:
#  replace_params   - Configurable parameters as dictionary key/value pair string passed from load
#                     This can be used to replace the matching parameters from subject/body dynamically from load.
#  display_date     - Date to be displayed in email. Default as current CDT date in format %A, %B %d, %Y
#  date_format      - Format of the date to be displayed in email. Default format %A, %B %d, %Y
###################################################################################################
## Change- 39
# Modified by : Ibney Hasan
# Date : 09-Aug-2023
# Details: Added s3_to_esftp to copy multiple files from s3 to esftp path
###################################################################################################
def get_traceback(e):
    lines = traceback.format_exception(type(e), e, e.__traceback__)
    return ''.join(lines)


def get_current_cdt_datetime():
    return datetime.now(gettz("America/Chicago"))


def get_current_utc_datetime():
    return datetime.now(pytz.utc)


def get_current_formatted_date_time(date_time, format=None):
    final_format = format if format is not None else "%Y-%m-%d %H:%M:%S"
    return date_time.strftime(final_format)


def get_current_formatted_cdt_date_time(format=None):
    final_format = format if format is not None else "%Y-%m-%d %H:%M:%S"
    return get_current_cdt_datetime().strftime(final_format)


def get_current_formatted_cdt_date(format=None):
    final_format = format if format is not None else "%Y-%m-%d"
    return get_current_cdt_datetime().strftime(final_format)


def parse_to_cdt(date_to_parse):
    return date_to_parse.astimezone(timezone('America/Chicago'))


def parse_to_cdt_datetime(date_to_parse, format=None):
    final_format = format if format is not None else "%Y-%m-%d %H:%M:%S"
    return parse_to_cdt(date_to_parse).strftime(final_format) if date_to_parse is not None else '0001-01-01 00:00:00'


def parse_to_cdt_date(date_to_parse, format=None):
    final_format = format if format is not None else "%Y-%m-%d"
    return parse_to_cdt(date_to_parse).strftime(final_format) if date_to_parse is not None else '0001-01-01'


def get_dag_run_id(execution_date):
    return str(int(parse_to_cdt(execution_date).timestamp() * 1000))


def fetch_dag_operator_metadata(dag_run, **kwargs):
    logger = get_logger(kwargs)
    task_instances = dag_run.get_task_instances()
    lst = []
    for ti in task_instances:
        if ti.job_id is not None and ti.state is not None:
            d = {}
            d['dag_id'] = ti.dag_id
            d['operator'] = ti.operator
            d['task_id'] = ti.task_id
            d['start_date'] = parse_to_cdt_datetime(ti.start_date)
            d['end_date'] = parse_to_cdt_datetime(ti.end_date)
            d['job_id'] = ti.job_id
            d['try_number'] = ti.try_number
            d['duration'] = ti.duration
            d['state'] = ti.state
            d['dag_run_id'] = get_dag_run_id(ti.execution_date)
            d['execution_date'] = parse_to_cdt_datetime(ti.execution_date)
            lst.append(d)
    data = json.dumps(lst)
    logger.info(str(data))
    return data

##Change -27 - START
def add_openlineage_to_args(args,correlationId,glue_job_name):
    logger = get_logger(None)
    #Getting spark params from airflow variables
    spark_listener = Variable.get("spark_listener")
    marquez_hostname = Variable.get("marquez_hostname")
    marquez_namespace = Variable.get("marquez_namespace")
    openlineage_jar = Variable.get("openlineage_jar")
    dag_id_run_id = '_'.join(correlationId.split('~')[1:])
    args["--dag_id_run_id"] = dag_id_run_id

    try:
        response = glue_client.batch_get_jobs(JobNames=[glue_job_name])
	#Raising exception if there is no 'Jobs' in response or 'Jobs' is empty
        if not "Jobs" in response or len(response["Jobs"]) == 0:
            raise Exception("Glue job name is not correct or job does not exist!")
    except Exception as e:
        logger.error(str(e))
        logger.error('Error while getting glue job response, failed to append open lineage parameters.')
        response = None
        
    #Adding openlineage parameters only if glue response is not null
    if response is not None:
        for item in response['Jobs']:
            glue_job_type = item["Command"]["Name"]
            glue_version = float(item["GlueVersion"])

        if glue_job_type == 'glueetl' and glue_version >= 3.0:
            #Iterating through glue job response and adding default arguments if any to args
            for item in response['Jobs']:
                if '--conf' in item['DefaultArguments'].keys():
                    job_def_conf_args = item['DefaultArguments']['--conf']
                    logger.info("job_def_conf_args ", job_def_conf_args)
                    if "--conf" in args:
                        val = args["--conf"]
                        args["--conf"] = val + f" --conf {job_def_conf_args}"
                    else:
                        args["--conf"] = job_def_conf_args
                if '--extra-jars' in item['DefaultArguments'].keys():
                    job_def_extra_args = item['DefaultArguments']['--extra-jars']
                    logger.info("job_def_extra_args ", job_def_extra_args)
                    if "--extra-jars" in args:
                        val = args["--extra-jars"]
                        args["--extra-jars"] = val + "," + job_def_extra_args
                    else:
                        args["--extra-jars"] = job_def_extra_args

            #Adding spark conf params for openlineage to args
            if "--conf" in args:
                logger.info('--conf exists')
                val = args["--conf"]
                args["--conf"] = val + f" --conf spark.extraListeners={spark_listener} --conf spark.openlineage.host={marquez_hostname} --conf spark.openlineage.namespace={marquez_namespace} --conf spark.openlineage.parentJobName={dag_id_run_id}"
            else:
                args["--conf"] = f"spark.extraListeners={spark_listener} --conf spark.openlineage.host={marquez_hostname} --conf spark.openlineage.namespace={marquez_namespace} --conf spark.openlineage.parentJobName={dag_id_run_id}"
            if "--extra-jars" in args:
                logger.info('extra-jars exists')
                val = args["--extra-jars"]
                args["--extra-jars"] = val + f",{openlineage_jar}"
            else:
                args["--extra-jars"] = openlineage_jar

    return args
##Change -27 - END

## Change- 25
def create_glue_task(taskId, kwargs, glue_job_name, current_dag=None):
    # Input args : taskId, args as json, job_name
    # Output Args : NA
    # Description : Common method to glue task with use of PythonOperator
    
    args: Dict[str, Any] = kwargs["args"]
    logger = get_logger(args)
    correlationId = get_correlation_id(**kwargs)
    args["--CORRELATION_ID"] = correlationId
    #Change - 27 - START
    # Calling function to add open lineage parameters to args
    args = add_openlineage_to_args(args, correlationId, glue_job_name)
    #Change - 27 - END
    logger.info("glue job arguments: ", args)
    if current_dag is None:
        op = AwsGlueJobOperator(
            task_id=taskId,
            job_name=glue_job_name,
            script_args=args,
            num_of_dpus=10
        )
    else:
        op = AwsGlueJobOperator(
            task_id=taskId,
            job_name=glue_job_name,
            script_args=args,
            num_of_dpus=10,
            dag=current_dag
        )
    return op
## Change- 25

def create_ssh_task(current_dag, sshHook, esft_script_path, task_id_name, command_to_execute, **kwargs):
    return SSHOperator(
        ssh_hook=sshHook,
        task_id=task_id_name,
        command=esft_script_path + command_to_execute,
        provide_context=True,
        dag=current_dag)


def get_preprocessing_args(s3_file_path, preprocess_path, **kwargs):
    # Input args : move_raw_path, s3_file_path
    # Output Args : Args as json
    # Description : Common method to create json object of all arguments which is used by copy runner glue job

    args = {
        '--file_path': s3_file_path,
        '--move_raw_path': preprocess_path,
    }
    return args


def get_clean_args(s3_file_path, clean_path, **kwargs):
    # Input args : copy_rs_sql_script_bucket_name, copy_sql_script_file_path, iam_role_arn, s3_file_path
    # Output Args : Args as json
    # Description : Common method to create json object of all arguments which is used by copy runner glue job

    args = {
        '--file_path': s3_file_path,
        '--clean_path': clean_path,
        '--save_mode': 'overwrite',
    }
    return args


def get_copy_runner_args(copy_rs_sql_script_bucket_name, copy_sql_script_file_path, iam_role_arn, s3_file_path,
                         udh_log_bucket_path,
                         **kwargs):
    # Input args : copy_rs_sql_script_bucket_name, copy_sql_script_file_path, iam_role_arn, s3_file_path
    # Output Args : Args as json
    # Description : Common method to create json object of all arguments which is used by copy runner glue job

    args = {
        '--COPY_SQL_SCRIPT_BUCKET_NAME': copy_rs_sql_script_bucket_name,
        '--COPY_SQL_SCRIPT_FILE_PATH': copy_sql_script_file_path,
        '--IAM_ROLE_ARN': iam_role_arn,
        '--S3_FILE_PATH': s3_file_path,
        '--LOG_BUCKET_PATH': udh_log_bucket_path,
        '--SECRET_ID': 'cbs-udh-redshift-airflow-secret-mgr',
    }
    return args


def get_audit_runner_args(dag_name, operation_type, load_detail, run_date, feed_date, udh_log_bucket_path,
                          audit_operator_detail, execution_date, dag_run_id, **kwargs):
    # Input args : operation_type
    # Output Args : Json object with argument information
    # Description : Common method to create json object of all arguments which used by audit_runner glue job
    args = {
        'DAG_NAME': dag_name,
        'OPERATION_TYPE': operation_type,
        'LOAD_DETAIL': load_detail,
        'RUN_DATE': run_date,
        'FEED_DATE': feed_date,
        'LOG_BUCKET_PATH': udh_log_bucket_path,
        'SECRET_ID': 'cbs-udh-rds-aurora-airflow-secret-mgr',
        'AUDIT_OPERATOR_DETAIL': audit_operator_detail,
        'EXECUTION_DATE': execution_date,
        'DAG_RUN_ID': dag_run_id
    }
    return args


def is_to_skip_alert(context, formatted_exception):
    logger = get_logger(context)
    logger.info("dag_id : %s",context['task_instance'].dag_id)
    logger.info("task_id : %s",context['task_instance'].task_id)
    # Change-2 Start
    if 'loyalty' in context['task_instance'].dag_id.lower() and (
            (('esftp_to_s3' in context['task_instance'].task_id.lower() or 's3keysensor' in context[
                'task_instance'].operator.lower()) and 'AirflowSensorTimeout' in formatted_exception) or 'check_load_ready_to_start' in
            context['task_instance'].task_id.lower()):
        logger.info("skip the alert")
        return True
    else:
        logger.info("do not skip the alert")
        return False
    # Change-2 End

def get_domain_name_project_name(load_detail):
    if load_detail != '' and len(load_detail.split('|')) > 3:
        domain_name = load_detail.split('|')[1]
        project_name = load_detail.split('|')[3]
    elif load_detail != '' and len(load_detail.split('|')) > 2:
        domain_name = load_detail.split('|')[1]
        project_name = "NA"
    else:
        domain_name = "NA"
        project_name = "NA"
    return domain_name, project_name


def send_email(load_detail, context):
    logger = get_logger(context)
    try:
        logger.info(load_detail)
        task_instance = context['task_instance']
        exception = context.get('exception')
        formatted_exception = " ".join(
            traceback.format_exception(
                etype=type(exception),
                value=exception,
                tb=exception.__traceback__
            )
        ).strip()
        logger.debug(formatted_exception)
        ### V5 added is_skip_alert_from_config_table to check entry for skip alert from config table
        if not(is_to_skip_alert(context, formatted_exception) or is_skip_alert_from_config_table(context,formatted_exception)):
            # exception: str = "Exception for testing"
            status: str = str(context['task_instance'].state).upper()
            dag_id: str = str(task_instance.dag_id)
            task_id: str = str(task_instance.task_id).upper()
            domain_name, project_name = get_domain_name_project_name(load_detail)
            udh_environment = Variable.get("udh_environment", "")
            prod_sns_topic = Variable.get("udh_email_alert_sns_topic_arn", "")
            uat_sns_topic = Variable.get("cbs_udh_sns_topic_arn_in_uat", "")
            is_jira_integrated = Variable.get("cbs_udh_jira_integration", "")
            uat_loads = Variable.get("cbs_udh_loyalty_in_uat_loads", "")
            uat_loads_array = uat_loads.split(",")
            sns_topic = prod_sns_topic
            url = task_instance.log_url
            logger.info("prod_sns_topic : %s", prod_sns_topic)
            logger.info("uat_sns_topic : %s", uat_sns_topic)
            logger.info("uat_loads : %s", uat_loads)
            logger.info("uat_loads_array : %s", str(uat_loads_array))
            logger.info("dag_id : %s", str(dag_id))
            logger.info("task_id : %s", str(task_id))
            logger.info("url : %s", str(url))
            if "" != uat_sns_topic and dag_id.lower().strip() in uat_loads_array:
                logger.info("Do not send email to Devops as this dag is not handed over to devops")
                sns_topic = uat_sns_topic
            logger.info("sns_topic : %s", str(sns_topic))
            # v2 Update
            if project_name.upper() == 'NA':
                subject: str = status + ' | ' + domain_name.upper() + ' | ' + udh_environment.upper() + " | " + dag_id.upper() + " | " + task_id
            else:
                subject: str = status + ' | ' + project_name.upper() + ' | ' + domain_name.upper() + ' | ' + udh_environment.upper() + " | " + dag_id.upper() + " | " + task_id
            message_body: str = """
    Status : {status} 
    Environment : {env}
    Project Name : {project_name} 
    Domain Name : {domain}
    Dag Id :  {dag_id}
    Task id : {task_id}
    Error log url : {log_url}
    {stacktrace}
    """.format(
                status=status.upper(),
                env=udh_environment.upper(),
                project_name=project_name.upper(),
                domain=domain_name.upper(),
                dag_id=dag_id.upper(),
                task_id=task_id.upper(),
                log_url=url,
                stacktrace=formatted_exception
            )

            subject_length: int = 100
            subject = (subject[:subject_length]) if len(subject) > subject_length else subject

            send_email: BaseOperator = SnsPublishOperator(
                task_id='send_email',
                aws_conn_id='aws_default',
                target_arn=sns_topic,
                message=message_body,
                subject=subject
            )
            send_email.execute(context)

            if is_jira_integrated == 'true' and dag_id.lower().strip() not in uat_loads_array:
                logger.info("Jira integration is enabled. Calling Jira utility.")
                jira_util.jira_flow(subject, message_body, status, udh_environment, project_name, domain_name, dag_id, task_id, url, formatted_exception, logger)
            else:
                logger.info("Skipping the JIRA call.")

    except Exception as err:
        logger.error(str(err))
        logger.error("".join(err.__traceback__))


def update_audit_operator_info(context, operation_type, load_detail, run_date, feed_date):
    logger = get_logger(context)
    audit_operator_detail = 'EmptyData'
    udh_log_bucket_path = "/".join(
        [Variable.get("udh_log_bucket_base_path", ""), context['task_instance'].dag_id, "audit_runner"])
    if operation_type == "U" or operation_type == "F":
        audit_operator_detail = fetch_dag_operator_metadata(context['dag_run'])
    execution_date = parse_to_cdt_datetime(context['execution_date'])
    dag_run_id = get_dag_run_id(context['execution_date'])
    logger.info("dag_id : %s", str(context['task_instance'].dag_id))
    logger.info("operation_type : %s", operation_type)
    logger.info("load_detail : %s", load_detail)
    logger.info("run_date : %s", run_date)
    logger.info("feed_date : %s", feed_date)
    logger.info("execution_date : %s", execution_date)
    logger.info("dag_run_id : %s", dag_run_id)
    args = get_audit_runner_args(context['task_instance'].dag_id, operation_type, load_detail, run_date, feed_date,
                                 get_formatted_path(udh_log_bucket_path), audit_operator_detail,
                                 execution_date, dag_run_id)
    ## Change- 36 start
    audit_runner.create_audit(args)
    # context['args'] = args
    # op = create_glue_task('audit_runner', context, 'cbs-udh-audit-runner')
    ## Change- 36 end

## Change- 22 start
def on_failure(load_detail, run_date, feed_date, context):
    logger = get_logger(context)
    task_instance = context['task_instance']
    noti_typ, noti_prio, app_identifier = "ERROR", "HIGH", "AIRFLOW"
    message=f"Dag failed with task_id - {str(task_instance.task_id).upper()}"
    logger.info(message)
    send_email(load_detail, context)
    update_audit_operator_info(context, 'F', load_detail, run_date, feed_date)
    send_message_with_correlation_id(load_detail, context, message, noti_typ, noti_prio, app_identifier, task_instance)

def on_success(load_detail, run_date, feed_date, context):
    logger = get_logger(context)
    task_instance = context['task_instance']
    noti_typ, noti_prio, app_identifier = "INFO", "LOW", "AIRFLOW"
    message=f"Dag sucessful with task_id - {str(task_instance.task_id).upper()}"
    logger.info(message)
    send_message_with_correlation_id(load_detail, context, message, noti_typ, noti_prio, app_identifier, task_instance)

def get_correlation_id(**context):
    # Input args : context
    # Output Args : correlation_id
    # Description : Creating correlation_id
    task_instance = context["task_instance"]
    dag_id: str = str(task_instance.dag_id)
    dag_run_id: str = str(context["dag_run"].run_id)
    pipeline_id = context["params"].get("pipeline_id", 0)
    correlation_id = str(pipeline_id) + "~" + str(dag_id) + "~" + str(dag_run_id)
    return correlation_id

def send_message_with_correlation_id(
    load_detail, context, message, noti_typ, noti_prio, app_identifier, task_instance, service_identifier=""
):
    # Input args : load_detail, context, message, noti_typ, noti_prio, app_identifier, task_instance
    # Output Args : NA
    # Description : It will send message to sns
    # feed_id and sequence_num will take default zero value for all non icon pipeline
    logger = get_logger(context)
    try:
        sns_topic_arn = Variable.get("cbs_udh_corelation_sns_topic_arn", "")
        configure_publisher(
            sns_topic_arn=sns_topic_arn, use_access_key=False, msg_group_id="1234"
        )
        publisher = StatusPublisher()
        status: str = str(context["task_instance"].state).upper()
        url: str = str(task_instance.log_url)
        task_id: str = str(task_instance.task_id).upper()
        project_name: str = str(context["params"].get("project_name", "")).lower()
        pipeline_name: str = str(context["params"].get("dag_id"))
        portfolio_name: str = str(load_detail.split('|')[0]).lower()
        domain_name: str = str(load_detail.split('|')[1]).lower()
        dag_run_id: str = get_dag_run_id(context["execution_date"])
        # for initial run we are getting value 2 from the try_number method of task_instance
        retry_num: str = int(task_instance.try_number) - 2
        correlation_id = get_correlation_id(**context)
        logger.info(correlation_id)
        if app_identifier == "AIRFLOW":
            service_identifier= str(context['dag_run'].run_id)
        publisher.publish(
            usecase_name=project_name,
            portofolio_name=portfolio_name,
            pipeline_name=pipeline_name,
            run_id=dag_run_id,
            app_identifier=app_identifier,
            app_task_id=task_id,
            service_identifier=service_identifier,
            triggered_by="AUTO",
            pipeline_type="BATCH",
            retry_num=retry_num,
            status=status,
            usecase_domain=domain_name,
            corelation_id=correlation_id,
            notification_type=noti_typ,
            notification_priority=noti_prio,
            message=message,
            log_link=url,
        )
        logger.info("******correlation message sent********")
    except Exception as err:
        logger.error(str(err))
        logger.error("".join(err.__traceback__))
## Change- 22 end

def get_formatted_path(path):
    path = path if path.endswith("/") else path + "/"
    path = path[1:] if path.startswith("/") else path
    return path


def inter_bucket_file_transfer(**kwargs):
    # Input args : source_bucket_name, destination_bucket_name, source_base_path, destination_base_path, load_name
    # Output Args : NA
    # Description : It will copy all the file from passed source path to destination path
    logger = get_logger(kwargs)
    my_bucket = s3_resource.Bucket(kwargs['source_bucket_name'])
    run_date = kwargs['run_date']
    source_base_path = get_formatted_path(kwargs['source_base_path'])
    destination_base_path = get_formatted_path(kwargs['destination_base_path'])
    logger.info("my_bucket : %s", my_bucket)
    logger.info("run_date : %s", run_date)
    logger.info("source_base_path : %s", source_base_path)
    logger.info("destination_base_path : %s", destination_base_path)
    for object_summary in my_bucket.objects.filter(Prefix=source_base_path):
        logger.info("Start Copying.... Inside For Loop.... %s", str(object_summary.key))
        if str(object_summary.key) != source_base_path and str(object_summary.key).find(
                kwargs['load_name'] + '/metadata') == -1:
            if str(object_summary.key).find("/" + run_date + "/") != -1:
                logger.info("Copying file - %s", object_summary.key)
                copy_source = {'Bucket': kwargs['source_bucket_name'], 'Key': object_summary.key}
                destination = destination_base_path + object_summary.key.split('/' + kwargs['load_name'] + '/')[-1]
                s3.copy_object(CopySource=copy_source, Bucket=kwargs['destination_bucket_name'], Key=destination)
                s3.delete_object(Bucket=kwargs['source_bucket_name'], Key=object_summary.key)


def rename_file(kwargs):
    # Input args  : SOURCE_BUCKET_NAME, INPUT_FILE_NAME, APPEND_CURRENT_TIMESTAMP, TIME_ZONE, FILE_EXTENSION, FILE_DATE_FORMAT
    logger = get_logger(kwargs)
    bucket_name = kwargs['SOURCE_BUCKET_NAME']
    # Change 34 start
    # Added condition to allow files other than 000
    if "INPUT_FILE_NAME" in kwargs:
        old_key = get_formatted_path(kwargs['S3_FILE_PATH'].split(bucket_name)[1]) + str(kwargs['INPUT_FILE_NAME'])
        new_key = old_key.replace(str(kwargs['INPUT_FILE_NAME']), kwargs['OUTPUT_FILE_NAME'])
    else:
        old_key = get_formatted_path(kwargs['S3_FILE_PATH'].split(bucket_name)[1]) + "000"
        new_key = old_key.replace('000', kwargs['OUTPUT_FILE_NAME'])
    logger.info("bucket_name : %s", str(bucket_name))
    logger.info("old_key : %s", str(old_key))
    logger.info("new_key : %s", str(new_key))
    # Added condition to append timestamp in filename
    if "APPEND_CURRENT_TIMESTAMP" in kwargs and kwargs['APPEND_CURRENT_TIMESTAMP'] == 'Y':
        kwargs['RUN_DATE'] = datetime.now()
        if "TIME_ZONE" in kwargs and kwargs['TIME_ZONE'].lower() == 'cdt':
            kwargs['RUN_DATE'] = parse_to_cdt(kwargs['RUN_DATE'])
        new_key = new_key.replace('FILE_DATE', get_formatted_date(kwargs['RUN_DATE'], kwargs['FILE_DATE_FORMAT']))
    else:
        new_key = new_key.replace('FILE_DATE',
                                  get_formatted_date(convert_string_to_date_object(kwargs['RUN_DATE'], '%Y-%m-%d'),
                                                     kwargs['FILE_DATE_FORMAT']))                                                   
    # Added condition to check for FILE_EXTENSION in kwargs
    if ('FILE_EXTENSION' in kwargs):
        new_key = new_key + "." + kwargs['FILE_EXTENSION']
    # Change 34 end
    logger.info("replaced_new_key : %s", str(new_key))
    s3_resource.Object(bucket_name, new_key).copy_from(CopySource=bucket_name + "/" + old_key)
    s3_resource.Object(bucket_name, old_key).delete()
    return new_key


def gzip_file(kwargs, new_key):
    logger = get_logger(kwargs)
    zip_buffer = io.BytesIO()
    file_name = new_key.split("/")[-1:][0]
    logger.info("file_name : %s", file_name)
    zip_key = get_formatted_path(kwargs['OUTPUT_PATH'].split(kwargs['SOURCE_BUCKET_NAME'])[1]) + file_name.replace(
        ".txt", "." + kwargs['ZIP_FILE_EXTENSION'])
    logger.info("zip_key : %s", zip_key)
    with zipfile.ZipFile(zip_buffer, "a", zipfile.ZIP_DEFLATED, False) as zipper:
        logger.info("Start zipping the files")
        infile_object = s3.get_object(Bucket=kwargs['SOURCE_BUCKET_NAME'], Key=new_key)
        infile_content = infile_object['Body'].read()
        zipper.writestr(file_name, infile_content)
    s3.put_object(Bucket=kwargs['SOURCE_BUCKET_NAME'], Key=zip_key, Body=zip_buffer.getvalue())
    s3_resource.Object(kwargs['SOURCE_BUCKET_NAME'], new_key).delete()


def rename_s3_file(**kwargs):
    logger = get_logger(kwargs)
    logger.info("rename_s3_file")
    new_key = rename_file(kwargs)
    # Change 34 start
    if new_key:
        file_name = new_key.split("/")[-1:][0]
        save_data_in_xcom(kwargs, "renamed_file_name", file_name)
    # Change 34 end


def gzip_s3_file(**kwargs):
    # Input args : source_bucket_name
    # Output Args : NA
    # Description : It will copy all the file from passed source path to destination path
    logger = get_logger(kwargs)
    logger.info("gzip_s3_file")
    new_key = rename_file(kwargs)
    logger.info("new_key : %s", str(new_key))
    gzip_file(kwargs, new_key)


def delete_s3_file(**kwargs):
    # Input args : source_bucket_name, S3_FILE_PATH
    # Output Args : NA
    # Description : It will delete all the file present at passed location
    logger = get_logger(kwargs)
    my_bucket = s3_resource.Bucket(kwargs['SOURCE_BUCKET_NAME'])
    logger.info("my_bucket : %s", str(my_bucket))
    for object_summary in my_bucket.objects.filter(Prefix=kwargs['S3_FILE_PATH']):
        logger.info("Deleting key : %s", str(object_summary.key))
        s3.delete_object(Bucket=kwargs['SOURCE_BUCKET_NAME'], Key=object_summary.key)


def get_feed_date_from_file(**kwargs):
    logger = get_logger(kwargs)
    try:
        my_bucket = s3_resource.Bucket(kwargs['source_bucket_name'])
        run_date = kwargs['run_date']
        source_base_path = get_formatted_path(kwargs['source_base_path'])
        file_regex = kwargs['file_regex']
        file_date_format = kwargs['file_date_format']
        logger.info("my_bucket : %s", my_bucket)
        logger.info("run_date : %s", run_date)
        logger.info("source_base_path : %s", source_base_path)
        logger.info("file_regex : %s", file_regex)
        logger.info("file_date_format : %s", file_date_format)
        if '*' in file_regex:
            file_regex = file_regex.split('*')[0]
        logger.info("replaced_file_regex : %s", file_regex)
        for object_summary in my_bucket.objects.filter(Prefix=source_base_path):
            logger.info("Start Copying.... Inside For Loop.... %s", str(object_summary.key))
            if str(object_summary.key) != source_base_path and str(object_summary.key).find("/" + run_date + "/") != -1:
                length = 2 + len(file_date_format) if '%Y' in file_date_format else len(file_date_format)
                file_date = str(object_summary.key).split(file_regex)[1][0:length]
                file_date_obj = convert_string_to_date_object(file_date, file_date_format)
                feed_date = get_formatted_date(file_date_obj)
                logger.info("length : %s", str(length))
                logger.info("file_date : %s", str(file_date))
                logger.info("feed_date : %s", str(feed_date))
                save_feed_date_in_variables(kwargs, feed_date)
                break
    except Exception as err:
        logger.error(str(err))
        raise "Failed to get run date check logs" + str(err)


def get_max_feed_date_by_load_name(domain, load_name):
    logger = get_logger(None)
    try:
        # Change- 26
        query = """SELECT max(feed_date) FROM airflow.run_control where domainname = %s and loadname = %s"""
        # Change- 26
        source_hook = PostgresHook(postgres_conn_id='airflow_postgres_conn')
        source_conn = source_hook.get_conn()
        source_cursor = source_conn.cursor()
        # Change- 26
        source_cursor.execute(query, (domain.strip().lower(),load_name.strip().lower()))
        # Change- 26
        records = source_cursor.fetchone()
        max_feed_date = records[0]
        logger.info("domain : %s", str(domain))
        logger.info("load : %s", str(load_name))
        logger.info("max_feed_date : %s", str(max_feed_date))
        return max_feed_date
    except Exception as err:
        logger.error(str(err))
        raise "Failed to get feed date check logs" + str(err)


def numOfDays(date1, date2):
    return (date2 - date1).days


def calculate_dff_between_date(from_date, to_date):
    return numOfDays(from_date, to_date)


def get_diff_feed_date_between_two_loads(kwargs):
    logger = get_logger(kwargs)
    try:
        from_date = get_max_feed_date_by_load_name(kwargs['from_domain'], kwargs['from_load'])
        if kwargs['is_today'] is not None and kwargs['is_today'].strip().lower() == 'y':
            logger.info("is_today = y")
            if kwargs is not None and kwargs["dag_run"] is not None and kwargs[
                "dag_run"].conf is not None and "feed_date" in kwargs["dag_run"].conf and kwargs["dag_run"].conf[
                "feed_date"] is not None:
                logger.info("get feed date from passed_argument")
                feed_date = kwargs["dag_run"].conf["feed_date"]
            else:
                logger.info("get current date")
                feed_date = get_current_formatted_cdt_date('%Y-%m-%d')
            logger.info("feed_date : %s", str(feed_date))
            to_date = convert_string_to_date_object(feed_date, '%Y-%m-%d')
        else:
            to_date = get_max_feed_date_by_load_name(kwargs['to_domain'], kwargs['to_load'])
            logger.info("is_today = n")
            logger.info("to_date : %s", str(to_date))
        if from_date is not None and to_date is not None:
            diff = calculate_dff_between_date(from_date, to_date)
        else:
            diff = 0
    except Exception as err:
        logger.error(str(err))
        raise "Failed to get feed date check logs" + str(err)
    logger.info("Difference between dates : %s", str(diff))
    return diff, from_date, to_date


def check_feed_date_diff(**kwargs):
    logger = get_logger(kwargs)
    try:
        diff, from_date, to_date = get_diff_feed_date_between_two_loads(kwargs)
        success_msg = "difference in feed_dt from {} to {} is {} which is {} {}, OK"
        failure_msg = "difference in feed_dt from {} to {} is {} which is not {} {}"
        from_msg = kwargs['to_load'] + "(" + str(to_date) + ")"
        to_msg = kwargs['from_load'] + "(" + str(from_date) + ")"
        if 'comparator' not in kwargs:
            sign = 'ge'
        else:
            sign = kwargs['comparator']
        if (sign == 'gt' and diff > kwargs['days_diff']) or (sign == 'ge' and diff >= kwargs['days_diff']) or (
                sign == 'lt' and diff < kwargs['days_diff']) or (sign == 'le' and diff <= kwargs['days_diff']) or (
                sign == 'eq' and diff == kwargs['days_diff']):
            logger.info(success_msg.format(from_msg, to_msg, str(diff), get_sign(sign),
                                     str(kwargs['days_diff'])))
            return
        else:
            logger.info(failure_msg.format(from_msg, to_msg, str(diff), get_sign(sign),
                                     str(kwargs['days_diff'])))
            raise Exception(failure_msg.format(from_msg, to_msg, str(diff), get_sign(sign),
                                               str(kwargs['days_diff'])))
    except Exception as err:
        logger.error(str(err))
        raise "Failed to get feed date check logs" + str(err)


def get_sign(argument):
    switcher = {
        'gt': ">",
        'ge': ">=",
        'lt': "<",
        'le': "<=",
        'eq': "="
    }
    return switcher.get(argument, "nothing")


def delete_s3_file_on_suffix_basis(**kwargs):
    """
    :description : It will delete all the file present at passed location based on suffix
    :param kwargs: SOURCE_BUCKET_NAME, S3_FILE_PATH, SUFFIX
    """
    logger = get_logger(kwargs)
    try:
        for i in kwargs['file_to_delete']:
            my_bucket = s3_resource.Bucket(i['SOURCE_BUCKET_NAME'])
            logger.info("my_bucket : %s", my_bucket)
            for object_summary in my_bucket.objects.filter(Prefix=i['S3_FILE_PATH']):
                if object_summary.key.endswith(i['SUFFIX']):
                    logger.info("Deleting key : %s", str(object_summary.key))
                    s3.delete_object(Bucket=i['SOURCE_BUCKET_NAME'], Key=object_summary.key)
                    logger.info("Key deleted : %s", str(object_summary.key))
    except Exception as err:
        logger.error(str(err))
        raise "Failed - delete_s3_file_on_suffix_basis : "+ str(err)


def suffix_check_delete_file(i, key):
    """
    :param i: i is the one array element which we pass from airflow yaml which contains SOURCE_BUCKET_NAME, S3_FILE_PATH, SKIP_BASE_PATH
    :param key: S3 object key
    """
    logger = get_logger(None)
    if ('SUFFIX' in i and key.endswith(i['SUFFIX'])) or 'SUFFIX' not in i:
        s3.delete_object(Bucket=i['SOURCE_BUCKET_NAME'], Key=key)
        logger.info("Key deleted : %s", str(key))


def delete_s3_files_v2(**kwargs):
    """
    :description : It will delete all the file present at passed location,
                   but in this method has been enhanced to handled if you want to skip any deletion on basis of any folder.
                   To skip folder from deletion you need to pass value in SKIP_BASE_PATH
    :param kwargs: SOURCE_BUCKET_NAME, S3_FILE_PATH, SKIP_BASE_PATH, SUFFIX
    """
    logger = get_logger(kwargs)
    try:
        for i in kwargs['file_to_delete']:
            my_bucket = s3_resource.Bucket(i['SOURCE_BUCKET_NAME'])
            logger.info("my_bucket : %s", my_bucket)
            for object_summary in my_bucket.objects.filter(Prefix=i['S3_FILE_PATH']):
                if i['S3_FILE_PATH'] != object_summary.key:
                    if 'SKIP_BASE_PATH' not in i:
                        logger.info("Skip base path not in : %s", str(i))
                        suffix_check_delete_file(i, object_summary.key)
                    elif i['SKIP_BASE_PATH'] not in object_summary.key:
                        logger.info("skip base path not present in key so deleting the file")
                        suffix_check_delete_file(i, object_summary.key)
                    else:
                        logger.info("Skiping the delete of file as SKIP_BASE_PATH : %s &&&&& key : %s ", i['SKIP_BASE_PATH'], str(object_summary.key))
    except Exception as err:
        logger.error(str(err))
        raise "Failed - delete_s3_files_v2 : " + str(err)


def delete_s3_files(**kwargs):
    """
    :description : It will delete all the file present at passed location
    :param kwargs: SOURCE_BUCKET_NAME, S3_FILE_PATH
    """
    logger = get_logger(kwargs)
    try:
        for i in kwargs['file_to_delete']:
            my_bucket = s3_resource.Bucket(i['SOURCE_BUCKET_NAME'])
            logger.info("my_bucket : %s", my_bucket)
            for object_summary in my_bucket.objects.filter(Prefix=i['S3_FILE_PATH']):
                logger.info("Deleting key : %s", str(object_summary.key))
                s3.delete_object(Bucket=i['SOURCE_BUCKET_NAME'], Key=object_summary.key)
    except Exception as err:
        logger.error(str(err))
        raise "Failed - delete_s3_files : "+ str(err)


def inter_bucket_files_transfer(**kwargs):
    """
    :description : method for copying all the file from passed source path to destination path
    :param kwargs: source_bucket_name, destination_bucket_name, source_base_path, destination_base_path, load_name
    """
    logger = get_logger(kwargs)
    try:
        for i in kwargs['file_to_move']:
            logger.info("Start Copying")
            my_bucket = s3_resource.Bucket(i['source_bucket_name'])
            source_base_path = get_formatted_path(i['source_base_path'])
            destination_base_path = get_formatted_path(i['destination_base_path'])
            logger.info("my_bucket : %s", my_bucket)
            logger.info("source_base_path : %s", source_base_path)
            logger.info("destination_base_path : %s", destination_base_path)
            for object_summary in my_bucket.objects.filter(Prefix=source_base_path):
                logger.info("Start Copying.... Inside For Loop.... " + str(object_summary.key))
                if str(object_summary.key) != source_base_path:
                    logger.info("Copying file -  " + object_summary.key)
                    destination_location = object_summary.key.split(source_base_path)[1]
                    copy_source = {'Bucket': i['source_bucket_name'], 'Key': object_summary.key}
                    destination = destination_base_path + destination_location
                    s3_resource.meta.client.copy(CopySource=copy_source, Bucket=i['destination_bucket_name'],
                                                 Key=destination)
                    s3_resource.Object(i['source_bucket_name'], object_summary.key).delete()
    except Exception as err:
        logger.error(str(err))
        raise "Failed - inter_bucket_files_transfer : "+ str(err)


def inter_bucket_files_transfer_without_metadata(**kwargs):
    """
    :description : method for copying all the file from passed source path to destination path
    :param kwargs: source_bucket_name, destination_bucket_name, source_base_path, destination_base_path, load_name
    """
    logger = get_logger(kwargs)
    try:
        for i in kwargs['file_to_move']:
            logger.info("Start Copying")
            my_bucket = s3_resource.Bucket(i['source_bucket_name'])
            source_base_path = get_formatted_path(i['source_base_path'])
            destination_base_path = get_formatted_path(i['destination_base_path'])
            logger.info("my_bucket : %s", my_bucket)
            logger.info("source_base_path : %s", source_base_path)
            logger.info("destination_base_path : %s", destination_base_path)
            for object_summary in my_bucket.objects.filter(Prefix=source_base_path):
                logger.info("Start Copying.... Inside For Loop.... " + str(object_summary.key))
                if str(object_summary.key) != source_base_path  and str(object_summary.key).find(i['source_base_path'] + 'metadata') == -1:
                    logger.info("Copying file -  " + object_summary.key)
                    destination_location = object_summary.key.split(source_base_path)[1]
                    copy_source = {'Bucket': i['source_bucket_name'], 'Key': object_summary.key}
                    destination = destination_base_path + destination_location
                    s3_resource.meta.client.copy(CopySource=copy_source, Bucket=i['destination_bucket_name'],
                                                 Key=destination)
                    s3_resource.Object(i['source_bucket_name'], object_summary.key).delete()
    except Exception as err:
        logger.error(str(err))
        raise "Failed - inter_bucket_files_transfer_without_metadata : "+ str(err)


def get_formatted_date(dt, format=None):
    return dt.strftime(format if format is not None else '%Y-%m-%d')


def convert_string_to_date_object(dt, format=None):
    return datetime.strptime(dt, format if format is not None else '%Y%m%d').date()


def get_run_date_for_catchup(context):
    logger = get_logger(context)
    try:
        if context is not None and context["dag_run"] is not None and context[
            "dag_run"].conf is not None and "run_date" in context["dag_run"].conf and context["dag_run"].conf["run_date"] is not None:
            run_date = context["dag_run"].conf["run_date"]
            logger.info("get_run_date from passed_argument : %s", str(run_date))
        else:
            # Change- 26
            query = """SELECT min(run_date) FROM airflow.catchup where domainname = %s and loadname = %s"""
            # Change- 26
            source_hook = PostgresHook(postgres_conn_id='airflow_postgres_conn')
            source_conn = source_hook.get_conn()
            source_cursor = source_conn.cursor()
            # Change- 26
            source_cursor.execute(query, (context['domain'],context['load_name']))
            # Change- 26
            records = source_cursor.fetchone()
            run_date = records[0]
            logger.info("run_date : %s", str(run_date))
            if run_date is None:
                run_date = get_current_formatted_cdt_date()
    except Exception as err:
        logger.error(str(err))
        raise "get_run_date_for_catchup : Failed to get run date check logs" + str(err)
    return run_date


def get_feed_date_to_process(context):
    logger = get_logger(context)
    feed_date = None
    try:
        if context is not None and context["dag_run"] is not None and context[
            "dag_run"].conf is not None and "feed_date" in context["dag_run"].conf and context["dag_run"].conf[
            "feed_date"] is not None:
            feed_date = context["dag_run"].conf["feed_date"]
            logger.info("get_feed_date from passed_argument : %s", str(feed_date))
        else:
            if context['increase_feed_date_by'] is not None and context['increase_feed_date_by'].lower() != 'current':
                logger.info("get_feed_date from run_control")
                # Change- 26
                query = """SELECT max(feed_date) FROM airflow.run_control where domainname = %s and loadname = %s"""
                # Change- 26
                # source hook
                source_hook = PostgresHook(postgres_conn_id='airflow_postgres_conn')
                source_conn = source_hook.get_conn()
                source_cursor = source_conn.cursor()
                # Change- 26
                source_cursor.execute(query, (context['domain'], context['load_name']))
                # Change- 26
                records = source_cursor.fetchone()
                feed_date = records[0]
                logger.info("feed_date : %s", str(feed_date))
                if feed_date is not None:
                    # Change 15 Start
                    if 'need_last_day_of_month' in context.keys():
                        need_last_day_of_month = context['need_last_day_of_month']
                    else:
                        need_last_day_of_month = False
                    # Change 15 Ends
                    feed_date = str(get_updated_feed_date(feed_date, context['increase_feed_date_by'], need_last_day_of_month))
                logger.info("feed_date : %s", str(feed_date))
    except Exception as err:
        logger.error(str(err))
        raise "get_feed_date_to_process : Failed to get feed date check logs" + str(err)
    return feed_date


def get_date_time_cst_to_process(context):
    logger = get_logger(context)
    try:
        if context is not None and context["dag_run"] is not None and context[
            "dag_run"].conf is not None and "load_run_timestamp_cst" in context["dag_run"].conf and \
                context["dag_run"].conf["load_run_timestamp_cst"] is not None:
            load_run_timestamp_cst = context["dag_run"].conf["load_run_timestamp_cst"]
            logger.info("get_date_time_cst_to_process from passed_argument : %s", str(load_run_timestamp_cst))
        else:
            load_run_timestamp_cst = get_current_formatted_date_time(get_current_cdt_datetime())
            logger.info("get_current_formatted_date_time : %s", str(get_current_formatted_date_time))
    except Exception as err:
        logger.error(str(err))
        raise "get_date_time_cst_to_process : Failed to get get_date_time_cst_to_process check logs" + str(err)
    return load_run_timestamp_cst


def get_date_time_gmt_to_process(context):
    logger = get_logger(context)
    try:
        if context is not None and context["dag_run"] is not None and context[
            "dag_run"].conf is not None and "load_run_timestamp_gmt" in context["dag_run"].conf and \
                context["dag_run"].conf["load_run_timestamp_gmt"] is not None:
            load_run_timestamp_gmt = context["dag_run"].conf["load_run_timestamp_gmt"]
            logger.info("get_date_time_gmt_to_process from passed_argument : %s", str(load_run_timestamp_gmt))
        else:
            load_run_timestamp_gmt = get_current_formatted_date_time(get_current_utc_datetime())
            logger.info("load_run_timestamp_gmt : %s", str(load_run_timestamp_gmt))
    except Exception as err:
        logger.error(str(err))
        raise "get_date_time_gmt_to_process : Failed to get load_run_timestamp_gmt check logs" + str(err)
    return load_run_timestamp_gmt


def get_feed_date(**kwargs):
    logger = get_logger(kwargs)
    try:
        run_date = get_run_date_for_catchup(kwargs)
        save_run_date_in_variables(kwargs, str(run_date))
        feed_date = get_feed_date_to_process(kwargs)
        if feed_date is None:
            feed_date = run_date
        logger.info("run_date : %s", str(run_date))
        logger.info("feed_date : %s", str(feed_date))
        save_feed_date_in_variables(kwargs, feed_date)
        date_time_cst = get_date_time_cst_to_process(kwargs)
        save_date_time_cst_in_variables(kwargs, str(date_time_cst))
        date_time_gmt = get_date_time_gmt_to_process(kwargs)
        save_date_time_gmt_in_variables(kwargs, str(date_time_gmt))
    except Exception as err:
        logger.error(str(err))
        raise "get_feed_date : Failed get feed_date and run_date " + str(err)


def save_date_time_cst_in_variables(context, value):
    save_data_in_variables(context['task_instance'].dag_id, 'load_run_timestamp_cst', value)


def save_date_time_gmt_in_variables(context, value):
    save_data_in_variables(context['task_instance'].dag_id, 'load_run_timestamp_gmt', value)


def save_run_date_in_variables(context, run_date):
    save_data_in_variables(context['task_instance'].dag_id, 'run_date', run_date)


def save_feed_date_in_variables(context, feed_date):
    save_data_in_variables(context['task_instance'].dag_id, 'feed_date', feed_date)


def save_data_in_variables(var_key, json_key, json_value):
    dag_variables = Variable.get(var_key, None)
    if dag_variables is None:
        dag_variables_dict = {}
    else:
        dag_variables_dict = json.loads(dag_variables)
    dag_variables_dict[json_key] = str(json_value)
    dag_variable_json = json.dumps(dag_variables_dict)
    Variable.set(var_key, dag_variable_json)


def save_feed_date_in_xcom(context, feed_date):
    save_data_in_xcom(context, 'feed_date', feed_date)


def save_data_in_xcom(context, key, xcom_value):
    task_instance = context['task_instance']
    task_instance.xcom_push(key=key, value=xcom_value)


def get_updated_feed_date(feed_date, increase_feed_date_by, need_last_day_of_month=False):
    """
        Input args : feed_date, increase_feed_date_by, need_last_day_of_month
        Output Args : feed_date
        Description : Common method to get next feed date based on user condition and arguments
    """
    if len(increase_feed_date_by.split('_')) == 2 and increase_feed_date_by.split('_')[0].isdigit():
        if 'day' == increase_feed_date_by.split('_')[1]:
            feed_date += timedelta(days=int(increase_feed_date_by.split('_')[0]))
        if 'month' == increase_feed_date_by.split('_')[1]:
            # Change 15 Starts
            if need_last_day_of_month:
                if check_for_last_day_of_month(**{'run_date': str(feed_date),
                                                  'next_task_if_last_day_of_month': True,
                                                  'next_task_if_not_last_day_of_month': False}):
                    feed_date += relativedelta(months=int(increase_feed_date_by.split('_')[0]))
                    feed_date = get_last_day_of_month(feed_date)
                else:
                    raise "Feed Date provided is not last day of month, Please check"
            else:
                # Change 15 Ends
                feed_date += relativedelta(months=int(increase_feed_date_by.split('_')[0]))
    return feed_date

# Change 15 starts


def get_last_day_of_month(feed_date):
    """
        Input args : feed_date
        Output Args : feed_date
        Description : Common method to get last day of month based on user condition and arguments
    """
    feed_date = date(feed_date.year, feed_date.month,
                     calendar.monthrange(feed_date.year, feed_date.month)[1])
    return feed_date

# Change 15 Ends


def delete_record_from_catchup_folder(domain, load_name, run_date):
    logger = get_logger(None)
    print("delete_record_from_catchup_folder")
    try:
        source_hook = PostgresHook(postgres_conn_id='airflow_postgres_conn')
        insert_query = "delete from airflow.catchup where domainname=':DOMAIN_NAME' and loadname = ':LOAD_NAME' and run_date = ':RUN_DATE'"
        insert_query = insert_query.replace(':DOMAIN_NAME', domain)
        insert_query = insert_query.replace(':LOAD_NAME', load_name)
        insert_query = insert_query.replace(':RUN_DATE', run_date)
        source_hook.run(insert_query)
    except Exception as err:
        logger.error(str(err))
        raise "Failed to update feed date check logs" + str(err)
    return "OK"


def update_feed_date(**kwargs):
    logger = get_logger(kwargs)
    try:
        feed_date = kwargs['feed_date']
        run_date = kwargs['run_date']
        logger.info("run_date : %s", str(run_date))
        logger.info("feed_date : %s", str(feed_date))
        delete_record_from_catchup_folder(kwargs['domain'], kwargs['load_name'], run_date)
        # feed_date = get_feed_date_from_xcom(kwargs)
        source_hook = PostgresHook(postgres_conn_id='airflow_postgres_conn')
        insert_query = "insert into airflow.run_control (domainname, loadname, run_date, feed_date, insert_timestamp) values (':DOMAIN_NAME',':LOAD_NAME',':RUN_DATE',':FEED_DATE',':INSERT_TIME')"
        insert_query = insert_query.replace(':DOMAIN_NAME', kwargs['domain'])
        insert_query = insert_query.replace(':LOAD_NAME', kwargs['load_name'])
        insert_query = insert_query.replace(':FEED_DATE', feed_date)
        insert_query = insert_query.replace(':RUN_DATE', run_date)
        insert_query = insert_query.replace(':INSERT_TIME', get_current_formatted_cdt_date_time())
        source_hook.run(insert_query)
        source_hook.run("commit;")
    except Exception as err:
        logger.error(err)
        raise "Failed to update feed date check logs" + str(err)


def update_catchup_table(**kwargs):
    logger = get_logger(kwargs)
    try:
        feed_date = get_dag_variable(kwargs, 'feed_date')
        run_date = get_dag_variable(kwargs, 'run_date')
        logger.info("run_date : %s", str(run_date))
        logger.info("feed_date : %s", str(feed_date))
        delete_record_from_catchup_folder(kwargs['domain'], kwargs['load_name'], run_date)
    except Exception as err:
        logger.error(str(err))
        raise "Failed to update_catchup_table check logs" + str(err)


def get_feed_date_from_variables(context):
    return get_dag_variable(context, 'feed_date')


def get_sys_key(**kwargs):
    logger = get_logger(kwargs)
    try:
        sys_key_val = get_sys_key_to_process(kwargs)
        logger.info("sys_key_val : %s", str(sys_key_val))
        if sys_key_val is None:
            logger.info("sys_key_val is None")
            raise Exception("get_sys_key : Failed get sys_key_val and might not passed")
        elif 'no_of_characters' in kwargs:
            sys_key_val = sys_key_val.rjust(int(kwargs['no_of_characters']), '0')
            logger.info("after conversion sys_key_val : %s", str(sys_key_val))
        save_sys_key_in_variables(kwargs, sys_key_val)
    except Exception as err:
        logger.error(str(err))
        raise "get_sys_key : Failed get key_name and sys_key_val " + str(err)


def get_sys_key_to_process(context):
    logger = get_logger(context)
    sys_key_val = "0"
    try:
        if context is not None and context["dag_run"] is not None and context[
            "dag_run"].conf is not None and "sys_key_val" in context["dag_run"].conf and context["dag_run"].conf[
            "sys_key_val"] is not None:
            sys_key_val = str(int(context["dag_run"].conf["sys_key_val"]))
            logger.info("get sys_key_val from passed argument : %s", str(sys_key_val))
        else:
            logger.info("get sys_key_val from sys_key table")
            if context['key_name'] is not None:
                query = "SELECT max(last_val) FROM airflow.sys_key where key_name = '" + context['key_name'] + "'"
                # source hook
                source_hook = PostgresHook(postgres_conn_id='airflow_postgres_conn')
                source_conn = source_hook.get_conn()
                source_cursor = source_conn.cursor()
                source_cursor.execute(query)
                records = source_cursor.fetchone()
                sys_key_val_old = records[0]
                if sys_key_val_old is not None:
                    sys_key_val = str(int(sys_key_val_old) + int(context['incremented_by']))
            else:
                logger.info("key name is not passed")
        logger.info("sys_key_val : %s", str(sys_key_val))
    except Exception as err:
        logger.error(str(err))
        raise "get_sys_key_to_process : Failed to get last value check logs" + str(err)
    return sys_key_val


def save_sys_key_in_variables(context, sys_key_val):
    save_data_in_variables(context['task_instance'].dag_id, 'sys_key_val', sys_key_val)


def update_sys_key(**kwargs):
    logger = get_logger(kwargs)
    try:
        logger.info("key_name : %s", str(kwargs['key_name']))
        logger.info("sys_key_val : %s", str(kwargs['sys_key_val']))
        source_hook = PostgresHook(postgres_conn_id='airflow_postgres_conn')
        insert_query = "insert into airflow.SYS_KEY (key_name, last_val) values (':key_name',':sys_key_val')"
        insert_query = insert_query.replace(':key_name', kwargs['key_name'])
        insert_query = insert_query.replace(':sys_key_val', kwargs['sys_key_val'])
        source_hook.run(insert_query)
    except Exception as err:
        logger.error(str(err))
        raise "Failed to update last_value check logs" + str(err)


def get_feed_date_from_xcom(context):
    return get_xcom_value(context, 'get_feed_date', 'feed_date')


def get_xcom_value(context, task_id, xcom_key):
    ti = context['ti']
    return ti.xcom_pull(task_ids=task_id, key=xcom_key)


def get_dag_variable(context, key):
    dag_id = context['task_instance'].dag_id
    dag_variables = Variable.get(dag_id)
    dag_variables_dict = json.loads(dag_variables)
    return dag_variables_dict[key]


def get_dependency(**kwargs):
    logger = get_logger(kwargs)
    logger.info(kwargs['template_fields'])
    domain_name = kwargs['template_fields']['domain_name']
    load_name = kwargs['template_fields']['load_name']
    feed_date = kwargs['template_fields']['feed_date']
    try:
        if 'check_redshift_status' in kwargs['template_fields']:
            check_redshift_status = kwargs['template_fields']['check_redshift_status']
        else:
            check_redshift_status="N"
        # Change- 26
        query = "select	airflow.func_check_dependencies(%s, %s, to_date(%s, 'YYYY-MM-DD'), %s);"
        # Change- 26
        source_hook = PostgresHook(postgres_conn_id='airflow_postgres_conn')
        source_conn = source_hook.get_conn()
        source_cursor = source_conn.cursor()
        # Change- 26
        source_cursor.execute(query, (domain_name.strip().lower(), load_name.strip().lower(), feed_date, check_redshift_status.strip().lower()))
        # Change- 26
        records = source_cursor.fetchone()
        dependency_count = records[0]

        if dependency_count:
            for output in source_conn.notices:
                logger.warn(output)
        else:
            for output in source_conn.notices:
                logger.info(output)

        logger.info("domain : " + str(domain_name) + " | load : " + str(load_name) + " | dependency_count : " + str(dependency_count))
        # save_data_in_xcom(kwargs "sequence_no", sequence_no)
        if dependency_count:
            return False
        else:
            return True
    except Exception as err:
        raise "Failed to check the dependency." + str(err)


# ===========================================================================================================================
# Below methods are as per old dags so need to delete once all dag migrate using yaml file
def run_audit_runner_job(**kwargs):
    # Input args : args as json, job_name
    # Output Args : NA
    # Description : Common method to call any glue job
    logger = get_logger(kwargs)
    args: Dict[str, Any] = kwargs["args"]
    operation_type = ''
    if kwargs['task_instance'].task_id.lower() == 'insert_run_control':
        logger.info("runing insert_run_control step")
        operation_type = 'I'
    if kwargs['task_instance'].task_id.lower() == 'update_run_control':
        logger.info("runing update_run_control step")
        operation_type = 'U'
    update_audit_operator_info(kwargs, operation_type, args['--LOAD_DETAIL'], args['--RUN_DATE'], args['--FEED_DATE'])


def run_cleansing_job(**kwargs):
    # Input args : args as json, job_name
    # Output Args : NA
    # Description : Common method to call any glue job
    logger = get_logger(kwargs)
    logger.info("Running cleansing job cbs_udh_cleansing_job")
    ## Change- 25
    op = create_glue_task('cleansing', kwargs, 'cbs_udh_cleansing_job')
    ## Change- 25
    op.execute(kwargs)



def run_copy_runner_job(**kwargs):
    # Input args : args as json, job_name
    # Output Args : NA
    # Description : Common method to call any glue job
    logger = get_logger(kwargs)
    logger.info("Running copy runner job cbs-udh-s3toRedshift-copyrunner")
    ## Change- 25
    op = create_glue_task('copy_runner', kwargs, 'cbs-udh-s3toRedshift-copyrunner')
    ## Change- 25
    op.execute(kwargs)


def run_bteq_runner_job(**kwargs):
    # Input args : args as json, job_name
    # Output Args : NA
    # Description : Common method to call any glue job
    logger = get_logger(kwargs)
    logger.info("Running bteq runner job cbs-udh-redshift-sql-runner")
    ## Change- 25
    op = create_glue_task('bteq_runner', kwargs, 'cbs-udh-redshift-sql-runner')
    ## Change- 25
    op.execute(kwargs)


def run_preprocessing_job(**kwargs):
    # Input args : args as json, job_name
    # Output Args : NA
    # Description : Common method to call any glue job
    logger = get_logger(kwargs)
    logger.info("Running preprocessing job cbs-udh_preprocessing_utility")
    ## Change- 25
    op = create_glue_task('preprocessing', kwargs, 'cbs-udh_preprocessing_utility')
    ## Change- 25
    op.execute(kwargs)


def run_teradata_to_s3_job(**kwargs):
    # Input args : args as json, job_name
    # Output Args : NA
    # Description : Common method to call any glue job
    logger = get_logger(kwargs)
    logger.info("Running run_teradata_to_s3_job cbs_udh_teradata_s3_sync")
    ## Change- 25
    op = create_glue_task('teradata_to_s3', kwargs, 'cbs_udh_teradata_s3_sync')
    ## Change- 25
    op.execute(kwargs)


def run_tkt_status_job(**kwargs):
    # Input args : args as json, job_name
    # Output Args : NA
    # Description : Common method to call any glue job
    logger = get_logger(kwargs)
    logger.info("Running run_tkt_status_job cbs-udh-corporate-ticketing-tkt-status")
    ## Change- 25
    op = create_glue_task('tkt_status_job', kwargs, 'cbs-udh-corporate-ticketing-tkt-status')
    ## Change- 25
    op.execute(kwargs)


def run_pnr_child_parent_xref_job(**kwargs):
    # Input args : args as json, job_name
    # Output Args : NA
    # Description : Common method to call any glue job
    logger = get_logger(kwargs)
    logger.info("Running run_pnr_child_parent_xref_job cbs-udh-pnr-child-parent-xref")
    ## Change- 25
    op = create_glue_task('pnr_child_parent_xref_job', kwargs, 'cbs-udh-pnr-child-parent-xref')
    ## Change- 25
    op.execute(kwargs)


def run_sharepoint_data_parse_job(**kwargs):
    # Input args : args as json, job_name
    # Output Args : NA
    # Description : Common method to call any glue job
    logger = get_logger(kwargs)
    logger.info("Running run_sharepoint_data_parse_job cbs-udh-excel-to-parquet-parser")
    ## Change- 25
    op = create_glue_task('sharepoint_data_parse_job', kwargs, 'cbs-udh-excel-to-parquet-parser')
    ## Change- 25
    op.execute(kwargs)


def run_teradata_to_s3_record_id_job(**kwargs):
    # Input args : args as json, job_name
    # Output Args : NA
    # Description : Common method to call any glue job
    logger = get_logger(kwargs)
    logger.info("Running run_teradata_to_s3_record_id_job cbs_udh_td_s3_record_id")
    ## Change- 25
    op = create_glue_task('teradata_to_s3_record_id', kwargs, 'cbs_udh_td_s3_record_id')
    ## Change- 25
    op.execute(kwargs)


def get_meta_sequence_no_from_table(context):
    logger = get_logger(context)
    try:
        if context['feed_id'] is not None:
            query = "select max(file_sequence_no) FROM icon.ingestion_job_history_status where feed_id =  '" + context[
                'feed_id'] + "' and job_status = 'SUCCEEDED' ;"
            # source hook
            source_hook = PostgresHook(postgres_conn_id='cce_edge_postgres_conn', schema='cbsrdsauroradb01')
            source_conn = source_hook.get_conn()
            source_cursor = source_conn.cursor()
            source_cursor.execute(query)
            records = source_cursor.fetchone()
            sequence_no = records[0]
            if sequence_no is not None:
                logger.info("sequence no from the table : %s", str(sequence_no))
                sequence_no = int(sequence_no)
            else:
                logger.info("sequence no from the table is none ")
                sequence_no = 0
            logger.info("sequence_no : %s", str(sequence_no))
        else:
            logger.error("Failed to get meta sequence from table")
            raise KeyError("get_meta_sequence_no_from_table : Failed to get feed_id , feed_id is None")
    except Exception as err:
        logger.error(str(err))
        raise "get_meta_sequence_no_from_table : Failed to get feed_id check logs" + str(err)
    return sequence_no


def get_meta_sequence_no(**kwargs):
    logger = get_logger(kwargs)
    try:
        sequence_no = get_meta_sequence_no_from_table(kwargs)
        sequence_no = sequence_no + 1
        logger.info("sequence_no : %s", str(sequence_no))
        logger.info("feed_id : %s", str(kwargs['feed_id']))
        save_data_in_xcom(kwargs, "sequence_no", sequence_no)
    except Exception as err:
        logger.error(str(err))
        raise "get_meta_sequence_no : Failed get feed_id and sequence_no " + str(err)


def run_sharepoint_ga_catg_sheet_crawler():
    glue_crawler = glue_client.start_crawler(
        Name='cbs_udh_ga_catg_sheet_crawler'
    )


def run_sharepoint_qm_catg_sheet_crawler():
    glue_crawler = glue_client.start_crawler(
        Name='cbs_udh_qm_catg_sheet_crawler'
    )


def handle_success_failure(context, flag_success_failure, load_detail, run_date, feed_date, udh_log_bucket_path,
                           udh_glue_job_audit_runner):
    logger = get_logger(context)
    if flag_success_failure == 'failure':
        logger.info("Task failure happens")
        # send_email(context)
        update_audit_operator_info(context, 'F', load_detail, run_date, feed_date)
    if flag_success_failure == 'success':
        logger.info("Task success happens")
        if context['task_instance'].task_id.lower() == 'insert_run_control':
            logger.info("Task insert_run_control success")
            update_audit_operator_info(context, 'I', load_detail, run_date, feed_date)
        if context['task_instance'].task_id.lower() == 'update_run_control':
            logger.info("Task insert_run_control success")
            update_audit_operator_info(context, 'U', load_detail, run_date, feed_date)



def inter_bucket_files_transfer_keep_latest_file(**kwargs):
    """
    :description : method for copying all the file from passed source path to destination path and keep latest file at source location
    :param kwargs: source_bucket_name, destination_bucket_name, source_base_path, destination_base_path
    """
    logger = get_logger(kwargs)
    try:
        for i in kwargs['file_to_move']:
            logger.info("Start Copying")
            source_bucket_name = i['source_bucket_name'].strip()
            source_base_path = get_formatted_path(i['source_base_path']).strip()
            destination_bucket_name = i['destination_bucket_name'].strip()
            destination_base_path = get_formatted_path(i['destination_base_path']).strip()
            file_name_to_search = i['file_name_to_search'].strip()
            is_to_move_files = i['is_to_move_files'].strip()
            logger.info("source_bucket_name : %s", source_bucket_name)
            logger.info("source_base_path : %s", source_base_path)
            logger.info("destination_bucket_name : %s", destination_bucket_name)
            logger.info("destination_base_path : %s", destination_base_path)
            logger.info("file_name_to_search : %s", file_name_to_search)
            logger.info("is_to_move_files : %s", is_to_move_files)
            my_bucket = s3_resource.Bucket(source_bucket_name)
            source_base_path = get_formatted_path(source_base_path)
            key_dict = {}
            for object_summary in my_bucket.objects.filter(Prefix=source_base_path):
                if str(object_summary.key) != source_base_path and file_name_to_search in object_summary.key:
                    file_date = object_summary.key.split("_")[-1]
                    key_dict[file_date] = object_summary.key
            logger.info("key_dict :  %s", key_dict)
            if len(key_dict) > 1:
                logger.info("S3 has more than 1 file received : ")
                key_dict_odered = OrderedDict(sorted(key_dict.items()))
                logger.info("key_dict_odered :  %s", key_dict_odered)
                key_dict_odered.popitem()
                logger.info("key_dict_odered after removing last item :  %s", key_dict_odered)
                for key in key_dict_odered:
                    logger.debug("Copying file -  %s", key_dict_odered[key])
                    if is_to_move_files.lower() == 'y':
                        destination_location = key_dict_odered[key].split(source_base_path)[1]
                        copy_source = {'Bucket': source_bucket_name, 'Key': key_dict_odered[key]}
                        destination = destination_base_path + destination_location
                        logger.debug("copy_source - %s ||| destination - %s", copy_source, destination)
                        s3_resource.meta.client.copy(CopySource=copy_source, Bucket=destination_bucket_name,
                                                     Key=destination)
                    s3_resource.Object(source_bucket_name, key_dict_odered[key]).delete()
            else:
                logger.info("S3 bucket has only one file")
    except Exception as err:
        logger.error(str(err))
        raise "Failed - inter_bucket_files_transfer : "+ str(err)


def is_history(**kwargs):
    logger = get_logger(kwargs)
    try:
        if kwargs is not None and kwargs["dag_run"] is not None and kwargs[
            "dag_run"].conf is not None and "is_history" in kwargs["dag_run"].conf and kwargs["dag_run"].conf["is_history"] is not None:
            is_history = kwargs["dag_run"].conf["is_history"]
            logger.info("is_history from passed_argument : %s", str(is_history))
            if is_history.strip().lower() == 'y':
                return kwargs['next_task_if_history']
        return kwargs['next_task_if_incremental']
    except Exception as err:
        logger.error(f"Failed to check_file_present_s3 : {str(err)}")
        raise "Failed to check_file_present_s3 " + str(err)


def get_folder_file_name(key):
    return key.split("/")[-1:][0].split(".")[0:1][0]


def get_unzip_destination_folder(is_folder_needed, destination_path, key):
    if is_folder_needed.strip().lower() == 'y':
        destination_path = get_formatted_path(destination_path) + get_folder_file_name(key)
    return get_formatted_path(destination_path)


def unzip_file(**kwargs):
    """
    :param kwargs: Airflow context
    Comment : Pass below parameters
              Mandatory:
                SOURCE_BUCKET_NAME, SOURCE_BUCKET_PATH, DESTINATION_BUCKET_NAME, DESTINATION_BUCKET_PATH
                EXTENSION_OF_THE_FILE,
              Optional :
                IS_FOLDER_NEEDED (Y means it will create folder with name of zip file and inside that it will extract file
                                N means it will directly extract file at passed destination path)
    """
    logger = get_logger(kwargs)
    my_bucket = s3_resource.Bucket(kwargs['SOURCE_BUCKET_NAME'])
    source_bucket = kwargs['SOURCE_BUCKET_NAME']
    source_path = kwargs['SOURCE_BUCKET_PATH']
    destination_bucket = kwargs['DESTINATION_BUCKET_NAME']
    destination_path = kwargs['DESTINATION_BUCKET_PATH']
    file_extension = kwargs['EXTENSION_OF_THE_FILE']
    is_folder_needed = kwargs['IS_FOLDER_NEEDED']

    logger.info("source_bucket : %s", source_bucket)
    logger.info("source_path : %s", source_path)
    logger.info("destination_bucket : %s", destination_bucket)
    logger.info("destination_path : %s", destination_path)
    logger.info("file_extension : %s", file_extension)
    logger.info("is_folder_needed : %s", is_folder_needed)

    for my_bucket_object in my_bucket.objects.filter(Prefix=source_path):
        key = my_bucket_object.key
        logger.info("Inside for loop key : %s", key)
        zip_obj = s3_resource.Object(bucket_name=source_bucket, key=key)
        logger.info("list of files in zip file :", zip_obj)
        if key.lower().endswith(file_extension.lower()) and file_extension.lower() == '.zip':
            logger.info("File has zip extension :")
            buffer = io.BytesIO(zip_obj.get()["Body"].read())
            z = zipfile.ZipFile(buffer)
            final_destination_path = get_unzip_destination_folder(is_folder_needed, destination_path, key)
            logger.info("final_destination_path : %s", final_destination_path)
            for filename in z.namelist():
                s3_resource.meta.client.upload_fileobj(z.open(filename), Bucket=destination_bucket,
                                                       Key=final_destination_path + f'{filename}')
        elif key.lower().endswith(file_extension.lower()) and file_extension.lower() == '.gz':
            logger.info("File has gz extension :")
            zip_obj = s3_resource.Object(bucket_name=source_bucket, key=key)
            print("list of files in gz file :", zip_obj)
            with gzip.GzipFile(fileobj=zip_obj.get()["Body"]) as gzipfile:
                content = gzipfile.read()
            final_destination_path = get_unzip_destination_folder(is_folder_needed, destination_path, key)
            logger.info("final_destination_path : %s", final_destination_path)
            object1 = s3_resource.Object(bucket_name=destination_bucket,
                                         key=final_destination_path + key.split("/")[-1].replace('.gz',''))
            object1.put(Body=content)
        else:
            logger.info("File has some other extension, code handling only .zip and .gz")


def get_logger(context):
    if context is None:
        generic_logger = logging.getLogger('common_utils')
        log_util = GenericLoggerFormatterUtility(generic_logger)
        logger = log_util.logger_adapter
    else:
        try:
            generic_logger = logging.getLogger('common_utils')
            log_util = GenericLoggerFormatterUtility(generic_logger, app_name=context['task_instance'].dag_id, correlation_id=context['run_id'])
            logger = log_util.logger_adapter
        except Exception as err:
            generic_logger = logging.getLogger('common_utils')
            log_util = GenericLoggerFormatterUtility(generic_logger)
            logger = log_util.logger_adapter
    return logger


def is_object_exists(context, input_location):
    """
    Checks whether the object exists in the passed s3 bucket and if data data present at file
    :param path: s3 path
    :return: True/False
    """
    logger = get_logger(context)
    logger.info("Intitiating check to determine if any fies present in input s3 location")
    if 'header_count' in context:
        logger.info("Header is present in FIle")
        header_count = context['header_count']
    else:
        header_count = None
    try:
        s3_resource = boto3.resource('s3')
        bucket_name, key = get_bucket_name_key(input_location)
        bucket = s3_resource.Bucket(bucket_name)
        count = 0

        logger.debug(f"bucket_name : {bucket_name}, key : {key}")
        logger.info(f"bucket_name : {bucket_name}, key : {key}")
        for object_summary in bucket.objects.filter(Prefix=key):
            if str(object_summary.key) != input_location and object_summary.size > 0:
                logger.info("s3 path" + input_location)
                object_ext = str(object_summary.key.split('.')[-1]).lower()
                logger.info(f" file has {object_summary.key.split('.')[-1]} Extension")
                if object_ext in ('dat', 'csv', 'txt') or (
                        'file_has_no_extension' in context and context['file_has_no_extension'].lower() == 'y'):
                #Change-11 starts
                    dataframe = pd.read_table(object_summary.get()['Body'], header=header_count, names=['Data'], skip_blank_lines=True,
                                            nrows=2)
                #Change-11 ends
                else:
                    raise Exception(
                        f"{object_summary.key.split('.')[-1]} Extension is not allowed for this functionality.")

                countrows = len(dataframe)
                logger.info(f"Count of file: {str(object_summary.key)} is {countrows}")
                logger.info("Sample records of file: ")
                logger.info(dataframe.head(2))
                if not countrows > 0:
                    return False

                count = count + 1

        logger.info(f"count value is {count}")
        if count > 0:
            return True
        else:
            return False

    except Exception as Err:
        logger.info("is_object_exists failed " + str(Err))


def empty_file_or_zero_record_check(**kwargs):
    """
    This method checks weither the s3 object exists or not.
    :param s3_input_location, stop_process_for_empty_file: The S3 path where the incremental file is located.
    :param stop_process_for_empty_file: Two possible values yes/no.
    :return: true/false
    Author : Parth Khambhayta
    """
    logger = get_logger(kwargs)
    try:
        process_status = kwargs['stop_process_for_empty_file']
        logger.info("stop_process_for_empty_file calling...........")
        if not is_object_exists(kwargs, kwargs['s3_input_location']):
            if str(process_status).lower() == 'no':
                logger.info(
                    f"Not stopping process for empty file received at: {kwargs['s3_input_location']} ")
            else:
                logger.error(f"Stopping process for empty file received at : {kwargs['s3_input_location']}")
                raise Exception(f"Empty file received at : {kwargs['s3_input_location']}")
    except Exception as err:
        raise Exception(f"empty_file_or_zero_record_check failed : {str(err)}")


def get_bucket_name_key(s3_input_location):
    """
    Method to get bucket name and file location
    :param s3_input_location:
    :return:
    """
    raw_path = s3_input_location.split("//")
    details = raw_path[1].split("/", 1)
    bucket_name = details[0]
    file_location = details[1]
    return bucket_name, file_location


def set_start_timestamp_in_xcom(**kwargs):
    """
    Description : method set the start timestamp into xcom variable
    :param kwargs: input parameters like load_name, domain and start_timestamp
    :return: sets value into xcom variable
    Author : Ibney Hasan
    """
    logger = get_logger(kwargs)
    try:
        if "load_name" in kwargs and kwargs["load_name"] is not None and "domain" in kwargs and kwargs[
            "domain"] is not None:
            load_name = kwargs['load_name']
            domain = kwargs['domain']
        else:
            raise Exception("loadname and domain cannot be absent or null")
        logger.info("load_name : %s", load_name)
        logger.info("domain : %s", domain)
        start_timestamp = get_max_query(domain, load_name, "start_timestamp")
        if start_timestamp is not None:
            start_timestamp = start_timestamp.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
            logger.info("maximum start_timestamp from database : %s", start_timestamp)
        else:
            if "start_timestamp" in kwargs and kwargs["start_timestamp"] is not None:
                start_timestamp = kwargs["start_timestamp"]
            else:
                raise Exception("start_timestamp cannot be absent or null")
        save_data_in_xcom(kwargs, "start_timestamp", str(start_timestamp))

    except Exception as err:
        logger.error(str(err))
        raise "Failed to set feed_date run_date and start_timestamp to xcom check logs" + str(err)

def upsert_start_date(**kwargs):
    """
    this method is used to insert the record if not exist else it will update the record.
    :param kwargs:
    :return:
    Author : Ibney Hasan
    """
    logger = get_logger(kwargs)
    try:
        feed_date = kwargs['feed_date']
        run_date = kwargs['run_date']
        s3_bucket_name = kwargs['bucket_name']
        s3_bucket_key = kwargs['bucket_key']
        logger.info("run_date : %s", str(run_date))
        logger.info("feed_date : %s", str(feed_date))

        start_date_col_name, start_date_value,empty_file_encountered = read_data_from_s3(s3_bucket_name, s3_bucket_key)
        logger.info("empty_file_encountered  : {}".format(empty_file_encountered))
        logger.info("column name : {}".format(start_date_col_name))
        logger.info("insert timestamp : {}".format(start_date_value))
        if empty_file_encountered == False:
            source_hook = PostgresHook(postgres_conn_id='airflow_postgres_conn')
            insert_query = "INSERT INTO airflow.job_execution_control (domainname, loadname, run_date, feed_date, insert_timestamp, start_timestamp) \
            VALUES(':DOMAIN_NAME', ':LOAD_NAME', ':RUN_DATE', ':FEED_DATE', ':INSERT_TIME',':START_DATE') ON CONFLICT(domainname,loadname) \
            DO UPDATE SET start_timestamp= ':START_DATE', update_timestamp = ':UPDATE_TIME',run_date = ':RUN_DATE',feed_date = ':FEED_DATE'"
            insert_query = insert_query.replace(':DOMAIN_NAME', kwargs['domain'])
            insert_query = insert_query.replace(':LOAD_NAME', kwargs['load_name'])
            insert_query = insert_query.replace(':FEED_DATE', feed_date)
            insert_query = insert_query.replace(':RUN_DATE', run_date)
            insert_query = insert_query.replace(':INSERT_TIME', get_current_formatted_cdt_date_time())
            insert_query = insert_query.replace(':UPDATE_TIME', get_current_formatted_cdt_date_time())
            insert_query = insert_query.replace(':START_DATE', start_date_value)

            source_hook.run(insert_query)
    except Exception as error:
        error_msg = "error while insert and update into table {}".format(error)
        logger.error(error_msg)
        raise error


def read_data_from_s3(s3_bucket_name, s3_bucket_key):
    """
    this method is used to read the csv from s3 and return column name with value
    :return:
    Author : Ibney Hasan
    """
    logger = get_logger(None)

    try:
        empty_file_encountered =  False
        bucket = s3_resource.Bucket(s3_bucket_name)
        logger.info("s3_bucket_key before : {}".format(s3_bucket_key))
        logger.info("bucket.objects.filter(Prefix=s3_bucket_key) : {}".format(bucket.objects.filter(Prefix=s3_bucket_key)))
        path=os.path.join(s3_bucket_name,s3_bucket_key)
        logger.info("path: {}".format(path))
        for object_summary in bucket.objects.filter(Prefix=s3_bucket_key):
            logger.info("object_summary.size : {}".format(object_summary.size))
            logger.info("object_summary.key : {}".format(object_summary.key))
            if object_summary.size > 0 and str(object_summary.key) != path:
                logger.info("object_summary is : {}".format(object_summary))
                bucket_key = str(object_summary.key)
                empty_file_encountered =  False
            else:
                empty_file_encountered=True
        logger.info("empty_file_encountered  : {}".format(empty_file_encountered))
        if empty_file_encountered == False:
            obj = s3.get_object(Bucket=s3_bucket_name, Key=bucket_key)
            logger.info("Response is : {}".format(obj))
            obj_df = pd.read_csv(obj["Body"])
            column_name = obj_df.columns[0]
            logger.info("column name is ", obj_df.columns[0])
            column_value = obj_df.iloc[0][column_name]
            logger.info("column value is ", column_value)
            return column_name, column_value,empty_file_encountered
        else:
            column_name=''
            column_value=''
            return column_name, column_value,empty_file_encountered
    except Exception as error:
        error_msg = "error while reading data from s3 location. error {}".format(error)
        logger.error(error_msg)
        raise error


def check_file_present_s3(**kwargs):
    """
    :description : method to check if the file present on s3 location or not.
    :param kwargs:source_bucket_name,source_base_path,file_name_to_search
    :return: next_task_if_no_file,next_task_if_has_file
    Author : Ibney Hasan
    """
    try:
        logger = get_logger(kwargs)
        my_bucket = s3_resource.Bucket(kwargs['source_bucket_name'])
        source_base_path = get_formatted_path(kwargs['source_base_path'])
        is_file_name_exist = False
        if 'file_name_to_search' in kwargs:
            is_file_name_exist = True
        count = 0
        msg = ""
        for object_summary in my_bucket.objects.filter(Prefix=source_base_path):
            logger.debug(f"check_file_present_s3.... Inside For Loop.... {str(object_summary.key)}")
            if str(object_summary.key) != source_base_path and object_summary.size > 0:
                if is_file_name_exist:
                    if object_summary.key.find(kwargs['file_name_to_search']) != -1:
                        count = count + 1
                    else:
                        logger.info("Input file_name_to_search argument is not correct")
                else:
                    count = count + 1
        if count >= 1:
            logger.info("File exists on S3 so running task")
            return kwargs['next_task_if_has_file']
        else:
            logger.info("File does not exists or empty file received on S3 so running task : ",
                    kwargs['next_task_if_no_file'])
            return kwargs['next_task_if_no_file']
    except Exception as err:
        logger.error(f"Failed to check_file_present_s3 : {str(err)}")
        raise "Failed to check_file_present_s3 " + str(err)

def get_max_query(domain, load_name, max_value_var):
    """
    Method to get max start_timestamp from job_execution_control based on load name
    :param domain : input domain name
    :load_name   :- input load name
    :max_value_var : max value column name
    """
    query = "SELECT max(" + max_value_var.strip().lower() + ") FROM airflow.job_execution_control where domainname = '" + domain.strip().lower() + "' and loadname = '" + load_name.strip().lower() + "'"
    source_hook = PostgresHook(postgres_conn_id='airflow_postgres_conn')
    source_conn = source_hook.get_conn()
    source_cursor = source_conn.cursor()
    source_cursor.execute(query)
    records = source_cursor.fetchone()
    max_date = records[0]
    return max_date

def remove_special_char_from_filename(**kwargs):
    """
    Description : Method to rename s3 files with invalid special characters.
    Special character is replaced by ' '
    :param kwargs: bucket_name, bucket_key,special_characters
    :return: None
    """
    logger = get_logger(kwargs)
    s3_bucket_name = kwargs['bucket_name']
    s3_bucket_key = kwargs['bucket_key']
    special_characters = kwargs['special_characters']
    bucket = s3_resource.Bucket(s3_bucket_name)
    path = os.path.join(s3_bucket_name, s3_bucket_key)
    for object_summary in bucket.objects.filter(Prefix=s3_bucket_key):
        if str(object_summary.key) != path:
            copy_source = {'Bucket': s3_bucket_name, 'Key': object_summary.key}
            targetfile=re.sub(special_characters, " ", str(object_summary.key))
            if targetfile != str(object_summary.key):
                logger.info("copy_source : {}".format(copy_source))
                logger.info("targetfile: {}".format(targetfile))
                # Copy input s3 file with new name in same bucket
                s3_resource.meta.client.copy(CopySource=copy_source, Bucket=s3_bucket_name, Key=targetfile)
                # Delete the old file
                s3.delete_object(Bucket=s3_bucket_name, Key=object_summary.key)

def check_for_last_day_of_month(**kwargs):
    """
    :description : method to check whether the passed day is the last day of the month
    :param kwargs:run_date
    :return: next_task_if_last_day_of_month,next_task_if_not_last_day_of_month
    """
    logger = get_logger(kwargs)
    logger.info("Intitiating check to determine the passed date is last date of the month")
    run_date = kwargs['run_date']
    rundate = datetime.strptime(run_date, '%Y-%m-%d').date()
    last_day_of_month = calendar.monthrange(rundate.year, rundate.month)[1]
    if rundate == date(rundate.year, rundate.month, last_day_of_month):
        logger.info("The passed date is the last date of the month returning the next task name for execution : %s",
                    kwargs['next_task_if_last_day_of_month'])
        return kwargs['next_task_if_last_day_of_month']
    logger.info("The passed date is not the last date of the month returning the next task name for execution : %s",
                kwargs['next_task_if_not_last_day_of_month'])
    return kwargs['next_task_if_not_last_day_of_month']



########### V5 new changes started


def change_case_of_string(str, mode):
    return str.upper() if mode.lower() == 'u' else str.lower()


def save_feed_date_formatted(**kwargs):
    # Input args : file_name_prefix, feed_date_format, feed_date_case, feed_date
    # Output xcom variable: return feed_Date in given formated and save data in xcom variable
    # Description : common method for get feed_Date in given formated

    logger = get_logger(kwargs)
    feed_date_format = kwargs['feed_date_format']
    feed_date = kwargs['feed_date']
    try:
        if "file_name_prefix" in kwargs:
            logger.info("file_name_prefix value : %s", str(kwargs['file_name_prefix']))
            logger.info("feed_date_format value : %s", str(feed_date_format))
            file_name_feed_date_formatted = str(kwargs['file_name_prefix']) + str(
                datetime.strptime(feed_date, '%Y-%m-%d').strftime(feed_date_format))
            if "feed_date_case" in kwargs:
                file_name_feed_date_formatted = change_case_of_string(file_name_feed_date_formatted, kwargs["feed_date_case"])
            logger.info("get_feed_date_formatted value : %s", str(file_name_feed_date_formatted))
            save_data_in_xcom(kwargs, "file_name_feed_date_formatted", file_name_feed_date_formatted)
        else:
            logger.info("feed_date_format value : %s", str(feed_date_format))
            feed_date_formatted = str(datetime.strptime(feed_date, '%Y-%m-%d').strftime(feed_date_format))
            if "feed_date_case" in kwargs:
                feed_date_formatted = change_case_of_string(feed_date_formatted, kwargs["feed_date_case"])
            logger.info("get_feed_date_formatted value : %s", str(feed_date_formatted))
            save_data_in_xcom(kwargs, "feed_date_formatted", feed_date_formatted)

    except Exception as err:
        raise "save_feed_date_formatted : Failed to get save_feed_date_formatted check logs" + str(err)


def get_postgres_conn_cursor(conn_string):
    try:
        source_hook = PostgresHook(postgres_conn_id=str(conn_string))
        source_conn = source_hook.get_conn()
        source_cursor = source_conn.cursor()
    except Exception as err:
        raise "Failed to get_postgress_conn." + str(err)

    return source_cursor


def get_next_item_to_run(context,current,domain_name,load_name):
    # Input args : context,current,domain_name,load_name
    # Output : return nextfile name from load_file_sequence_info based on input
    # Description : get nextt item run from table based on input(load name, domain name and current filename)

    logger = get_logger(context)
    try:
        query = "select next from airflow.load_file_sequence_info " \
                "where domain_name = ':domain_name' and load_name = ':load_name'  and current = ':current'"
        query = query.replace(':current',current)
        query = query.replace(':load_name',load_name)
        query = query.replace(':domain_name', domain_name)
        # source hook
        source_cursor = get_postgres_conn_cursor('airflow_postgres_conn')
        source_cursor.execute(query)
        records = source_cursor.fetchone()
        next_run = records
        if records is not None:
            next_run = records[0]
        logger.info("next filename for run : %s", str(next_run))
    except Exception as err:
        raise "Failed to check the getNextItemToRun." + str(err)
    return next_run

# Below method is return next filename from load_file_sequance_info table
def get_filename_to_process(**kwargs):
    # Input args : LOAD_NAME, FEED_DATE, DEFAULT_FILENAME
    # Output xcom variable : store nextfile name in xcom variable name is -> next_filename
    # Description : Common method to get nextfile name from load_file_sequance_info table

    logger = get_logger(kwargs)
    load_detail = kwargs["LOAD_DETAIL"]
    domain_name = load_detail.split('|')[1]
    load_name = load_detail.split('|')[2]
    feed_date = kwargs["FEED_DATE"]
    default_val = kwargs["DEFAULT_FILENAME"]
    logger.info("load_detail : %s", str(load_detail))
    logger.info("domain_name : %s", str(domain_name))
    logger.info("load_name : %s", str(load_name))
    logger.info("feed_date : %s", str(feed_date))
    logger.info("default_val : %s", str(default_val))
    logger.info("dag_name : %s", str(kwargs['task_instance'].dag_id.lower()))
    try:
        # use only for testing if user want to trigger manual run for specific sequence then passing argument file_sequence_to_process in json
        if kwargs is not None and kwargs["dag_run"] is not None and kwargs["dag_run"].conf is not None and "file_sequence_to_process" in kwargs["dag_run"].conf and kwargs["dag_run"].conf["file_sequence_to_process"] is not None:
            next_run = kwargs["dag_run"].conf["file_sequence_to_process"]
            logger.info("file_sequence_to_process from passed_argument : %s", str(next_run))
        else:
            query = "select loadname from airflow.dag_audit_info " \
                    "where run_id = (select max(run_id) as max_run_id from airflow.dag_audit_info " \
                    "where feeddate = ':feed_date' and domainname = ':domain_name' " \
                    "and loadname like ':load_name%'  and status = 'completed')" \
                    "and lower(dag_name) = ':dagId' order by end_time desc"
            query = query.replace(':feed_date',feed_date)
            query = query.replace(':load_name',load_name)
            query = query.replace(':domain_name', domain_name)
            query = query.replace(':dagId', kwargs['task_instance'].dag_id.lower())
            # source hook
            source_cursor = get_postgres_conn_cursor('airflow_postgres_conn')
            source_cursor.execute(query)
            records = source_cursor.fetchone()
            load_name_with_filename = records
            logger.info("load_name_with_filename : %s", str(load_name_with_filename))
            if load_name_with_filename is not None:
                next_run = get_next_item_to_run(kwargs,load_name_with_filename[0].split('::')[-1],domain_name,load_name)
            else:
                next_run = default_val

        logger.info("next filename for run : %s", str(next_run))
        save_data_in_xcom(kwargs, "next_filename", next_run)

    except Exception as err:
        raise "Failed to check the get_filename_to_process." + str(err)


def check_next_task_run(**kwargs):
    # Input args : FEED_DATE, RUN_DATE
    # Output : return taskid based on condition
    # Description : This method is used only for or_Cas_Secondary load to check
    #               if feed_Date is greater and equal to run date then its return seccess if else failure taskid

    logger = get_logger(kwargs)
    feed_date = kwargs["FEED_DATE"]
    run_date = kwargs["RUN_DATE"]
    try:
        logger.info("feed_date : %s", str(feed_date))
        logger.info("run_date : %s", str(run_date))
        if feed_date >= run_date:
            return kwargs['ON_SUCCESS_TASKID']
        else:
            return kwargs['ON_FAILURE_TASKID']
    except Exception as err:
        raise "Failed to check the check_next_task_run." + str(err)


def check_for_update_feeddate(**kwargs):
    # Input args : NEXT_FILENAME, FILENAME_FOR_UPD_FEEDATE
    # Output : return taskid based on condition
    # Description : This method is common method used for update run control table based on if filename is last of current load

    logger = get_logger(kwargs)
    next_filename = kwargs["NEXT_FILENAME"]
    FILENAME_FOR_UPD_FEEDATE = kwargs["FILENAME_FOR_UPD_FEEDATE"]
    try:
        logger.info("next_filename : %s", str(next_filename))
        if next_filename == FILENAME_FOR_UPD_FEEDATE:
            return kwargs['ON_SUCCESS_TASKID']
        else:
            return kwargs['ON_FAILURE_TASKID']
    except Exception as err:
        raise "Failed to check the check_for_update_feeddate." + str(err)


def is_skip_alert_from_config_table(context, formatted_exception):
    # Input args : context, formatted_exception
    # Output : return boolean value
    # Description : This method is common method used for check skip alter email for given param currect dagid and taskid entry in config table

    logger = get_logger(context)
    logger.info("dag_id : %s", context['task_instance'].dag_id)
    logger.info("task_id : %s", context['task_instance'].task_id)
    query = "select count(1) from airflow.skip_alert_config where lower(dag_id) = ':dagId' and lower(task_id_list) like '%:taskId%' and upper(skip_alert) = 'Y'"
    query = query.replace(':dagId',context['task_instance'].dag_id.lower())
    query = query.replace(':taskId', context['task_instance'].task_id)
    # source hook
    source_cursor = get_postgres_conn_cursor('airflow_postgres_conn')
    source_cursor.execute(query)
    records = source_cursor.fetchone()
    logger.info("records : %s", str(records))
    if records[0] > 0:
        logger.info("skip the alert from config table")
        return True
    else:
        logger.info("do not skip the alert from config table")
        return False

def custom_file_renaming(**kwargs):
    """
    :description : It will rename all the files present in the given location and remove the old files
    :Input args : file_prefix, timestamp_type, s3_input_path, timestamp_format
    """
    logger = get_logger(None)
    file_prefix = kwargs["file_prefix"]
    logger.info("file_prefix : %s", str(file_prefix))
    timestamp_type = kwargs["timestamp_type"]
    logger.info("timestamp_type : %s", str(timestamp_type))
    s3_input_path = kwargs["s3_td_path"]
    logger.info("s3_input_path : %s", str(s3_input_path))
    timestamp_format = kwargs["timestamp_format"]
    logger.info("timestamp_format : %s", str(timestamp_format))
    file_extension = kwargs["file_extension"]
    logger.info("file_extension : %s", str(file_extension))
    logger.info("Starting renaming the file.....")
    s3_resource = boto3.resource('s3')
    bucket_name, file_location = get_bucket_name_key(s3_input_path)
    logger.info("bucket_name : %s", str(bucket_name))
    logger.info("file_location %s", str(file_location))
    bucket = s3_resource.Bucket(bucket_name)
    counter = 0
    formatted_date = new_file_name(timestamp_type, timestamp_format)
    for files in bucket.objects.filter(Prefix=file_location):
        logger.info("files.key : %s", str(files.key))
        if str(files.key) != file_location:
            old_key = files.key
            logger.info("old_key: %s", str(old_key))
            counter += 1
            logger.info("file_prefix", str(file_prefix))
            logger.info(f"bucket.objects.filter(Prefix=file_location):: {list(bucket.objects.filter(Prefix=file_location))}")
            if timestamp_format == "%Y%m%d%H%M%S":
                file_name = file_prefix + "_" + str(int(formatted_date) + counter) + "." + file_extension
            elif len(list(bucket.objects.filter(Prefix=file_location))) == 1:
                logger.info("Only one file key is available for copy !!!" )
                file_name = file_prefix + formatted_date + "." + file_extension
            else:
                file_name = file_prefix + formatted_date + "_" + str(counter) + "." + file_extension
            new_key = file_location + file_name
            logger.info("value of new_key", str(new_key))

            logger.info("new_key: %s", str(new_key))
            copy_source = {
                'Bucket': bucket_name,
                'Key': old_key
            }
            s3_resource.meta.client.copy(copy_source, bucket_name, new_key)
            s3_resource.Object(bucket_name, old_key).delete()

def new_file_name(timestamp_type, timestamp_format):
    """
    :description : It will derive the new names for all the files present in the given location
    :Input args : file_location, file_prefix, timestamp_type, timestamp_format
    :return: processed final s3 new_key
    """
    logger = get_logger(None)
    logger.info("Starting get_new_key method....")
    if timestamp_type == "multi_timestamp":
        curr_date = datetime.now(timezone('America/Chicago')).strftime(timestamp_format)
        end_date = (datetime.now(timezone('America/Chicago')) + timedelta(days=1.5)).strftime(timestamp_format)
        formatted_date = curr_date + "-" + end_date
    else:
        formatted_date = datetime.now(timezone('America/Chicago')).strftime(timestamp_format)
    return formatted_date

def check_count_for_feed_date(**kwargs):
    """
    :description : Method to get expected count from dag_audit_info based on domain name ,load name, status and feed_date
	:This method will be used in branch operator to check that expected count is matching or not.
	:If condition is true then next task (next_task_id) will run otherwise it will move to skip_task_id.
    :param kwargs: load_detail, feed_date, expected_count, next_task_id, skip_task_id
    """
    logger = get_logger(kwargs)
    load_detail = kwargs["load_detail"]
    feed_date = kwargs["feed_date"]
    expected_count = int(kwargs["expected_count"])
    domain_name = load_detail.split('|')[1]
    load_name = load_detail.split('|')[2]
    try:
        query = "select count(*) from airflow.dag_audit_info dai where domainname = ':domain_name' and " \
                "loadname =':load_name' and status='completed' and feeddate = ':feed_date'"
        query = query.replace(':domain_name', domain_name)
        query = query.replace(':load_name', load_name)
        query = query.replace(':feed_date', feed_date)
        source_cursor = get_postgres_conn_cursor('airflow_postgres_conn')
        source_cursor.execute(query)
        records = source_cursor.fetchone()
        result_count = records[0]
        if result_count is not None and result_count == expected_count:
            logger.warn(
                "All files for a given feed date is completed %s",str(result_count))
            return kwargs['next_task_id']
        else:
            logger.warn("All files for a given feed date is not completed %s",str(result_count))
            return kwargs['skip_task_id']

    except Exception as err:
        raise "Failed to check_count_for_feed_date" + str(err)

def inter_bucket_files_transfer_move_latest_file(**kwargs):
    """
    :description : Method for moving first file from passed source path to destination path.
    :param kwargs: source_bucket_name, destination_bucket_name, source_base_path, destination_base_path, file_index_to_move, is_sorting_true
    """
    logger = get_logger(kwargs)
    try:
        for i in kwargs['file_to_move']:
            my_bucket = s3_resource.Bucket(i['source_bucket_name'])
            source_base_path = get_formatted_path(i['source_base_path'])
            destination_base_path = get_formatted_path(i['destination_base_path'])
            file_index_to_move = int(i['file_index_to_move']) if 'file_index_to_move' in i else 0
            is_sorting_true = i['is_sorting_true']
            logger.info("my_bucket : %s", my_bucket)
            logger.info("source_base_path : %s", source_base_path)
            logger.info("destination_base_path : %s", destination_base_path)
            logger.info("file_index_to_move : %s", type(file_index_to_move))
            logger.info("is_sorting_true : %s", is_sorting_true)
            unsorted_files = [object_summary.key for object_summary in my_bucket.objects.filter(Prefix=source_base_path) if (object_summary.key != source_base_path)]
            file_list = []
            if is_sorting_true == 'y':
                file_list = sorted(unsorted_files)
                logger.warn("sorted data - %s", file_list)
            else:
                file_list = unsorted_files

            logger.info("Copying file - %s", file_list[file_index_to_move])
            destination_location = file_list[file_index_to_move].split(source_base_path)[1]
            copy_source = {'Bucket': i['source_bucket_name'], 'Key': file_list[file_index_to_move]}
            destination = destination_base_path + destination_location
            s3_resource.meta.client.copy(CopySource=copy_source, Bucket=i['destination_bucket_name'],
                                         Key=destination)
            s3_resource.Object(i['source_bucket_name'], file_list[file_index_to_move]).delete()

    except Exception as err:
        logger.error(str(err))
        raise "Failed - inter_bucket_files_transfer_move_latest_file : " + str(err)


def get_dag_status(**kwargs):
    """
        :description : It is checking load status based on condition it will return taskid
        :Input args(Required) : skip_task_id, next_task_id, load_detail/domain_name,load_name
        :Input args(Optional) : status(default: "'started'")("'started','failed'"), wildcard_match ,last_run_status
        :return: task_id based on condition
    """
    logger = get_logger(kwargs)
    # Change-10 starts
    try:
        domain_name = kwargs["domain_name"] if "domain_name" in kwargs else kwargs["load_detail"].split('|')[1]
        load_name = kwargs["load_name"] if "load_name" in kwargs else kwargs["load_detail"].split('|')[2]
        status = kwargs["status"] if "status" in kwargs else "'started'"
        # Change-14 starts
        last_run_status = kwargs["last_run_status"] if "last_run_status" in kwargs else 'n'
        logger.info("Status check for {0} domain and {1} load with status {2} wildcard_match {3} last_run_status {4}".format(domain_name, load_name, status, kwargs["wildcard_match"] if "wildcard_match" in kwargs else " None", last_run_status))

        if "wildcard_match" in kwargs and kwargs["wildcard_match"].lower() == 'n':
            logger.info("Query to be executed without wildcard")
            query = "select count(1) from airflow.dag_audit_info dai where domainname = ':domain_name' and " \
                    "loadname = ':load_name' and status in (:status) "
            query1 = "(select max(run_id) from airflow.dag_audit_info dai where domainname = ':domain_name' and loadname = ':load_name')"
        else:
            logger.info("Query to be executed with wildcard")
            query = "select count(1) from airflow.dag_audit_info dai where domainname = ':domain_name' and " \
                    "loadname like '%:load_name%' and status in (:status) "
            query1 = "(select max(run_id) from airflow.dag_audit_info dai where domainname = ':domain_name' and loadname like '%:load_name%')"

        query = query.replace(':domain_name', domain_name)
        query = query.replace(':load_name', load_name)
        query = query.replace(':status', status)
        # Change-10 ends
        query1 = query1.replace(':domain_name', domain_name)
        query1 = query1.replace(':load_name', load_name)
        if last_run_status.lower() == 'y':
            logger.info("Query to be executed with last_run_status")
            query = query + "and run_id = " + query1
        source_cursor = get_postgres_conn_cursor('airflow_postgres_conn')
        source_cursor.execute(query)
        records = source_cursor.fetchone()
        if records[0] > 0:
            logger.info("{0}.{1} is not ready to execute now.".format(str(domain_name), str(load_name)))
            return kwargs['skip_task_id']
        else:
            logger.info("{0}.{1} is ready to execute now.".format(str(domain_name), str(load_name)))
            # Change-14 ends
            return kwargs['next_task_id']
    except Exception as err:
        raise "Failed to get_dag_status." + str(err)


def get_operator(argument):
    """
        :description : It will return operator function name based on given string argument for checking value
        :Input args : operator value in string
        :return: return operator function name
    """
    operator_val = {'>': operator.gt,
                    '<': operator.lt,
                    '>=': operator.ge,
                    '<=': operator.le,
                    '==': operator.eq}
    return operator_val.get(argument, "nothing")


def get_dependency_by_operator(**kwargs):
    """
        This method checks dependency by using operator as per teradata scheduling and check self dependency using operator.
        :param: template_fields, domain_name, load_name: source domain name and source load name
        :param: list_dependencies, dependent_domainname, dependent_loadname: dependent domain name and dependent load name.
        :param: check_self_dependency : y
        :param: operator(>,>=,<,<=,==), offset(in number of days), feed_date
        :reference: https://github.ual.com/DEUCLOUD/inventory/blob/master/src/com/united/dags_v2/v2b_sch_chg.yaml
        :return: true/false
        Author : Parth Khambhayta
    """
    logger = get_logger(kwargs)
    flag = 1

    try:
        logger.info("-------------------------get_dependency_by_operator started------------------------------")
        if 'domain_name' not in kwargs['template_fields'] or 'load_name' not in kwargs['template_fields']:
            raise Exception("domain_name and load_name are missing in arguments in template_fields.")

        domain_name = kwargs['template_fields']['domain_name']
        load_name = kwargs['template_fields']['load_name']

        for dependency in kwargs['template_fields']['list_dependencies']:
            logger.info(f"Dependency check started for {dependency}")

            if ('operator' in dependency) and ('offset' in dependency):

                if ('feed_date' in kwargs) and (
                        'check_self_dependency' in dependency and dependency['check_self_dependency'].lower() == 'y'):
                    logger.info("checking dependecy with itself")
                    from_date = convert_string_to_date_object(kwargs['feed_date'], '%Y-%m-%d')
                    to_date = get_max_feed_date_by_load_name(domain_name, load_name)
                    dependent_domainname = domain_name
                    dependent_loadname = load_name
                else:
                    from_date = get_max_feed_date_by_load_name(dependency['dependent_domainname'],
                                                               dependency['dependent_loadname'])
                    to_date = get_max_feed_date_by_load_name(domain_name, load_name)
                    dependent_domainname = dependency['dependent_domainname']
                    dependent_loadname = dependency['dependent_loadname']

                if from_date is not None and to_date is not None:
                    diff = calculate_dff_between_date(to_date, from_date)
                else:
                    diff = 0

                logger.info(
                    f"dependent load {dependent_domainname}.{dependent_loadname} difference of feeddate is : {str(diff)} ")

                if get_operator(dependency['operator'])(diff, dependency['offset']):
                    logger.info(f"{dependent_domainname}.{dependent_loadname} dependency has met.")
                else:
                    flag = 0
                    logger.info(f"{dependent_domainname}.{dependent_loadname} dependency has not met.")

            else:
                raise Exception(
                    f"for {dependency} feed_date or operator and offset value is missing in argument.")

            logger.info(f"Dependency check completed for {dependency}")

        logger.info(f"Flag values is {flag}")
        if flag:
            logger.info("Good to run the load, as dependency is met.")
            logger.info("-------------------------get_dependency_by_operator completed------------------------------")
            return True
        else:
            logger.info("Can not run the load now, as dependency not met.")
            logger.info("-------------------------get_dependency_by_operator completed------------------------------")
            return False

    except Exception as err:
        raise "Failed to check the dependency." + str(err)

def inter_bucket_file_pattern_transfer(**kwargs):
    """
    :description : method for copying all the file from passed source path to destination path
    :param kwargs: source_bucket_name, destination_bucket_name, source_base_path, destination_base_path, load_name,file_pattern
    """
    logger = get_logger(kwargs)
    try:
        for i in kwargs['file_to_move']:
            logger.info("Start Copying")
            my_bucket = s3_resource.Bucket(i['source_bucket_name'])
            source_base_path = get_formatted_path(i['source_base_path'])
            destination_base_path = get_formatted_path(i['destination_base_path'])
            file_pattern = kwargs['file_pattern']
            logger.info("my_bucket : %s", my_bucket)
            logger.info("source_base_path : %s", source_base_path)
            logger.info("destination_base_path : %s", destination_base_path)
            logger.info(" file_pattern  : %s", file_pattern)
            for object_summary in my_bucket.objects.filter(Prefix=source_base_path):
                logger.info("Start Copying.... Inside For Loop.... " + str(object_summary.key))
                if str(object_summary.key) != source_base_path and re.search(file_pattern, str(object_summary.key)):
                    logger.info("Copying file -  " + object_summary.key)
                    destination_location = object_summary.key.split(source_base_path)[1]
                    copy_source = {'Bucket': i['source_bucket_name'], 'Key': object_summary.key}
                    destination = destination_base_path + destination_location
                    s3_resource.meta.client.copy(CopySource=copy_source, Bucket=i['destination_bucket_name'],Key=destination)
                    s3_resource.Object(i['source_bucket_name'], object_summary.key).delete()
    except Exception as err:
        logger.error(str(err))
        raise "Failed - inter_bucket_file_pattern_transfer : " + str(err)


def custom_sftp_file_sensor(**kwargs):
    # Input args : conn_id, path, domain_name, load_name, validation_mode, selection_mode, RegEx
    # Output Args : True/False
    # Xcom output Args : sensed_file_name, sensed_file_path, sensed_file_mode
    # Description : It will sensed latest file from given remote directory.
    # Doc : https://edaconfluence.ual.com/display/DMT/Custom+sftp+file+sensor
    logger = get_logger(kwargs)

    # Collect input args
    logger.info(f"{kwargs}")
    conn_id = kwargs["template_fields"]["conn_id"]
    path = kwargs["template_fields"]["path"]
    domain = kwargs["template_fields"]["domain_name"]
    load_name = kwargs["template_fields"]["load_name"]
    validation_mode = kwargs["template_fields"]["validation_mode"]
    selection_mode = kwargs["template_fields"]["selection_mode"]
    reg_ex = kwargs["template_fields"]["RegEx"]
    if 'check_mod_time_change' in kwargs['template_fields']:
        check_mod_time_change = kwargs["template_fields"]["check_mod_time_change"]
    else:
        check_mod_time_change = 'N'
    if 'poke_interval_mod_time' in kwargs['template_fields']:
        poke_interval_mod_time = int(kwargs["template_fields"]["poke_interval_mod_time"])
    else:
        poke_interval_mod_time = 300
    if 'retries_mod_time' in kwargs['template_fields']:
        retries_mod_time = int(kwargs["template_fields"]["retries_mod_time"])
    else:
        retries_mod_time = 4        

    # Initializing previous file detail with default values
    # previous_mod_time is long type which represent %Y%m%d%H%M%S
    previous_mod_time = "19000530095943"
    previous_file_name = ""

    # Query on airflow.custom_sftp_sensor_audit to get previous file detail
    query = "SELECT file_mod_time,file_name from airflow.custom_sftp_file_sensor_audit where domain_name = ':DOMAIN_NAME' and load_name = ':LOAD_NAME' and IS_SENSED=true order by file_mod_time desc limit 1"
    query = query.replace(":DOMAIN_NAME", domain.strip().lower())
    query = query.replace(":LOAD_NAME", load_name.strip().lower())

    source_hook = PostgresHook(postgres_conn_id="airflow_postgres_conn")
    source_conn = source_hook.get_conn()
    source_cursor = source_conn.cursor()
    source_cursor.execute(query)
    records = source_cursor.fetchone()
    logger.info(f"number of row in result set : {source_cursor.rowcount}")

    if source_cursor.rowcount:
        previous_mod_time = records[0]
        previous_file_name = records[1]

    logger.info(f"Previous proccessed file on {previous_mod_time}")
    is_sense = False
    newer_file_dict = {}
    try:
        hook = SFTPHook(ftp_conn_id=str(conn_id))

        list_file = hook.list_directory(path)
        logger.info(f"List of file in remote directory {list_file}")
        logger.info(f"Check files which start with {reg_ex}")
        logger.info(f"File validation mode is {validation_mode}")
        for file in list_file:
            if re.search(reg_ex, file):
                temp_path = path + file
                mod_time = hook.get_mod_time(temp_path)
                logger.info(f"File name {file} proccessed on {mod_time}")
                _mod_time = convert_to_utc(datetime.strptime(mod_time, "%Y%m%d%H%M%S"))

                # In case if previous sensed file detail are available but previous_mod_time is not present than
                # assign value with default mod time.
                if previous_mod_time is None:
                    _newer_than = convert_to_utc(
                        datetime.strptime("19000530095943", "%Y%m%d%H%M%S")
                    )
                else:
                    _newer_than = convert_to_utc(
                        datetime.strptime(str(previous_mod_time), "%Y%m%d%H%M%S")
                    )

                if validation_mode == "FILENAME":
                    if _newer_than < _mod_time and file != previous_file_name:
                        newer_file_dict[file] = mod_time

                elif validation_mode == "FILEMODTIME":

                    if _newer_than < _mod_time:
                        newer_file_dict[file] = mod_time

                else:
                    logger.error(
                        f"Provide correct mode of validation. mode validation should be FILENAME or FILEMODTIME"
                    )
                    raise Exception(
                        "Provide correct mode of validation. mode validation should be FILENAME or FILEMODTIME."
                    )
        hook.close_conn()
        if not newer_file_dict:
            logger.warn(f"newer files are not available")
        else:
            # Sort value in ascending order based on value and key
            newer_file_dict_sort=OrderedDict([v for v in sorted(newer_file_dict.items(), key=lambda kv: (kv[1], kv[0]))])
            logger.info(f"sorted value in ascending order based on value and key : {newer_file_dict_sort}")
            newer_file_list = list(newer_file_dict_sort.items())

            dag_variables_dict = {}
            logger.info(f"File selection mode is {selection_mode}")
            if selection_mode == "NEWER_OLDEST":
                dag_variables_dict = newer_file_list[0]
            elif selection_mode == "NEWER_LATEST":
                dag_variables_dict = newer_file_list[-1]
            else:
                logger.error(
                    f"Provide correct mode of selection. selection mode should be NEWER_OLDEST, NEWER_LATEST"
                )
                raise Exception(
                    "Provide correct mode of selection. selection mode should be NEWER_OLDEST, NEWER_LATEST."
                )
            if check_mod_time_change.lower() == 'y': 
                file_path = path + dag_variables_dict[0]
                logger.info("mod time change check started")
                check_file_time_change(str(_mod_time), str(file_path), str(conn_id),poke_interval_mod_time,retries_mod_time)
            else:
                logger.info("Mod time check not required")           
            is_sense = True
            task_instance = kwargs["task_instance"]
            task_instance.xcom_push(key="sensed_file_name", value=dag_variables_dict[0])
            task_instance.xcom_push(key="sensed_file_path", value=path)
            task_instance.xcom_push(
                key="sensed_file_mod_time", value=dag_variables_dict[1]
            )

    except OSError as e:
        logger.error(
            f" Exception occurred while connecting sftp server / list_directory / get_mod_time {e}"
        )
        return False

    return is_sense


def update_custom_sftp_file_sensor_status(**kwargs):
    # Input args : feed_date, run_date, sensed_file_name, sensed_file_path, sensed_file_mod_time
    # Output Args : NA
    # Description : It will insert sensed file detail into table airflow.custom_sftp_sensor_audit
    # Doc : https://edaconfluence.ual.com/display/DMT/Custom+sftp+file+sensor
    logger = get_logger(kwargs)

    try:
        # Collect input args
        feed_date = kwargs["feed_date"]
        sensed_file_name = kwargs["sensed_file_name"]
        sensed_file_path = kwargs["sensed_file_path"]
        sensed_file_mod_time = kwargs["sensed_file_mod_time"]
        logger.info("feed_date : %s", str(feed_date))
        logger.info("custom_sensor_file : %s", sensed_file_name)
        logger.info("custom_sensor_path : %s", sensed_file_path)
        logger.info("sensed_file_mod_time : %s", sensed_file_mod_time)

        if (
            sensed_file_name is not None
            and sensed_file_path is not None
            and sensed_file_mod_time is not None
        ):
            source_hook = PostgresHook(postgres_conn_id="airflow_postgres_conn")
            insert_query = "insert into airflow.custom_sftp_file_sensor_audit (domain_name, load_name, file_path, file_name, feed_date,file_mod_time,is_sensed) values (':DOMAIN_NAME',':LOAD_NAME',':FILE_PATH',':FILE_NAME',':FEED_DATE',':FILE_MOD_TIME',':IS_SENSED')"
            insert_query = insert_query.replace(":DOMAIN_NAME", kwargs["domain"])
            insert_query = insert_query.replace(":LOAD_NAME", kwargs["load_name"])
            insert_query = insert_query.replace(":FILE_PATH", sensed_file_path)
            insert_query = insert_query.replace(":FILE_NAME", sensed_file_name)
            insert_query = insert_query.replace(":FEED_DATE", feed_date)
            insert_query = insert_query.replace(":FILE_MOD_TIME", sensed_file_mod_time)
            insert_query = insert_query.replace(":IS_SENSED", "True")
            source_hook.run(insert_query)
            source_hook.run("commit;")

    except Exception as err:
        logger.error(
            f"Exception occurred during updating status into airflow.custom_sftp_sensor_audit {err}"
        )
        raise "Failed to update feed date check logs" + str(err)


def get_redshift_connection(secret_name):
    """
    :Input: param secret_name: name of secret which you want to get from secret manager
    :output: calls to get_pg_connection method
    :Description: This method returns redshift connection object based on secret manager provided,
     please remember to add the respecive secret manager entry in the MWAA Airflow execution role for respective environment
    :Author : Amit Garg
    """
    return get_pg_connection(secret_name,"redshift")


def get_rds_connection(secret_name):
    """
    :Input: param secret_name: name of secret which you want to get from secret manager
    :output: calls to get_pg_connection method
    :Description: This method returns rds connection object based on secret manager provided,
     please remember to add the respecive secret manager entry in the MWAA Airflow execution role for respective environment
    :Author : Amit Garg
    """
    return get_pg_connection(secret_name,"rds")


def get_pg_connection(secret_name, db):
    """
    :Input: param secret_name: name of secret which you want to get from secret manager, db name
    :output: return DB connection w.r.t to the source defined
    :Description: This method returns postgres engine based either RDS or RS connection object based on secret manager provided,
     please remember to add the respecive secret manager entry in the MWAA Airflow execution role for respective environment
    :Author : Amit Garg
    """
    logger = get_logger(None)
    logger.info("Inside get_pg_connection method :: secret_name : " + secret_name)
    possible_keys = {
    "username": ["db_username", "username", "user", "Username", "RedShiftMasterUsername", "AuroraMasterUsername"],
    "password": ["db_password", "password", "pwd", "Password", "RedShiftMasterUserPassword", "AuroraMasterUserPassword"],
    "host": ["db_host","hostname", "host", "Host", "Hostname", "RedShiftHost", "AuroraHost", "AuroraHostName"],
    "port": ["db_port","port", "Port", "RedShiftPort", "AuroraPort"],
    "dbname": ["db_name", "name", "dbname", "DBName", "DBname", "RedShiftDBName", "AuroraDBName"]
    }
    try:
        client = boto3.client("secretsmanager", region_name=region)
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
        if 'SecretString' in get_secret_value_response:
            secret_from_secret_mgr = json.loads(get_secret_value_response['SecretString'])
        else:
            secret_from_secret_mgr = json.loads(base64.b64decode(get_secret_value_response['SecretBinary']))
        secret = {k: sv for k in possible_keys.keys() for sk, sv in secret_from_secret_mgr.items() if sk in possible_keys.get(k)}
        # segregating DB connection based on RDS and Redshift due to different library use
        if db.lower() == "redshift" :
            pg_conn = psycopg2.connect(dbname=secret.get('dbname'), user=secret.get('username'),
                                       host=secret.get('host'), password=secret.get('password'),
                                       port=secret.get('port'))
            logger.info("get_pg_connection:: Before returning redshift pg_conn object")
            return pg_conn
        elif db.lower() == "rds" :
            logger.info("get_pg_connection:: inside RDS connection process")
            url = 'postgresql://{user}:{passwd}@{host}:{port}/{db}'.format(user=secret.get('username'), passwd=secret.get('password'), host=secret.get('host'), port=secret.get('port'), db=secret.get('dbname'))
            # Change-23 Start
            engine = create_engine(url, pool_size=5,connect_args={'sslmode':"require"})
            # Change-23 End
            con = engine.connect()
            logger.info("get_pg_connection:: Before returning rds con object")
            return con
        else:
            logger.info("get_pg_connection:: db value should be either rds or redshift !!!")
            raise "Failed to get_pg_connection :: db value should be either rds or redshift !!!"
    except Exception as err:
        logger.error(str(err))
        raise "Failed to get_pg_connection method for getting pg connection" + str(err)


def rds_conditional_check(**kwargs):
    """
        Input args : kwargs
        Output args : kwargs task_id
        Description : This method used to execute the conditional sql query on rdb to determine airflow task branching strategy,
        query provided should return based on the task dict keys.
        Author      : Amit Garg
    """
    logger = get_logger(None)
    try:
        query = kwargs['statement']
        secret_name = kwargs['secret_id']
        task_dict = kwargs['branching_task_dict']
        conn = get_rds_connection(secret_name)
        logger.info("rds_conditional_check:: before running query in the rds database: %s", str(conn))
        if conn is not None:
            results = conn.execute(query)
            records = results.fetchone()
            return get_branching_task_details(records, task_dict)
        else:
            logger.info("rds_conditional_check:: conn is None !!!")
            raise "Failed to rds_conditional_check :: conn is None"
    except Exception as err:
        logger.error(str(err))
        raise "Failed to get rds_conditional_check" + str(err)
    finally:
        conn.close()

def redshift_conditional_check(**kwargs):
    """
        Input args : kwargs
        Output args : kwargs task_id
        Description : This method used to execute the conditional sql query on redshift to determine airflow task branching strategy,
        query provided should return based on the task dict keys.
        Author      : Amit Garg
    """
    logger = get_logger(kwargs)
    try:
        secret_name = kwargs['secret_id']
        task_dict = kwargs['branching_task_dict']
        pg_conn = get_redshift_connection(secret_name)
        logger.info("redshift_conditional_check:: before running cursor in the database: %s", str(pg_conn))
        if pg_conn is not None:
            cursor = pg_conn.cursor()
            cursor.execute("set statement_timeout = 3600000")
            cursor.execute(kwargs['statement'])
            records = cursor.fetchone()
            logger.info("Response redshift query: %s ", str(records[0]))
            return get_branching_task_details(records, task_dict)
        else:
            logger.info("redshift_conditional_check:: pg_conn is is None !!!")
            raise "Failed to redshift_conditional_check :: pg_conn is None"
    except Exception as err:
        logger.error(str(err))
        raise "Failed to redshift_conditional_check method for processing" + str(err)
    finally:
        cursor.close()
        pg_conn.close()



def get_branching_task_details(record_list, task_dict):
    """
        Input args : record_list, task_dict
        Output args : task_dict value for particular task_dict key
        Description : This method used to return value based on the task dict keys.
        Author      : Amit Garg
    """
    logger = get_logger(None)
    try:
        if record_list is not None and record_list[0] is not None:
            result = str(record_list[0])
            if result in task_dict.keys() :
                logger.info("get_task_details:: task_dict value processed.")
                return task_dict.get(result)
            else:
                logger.error("get_task_details:: record_list value does not exist in task dict key.")
                raise "Failed to get_task_details:: record_list value does not exist in task dict key"
        else:
            logger.info("get_task_details:: record_list or record_list[0] is None !!!")
            raise "Failed to get_task_details for processing as record_list or record_list[0] is None"
    except Exception as err:
        logger.error(str(err))
        raise "Failed to get_task_details method for processing" + str(err)


def get_branching_task(context, task_dict, task_key):
    """
    This is a utility method to check for a key in a dictionary and returns value
    """
    logger = get_logger(context)
    if task_key in task_dict:
        logger.info("{0} is present in dictionary.".format(task_key))
        return task_dict[task_key]
    else:
        logger.info("{0} is not present in dictionary.".format(task_key))
        raise "{0} is not present in dictionary.".format(task_key)


def check_empty_file_branching(**kwargs):
    """
    This method to be used in branch operator, checks for empty file and return task id
    :param s3_input_location: The S3 path where the incremental file is located.
    :param task_dict: Dictionary with key as return type of function and value as task_id
    :return: task_id
    Author : Karan Mohadikar
    """
    logger = get_logger(kwargs)
    try:
        logger.info(kwargs['task_dict'])
        task_key = is_object_exists(kwargs, kwargs['s3_input_location'])
        task_id = get_branching_task(kwargs, kwargs['task_dict'], task_key)
        logger.info("Next task to be executed : " + str(task_id))
        return task_id
    except Exception as err:
        logger.error(str(err))
        raise "check_empty_file_branching failed: " + str(err)

# Change-17 START
def custom_sns_send_email(**kwargs):
    """
    This method to be used in sending email with custom message and subject
    :param env_variable: Getting env_variable from airflow variables
    :param sns_variable: sns arn topic from airflow variables
    :param subject : custom subject
    :param body: custom body
    Author : Gourav Thakur
    """
    logger = get_logger(kwargs)
    try:
        udh_environment = Variable.get(kwargs['env_variable'], "")
        logger.info("udh_environment %s ", str(udh_environment))
        sns_topic = Variable.get(kwargs['sns_variable'], "")
        logger.info("sns_topic %s ", str(sns_topic))
        subject: str = udh_environment.upper() + " | " + kwargs['subject']
        message_body: str = kwargs['body']
        subject_length: int = 100
        logger.info("subject %s ", str(subject))
        logger.info("message_body %s ", str(message_body))
        subject = (subject[:subject_length]) if len(subject) > subject_length else subject
        send_email: BaseOperator = SnsPublishOperator(
            task_id='send_email',
            aws_conn_id='aws_default',
            target_arn=sns_topic,
            message=message_body,
            subject=subject
        )
        send_email.execute(kwargs)
    except Exception as err:
        logger.error(str(err))
        raise "Failed to custom_sns_send_email" + str(err)
# Change-17 END
# Change-18 START
def create_empty_file(**kwargs):
    """
   This method to be used in creating empty file with suffix timestamp format
   :param S3_BUCKET_NAME: Getting input S3_BUCKET_NAME
   :param SUFFIX_FILE_FORMAT: input SUFFIX_FILE_FORMAT
   :param TIME_ZONE : input TIME_ZONE airflow
   :param dag_run:  run config pass from dag execution
   """
    logger = get_logger(None)
    s3_bucket_name = kwargs['S3_BUCKET_NAME']
    suffix_file_format = kwargs['SUFFIX_FILE_FORMAT']
    time_zone = kwargs['TIME_ZONE']
    file_extension = kwargs['FILE_EXTENSION']
    run_config = kwargs["dag_run"].conf
    logger.info(" run config value %s", run_config)
    logger.info("time zone  %s", time_zone)
    logger.info("suffix_file_format %s", suffix_file_format)
    logger.info("file extension  %s", file_extension)
    if len(run_config.keys()) > 0:
        logger.info("run config is not  None")
        for file_prefix in run_config.keys():
            current_timestamp = datetime.now(timezone(time_zone)).strftime(suffix_file_format)
            empty_file_name = run_config[file_prefix] + file_prefix + current_timestamp + file_extension
            object = s3_resource.Object(s3_bucket_name, empty_file_name)
            object.put()
    else:
        logger.info("run config is None")
        file_prefix= kwargs['FILE_PREFIX']
        file_output_file_path = kwargs['FILE_OUTPUT_PATH']
        logger.info("file_prefix %s : ",file_prefix)
        logger.info("file_output_file_path  %s : ", file_output_file_path)
        current_timestamp = datetime.now(timezone(time_zone)).strftime(suffix_file_format)
        empty_file_name = file_output_file_path + file_prefix + current_timestamp + file_extension
        object = s3_resource.Object(s3_bucket_name, empty_file_name)
        object.put()

# Change-18 END

# Change-19 START

def insert_job_exec_control(**kwargs):
    """
    This method will maintain start_time of the job, required by next run as feed_start_time.
    This method is simillar to upsert_start_date, but start_date_value is received from DAG
    It will insert the record if not exist else it will update the record.
    :param feed_date: feed date passed from Dag execution
    :param run_date: run date passed from Dag execution
    :param start_date: start_time passed from Dag execution
    :return:
     Author : pavan kumar ramanna
    """
    logger = get_logger(kwargs)
    try:
        logger.info("Started")
        domain_name = kwargs["domain_name"] if "domain_name" in kwargs else kwargs["load_detail"].split('|')[1]
        load_name = kwargs["load_name"] if "load_name" in kwargs else kwargs["load_detail"].split('|')[2]

        feed_date = kwargs['feed_date']
        run_date = kwargs['run_date']
        start_date = kwargs['start_date']
        logger.info("run_date : %s", str(run_date))
        logger.info("feed_date : %s", str(feed_date))

        start_date_value = start_date
        logger.info("start_timestamp : {}".format(start_date_value))

        source_hook = PostgresHook(postgres_conn_id='airflow_postgres_conn')
        insert_query = "INSERT INTO airflow.job_execution_control (domainname, loadname, run_date, feed_date, insert_timestamp, start_timestamp) \
        VALUES(':DOMAIN_NAME', ':LOAD_NAME', ':RUN_DATE', ':FEED_DATE', ':INSERT_TIME',':START_DATE') ON CONFLICT(domainname,loadname) \
        DO UPDATE SET start_timestamp= ':START_DATE', update_timestamp = ':UPDATE_TIME',run_date = ':RUN_DATE',feed_date = ':FEED_DATE'"
        insert_query = insert_query.replace(':DOMAIN_NAME', domain_name)
        insert_query = insert_query.replace(':LOAD_NAME', load_name)
        insert_query = insert_query.replace(':FEED_DATE', feed_date)
        insert_query = insert_query.replace(':RUN_DATE', run_date)
        insert_query = insert_query.replace(':INSERT_TIME', get_current_formatted_cdt_date_time())
        insert_query = insert_query.replace(':UPDATE_TIME', get_current_formatted_cdt_date_time())
        insert_query = insert_query.replace(':START_DATE', start_date_value)

        source_hook.run(insert_query)
    except Exception as error:
        error_msg = "error while insert and update into table {}".format(error)
        logger.error(error_msg)
        raise error

# Change-19 END

# Changed-21 Start
def build_foundry_config(rid, branch):
    """
    This is the method for creating a json object for the foundry dataset execution history
    :param rid: it is the dataset rid
    :param branch: It the branch name
    :return: It returns a JSON Object
    """
    jsonObject = {
        "rid": rid,
        "branch": branch,
        "pageSize": 1
    }
    return jsonObject


def get_foundry_sync_response(history_url, data, headers, proxyDict):
    """
    get_foundry_sync_response is the method used for creating a request to foundry for fetching execution history details
    :param history_url: This is the url for the History runs of foundry dataset
    :param data: this is the json object containing information about the dataset rid and branch
    :param headers: It is the header for creating a post request
    :param proxyDict: Proxy Dict contains the details for HOST and PORT
    :return: It returns the reponse after creating the post request
    """
    response = requests.post(history_url, headers=headers, data=json.dumps(data), proxies=proxyDict)
    return response


def trigger_foundry_sync(**kwargs):
    """
    This is the method for triggering foundry sync. It takes argument from the airflow
    :param kwargs: These contains the arguments
    :return: None
    """
    logger = get_logger(kwargs)
    logger.info('Triggering Foundry sync through Python...')
    # These are the magritte and branch name fetched from the args
    magritte_rid = kwargs['magritte_rid']
    branch = kwargs['branch']
    # Foundry Token
    foundry_token = kwargs['foundry_token']

    # Creation of the api trigger url
    url = "https://luster.palantircloud.com/magritte-coordinator/api/build/trigger/{}?branchName={}&".format(
        magritte_rid,
        branch)
    # Headers for post request
    headers = {'Content-Type': 'application/json', 'Authorization': 'Bearer ' + foundry_token,
               'Cookie': "PALANTIR_TOKEN=" + foundry_token}

    # Creating a post request for triggering the build in foundry
    response = requests.post(url=url, headers=headers)

    # Checking if the api trigger is successful using response code
    logger.info("Response status code is: {}".format(str(response.status_code)))
    if response.ok:
        logger.info('Foundry sync triggered successfully...')
    else:
    # Exiting the code if the trigger is not successful
        raise Exception("Foundry sync triggered failed")

def check_foundry_sync_status(**kwargs):
    """
    This is the method used for checking the status of the foundry sync job
    :param kwargs: These are the input params
    :return:
    """
    logger = get_logger(kwargs)
    logger.info('Checking Foundry sync job status through Python...')
    # These are the magritte,foundry token  and branch name fetched from the args
    dataset_rid = kwargs['dataset_rid']
    branch = kwargs['branch']
    foundry_token = kwargs['foundry_token']
    logger.info("Input dataset RID : %s",dataset_rid)
    logger.info("Input dataset RID branch  : %s", branch)
    # History Job URL
    history_url = "https://luster.palantircloud.com/monocle/api/transaction/history"
    # Proxy Dictionary
    proxyDict = {'gateway2opc.ual.com': '80'}
    # Headers for post request
    headers = {'Content-Type': 'application/json', 'Authorization': 'Bearer ' + foundry_token,
               'Cookie': "PALANTIR_TOKEN=" + foundry_token}
    # Creating the JSON Object for post request
    request = build_foundry_config(dataset_rid, branch)
    # Creating a post request and fetching the response
    response_check = get_foundry_sync_response(history_url=history_url, data=request, headers=headers, proxyDict=proxyDict)
    response_check = response_check.json()
    logger.info("Foundry sync api response  is {}".format(response_check))
    # Fetching the last execution status from foundry
    try:
        last_execution_status = str(response_check["history"][0]["dataChange"]["securedTransaction"]["transaction"]["status"])
    except:
        last_execution_status = "WAITING FOR RESOURCES"
    logger.info("last_execution_status is {}".format(last_execution_status))
    # Exiting the lopp if the last execution status is success
    if (last_execution_status in ['COMMITTED', 'SUCCESS']):
        logger.info("last_execution_status is {}".format(last_execution_status))
        return True
    elif (last_execution_status in ['ABORTED']):
        logger.error(" Sync execution Aborted ,Please execute sync manually  for this RID : {}".format(dataset_rid))
        # Sync Job Aborted  so exit from the loop with error
        raise Exception(" Sync execution Aborted ,Please execute sync manually  for this RID : {}".format(dataset_rid))
    # Continue running if the last execution status is running
    elif (last_execution_status in ['RUNNING', 'WAITING FOR RESOURCES', 'OPEN']):
        logger.info("JOB IS RUNNING...")
        return False
    else:
        # Exiting the Job if job fails
        # Sync Job Failed so exit from the loop with error
        logger.error(" Sync execution Failed ,Please execute manually for this RID : {}".format(dataset_rid))
        raise Exception(" Sync execution Aborted ,Please execute manually for this RID : {}".format(dataset_rid))
# Change-21 END

				
# Change 24 Start
def get_objects_in_bucket_count(context, bucket_name, bucket_path):
    """
    This is the method used to get the count of objects in any Specific Bucket Folder
    :param : logger, bucket_name, bucket_path
    :return : int (count of objects)
    """
    logger = get_logger(context)
    my_bucket = s3_resource.Bucket(bucket_name)
    object_list = []

    for object_summary in my_bucket.objects.filter(Prefix=bucket_path):
        if object_summary.key[-1] != "/":
            logger.debug("Object_name -> " + str(object_summary.key))
            object_list.append(object_summary.key)
    l1 = len(object_list)
    logger.info("Total number of objects in bucket folder = " + str(l1))
    return l1


def fail_on_specific_object_count(**kwargs):
    """
    This is the method used for checking the number of files in a specific bucket folder
    :param kwargs: bucket_name, bucket_path
    :return: None
    """
    logger = get_logger(kwargs)
    bucket_name = kwargs['bucket_name']
    bucket_path = kwargs['bucket_path']
    fail_on_num_of_objects = kwargs["fail_on_num_of_objects"] if "fail_on_num_of_objects" in kwargs else 1
    fail_on_num_of_objects = int(fail_on_num_of_objects)

    object_count = get_objects_in_bucket_count(kwargs, bucket_name, bucket_path)
    if object_count > fail_on_num_of_objects:
        logger.info("### Object count in bucket folder is > {}###").format(fail_on_num_of_objects)
        raise ValueError("Failing the process as object count in bucket folder is > {}").format(fail_on_num_of_objects)

# Change 24 End

# Change-28 Start
def get_secret_values(secret, **kwargs):
    """
    This method is used to retrieve secret values from secret manager
    param secret: Name of the secret
    return: Secret object in the form of json
    """
    logger = get_logger(kwargs)
    try:
        session = boto3.client("secretsmanager", region_name=region)
        get_secret_value_response = session.get_secret_value(
            SecretId= secret
        )
        #handling secret key retrievel in case it is binary decoded
        if 'SecretString' in get_secret_value_response:
            secret = get_secret_value_response['SecretString']
        else:
            secret = get_secret_value_response['SecretBinary']
        secret_values = json.loads(secret)
    except Exception as er:
        logger.error("Error occurred while fetching secrets from secret manager " +
                     str(er.__str__()))
        traceback_str = ''.join(traceback.format_tb(er.__traceback__))
        logger.error(traceback_str)
        raise Exception("Exception occurred while Fetching secret from the Secret Manager : " +
                        traceback_str)
    logger.info("Value retrieved successfully from secret...")
    return secret_values


def trigger_foundry_dataset_build(**kwargs):
    """
    This method is used to build the foundry dataset. It takes the argument from Airflow
    :param kwargs: dataset_rid, branch, foundry_token_secret
    :return None
    """
    logger = get_logger(kwargs)
    logger.info('Triggering foundry dataset build through python...')

    dataset_rid = kwargs['dataset_rid']
    branch = kwargs['branch']
    foundry_token_secret = kwargs['foundry_token_secret']
    
    # retrieving foundry token from secret
    foundry_token = get_secret_values(foundry_token_secret).get('foundry_token')
    logger.info("dataset_rid={}, branch={}, foundry_token_secret={}".format(dataset_rid,branch,foundry_token_secret))

    # Header of POST URL
    headers = {
        'Content-Type': 'application/json',
        'Authorization': 'Bearer ' + foundry_token,
        'Cookie': "PALANTIR_TOKEN=" + foundry_token
    }

    # Body of POST URL
    payload = {
        "branchFallbacks": {
            "branches": [
                "master"
            ]
        },
        "buildParameters": {},
        "datasetRid": dataset_rid,
        "datasetRidsToIgnore": [],
        "dryRun": False,
        "forceBuild": True,
        "ignoreAllDependencies": True,
        "ignoreBuildPolicy": False
    }

    # API to trigger dataset build
    datset_build_trigger_url = 'https://luster.palantircloud.com/build2/api/manager/runDatasetBuild/branches/{}'.format(branch)

    response = requests.post(url=datset_build_trigger_url, data=json.dumps(payload), headers=headers)

    # checking if api is trigger successfully using response code
    logger.info("Response status code is: {}".format(str(response.status_code)))

    if response.ok:
        logger.info('Foundry dataset build trigger successfully...')

    else:
        # exiting the code if trigger is not successful
        raise Exception('Foundry dataset build triggered failed')
# change-28 End

# Change 30 Start
def task_decider_for_ICON_process(**kwargs):
    """
    :description : method to check if historical data and source data same. Do not use this method for big json.
    :param context: dag_run
    :return:next_task_if_count_match, next_task_if_count_no_match
    """
    logger = get_logger(kwargs)
    bucket_name=kwargs['BUCKET_NAME']
    file_name=kwargs['COUNT_STATUS_FILE_BASE_PATH']
    count_details=s3.get_object(Bucket=bucket_name, Key=file_name)
    file_content = count_details['Body'].read().decode('utf-8')
    file_content = json.loads(file_content)
    print('reader value is:', file_content['count'])
    try:
        if int(file_content['count'])>=1:
            logger.info("New Data avaiable"),
            return kwargs['NEXT_TASK_IF_HAS_NEW_DATA']
        else:
            return kwargs['NEXT_TASK_IF_NO_NEW_DATA']
    except Exception as err:
        logger.error(f"FAILED in task_decider : {str(err)}")
        raise "FAILED - task_decider : " + str(err)
# Change 30 End

# Change 31 Start

def check_file_time_change(last_mod_time, file_path_final, connection_id,poke_intervals,retries,count_var=int(0)):
    # Input args : last_mod_time, file_path_final, connection_id , count_var , poke_intervals , retries
    # Output Args : NA
    # Description : This function is used to check if file mod time is not changing in next poke time defualt 5 minutes .
    retries_count = retries
    poking_interval = poke_intervals
    logger = get_logger(None)
    time.sleep(poking_interval)
    count = count_var + 1
    path = file_path_final
    conn_id = connection_id
    hook = SFTPHook(ftp_conn_id=str(conn_id))
    latest_mod_time = hook.get_mod_time(path)
    latest_mod_time = str(convert_to_utc(datetime.strptime(latest_mod_time, "%Y%m%d%H%M%S")))
    hook.close_conn()
    if latest_mod_time == last_mod_time:
        logger.info("file has arrived & load ready to be executed ")  
    elif count >= retries_count:
         raise Exception(
                    "mod time check attemps completed. Failed to get check mod time match"
                )        
    else:
        logger.info("file mod time changing. will check again in next 5 minutes")
        check_file_time_change(latest_mod_time, path, conn_id,poking_interval,retries_count,count)
        
# Change 31 End

#Change 33 start
def monthly_30th_scheduling(**kwargs):
    """ 
    The function purpose is to schedule a Dag every 30th month, and for february month, it should be schd on march 1st
    :param : feed_date,next_task_if_dag_scheduled,skip_complete_dag
    :return : next_task_if_dag_scheduled,skip_complete_dag
    """
    try:
        feed_date=kwargs['FEED_DATE']
        current_date =datetime.strptime(feed_date,"%Y-%m-%d")
        current_month=current_date.month
        current_month_name=current_date.strftime('%B')
        previous_month=current_date - relativedelta(months=1)
        previous_month_name=previous_month.strftime('%B')
        current_day=current_date.day
        if current_month_name != 'February' and current_day == 30:
            print("....................." + current_month_name + " month run started..................")
            return kwargs['next_task_if_dag_scheduled']
        elif   previous_month_name == 'February' and current_day == 1:
            print("....................." + previous_month_name + " month run started..................")
            return kwargs['next_task_if_dag_scheduled']    
        elif current_month_name != 'March' and current_day == 1:
            return kwargs['skip_complete_dag']
        else:
            raise ".....................invalid condition .................." 
    except Exception as err:
        raise "Failed to get feed date" + str(err)				
# Change 33 End

# Change 37 Start

def send_load_completion_email(**kwargs):
    """
    Mandatory parameters:
     environment      - The environment where load executes (Dev, Stg, Prod)
     domain_name      - Domain name for the load (cargo, loyalty etc.)
     load_name        - Load name
    Optional parameters:
     replace_params   - Configurable parameters as dictionary key/value pair string passed from load
                        This can be used to replace the matching parameters from subject/body dynamically from load.
     display_date     - Date to be displayed in email. Default as current CDT date in format %A, %B %d, %Y
     date_format      - Format of the date to be displayed in email. Default format %A, %B %d, %Y
    """
    logger = get_logger(None)
    total_attachment_size = 0  # Initialize counter
    max_attachment_size = 20  # In MB
    msg = MIMEMultipart()
    environment = kwargs['environment'].upper()
    domain_name = kwargs['domain_name']
    load_name = kwargs['load_name']
    recipient_type = 'TO'  # Defaulted the value to 'TO' for now

    if 'environment' not in kwargs or 'domain_name' not in kwargs or 'load_name' not in kwargs:
        raise Exception("Mandatory parameters - environment, domain_name, load_name are missing in arguments.")

    if 'replace_params' in kwargs:
        replace_params = kwargs['replace_params']
        if type(replace_params) is not dict:
            replace_params = replace_params.replace("'", "\"")
            replace_params = json.loads(replace_params)  # Convert String parameter to dictionary
    else:
        replace_params = {}

    if 'date_format' in kwargs:
        date_format = kwargs['date_format']
    else:
        date_format = "%A, %B %d, %Y"

    if 'display_date' in kwargs:
        display_date = datetime.strptime(kwargs['display_date'], '%Y-%m-%d')
        display_date = get_current_formatted_date_time(display_date, date_format)
    else:
        display_date = get_current_formatted_date_time(get_current_cdt_datetime(), date_format)

    logger.info("environment : %s", str(environment))
    logger.info("domain_name : %s", str(domain_name))
    logger.info("load_name : %s", str(load_name))
    logger.info("display_date : %s", str(display_date))
    logger.info("date_format : %s", str(date_format))

    logger.info("replace_params : %s", str(replace_params))

    try:
        # Get email recipients from airflow.send_email_distribution_list
        query = """SELECT recipient_email FROM airflow.send_email_distribution_list where domainname = %s and loadname = %s and recipient_type = %s"""
        source_hook = PostgresHook(postgres_conn_id='airflow_postgres_conn')
        source_conn = source_hook.get_conn()
        source_cursor = source_conn.cursor()
        source_cursor.execute(query, (domain_name, load_name, recipient_type))
        records = source_cursor.fetchall()
        recipients_to = ','.join(str(record[0]) for record in records)
        logger.info("recipients_to : %s", str(recipients_to))

        # Get email subject, sender email, email body from airflow.send_email_reference
        query = """SELECT subject, sender, body FROM airflow.send_email_reference where domainname = %s and loadname = %s"""
        source_cursor.execute(query, (domain_name, load_name))
        records = source_cursor.fetchone()
        subject = records[0]
        sender = records[1]
        BODY_HTML = records[2]
        subject = subject.replace('%DISPLAY_DATE%', display_date)
        BODY_HTML = BODY_HTML.replace('%DISPLAY_DATE%', display_date)
        logger.info("subject : %s", subject)
        logger.info("sender : %s", sender)
        logger.info("BODY_HTML : %s", BODY_HTML)

        # Change - 38 Start
        for key, value in replace_params.items():
            subject = subject.replace(key, value)
            BODY_HTML = BODY_HTML.replace(key, value)

        # To Differentiate the environments where the email sent from
        if environment not in ('PRD', 'PROD'):
            subject = subject + " ("+environment+")"

        logger.info("Modified subject : %s", subject)
        logger.info("Modified BODY_HTML : %s", BODY_HTML)


        # Change - 38 End

        mail_body = MIMEText(BODY_HTML, 'html')
        msg.attach(mail_body)
        # Change - 38 Start
        # Get email attachment from airflow.send_email_attachment
        query = """SELECT s3_bucket, s3_key, file_name FROM airflow.send_email_attachment where domainname = %s and loadname = %s"""
        source_cursor.execute(query, (domain_name, load_name))
        records = source_cursor.fetchall()
        for record in records:
            if record is not None:
                s3_bucket = record[0]
                s3_key = record[1]
                file_name = record[2]
                logger.info("s3_bucket : %s", s3_bucket)
                logger.info("s3_key : %s", s3_key)
                logger.info("file_name : %s", file_name)

                for key, value in replace_params.items():
                    s3_bucket = s3_bucket.replace(key, value)
                    s3_key = s3_key.replace(key, value)
                    file_name = file_name.replace(key, value)

                logger.info("Modified s3_bucket : %s", s3_bucket)
                logger.info("Modified s3_key : %s", s3_key)
                logger.info("Modified file_name : %s", file_name)

                bucket_name = s3_bucket
                output_key = s3_key
                filename = file_name
                s3_object = s3_resource.Object(bucket_name=str(bucket_name), key=str(output_key))
                object_size = s3_object.content_length / (1024 * 1024)  # Conversion from Byte to MB
                print('object_size : ' + str(object_size))
                total_attachment_size = total_attachment_size + object_size
                if total_attachment_size > max_attachment_size:
                    logger.warn("Discarding the current email attachment !!. "
                                "Current email attachment size(" + str(object_size) + ")/" +
                                "Total email attachment size(" + str(total_attachment_size) + ") is " +
                                "more than the allowed limit - " + str(max_attachment_size))
                    logger.warn("Please reach out the recipients from airflow.send_email_distribution_list wrt load")
                    raise Exception("Email attachment size is more than the allowed limit")
                else:
                    s3_object = s3.get_object(Bucket=str(bucket_name), Key=str(output_key))
                    body = s3_object['Body'].read()
                    part = MIMEApplication(body, filename)
                    part.add_header("Content-Disposition", 'attachment', filename=filename)
                    msg.attach(part)
            else:
                logger.info("Proceeding with No email attachment")
    finally:
        # Close the DB connection
        source_cursor.close()
        source_conn.close()
    # Change - 38 End

    # Sending email
    msg['Subject'] = subject
    msg['From'] = sender
    msg['To'] = recipients_to
    try:
        smtpObj = smtplib.SMTP('mailout.ual.com', 25)
        smtpObj.starttls()
        smtpObj.sendmail(sender, recipients_to.split(','), msg.as_string())
        logger.info("Successfully sent email")
    except Exception as err:
        logger.info(err)
        logger.info("Error: Unable to send email")
        raise Exception("Error: Unable to send email")
    finally:
        smtpObj.quit()

# Change 37 End

# Change 39 Start
def s3_to_esftp(**kwargs):
    """ s3_to_esftp : multiple file transfer """
    logger = get_logger(kwargs)
    logger.info(" s3_to_esftp task started ")
    s3_bucket_name = kwargs["s3_bucket_name"]
    source_base_path = kwargs["source_base_path"]
    esftp_connection_name = kwargs["esftp_connection_name"]
    esftp_path = kwargs["esftp_path"]
    file_pattern = kwargs['file_pattern']
    logger.info("s3_bucket_name : %s", s3_bucket_name)
    logger.info("source_base_path : %s", source_base_path)
    logger.info("esftp_connection_name : %s", esftp_connection_name)
    logger.info("esftp_path : %s", esftp_path)
    logger.info("file_pattern : %s", file_pattern)
    s3_bucket = s3_resource.Bucket(s3_bucket_name)
    for object_summary in s3_bucket.objects.filter(Prefix=source_base_path):
        logger.info("Start Copying to esftp.... Inside For Loop.... " + str(object_summary.key))
        if str(object_summary.key) != source_base_path and re.search(file_pattern, str(object_summary.key)):
            logger.info("Copying file -  " + object_summary.key)
            destination_file_name = object_summary.key.split(source_base_path)[1]
            esftp_path_with_file_name = os.path.join(esftp_path,destination_file_name)
            transfer_file: BaseOperator = S3ToSFTPOperator(
                task_id="create_s3_to_sftp_job",
                sftp_conn_id=esftp_connection_name,
                sftp_path=esftp_path_with_file_name,
                s3_bucket=s3_bucket_name,
                s3_key=object_summary.key
            )
            transfer_file.execute(kwargs)
            logger.info(" %s : files transfered to esftp ", destination_file_name)
    logger.info("All files transfered to esftp")
# Change 39 End
