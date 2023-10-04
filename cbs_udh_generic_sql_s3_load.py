"""
Utility to fetch data from SQL to S3
Supported Databases:
1. MySql
2. Postgres
3. Oracle
4. Teradata
5. MSSql
6. DB2
"""
import sys
from datetime import datetime, date
from io import StringIO

import boto3
import pandas as pd
import pg8000
import traceback
import json
from awsglue.context import GlueContext
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as f

from src.log.CustomLoggerAdapter import CustomLoggerAdapter
from icon_data_sync_and_meta_file_creation import *

def load_data_into_s3(output_location_):
    # Variables
    if (DATABASE_TYPE.lower()) == 'mysql':
        driver = "com.mysql.cj.jdbc.Driver"
    elif (DATABASE_TYPE.lower()) == 'mssql':
        driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    elif (DATABASE_TYPE.lower()) == 'oracle':
        driver = "oracle.jdbc.OracleDriver"
    elif (DATABASE_TYPE.lower()) == 'teradata':
        driver = "com.teradata.jdbc.TeraDriver"
    elif (DATABASE_TYPE.lower()) == 'postgres':
        driver = "org.postgresql.Driver"
    elif (DATABASE_TYPE.lower()) == 'db2':
        driver = "com.ibm.db2.jcc.DB2Driver"
    else:
        logger.error("Incorrect database_type passed to the job" + DATABASE_TYPE)
        sys.exit(0)

    # Getting Database Credentials from Secrets Manager
    try:
        client = boto3.client("secretsmanager", region_name="us-east-1")
        get_secret_value_response = client.get_secret_value(
            SecretId=SECRET_MANAGER
        )
        secret = get_secret_value_response['SecretString']
        secret = json.loads(secret)
        user = secret.get('db_username')
        password = secret.get('db_password')
        port = secret.get('db_port')
        host = secret.get('db_host')
        dbname = secret.get('db_name')
        jdbc_url = secret.get('db_jdbc_url')

    except Exception as er:
        logger.error("Error occurred while fetching secrets from secret manager " +
                     str(er.__str__()))
        traceback_str = ''.join(traceback.format_tb(er.__traceback__))
        logger.error(traceback_str)
        raise Exception("Exception occurred while secret Fetching from the Secret Manager : " +
                        traceback_str)

    logger.info(f"jdbc url : {str(jdbc_url)}")
    logger.debug(f"user : {user}")

    logger.info(str(history_load))
    if history_load:
        logger.info("History Load")
        if args_complete_query:
            logger.info("Complete Query Provided")
            query = args_complete_query
        else:
            logger.info("Provided DatabaseName : " + str(database_name) +
                        ", TableName : " + str(table_name))
            query = 'Select * from ' + database_name + '.' + table_name
    else:
        if args_filter_condition:
            logger.info("Incremental Load")
            query = 'Select * from ' + database_name + '.' + table_name + \
                    ' where ' + args_filter_condition
        elif args_complete_query:
            query = args_complete_query
        else:
            raise Exception('Provide Filter Condition if it is not a complete table load ')

    logger.info("Query : " + query)

    try:
        if ssl_flag.upper() == 'TRUE' and (DATABASE_TYPE.lower()) == 'db2':
            try:
                ssl_password = secret.get('ssl_password')
            except Exception as er:
                logger.error("ssl_password key is not available in secret manager " +
                     str(er.__str__()))
                traceback_str = ''.join(traceback.format_tb(er.__traceback__))
                logger.error(traceback_str)
                raise Exception("Exception occurred while secret Fetching from the Secret Manager : " +
                        traceback_str)
            connection_properties = {"sslConnection": "true", "sslTrustStoreLocation": "/tmp/"+cert_filename,
                                     "sslTrustStorePassword": ssl_password}
            sql_s3_df = sparkSession.read.format("jdbc") \
                .option("driver", driver) \
                .option("url", jdbc_url) \
                .option("user", user) \
                .option("query", query) \
                .option("password", password) \
                .option("pushDownPredicate", True) \
                .options(**connection_properties) \
                .load()
        else:
            sql_s3_df = sparkSession.read.format("jdbc") \
                .option("driver", driver) \
                .option("url", jdbc_url) \
                .option("user", user) \
                .option("query", query) \
                .option("password", password) \
                .option("pushDownPredicate", True) \
                .load()
    except Exception as er:
        logger.error("Error occurred while to connecting though jdbc url : " +
                     str(jdbc_url) + " : " + str(er.__str__()))
        traceback_str = ''.join(traceback.format_tb(er.__traceback__))
        logger.error(traceback_str)
        raise Exception("Exception occurred while getting connection calling stacktrace : " +
                        traceback_str)

    # Append output location with current date
    if append_current_date != 'N':
        logger.info("Appending Current Date")
        current_date = date.today().strftime("%Y-%m-%d")
        output_location_ = output_location_ + "/" + str(current_date)
    if persist_df == 'Y':
        sql_s3_df.persist()
        logger.info("Inside persist df")
    record_count = sql_s3_df.count()
    logger.info("Output Location is .." + str(output_location_))

    if max_records_per_file:
        sql_s3_df.write.mode(save_mode).option("maxRecordsPerFile", int(max_records_per_file)) \
            .format(file_format).save(output_location_)
    else:
        sql_s3_df.write.mode(save_mode).format(file_format).save(output_location_)

    # Change - 1 start
    # fetch max timestamp from raw df and write to csv if there are incremental records present
    if is_incremental and is_incremental.upper() == 'Y':
        if incremental_column_name and s3_incremental_col_value_loc:
            # checking if there are any incremental files present or not
            # Change - 2 start
            if sql_s3_df is not None and sql_s3_df.head(1):
                logger.info("Incremental records found for this run, initiating method get_max_incremental_value")
                max_incr_df = get_max_incremental_value(sql_s3_df, incremental_column_name,incremental_column_datatype, incremental_column_alias)

                if max_incr_df is not None and max_incr_df.head(1):
                    max_incr_df.repartition(1).write.mode(incremental_write_mode).format(file_write_format).option("header","true").save(s3_incremental_col_value_loc)
            else:
                logger.error("No incremental records found for this run")
        # Change - 2 end
        else:
            raise Exception('Provide incremental_column_name, s3_incremental_col_value_loc if is_incremental is Y')
    return record_count, output_location_
    # Change - 1 end


def get_max_incremental_value(df, incremental_column_name, incremental_column_datatype, incremental_column_alias):
    """
    Description: Method to retrieve max incremental column value
    :param incremental_column_name: incremental_column_name
    :return: None
    """
    try:
        max_incr_df = df.select(
            f.max(incremental_column_name).cast(incremental_column_datatype).alias(incremental_column_alias))
    except Exception as er:
        logger.error("Error occurred while trying to retrieve max incremenal column value : " +
                     str(er.__str__()))
        traceback_str = ''.join(traceback.format_tb(er.__traceback__))
        logger.error(traceback_str)
        raise Exception("Exception occurred while retrieving max incremenal column value calling stacktrace : " +
                        traceback_str)

    return max_incr_df


if __name__ == '__main__':
    # Spark Context
    sc = SparkContext()
    glueContext = GlueContext(sc)
    sparkSession = glueContext.spark_session
    # sparkSession.conf.set("spark.sql.session.timeZone", "UTC")

    # Spark Resources
    s3_resource = boto3.resource('s3')
    s3_client = boto3.client('s3')

    # Get values from mandatory JOB arguments
    args = getResolvedOptions(sys.argv,
                              ['TABLE_NAME', 'DATABASE_NAME', 'OUTPUT_LOCATION', 'SECRET_MANAGER', 'DATABASE_TYPE', 'STANDARD_ICON_RDS_SECRET_MANAGER'])
    table_name = args['TABLE_NAME']
    database_name = args['DATABASE_NAME']
    output_location = args['OUTPUT_LOCATION']
    SECRET_MANAGER = args['SECRET_MANAGER']
    DATABASE_TYPE = args['DATABASE_TYPE']
    STANDARD_ICON_RDS_SECRET_MANAGER = args['STANDARD_ICON_RDS_SECRET_MANAGER']

    # Get values from optional JOB arguments
    if ('--{}'.format('MAX_RECORDS_PER_FILE') in sys.argv):
        args = getResolvedOptions(sys.argv, ['MAX_RECORDS_PER_FILE'])
    else:
        args = {'MAX_RECORDS_PER_FILE': "500000"}
    max_records_per_file = args['MAX_RECORDS_PER_FILE']

    if ('--{}'.format('SAVE_MODE') in sys.argv):
        args = getResolvedOptions(sys.argv, ['SAVE_MODE'])
    else:
        args = {'SAVE_MODE': 'append'}
    save_mode = args['SAVE_MODE']

    if ('--{}'.format('FILE_FORMAT') in sys.argv):
        args = getResolvedOptions(sys.argv, ['FILE_FORMAT'])
    else:
        args = {'FILE_FORMAT': 'parquet'}
    file_format = args['FILE_FORMAT']

    if ('--{}'.format('DATE_FILTER_COLUMN_NAME') in sys.argv):
        args = getResolvedOptions(sys.argv, ['DATE_FILTER_COLUMN_NAME'])
    else:
        args = {'DATE_FILTER_COLUMN_NAME': None}
    filter_column_name = args['DATE_FILTER_COLUMN_NAME']

    if ('--{}'.format('FILTER_STARTING_VALUE') in sys.argv):
        args = getResolvedOptions(sys.argv, ['FILTER_STARTING_VALUE'])
    else:
        args = {'FILTER_STARTING_VALUE': None}
    filter_value = args['FILTER_STARTING_VALUE']

    if ('--{}'.format('APPEND_CURRENT_DATE') in sys.argv):
        args = getResolvedOptions(sys.argv, ['APPEND_CURRENT_DATE'])
    else:
        args = {'APPEND_CURRENT_DATE': 'N'}
    append_current_date = args['APPEND_CURRENT_DATE']

    if ('--{}'.format('HISTORY_LOAD') in sys.argv):
        args = getResolvedOptions(sys.argv, ['HISTORY_LOAD'])
    else:
        args = {'HISTORY_LOAD': None}
    history_load = args['HISTORY_LOAD']

    if ('--{}'.format('COMPLETE_QUERY') in sys.argv):
        args = getResolvedOptions(sys.argv, ['COMPLETE_QUERY'])
    else:
        args = {'COMPLETE_QUERY': None}
    args_complete_query = args['COMPLETE_QUERY']

    if ('--{}'.format('POPULATE_META_FILE') in sys.argv):
        args = getResolvedOptions(sys.argv, ['POPULATE_META_FILE'])
    else:
        args = {'POPULATE_META_FILE': False}
    populate_meta_file = args['POPULATE_META_FILE']

    if ('--{}'.format('ICON_RDS_SECRET_MANAGER') in sys.argv):
        args = getResolvedOptions(sys.argv, ['ICON_RDS_SECRET_MANAGER'])
    else:
        args = {'ICON_RDS_SECRET_MANAGER': 'cbs-udh-datalake-secret-mgr-icon-rds'}
    icon_rds_secret_manager = args['ICON_RDS_SECRET_MANAGER']

    if ('--{}'.format('ICON_RDS_SECRET_MANAGER_REGION') in sys.argv):
        args = getResolvedOptions(sys.argv, ['ICON_RDS_SECRET_MANAGER_REGION'])
    else:
        args = {'ICON_RDS_SECRET_MANAGER_REGION': 'us-east-1'}
    icon_rds_secret_manager_region = args['ICON_RDS_SECRET_MANAGER_REGION']

    if ('--{}'.format('ICON_AUDIT_FEED_ID') in sys.argv):
        args = getResolvedOptions(sys.argv, ['ICON_AUDIT_FEED_ID'])
    else:
        args = {'ICON_AUDIT_FEED_ID': None}
    audit_feed_id = args['ICON_AUDIT_FEED_ID']

    if ('--{}'.format('ICON_AUDIT_FEED_VERSION') in sys.argv):
        args = getResolvedOptions(sys.argv, ['ICON_AUDIT_FEED_VERSION'])
    else:
        args = {'ICON_AUDIT_FEED_VERSION': '1'}
    audit_feed_version = args['ICON_AUDIT_FEED_VERSION']

    if ('--{}'.format('META_FILE_LOCATION') in sys.argv):
        args = getResolvedOptions(sys.argv, ['META_FILE_LOCATION'])
    else:
        args = {'META_FILE_LOCATION': None}
    meta_file_location = args['META_FILE_LOCATION']

    if ('--{}'.format('FILTER_CONDITION') in sys.argv):
        args = getResolvedOptions(sys.argv, ['FILTER_CONDITION'])
    else:
        args = {'FILTER_CONDITION': None}
    args_filter_condition = args['FILTER_CONDITION']

    if ('--{}'.format('LOG_LEVEL') in sys.argv):
        args = getResolvedOptions(sys.argv, ['LOG_LEVEL'])
    else:
        args = {'LOG_LEVEL': 'INFO'}
    log_level_str = args['LOG_LEVEL']

    if ('--{}'.format('PERSIST_DF') in sys.argv):
        args = getResolvedOptions(sys.argv, ['PERSIST_DF'])
    else:
        args = {'PERSIST_DF': 'Y'}
    persist_df = args['PERSIST_DF']

    if ('--{}'.format('IS_INCREMENTAL') in sys.argv):
        args = getResolvedOptions(sys.argv, ['IS_INCREMENTAL'])
    else:
        args = {'IS_INCREMENTAL': None}
    is_incremental = args['IS_INCREMENTAL']

    if ('--{}'.format('INCREMENTAL_COL_NAME') in sys.argv):
        args = getResolvedOptions(sys.argv, ['INCREMENTAL_COL_NAME'])
    else:
        args = {'INCREMENTAL_COL_NAME': None}
    incremental_column_name = args['INCREMENTAL_COL_NAME']

    if ('--{}'.format('INCREMENTAL_VALUE_OUTPUT_LOC') in sys.argv):
        args = getResolvedOptions(sys.argv, ['INCREMENTAL_VALUE_OUTPUT_LOC'])
    else:
        args = {'INCREMENTAL_VALUE_OUTPUT_LOC': None}
    s3_incremental_col_value_loc = args['INCREMENTAL_VALUE_OUTPUT_LOC']

    if ('--{}'.format('INCREMENTAL_FILE_WRITE_MODE') in sys.argv):
        args = getResolvedOptions(sys.argv, ['INCREMENTAL_FILE_WRITE_MODE'])
    else:
        args = {'INCREMENTAL_FILE_WRITE_MODE': 'overwrite'}
    incremental_write_mode = args['INCREMENTAL_FILE_WRITE_MODE']

    if ('--{}'.format('INCREMENTAL_FILE_WRITE_FORMAT') in sys.argv):
        args = getResolvedOptions(sys.argv, ['INCREMENTAL_FILE_WRITE_FORMAT'])
    else:
        args = {'INCREMENTAL_FILE_WRITE_FORMAT': 'csv'}
    file_write_format = args['INCREMENTAL_FILE_WRITE_FORMAT']

    if ('--{}'.format('INCREMENTAL_COLUMN_DATATYPE') in sys.argv):
        args = getResolvedOptions(sys.argv, ['INCREMENTAL_COLUMN_DATATYPE'])
    else:
        args = {'INCREMENTAL_COLUMN_DATATYPE': 'timestamp'}
    incremental_column_datatype = args['INCREMENTAL_COLUMN_DATATYPE']

    if ('--{}'.format('INCREMENTAL_COLUMN_ALIAS') in sys.argv):
        args = getResolvedOptions(sys.argv, ['INCREMENTAL_COLUMN_ALIAS'])
    else:
        args = {'INCREMENTAL_COLUMN_ALIAS': 'max_start_timestamp'}
    incremental_column_alias = args['INCREMENTAL_COLUMN_ALIAS']

    if ('--{}'.format('ICON_AUDIT_TABLE_NAME') in sys.argv):
        args = getResolvedOptions(sys.argv, ['ICON_AUDIT_TABLE_NAME'])
    else:
        args = {'ICON_AUDIT_TABLE_NAME': 'ingestion_job_history_status'}
    icon_audit_table_name = args['ICON_AUDIT_TABLE_NAME']

    if ('--{}'.format('ICON_AUDIT_DB_NAME') in sys.argv):
        args = getResolvedOptions(sys.argv, ['ICON_AUDIT_DB_NAME'])
    else:
        args = {'ICON_AUDIT_DB_NAME': 'icon'}
    icon_audit_db_name = args['ICON_AUDIT_DB_NAME']

    if ('--{}'.format('FILE_HAS_NO_EXTENSION') in sys.argv):
        args = getResolvedOptions(sys.argv, ['FILE_HAS_NO_EXTENSION'])
    else:
        args = {'FILE_HAS_NO_EXTENSION': 'n'}
    file_has_no_extension = args['FILE_HAS_NO_EXTENSION']

    if '--{}'.format('CORRELATION_ID') in sys.argv:
        args = getResolvedOptions(sys.argv, ['CORRELATION_ID'])
        dag_run_id = args['CORRELATION_ID']
    elif '--{}'.format('DAG_RUN_ID') in sys.argv:
        args = getResolvedOptions(sys.argv, ['DAG_RUN_ID'])
        dag_run_id = args['DAG_RUN_ID']
    else:
        dag_run_id = 'na'

    if ('--{}'.format('META_FILE_NAME') in sys.argv):
        args = getResolvedOptions(sys.argv, ['META_FILE_NAME'])
    else:
        args = {'META_FILE_NAME': f'{table_name}_{datetime.now().strftime("%Y%m%d")}'}
    meta_file_name = args['META_FILE_NAME']

    if ('--{}'.format('IS_HEADER') in sys.argv):
        args = getResolvedOptions(sys.argv, ['IS_HEADER'])
    else:
        args = {'IS_HEADER': 'N'}
    is_header = args['IS_HEADER']

    if ('--{}'.format('FIELD_DELIMITER') in sys.argv):
        args = getResolvedOptions(sys.argv, ['FIELD_DELIMITER'])
    else:
        args = {'FIELD_DELIMITER': ','}
    field_delimiter = args['FIELD_DELIMITER']

    if ('--{}'.format('SSL') in sys.argv):
        args = getResolvedOptions(sys.argv, ['SSL'])
    else:
        args = {'SSL': "FALSE"}
    ssl_flag = args['SSL']

    if ('--{}'.format('CERT_FILENAME') in sys.argv):
        args = getResolvedOptions(sys.argv, ['CERT_FILENAME'])
    else:
        args = {'CERT_FILENAME': None}
    cert_filename = args['CERT_FILENAME']

    # Logger Setup
    CustomLoggerAdapter.setup_logger('cbs_udh_generic_sql_s3_load', log_level_str.upper(),
                                     serviceName='Glue_Job', correlationId=audit_feed_id)
    logger = CustomLoggerAdapter()
    logger.info(sys.argv)

    # Validation of input arguments for Meta file generation
    if populate_meta_file:
        if not (icon_rds_secret_manager and audit_feed_id):
            raise Exception(
                'Provide ICON_RDS_SECRET_MANAGER and ICON_AUDIT_FEED_ID '
                'to create meta file')

    # Load data from TD to S3
    logger.info("Output location : " + str(output_location))

    record_count, data_file_location = load_data_into_s3(output_location)

    if populate_meta_file:
        # Generate Metadata file
        configure_icon_data_sync(file_location=data_file_location,
                                 rds_secrets_manager=STANDARD_ICON_RDS_SECRET_MANAGER,
                                 rds_secrets_manager_region=icon_rds_secret_manager_region,
                                 append_current_date=append_current_date, audit_table_name=icon_audit_table_name,
                                 audit_feed_id=audit_feed_id,
                                 audit_feed_version=audit_feed_version, audit_db_name=icon_audit_db_name, file_has_no_extension=file_has_no_extension,
                                 file_format=file_format, dag_run_id=dag_run_id, meta_file_location=meta_file_location,
                                 meta_file_name=meta_file_name, is_header=is_header, delimiter=field_delimiter, record_count=record_count)

        icon_meta_file_creation = IconMetaFileCreation()
    logger.info("Job Completed")