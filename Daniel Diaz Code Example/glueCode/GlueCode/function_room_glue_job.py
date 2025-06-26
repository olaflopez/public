import sys

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import types, SparkSession

import time
import os
from datetime import date,datetime

from function_room_transformations import (
    read_csv_to_df, cast_and_select_headers_df, refine_parts_df, refine_setup_df, format_parts_df, replace_null_string_with_none, format_setup_df,
    format_ooo_df, aggregate_parts, aggregate_setup, drop_datetime_columns, consolidate_dataframes, format_dataframe_for_output,
    format_for_database_insertion, store_file_in_s3, get_job_parameters,remove_files
    )

spark = SparkSession.builder.getOrCreate()

sc = spark.sparkContext
glueContext = GlueContext(sc)
logger = glueContext.get_logger()

args = getResolvedOptions(sys.argv, ["JOB_NAME", "header", "parts", "setup", "uuid", "sync_type", "environment", "bucket_name", "property_code"])

job_args = get_job_parameters(args, ["uuid", "sync_type"])
logger.info(f"args for jobs: {job_args}")

# # for debug only
# args = {'job_bookmark_option': 'job-bookmark-disable', 'job_bookmark_from': None, 'job_bookmark_to': None, 'JOB_ID': 'j_bfe6a2278c4a00a121f69eade273629d7f64e40e2acb54d6cb23a13669abae60', 'JOB_RUN_ID': 'jr_6e4319996ad95a647c7e6b1220e54bb9ed54e62f53a28e51b9316031ee53ca78', 'SECURITY_CONFIGURATION': None, 'encryption_type': 'sse-s3', 'enable_data_lineage': None, 'RedshiftTempDir': 's3://aws-glue-assets-885531618288-us-east-1/temporary/', 'TempDir': 's3://aws-glue-assets-885531618288-us-east-1/temporary/', 'JOB_NAME': 'function_room_jn', 'header': 's3://envision-integration-us-east-2-992174126200-dev-inbound/eii/csv_files/F_BEAVE_FUNCTION_ROOM_2023-05-23 15:43.csv', 'parts': 's3://envision-integration-us-east-2-992174126200-dev-inbound/eii/csv_files/F_BEAVE_FUNCTION_ROOM_PARTS_2023-05-23 15:43.csv', 'setup': 's3://envision-integration-us-east-2-992174126200-dev-inbound/eii/csv_files/F_BEAVE_FUNCTION_ROOM_SETUP_AND_TYPE_2023-05-23 15:43.csv'}
# job.init(args['JOB_NAME'], args)

headers_DF = read_csv_to_df(spark, args["header"])
parts_DF = read_csv_to_df(spark, args["parts"])
setup_DF = read_csv_to_df(spark, args["setup"])


start_transform_time = time.time()


required_parts_DF = refine_parts_df(parts_DF)
required_setup_DF = refine_setup_df(setup_DF)


required_headers_DF = replace_null_string_with_none(headers_DF)
headers_DF_no_nulls = cast_and_select_headers_df(required_headers_DF)
required_setup_DF_no_nulls = replace_null_string_with_none(required_setup_DF)
required_parts_DF_no_nulls = replace_null_string_with_none(required_parts_DF)


required_parts_formatted_DF = format_parts_df(required_parts_DF_no_nulls)
required_setup_formatted_DF = format_setup_df(required_setup_DF_no_nulls)


required_ooo_primary_formatted =  format_ooo_df(headers_DF_no_nulls)


parts_join = aggregate_parts(required_parts_formatted_DF)
setup_join = aggregate_setup(required_setup_formatted_DF)


headers_join = drop_datetime_columns(headers_DF_no_nulls)


consolidated = consolidate_dataframes(headers_DF_no_nulls, parts_join, setup_join, required_ooo_primary_formatted)

final = format_dataframe_for_output(consolidated, job_args)


final_to_db = format_for_database_insertion(final)

end_transform_time = time.time()
# property_code = args['header'].split(os.sep)[-1].split('_')[1]

logger.info(f"PROPERTIES: {args['property_code']}.")
logger.info(f"args: {args}")

timestamp = args['header'].split(os.sep)[-2].split('_')[0]
# s3_path = f"s3://{args['bucket_name']}/output/function_{args['property_code']}"
filename = f"function_{args['uuid'].split(',')[0]}_{datetime.now().strftime('%h-%d-%y')}"
s3_path = f"s3://{args['bucket_name']}/output/{filename}"


logger.info("s3_path: " + s3_path)
store_file_in_s3(final_to_db, s3_path)

remove_files(args)

