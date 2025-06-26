from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
import pyspark.sql.types as types
from typing import Dict
import pymysql
import boto3
import sys
import json

s3_client = boto3.client('s3')


def get_job_parameters(args, keys):
    new_args = {}
    property_codes = args["property_code"].split(',')

    for key in keys:
        values = args[key].split(',')
        for i, (value, property_code) in enumerate(zip(values, property_codes)):
            if i >= len(new_args):
                new_args[property_code] = {}
            new_args[property_code][key] = value
    return new_args

    # response = s3_client.get_object(Bucket=bucket_name, Key=s3_path)
    # json_data = json.loads(response['Body'].read().decode('utf-8'))
    # print(f"[get_job_parameters]\tdeleting {s3_path} file")
    # s3_client.delete_object(Bucket=bucket_name, Key=s3_path)
    # json_data = json_data['Function_Room']
    # json_data = [ {key.replace('-',''):value for key, value in json_item.items() } for json_item in json_data ]
    # print(f"[get_job_parameters]\tRunning: {json_data}")
    # return json_data

def read_csv_to_df(spark: SparkSession, file_path: str, delimiter: str = "|") -> DataFrame:
    """
    Reads a CSV file into a DataFrame using specified settings and applies custom transformations.

    Parameters:
    - spark: A SparkSession instance.
    - file_path: Path to the CSV file.
    - delimiter: The delimiter used in the CSV file (default is "|").

    Returns:
    - DataFrame: The loaded DataFrame.
    """

    # Read the CSV file
    dataframe = spark.read.format("csv") \
        .option("header", "true") \
        .option("escape", '"') \
        .option("sep", delimiter) \
        .option("inferSchema", "false") \
        .option("multiLine", "true") \
        .load(file_path)

    # Check if 'functionroomid' and 'propertycode' columns exist before applying special logic
    if "functionroomid" in dataframe.columns:
        dataframe = dataframe.withColumn(
            "functionroomid",
            F.when(
                (F.col("propertycode") == "CHAMP") & (F.col("functionroomid") == " SS8"),
                F.lit("XSS8")
            ).otherwise(F.col("functionroomid"))
        )
        
    if "abbreviation" in dataframe.columns:
        dataframe = dataframe.withColumn(
        "abbreviation",
        F.when(
            (F.col("propertycode") == "CHAMP") & (F.col("abbreviation") == " SS8"),
            F.lit("XSS8")
        ).otherwise(F.col("abbreviation"))
    )

    # Pattern to remove unwanted characters and new lines
    pattern = r'[{}\\\[\]|\;<>?/^](?<!\\\\)'

    # Columns that require special handling
    special_columns = {"functionroomid", "abbreviation"}

    for column in dataframe.columns:
        # Remove new lines from all columns
        dataframe = dataframe.withColumn(column, F.regexp_replace(F.col(column), '[\r\n]', ''))

        if column in special_columns and column in dataframe.columns:
            # Replace unwanted characters with '%'
            dataframe = dataframe.withColumn(column, F.regexp_replace(F.col(column), pattern, '%'))
        else:
            # Remove unwanted characters for all other columns
            dataframe = dataframe.withColumn(column, F.regexp_replace(F.col(column), pattern, ''))

        # Trim whitespace after all transformations
        dataframe = dataframe.withColumn(column, F.trim(F.col(column)))

    return dataframe


def cast_and_select_headers_df(df: DataFrame) -> DataFrame:
    """
    Transforms and refines the headers and values of a given DataFrame `df`.

    This function carries out the following transformations:
    1. Renames certain columns to camelCase notation.
    2. Trims any leading or trailing whitespaces.
    3. Casts specific columns to their respective datatypes such as Float, Int, and Boolean.
    4. Updates 'startdatetime' and 'enddatetime' columns to replace specific placeholder values.

    Parameters:
    ----------
    df : DataFrame
        The input DataFrame with original column headers and values.

    Returns:
    -------
    DataFrame
        The transformed DataFrame with refined headers and values.

    Note:
    -----
    It is assumed that the original DataFrame `df` contains all the necessary columns 
    mentioned in the transformation, and that the DataFrame uses functions from the PySpark 
    SQL library.

    Examples:
    --------
    >>> df = spark.read.csv("path_to_file.csv", header=True, inferSchema=True)
    >>> transformed_df = cast_headers_df(df)
    >>> transformed_df.show()

    """

    transformed_df = df.select(
        "propertycode",
        F.col("functionroomid").alias("functionRoomId"),
        "abbreviation",
        "description",
        F.coalesce(F.col("functionroomname"),F.col("functionroomid")).alias("functionRoomName"),
        # remove spaces and cast to float datatype
        F.coalesce(F.trim(F.col("areainsquarefeet")),F.lit(0)).alias("areaInSquareFeet").cast(types.FloatType()),
        F.coalesce(F.trim(F.col("lengthinfeet")),F.lit(0)).alias("lengthInFeet").cast(types.FloatType()),
        F.coalesce(F.trim(F.col("widthinfeet")),F.lit(0)).alias("widthInFeet").cast(types.FloatType()),
        F.trim(F.col("areainsquaremeters")).alias("arealnSquareMeters").cast(types.FloatType()),
        F.trim(F.col("lengthinmeters")).alias("lengthInMeters").cast(types.FloatType()),
        F.trim(F.col("widthinmeters")).alias("widthInMeters").cast(types.FloatType()),
        # remove spaces and cast to int datatype
        F.trim(F.col("minimumoccupancy")).alias("minimumOccupancy").cast(types.IntegerType()),
        F.trim(F.col("maxmumoccupancy")).alias("maximumOccupancy").cast(types.IntegerType()),
        F.trim(F.col("optimaloccupancy")).alias("optimalOccupancy").cast(types.IntegerType()),
        F.trim(F.col("status")).alias("status"),
        F.trim(F.col("statuscode")).alias("statusCode"),
        # remove spaces and cast to Boolean datatype
        F.trim(F.col("combinationFunctionRoom")).alias("combinationFunctionRoom").cast(types.BooleanType()),
        F.trim(F.col("shareable")).alias("shareable").cast(types.BooleanType()),
        "startdatetime",
        "enddatetime"
    )
    
    refined_df = transformed_df.select(
        *[
            "propertycode",
            "functionRoomId",
            "abbreviation",
            "description",
            "functionRoomName",
            "areaInSquareFeet",
            "lengthInFeet",
            "widthInFeet",
            "arealnSquareMeters",
            "lengthInMeters",
            "widthInMeters",
            "minimumOccupancy",
            "maximumOccupancy",
            "optimalOccupancy",
            "status",
            "statusCode",
            "combinationFunctionRoom",
            "shareable",
            F.when(F.trim(transformed_df["startdatetime"]).isin("",'(null)'), "1900-01-01T00:00:00").when(F.trim(transformed_df["startdatetime"]).isNull(),"1900-01-01T00:00:00").otherwise(transformed_df["startdatetime"]).alias("startdatetime"),
            F.when(F.trim(transformed_df["enddatetime"]).isin("",'(null)',"2099-12-31","2199-12-31"), "9999-12-31T23:59:59").when(F.trim(transformed_df["enddatetime"]).isNull(),"9999-12-31T23:59:59").otherwise(transformed_df["enddatetime"]).alias("enddatetime")
        ]
    )

    return refined_df


def refine_parts_df(df: DataFrame) -> DataFrame:
    """
    Transforms the parts DataFrame with specific column operations.

    Parameters:
    - df: The input DataFrame.

    Returns:
    - DataFrame: The transformed DataFrame.
    """
    
    refined_parts_df = df.select(
        F.col("functionroomid").alias("functionRoomId"),
        "basefunctionroomid",
        "basefunctionroomname"
    )
    
    return refined_parts_df


def refine_setup_df(df: DataFrame) -> DataFrame:
    """
    Transforms the setup DataFrame with specific column operations.

    Parameters:
    - df: The input DataFrame.

    Returns:
    - DataFrame: The transformed DataFrame.
    """
    
    refined_setup_df = df.select(
        F.col("functionroomid").alias("functionRoomId"),
        "functionroomsetuptypename",
        "functionroomdescription",
        "setuptypecapacity"
    )
    
    return refined_setup_df


def replace_null_string_with_none(df: DataFrame) -> DataFrame:
    """
    Replaces any occurrence of the string '(null)' in the DataFrame with a Python None.

    Parameters:
    - df: The input DataFrame.

    Returns:
    - DataFrame: The DataFrame with '(null)' values replaced.
    """
    
    df_no_nulls = df.select(
        *[
            F.when(F.trim(df[column_name]) == '(null)', None).when(F.trim(df[column_name]) == '', None).otherwise(df[column_name]).alias(column_name)
            for column_name in df.columns
        ]
    )
    
    return df_no_nulls


def format_parts_df(df: DataFrame) -> DataFrame:
    """
    Formats the parts DataFrame by creating a 'functionRoomParts' struct column.

    Parameters:
    - df: The input parts DataFrame.

    Returns:
    - DataFrame: The formatted DataFrame.
    """
    
    formatted_df = df.withColumn(
        "functionRoomParts", 
        F.struct(
            F.col("basefunctionroomid").alias("functionRoomId"), 
            F.col("basefunctionroomname").alias("functionRoomName")
        )
    ).select("functionRoomId", "functionRoomParts")
    
    return formatted_df


def format_setup_df(df: DataFrame) -> DataFrame:
    """
    Formats the setup DataFrame by creating a 'functionRoomSetupTypes' struct column.

    Parameters:
    - df: The input setup DataFrame.

    Returns:
    - DataFrame: The formatted DataFrame.
    """
    
    formatted_df = df.withColumn(
        "functionRoomSetupTypes", 
        F.struct(
            F.col("functionroomsetuptypename").alias("functionRoomSetupTypeName"), 
            F.col("functionroomdescription").alias("functionRoomSetupTypeDescription"), 
            F.col("setuptypecapacity").cast(types.IntegerType()).alias("setUpTypeCapacity")
        )
    ).select("functionRoomId", "functionRoomSetupTypes")
    
    return formatted_df


def format_ooo_df(df: DataFrame) -> DataFrame:
    """
    Formats the headers DataFrame by creating a 'functionRoomOOO' column based on conditions.

    Parameters:
    - df: The input headers DataFrame.

    Returns:
    - DataFrame: The formatted DataFrame with 'functionRoomId' and 'functionRoomOOO' columns.
    """
    formatted_df = df.withColumn(
        "functionRoomOOO",
        F.when(
            (df.startdatetime != "1900-01-01T00:00:00") & 
            (df.enddatetime != "9999-12-31T23:59:59") & 
            (F.trim(df.statusCode) == 'AV'), 
            F.array(
                F.struct(
                    F.date_format(F.lit("1900-01-01T00:00:00"), "yyyy-MM-dd'T'00:00:00").alias('startDateTime'),
                    F.date_format(F.date_sub("startdatetime",1), "yyyy-MM-dd'T'23:59:59").alias('endDateTime')
                ),
                F.struct(
                    F.date_format(F.date_add("enddatetime",1), "yyyy-MM-dd'T'00:00:00").alias("startDateTime"),
                    F.date_format(F.lit("9999-12-31T23:59:59"), "yyyy-MM-dd'T'23:59:59").alias('endDateTime')
                )
            )
        ).otherwise(
            F.when(
                (df.enddatetime == "9999-12-31T23:59:59") & 
                (F.trim(df.statusCode) == 'AV'), 
                F.array(F.struct(
                    F.date_format(F.lit("1900-01-01T00:00:00"), "yyyy-MM-dd'T'00:00:00").alias('startDateTime'),
                    F.date_format(F.date_sub("startdatetime",1), "yyyy-MM-dd'T'23:59:59").alias('endDateTime')
                ))
            ).otherwise(
                F.array(F.struct(
                    F.date_format(F.col("startdatetime"), "yyyy-MM-dd'T'HH:mm:ss").alias("startDateTime"),
                    F.date_format(F.col("enddatetime"), "yyyy-MM-dd'T'23:59:59").alias("endDateTime")
                ))
            )
            
        )
    ).select(F.col("functionRoomId"), F.col("functionRoomOOO"))
    
    return formatted_df


def aggregate_parts(df: DataFrame) -> DataFrame:
    """
    Aggregates the parts DataFrame by grouping by 'functionroomid' and collecting 'functionRoomParts' into a list.

    Parameters:
    - df: The input parts DataFrame.

    Returns:
    - DataFrame: The aggregated DataFrame.
    """
    aggregated_df = df.groupBy(
        "functionroomid"
    ).agg(
        F.collect_list(F.col("functionRoomParts")).alias("functionRoomParts")
    )
    return aggregated_df


def aggregate_setup(df: DataFrame) -> DataFrame:
    """
    Aggregates the setup DataFrame by grouping by 'functionroomId' and collecting 'functionRoomSetupTypes' into a list.

    Parameters:
    - df: The input setup DataFrame.

    Returns:
    - DataFrame: The aggregated DataFrame.
    """
    aggregated_df = df.groupBy(
        "functionroomId"
    ).agg(
        F.collect_list(F.col("functionRoomSetupTypes")).alias("functionRoomSetupTypes")
    )
    return aggregated_df


def drop_datetime_columns(df: DataFrame) -> DataFrame:
    """
    Drops the 'enddatetime' and 'startdatetime' columns from the provided DataFrame.

    Parameters:
    - df: The input DataFrame.

    Returns:
    - DataFrame: The DataFrame with 'enddatetime' and 'startdatetime' columns removed.
    """
    
    return df.drop("enddatetime", "startdatetime")


def consolidate_dataframes(headers_df: DataFrame, parts_df: DataFrame, setup_df: DataFrame, ooo_df: DataFrame) -> DataFrame:
    """
    Consolidates the provided DataFrames based on the 'functionRoomId' column using left joins.

    Parameters:
    - headers_df: The main headers DataFrame.
    - parts_df: The parts DataFrame.
    - setup_df: The setup DataFrame.
    - ooo_df: The out-of-office DataFrame.

    Returns:
    - DataFrame: The consolidated DataFrame.
    """
    
    consolidated_df = headers_df\
        .join(parts_df, "functionRoomId", "left")\
        .join(setup_df, "functionRoomId", "left")\
        .join(ooo_df, "functionRoomId", "left")
    
    return consolidated_df


def format_dataframe_for_output(df: DataFrame, args: dict) -> DataFrame:
    """
    Formats the input DataFrame `df` for output purposes by applying several transformations and enrichments.

    Parameters:
    - df (DataFrame): The input DataFrame to format. This DataFrame should have columns such as 'functionRoomId', 
      'abbreviation', 'description', and so on, up to 'functionRoomOOO' (as used in the function).
    - args (dict): A dictionary containing additional arguments for the transformation. Currently, this function 
      expects 'sync_type' and 'uuid' as keys within this dictionary.

    The function performs the following operations:
    1. Converts a set of columns into a JSON blob named "json_blob".
    2. Sets the "status" of all rows to "PENDING".
    3. Adds columns "json_size", "create_id", "update_id", "sync_type", "json_type", and "uuid", populating them with 
       specific default values or values from the `args` dictionary.
    4. Selects a subset of columns to be present in the output DataFrame.
    5. Calculates the size of the "json_blob" column and populates the "json_size" column.

    Returns:
    DataFrame: The transformed and enriched DataFrame.

    Note:
    This function uses Spark's DataFrame API for transformations, and it assumes the presence of specific functions like 
    `F.udf`, `F.to_json`, etc. Make sure to have the necessary imports and context set up when using this function.
    """

    @F.udf(types.StringType())
    def map_name_to_lastname(property_code, value):
        return args.get(property_code, {}).get(value, "Unknown")

    formatted_df = df\
        .withColumn(
            "json_blob", 
            F.to_json(
                F.struct(
                    'functionRoomId', 'abbreviation', 'description', 'functionRoomName', 'areaInSquareFeet', 'lengthInFeet',
                    'widthInFeet', 'arealnSquareMeters', 'lengthInMeters', 'widthInMeters', 'minimumOccupancy', 'maximumOccupancy',
                    'optimalOccupancy', 'status', 'combinationFunctionRoom', 'shareable', 'functionRoomSetupTypes', 'functionRoomParts', 'functionRoomOOO'
                )
            )
        )\
        .withColumn("status", F.lit("PENDING"))\
        .withColumn("create_id", F.lit("aws glue fr job"))\
        .withColumn("update_id", F.lit("aws glue fr job"))\
        .withColumn("sync_type", map_name_to_lastname(df['propertyCode'], F.lit('sync_type')))\
        .withColumn("json_type", F.lit("F"))\
        .withColumn('uuid', map_name_to_lastname(df['propertyCode'], F.lit('uuid')))\
        .select(
            "sync_type",
            "json_blob", 
            "propertycode", 
            "functionRoomId", 
            "status", 
            "create_id", 
            "update_id", 
            "json_type",
            "uuid"
           )
    return formatted_df


def format_for_database_insertion(df: DataFrame) -> DataFrame:
    """
    Formats a given DataFrame `df` to match the naming conventions for database insertion.

    This function renames columns from snake_case notation to UPPERCASE convention, 
    which is a common requirement for many databases.

    Parameters:
    ----------
    df : DataFrame
        The input DataFrame with original column headers.

    Returns:
    -------
    DataFrame
        The formatted DataFrame with updated column headers.

    Note:
    -----
    It is assumed that the original DataFrame `df` contains all the columns 
    mentioned in the transformation, and that the DataFrame uses functions 
    from the PySpark SQL library.

    Examples:
    --------
    >>> df = spark.read.csv("path_to_file.csv", header=True, inferSchema=True)
    >>> formatted_df = format_for_database_insertion(df)
    >>> formatted_df.show()
    """
    formatted_df = df.select(
        F.col('json_type').alias("JSON_TYPE"),
        F.col('sync_type').alias("SYNC_TYPE"), 
        F.col('propertycode').alias("PROPERTY_CODE"), 
        F.col('functionRoomId').alias("JSON_ID"), 
        F.col('json_blob').alias("JSON_CONTENT"),
        F.col('status').alias("STATUS"), 
        F.col('create_id').alias("CREATE_ID"),
        F.col('update_id').alias("UPDATE_ID"),
        F.col('uuid').alias('UUID')
    )
    return formatted_df


def store_file_in_s3(final: DataFrame, bucket_path:str):
    final.coalesce(1).write.mode("overwrite").csv(bucket_path, header=True, sep='^')

def remove_files(args):
    args_keys = list(args.values())
    print(f"[remove_files]\targuments to filter: {args_keys}")
    s3_files = filter(lambda x: not x is None, args_keys)
    s3_files = map(
        lambda x: x.split('/', 3), filter(
            lambda x: x.lower().startswith("s3://"), s3_files
            )
        )
    s3_files = list(map(lambda x: (x[2], x[3]), s3_files))

    print(f"[remove_files]\tremoving files: {s3_files}")
    [
        s3_client.delete_object(Bucket=bucket_name, Key=obj_path)
        for bucket_name, obj_path in s3_files
    ]