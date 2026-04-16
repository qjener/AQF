from datetime import datetime, timezone
import pandas as pd
import json
import ast

from pyspark.sql.functions import lit
from pyspark.sql.types import StructType, StructField, StringType, LongType, BooleanType, TimestampType
from typing import Optional
from pyspark.sql import DataFrame, SparkSession, Row
import notebookutils
from .dqf_utils import create_spark, Status, normalize_timestamps



def result_writer(
    log_path: str,
    run_id: str,
    test_id: int,
    rule_id: int,
    description: str, 
    table_id: str, 
    start_time: datetime, 
    criticality: bool,
    bad_data_action: str, 
    result_values,
    jdbc_url: str,
    spark: SparkSession,
    end_time: Optional[datetime] = None,
    error_message: Optional[str] = None, 
    job_run_id: int = 0
    
) -> None:
    
    """
    Persists data quality test results to the DQF audit trail.
    
    Records comprehensive test execution metadata including performance metrics,
    test outcomes, and error details for monitoring and compliance reporting.
    
    Args:
        run_id: unique identifier for each test session
        test_id (int): Unique identifier of the executed test
        rule_id (int): Unique identifier of the executed rule
        description (str): Human-readable description of the test
        table (str): name of the tested table
        start_time (datetime): Test execution start timestamp (UTC)
        end_time (datetime): Test execution completion timestamp (UTC)
        criticality: what if a test fails
        bad_data_action (Optional[str]): Action taken for failed records
                                        ('process', 'delete', 'ignore', 'quarantine')
        result_values (Dict): Test-specific metrics and measurements
                             Examples: {"status": Status.PASS, "null_count": 5, "total_count": 1000}
        error_message (Optional[str]): Exception details if status=ERROR
        job_id (int): Job run identifier for grouping related tests
                     Defaults to 0 if not specified
    
    Result Table Schema:
        - run_id: Auto-incrementing primary key per test execution
        - job_id: Groups related tests in the same batch run
        - Execution metadata: rule_id, test_type_id, description, config_id
        - Timing data: start_time, end_time (for performance analysis)
        - Outcome data: status, result_values, error_message
        - Action taken: bad_data_action
    
    Usage:
        Called automatically by test functions in test_engine.py to ensure
        comprehensive audit trail of all DQ validations.
    
    Note:
        Uses string interpolation in SQL. Consider JSON serialization
        for result_values to handle complex data types safely.
    """


    #insert optional values that are dependend on the test
    if "new_data_count" in result_values:
        new_data_count = result_values["new_data_count"]
    else:
        new_data_count = None
    
    if "rows_test_failed" in result_values:
        rows_test_failed = result_values["rows_test_failed"]
    else:
        rows_test_failed = None
    
    if "rows_test_passed" in result_values:
        rows_test_passed = result_values["rows_test_passed"]
    else:
        rows_test_passed = None
    

    #start_time = start_time.astimezone(timezone.utc).replace(tzinfo=None)
    #end_time = end_time.astimezone(timezone.utc).replace(tzinfo=None)

    if not end_time:
        status = "running"
    else:
        status = result_values["status"].as_name()
    
    # Dictionary für DataFrame
    df_dqf_test_log = (
        run_id,                         # Unique identifier for each test engine run
        job_run_id,                 # Batch execution identifier
        test_id,                       # Reference to rule configuration
        rule_id,             # Test function type for categorization
        criticality,               
        description,               # Human-readable test description
        table_id,                 # Target table for auditing 
        start_time,                 # Execution start for performance tracking
        end_time,                     # Execution end for duration calculation
        status,                         # Test outcome (PASS/FAIL/ERROR)
        json.dumps(result_values),      # Test-specific metrics as VARIANT (STRUCT, ARRAY, "JSON" etc.)    
        new_data_count,
        rows_test_failed,
        rows_test_passed, 
        bad_data_action,       # Action taken for quality violations
        error_message            # Exception details for troubleshooting
    )
    #print(df_dqf_test_log)
    #print(type(run_id))
    schema = StructType([
        StructField('run_id', StringType(), True), 
        StructField('job_run_id', LongType(), True), 
        StructField('test_id', LongType(), True), 
        StructField('rule_id', LongType(), True), 
        StructField('criticality', BooleanType(), True), 
        StructField('description', StringType(), True), 
        StructField('table_id', StringType(), True), 
        StructField('start_time', TimestampType(), True), 
        StructField('end_time', TimestampType(), True), 
        StructField('status', StringType(), True), 
        StructField('result_values', StringType(), True), 
        StructField('new_data_count', LongType(), True), 
        StructField('rows_test_failed', LongType(), True), 
        StructField('rows_test_passed', LongType(), True), 
        StructField('bad_data_action', StringType(), True), 
        StructField('error_message', StringType(), True)
    ])


    #old = spark.read.option("url", jdbc_url).mssql("dbo.dqf_test_log")

    log_df = spark.createDataFrame(data=[df_dqf_test_log], schema=schema)
    
    log_df = normalize_timestamps(log_df)

    log_df.write.option("url", jdbc_url).mode("append").mssql(f"dbo.{log_path}")


def bad_data_writer(table_name: str, df: DataFrame, run_id: str, spark: SparkSession, jdbc_url: str, quarantine_table: str) -> None:
    """
    Persists data quality test results to the DQF audit trail.
    
    Records comprehensive test execution metadata including performance metrics,
    test outcomes, and error details for monitoring and compliance reporting.
    
    Args:
        table_name (str): name for the quarantine table
        df (DataFrame): DataFrame containing the test results
        run_id (str): Unique identifier for the test run
        spark: Sparksession
        jdbc_url: connections string to sql db
        quarantine_table: path to quarantine lakehouse
    
    Result Table Schema:
        - run_id: Unique identifier per test execution
        - Additional columns from the input DataFrame
    
    Usage:
        Called automatically by row test functions if they fail to ensure
        comprehensive audit trail of all DQ validations.
    """
    
    # Insert test execution record into audit table
    # All test executions are logged regardless of outcome for compliance
    df = df.withColumn("run_id", lit(run_id))

    
    try:
        # date table
        ddl_error_table = (
        f"""CREATE TABLE IF NOT EXISTS {quarantine_table}.{table_name}""")
        spark.sql(ddl_error_table)
    
        df.write.mode("append").option("mergeSchema", "true").saveAsTable(f"{quarantine_table}.{table_name}")
    except:
        ws = notebookutils.runtime.context.get("currentWorkspaceName")
        ddl_error_table = (
        f"""CREATE TABLE IF NOT EXISTS {ws}.{quarantine_table}.dbo.{table_name}""")
        spark.sql(ddl_error_table)
    
        df.write.mode("append").option("mergeSchema", "true").saveAsTable(f"{ws}.{quarantine_table}.dbo.{table_name}")