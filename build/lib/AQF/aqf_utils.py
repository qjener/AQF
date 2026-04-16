import uuid
from pyspark.sql import SparkSession
from enum import IntEnum
from pyspark.sql import functions as F


def generate_run_id() -> str:
    """
    Generate a globally unique identifier for DQF test execution sessions.
    
    Creates a UUID1 string that uniquely identifies each invocation of the
    DQF engine. This enables tracking and correlation of test results across
    related validations within the same processing batch.
    
    Returns:
        str: UUID1 string in format 'xxxxxxxx-xxxx-1xxx-yxxx-xxxxxxxxxxxx'
             Example: '550e8400-e29b-11d4-a716-446655440000'
    
    Usage:
        Used by engine.py to tag all test executions within a single run,
        enabling batch-level reporting and correlation of results.
    
    Benefits:
        - Enables grouping of related test executions
        - Supports distributed processing scenarios
        - Provides audit trail for compliance reporting
        - Facilitates troubleshooting of failed validation runs
    
    Example:
        >>> run_id = generate_run_id()
        >>> print(f"Starting DQF execution with run_id: {run_id}")
    """
    return str(uuid.uuid1())

def create_spark():
    """
    creates spark session if not directly run in notebook
    """
    return SparkSession.builder.getOrCreate()

def normalize_timestamps(df):
    """
    helper to fix timestamp issues
    """
    for col, dtype in df.dtypes:
        if dtype == "timestamp":
            df = df.withColumn(
                col,
                F.to_timestamp(F.date_format(F.col(col), "yyyy-MM-dd HH:mm:ss"))
            )
    return df


class Status(IntEnum):
    """
    User friendly enum for status type
    """
    PASS = 0
    WARNING = 1
    FAIL = 2
    ERROR = 3

    def as_name(self):
        match self:
            case Status.PASS:
                return "pass"
            case Status.WARNING:
                return "warning"
            case Status.FAIL:
                return "fail"
            case Status.ERROR:
                return "error"
            case _:
                "no status"