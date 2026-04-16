from pyspark.sql import DataFrame
from .aqf_utils import Status
from datetime import datetime, timezone

def null_check(test: dict, df: DataFrame) -> (dict, DataFrame, DataFrame):
    """
    Validates data completeness by checking for NULL values in specified column.
    
    Identifies and optionally quarantines records with NULL values in critical fields.
    Supports data quality enforcement through configurable bad data actions.
    
    Args:
        test: Test object containing test configuration with attributes:
              - column_name: Target column for NULL validation
              - bad_data_action: Action for NULL records ('quarantine', 'exclude', 'process')
              - rule_id, test_type_id, description, table_name
        df (DataFrame): Source DataFrame to validate
    
    Returns:
       Tuple(
          result_values: status and additional infos for logging
          return_df: df that has the failed rows removed
          df_bad_data: df that has the failed rows
       )
    
    Bad Data Actions:
        - 'quarantine': Move NULL records to bad_data table, continue with clean data
        - 'delete': Remove NULL records from processing pipeline
        - 'ignore': Allow NULL records to continue (no filtering)
        - 'process': Copy NULL records to bad_data_table, dont remove bda data from df


    """
    # create df with only null values
    df_bad_data = df.filter(df[eval(test.columns)[0]].isNull())
    null_count = df_bad_data.count()

    if not df_bad_data.count():
        # Perfect data quality - no NULLs detected
        status = Status.PASS  # PASS
        return_df = None
    else:
        match test.bad_data_action:
            case "quarantine" | "delete":
                return_df = df.filter(df[eval(test.columns)[0]].isNotNull())
            case _: # ignore, process
                return_df = df

        status = Status.WARNING  # FAIL (but handled appropriately)
    
    if return_df:
        new_data_count = return_df.count()
    else:
        new_data_count = 0
    
    # Prepare comprehensive metrics for analysis
    result_values = {
        "status": status,
        "new_data_count": new_data_count,
        "null_count": null_count,
        "none_null_count": df.count() - null_count
    }
    return (result_values, return_df, df_bad_data)

# ---------------------------------------------------------------------------------------

def compare(test: dict, df: DataFrame):
    """
    Validates data by comparing a value to a given expression

    Args:
        test: information about the test
        df: dataframe to validate
    
    Returns:
       Tuple(
          result_values: status and additional infos for logging
          return_df: df that has the failed rows removed
          df_bad_data: df that has the failed rows
       )
    Bad Data Actions:
        - 'quarantine': Move NULL records to bad_data table, continue with clean data
        - 'delete': Remove NULL records from processing pipeline
        - 'ignore': Allow NULL records to continue (no filtering)
        - 'process': Copy NULL records to bad_data_table, dont remove bda data from df
    """
    comp = ""
    comp += "df[eval(test.columns)[0]]" + test.expression

    df_bad_data = df.filter(~eval(comp))
    fail_count = df_bad_data.count()

    if not df_bad_data.count():
        # Perfect data quality - no NULLs detected
        status = Status.PASS  # PASS
        return_df = None
    else:
        match test.bad_data_action:
            case "quarantine" | "delete":
                return_df = df.filter(eval(comp))
            case _: # ignore, process
                return_df = df

        status = Status.WARNING  # FAIL (but handled appropriately)
    
    if return_df:
        new_data_count = return_df.count()
    else:
        new_data_count = 0

    # Prepare comprehensive metrics for analysis
    result_values = {
        "status": status,
        "new_data_count": new_data_count,
        "fail_count": fail_count,
        "pass_count": df.count() - fail_count
    }
    return (result_values, return_df, df_bad_data)

# -------------------------------------------------------------------------------------

def is_not_in_future(test: dict, df: DataFrame):
    """
    Validates data by checking if a date or timestamp is a valid past date.

    Args:
        test: information about the test
        df: dataframe to validate

    Return:
        Tuple(
          result_values: status and additional infos for logging
          return_df: df that has the failed rows removed
          df_bad_data: df that has the failed rows
       )
    Bad Data Actions:
        - 'quarantine': Move NULL records to bad_data table, continue with clean data
        - 'delete': Remove NULL records from processing pipeline
        - 'ignore': Allow NULL records to continue (no filtering)
        - 'process': Copy NULL records to bad_data_table, dont remove bda data from df
    """
    now = datetime.now(timezone.utc)

    df_bad_data = df.filter(df[eval(test.columns)[0]] > now)
    fail_count = df_bad_data.count()

    if not df_bad_data.count():
        # Perfect data quality - no NULLs detected
        status = Status.PASS  # PASS
    else:
        match test.bad_data_action:
            case "quarantine" | "delete":
                return_df = df.filter(df[eval(test.columns)[0]] <= now)
            case _: # ignore, process
                return_df = df

        status = Status.WARNING  # FAIL (but handled appropriately)
    
    new_data_count = return_df.count()

    # Prepare comprehensive metrics for analysis
    result_values = {
        "status": status,
        "new_data_count": new_data_count,
        "fail_count": fail_count,
        "pass_count": df.count() - fail_count
    }
    return (result_values, return_df, df_bad_data)