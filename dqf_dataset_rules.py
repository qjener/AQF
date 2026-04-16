from pyspark.sql import DataFrame
from .dqf_utils import create_spark, Status
from pyspark.sql.functions import col
from functools import reduce
import operator

def join_basic_inner_count_check(test, df: DataFrame, **kwargs) -> dict:
    """
    Validates referential integrity by performing inner join with lookup table.
    
    Checks if records in the source DataFrame have corresponding matches in a reference table.
    This test ensures data consistency across related tables and identifies orphaned records.
    
    Args:
        rule: Rule object containing test configuration with attributes:
              - join_table: Target table name for join operation
              - column_name: Source column for join condition
              - join_column: Target column for join condition
              - rule_id, test_type_id, description, table_name, bad_data_action
        df (DataFrame): Source DataFrame to validate
    
    Returns:
        (dict): Single-element list containing status code:
                  [1] = PASS (all records have matches)
                  [0] = FAIL (some records lack matches) 
                  [2] = ERROR (exception occurred)
    
    Test Logic:
        - Performs inner join between source and reference table
        - Compares join result count with source record count
        - PASS if join_count > 0, FAIL otherwise
    """

    spark = kwargs["spark"]

    # Load reference table for join operation
    df_join_target = spark.sql(f"SELECT * FROM {test.join_table}").alias("df_join_target")
    df_comp = df.alias("df_comp")


    # prepare string to join tables on all columns
    columns = eval(test.columns)
    join_columns = eval(test.join_column)

    if len(columns) != len(join_columns):
        raise Exception("column count doesn't match join column count")

    # Build join conditions
    join_condition = reduce(
        operator.and_,
        [col(f"df_comp.{c1}") == col(f"df_join_target.{c2}") for c1, c2 in zip(columns, join_columns)]
    )

    # Perform inner join
    joined_df = df_comp.join(
        df_join_target,
        join_condition,
        "inner"
    )

    # Calculate metrics for test evaluation
    join_count = joined_df.count()       # Records with valid references
    new_data_count = df_comp.count()          # Total source records
    

    # Prepare metrics for result logging
    result_values = {
        "status": Status.PASS if join_count > 0 else Status.WARNING,  # PASS if any matches found
        "new_data_count": new_data_count,
        "join_count": join_count,
    }
    return result_values

#----------------------------------------------------------------------------------


def unique_check(test, df: DataFrame) -> dict:
    """
    Validates data uniqueness by checking for duplicate values in specified column.
    
    Ensures primary key constraints and data integrity by identifying duplicate records.
    Supports automatic deduplication based on configured bad data action.

    !!! Removing/quarantining any data doesnt make sense here. We cant decide what the bad data is, based on only one column. !!!
    
    Args:
        rule: Rule object containing test configuration with attributes:
              - column_name: Target column for uniqueness validation0293
              - bad_data_action: Action for duplicate records ('exclude', 'process')
              - rule_id, test_type_id, description, table_name
        df (DataFrame): Source DataFrame to validate
    
    Returns:
        List[Union[int, DataFrame]]: Status code and optionally deduplicated DataFrame:
                                   [status, clean_df] if deduplication applied
                                   [status] if no filtering
                                   
                                   Status codes:
                                   1 = PASS (all values unique)
                                   0 = FAIL (duplicates found and handled)
                                   2 = ERROR (exception occurred)
    
    Deduplication Logic:
        - 'exclude': Remove duplicate records keeping first occurrence
        - 'process': Allow duplicates to continue through pipeline
        
    Note:
        Current implementation uses hardcoded 'id' column for deduplication.
        Consider making this configurable via rule.column_name.
    """
    columns = eval(test.columns)
    if not columns:
        unique_count = df.distinct().count()
    else:
        unique_count = df.select(columns).distinct().count()  # Unique values in target column

    # Calculate uniqueness metrics
    total_count = df.count()  # Total records in DataFrame
    
    ""
    if unique_count == total_count:
        # Perfect uniqueness - all values are distinct
        status = Status.PASS  # PASS
        return_df = df
    # What happens with quarantine? All duplicates should be moved to quarantine table
    else:
        status = Status.WARNING

    # Prepare detailed metrics for analysis
    result_values = {
        "status": status,
        "total_count": total_count,
        "unique_count": unique_count,
        "difference": total_count - unique_count  # Number of duplicate records
    }

    return result_values

