from pyspark.sql import DataFrame, SparkSession
from .dqf_utils import create_spark
import com.microsoft.sqlserver.jdbc.spark
from pyspark.sql.types import StructType

def retrieve_dqf_tests_by_table_id(table_id: str, test_table: str, spark: SparkSession, jdbc_url: str) -> DataFrame:
    """
    Retrieves all configured Data Quality Framework rules for a specific table.
    
    Queries the DQF metadata repository to fetch test configurations associated
    with the specified table. This enables dynamic test execution based on
    stored rule definitions rather than hardcoded test logic.
    
    Args:
        config_id (int): id
        test_table: Fully qualified table name (e.g., 'catalog.schema.table')
        spark: Sparksession
        jdbc_url: connections string to the sql db of the test table
        
    
    Returns:
        DataFrame: Spark DataFrame containing rule configurations with columns:
                  - test_id: Unique identifier for the test
                  - rule_id: ID for the rule
                  - description: Human-readable test description
                  - table: Target table for validation
                  - columns: Target columns
                  - expression: Join strategy (inner/left/right) for referential tests or comparison based expression ("<10")
                  - join_table: Reference table for join operations
                  - join_column: Reference column for join operations
                  - bad_data_action: Action for failed validations ('process'/'exclude'/'quarantine')
                  - criticality: action in case the test fails
    
    Usage:
        Rules are typically configured via rule_writer.py and stored in utility.dqf.dqf_rules.
        The engine uses this function to discover applicable tests at runtime.
    
    Example:
        >>> rules_df = retrieve_dqf_rules_by_table_name('bronze.sales.orders')
        >>> rules_list = rules_df.collect()  # Convert to list for iteration
    """
    # Query DQF metadata table for rules matching the specified table
    # Selects all rule configuration fields needed for test execution
    all_tests = spark.read.option("url", jdbc_url).mssql(f"dbo.{test_table}")
    all_tests.createOrReplaceTempView("pdf")
    tests_sql = f"""
        SELECT   
            test_id,           -- Unique test identifier
            rule_id,           -- Maps to rule
            description,       -- Human-readable test description,
            stage,
            table_id,          -- full Target table name
            columns,           -- Target columns (nullable for table-level tests)
            expression,        -- Join strategy (inner/left/right) for referential tests or comparison based expression ("<10")
            join_table,        -- Reference table for join operations
            join_column,       -- Reference column for join operations
            bad_data_action,   -- Bad data handling strategy
            criticality
        FROM pdf 
        WHERE table_id = '{table_id}'
        ORDER BY rule_id      -- Ensure consistent execution order
    """
    tests = spark.sql(tests_sql)

    if tests.rdd.isEmpty():
        print(f"Warning: No DQF tests for table '{table_id}' found.")
        
    print("Retrieving of tests successful")
    return tests


def retrieve_dqf_rules_by_id(id_list, spark: SparkSession, jdbc_url: str, rule_table: str) -> DataFrame:
    """
    retrieves the rules based on their id

    Args:
        test_table: Fully qualified table name (e.g., 'catalog.schema.table')
        spark: Sparksession
        jdbc_url: connections string to the sql db of the test table

    Returns:
        Dataframe with the info for the rules
    """

    try:
        all_rules = spark.read.option("url", jdbc_url).mssql(f"dbo.{rule_table}")
        rules = all_rules.filter(all_rules.rule_id.isin(id_list))

        if rules.rdd.isEmpty():
            return spark.createDataFrame([], schema=["rule_id", "rule_type_id", "reference_type_id", "name", "descripton", "connection"])
        
        print("Retrieving of rules successful")
        return rules

    except Exception as e:
        print(f"Error retrieving rules: {e}")
        return spark.createDataFrame(data=[], schema=["rule_id", "rule_type_id", "reference_type_id", "name", "descripton", "connection"])