from .aqf_utils import generate_run_id, create_spark, Status, normalize_timestamps
from .aqf_rule_retriever import *
from .aqf_dataset_rules import *
from .aqf_row_rules import *
from .aqf_logging import result_writer, bad_data_writer

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import max
from functools import reduce
from datetime import datetime, timezone

import notebookutils


class AQF_Engine():

    """
    Class that manages the DQF testing procedure

    Args:
        run_id: id string to make every test run unique
        job_id: id for every hsi session?
        config: name of the variable in the workspace
        spark: Optional SparkSession to use. If not provided, the active session is used.
    
    """

    def __init__(
        self,
        job_id: str,
        run_id: str,
        config: str,
        spark: SparkSession | None = None
        ):
        self.spark = create_spark() if spark is None else spark
        self.run_id = run_id

        # list of the outcomes of each test
        self.status_list = []
        
        # env variables
        self.config = notebookutils.variableLibrary.getLibrary(config)
        
        # represents the tested and updated dataframe
        self.consolidated_df = None

        self.cancel = False
    

    def run_tests(
        self,
        table_path: str,
        table_name: str,
        df: DataFrame,
        check_type: str | None = None,
        spark: SparkSession | None = None,
        ) -> DataFrame:
        """
        Main orchestration function for the Data Quality Framework (DQF).
        
        Executes all configured data quality tests for a given table and consolidates results.
        This function retrieves rules, executes corresponding tests, and tracks success/failure rates.
        
        Args:
            config_id: 
            table_name: string for the table name, only used for quaratine table name
            table_path:
            df (DataFrame): Input Spark DataFrame to validate
            check_type: ?

            
        Returns:
            DataFrame: Consolidated DataFrame containing only records that passed all tests
                    (when bad_data_action is 'quarantine' or 'delete')
        
        Notes:
            - Test results are automatically logged to dbo.dqf.dqf_test_log table
            - Supports multiple bad data actions: 'process', 'delete', 'quarantine', ignore
            - Generates unique run_id for tracking test execution sessions
        
        Example:
           >>> clean_df = engine.run_tests(test_table="dqf_tests", df=df, table_path="lh_bronze", table_name="bronze_hsi", check_type="")
        """

        if spark:
            self.spark = spark
        elif not self.spark:
            self.spark = create_spark()

        # table (str): Fully qualified table name (e.g., 'catalog.schema.table')
        #        Used to retrieve applicable DQ rules from metadata table
        self.table_id = table_path + "." + table_name
        self.table_name = table_name

        # clean df for return after tests are done
        self.consolidated_df = df

        print(f"Starting Data Quality checks for {self.table_id}")
        
        # Retrieve all configured DQ rules for the specified table
        df_tests = retrieve_aqf_tests_by_table_id(table_id=self.table_id, test_table=self.config.test_table, spark=self.spark, jdbc_url=self.config.jdbc_url)
        tests = df_tests.collect()
        number_of_tests = len(tests)
        if not number_of_tests:
            print(f"No tests found for {self.table_id}")
            return df

        test_list = df_tests.toPandas()["rule_id"].tolist()
        rules = retrieve_aqf_rules_by_id(test_list, spark=self.spark, jdbc_url=self.config.jdbc_url, rule_table=self.config.rule_table)

        # Execute each rule sequentially
        for test in tests:
            print(test)
            rule = rules.filter(rules.rule_id.isin([test.rule_id])).collect()
            self.testing(test=test, df=df, rule=rule[0])
            
            if self.status_list[-1] == Status.FAIL:
                self.cancel = True

        self.output_results(number_of_tests)
        
        return self.consolidated_df
    


    def consolidate(self, new_df: DataFrame):
        """
        Takes the dataframe that just got reduced by some row level test and removes the missing rows also from the consolidated df

        Args:
            new_df: the dataframe that got changed by a test
        """
        # Verwende INTERSECT statt UNION
        self.consolidated_df = self.consolidated_df.intersect(new_df)
        #self.consolidated_df.show()


    def output_results(self, number_of_tests: int):
        """
        Might get scrapped or changed to something else since output to notebook doesnt make sense in a regular environment
        
        Evaluates how the test session went

        Args:
            number_of_tests: number of tests is inferred from the test retrievel at the start
        """
        # Calculate final test statistics
        incomplete_tests = self.status_list.count(Status.ERROR)  # ERROR status
        passed_tests = self.status_list.count(Status.PASS)       # PASS status
        failed_tests = self.status_list.count(Status.FAIL)      # FAIL status
        warnings = self.status_list.count(Status.WARNING)
        #canceled = number_of_tests - len(self.status_list)

        # Summary output for monitoring (minimal logging)
        print(f"AQF Results - Tests: {number_of_tests} | Passed: {passed_tests} | Failed: {failed_tests+warnings} | Errors: {incomplete_tests}")


    def get_kwargs(self, connection: str) -> dict:
        """
        Some rules require an additional table so this puts the connections into the extra parameters
        """
        con = eval(connection)
        kwargs = {}
        for c in con:
            match c:
                case "spark":
                    kwargs["spark"] = self.spark
                case "sql":
                    kwargs["jdbc_url"] = self.config.jdbc_url
        return kwargs
    
    def row_level(self, test: dict, df: DataFrame, rule: dict, **kwargs) -> dict:
        """
        this is only for tests that are row based
        checks each cell in a column with a value
        reference value str optional

        Args:
            test: dict with the test info
            df: Dataframe to evaluate
            rule: dict with the rule info

        Returns:
            logging data
        """
        # results = (result_values, good_df, bad_df)
        results = eval(rule.name)(test=test, df=df, **kwargs)

        # Write bad data to quarantine table for later analysis
        # Table naming: utility.bad_data.{source_table_name}
        if results[2]:
            bad_data_writer(table_name=self.table_name, df=results[2], run_id=self.run_id, spark=self.spark, jdbc_url=self.config.jdbc_url, quarantine_table=self.config.quarantine_table)
            if results[1]:
                self.consolidate(results[1])

        return results[0]


    def testing(self, test: dict, df: DataFrame, rule: dict):
        """
        This manages a single test
        It runs it and logs it

        Args:
            test: dict with the test info
            df: Dataframe to evaluate
            rule: dict with the rule info
        """

        start_time = datetime.now(timezone.utc)
        start_time.strftime("%Y-%m-%d %H:%M:%S.%f")
        try:
            result_writer(
                    log_path = self.config.log_table,
                    run_id = self.run_id,
                    test_id = test.test_id,
                    rule_id = test.rule_id,
                    description = test.description,
                    table_id = test.table_id,
                    start_time = start_time,
                    result_values = {},
                    bad_data_action = test.bad_data_action,
                    criticality = test.criticality,
                    spark = self.spark,
                    jdbc_url = self.config.jdbc_url
            )
            print("logging (start) successful")
        except:
            pass
        

        kwargs = self.get_kwargs(rule.connection)

        try:
            # result: result_values
            if rule.rule_type == "dataset":
                # dataset level
                result_values = eval(rule.name)(test, df, **kwargs)
            elif rule.rule_type == "row":
                # row level
                result_values = self.row_level(test=test, df=df, rule=rule, **kwargs)
            else:
                raise Exception(f"Invalid rule type {rule.rule_type}")
            #print(results)
            if result_values["status"] == Status.WARNING and test.criticality:
                result_values["status"] = Status.FAIL
            
            self.status_list.append(result_values["status"])
            end_time = datetime.now(timezone.utc)
            end_time.strftime("%Y-%m-%d %H:%M:%S.%f")
            error_message = None

            print(f"Test successful: {test.test_id}")
        except Exception as e:
            # Handle unexpected errors during test execution
            print(f"Testing (id: {test.test_id}) failed: {e}")
            error_message = str(e)
            result_values = {"status": Status.ERROR}
            end_time = datetime.now(timezone.utc)
            end_time.strftime("%Y-%m-%d %H:%M:%S.%f")
            self.status_list.append(Status.ERROR)

        try:
            result_writer(
                    log_path = self.config.log_table,
                    run_id = self.run_id,
                    test_id = test.test_id,
                    rule_id = test.rule_id,
                    description = test.description,
                    table_id = test.table_id,
                    start_time = start_time,
                    end_time = end_time,
                    result_values = result_values,
                    error_message = error_message,
                    bad_data_action = test.bad_data_action,
                    criticality = test.criticality,
                    spark = self.spark,
                    jdbc_url = self.config.jdbc_url
            )
            print("Logging successful")
        except Exception as e:
            print(f"Logging failed: {e}")
        
        
    def is_critical(self):
        """
        Returns wether a critical test failed
        """
        return self.cancel

    def get_fail_count(self):
        return self.status_list.count(Status.FAIL)

    def get_log_table(self):
        spark = self.spark
        return spark.read.option("url", self.config.jdbc_url).mssql(self.config.log_table)

def create_test(
    jdbc_url: str,
    test_table: str,
    rule_table: str,
    rule_id: int,
    stage: str,
    table_id: str,
    desc: str = None,
    columns: list[str] = None,
    expression: str = None,
    join_table: str = None,
    join_column: str = None,
    bad_data_action: str = "process", # in case no bad data action is given for row based test
    citicality: bool = False, # in case no criticality is given
    spark: SparkSession | None = None
    ) -> int:
    """
    Create an entry in the test table with all necessary values
    """
    spark = create_spark() if not spark else spark

    # create new test_id
    tests = spark.read.option("url", jdbc_url).mssql(test_table)
    test_id = tests.select(max(tests.test_id).alias("test_id")).collect()[0][0]+1
    
    # check rule_id validity
    rules = spark.read.option("url", jdbc_url).mssql(rule_table)
    if not rules.filter(rules.rule_id == rule_id):
        raise Exception(f"rule {rule_id} doesn't exist")
        return -1
    
    #check for table
    try:
        df = spark.read.format("delta").load(table_id)
    except:
        try:
            query = f"""SELECT * FROM {table_id}"""
            df = spark.sql(query)
        except:
            try:
                ws = notebookutils.runtime.context.get("currentWorkspaceName")
                query = f"""SELECT * FROM {ws}.{table_id}"""
                df = spark.sql(query)
            except:
                raise Exception(f"table {table} doesn't exist")
                return -1

    # create table
    schema = StructType([
        StructField("test_id",         LongType(), True),
        StructField("rule_id",         LongType(), True),
        StructField("description",     StringType(),  True),
        StructField("stage",           StringType(),  True),
        StructField("table_id",        StringType(),  True),
        StructField("columns",         StringType(),  True),
        StructField("expression",      StringType(),  True),
        StructField("join_table",      StringType(),  True),
        StructField("join_column",     StringType(),  True),
        StructField("bad_data_action", StringType(),  True),
        StructField("criticality",     BooleanType(),  True),
    ])

    data = (
        test_id,
        rule_id,
        desc,
        stage,
        table_id,
        columns,
        expression,
        join_table,
        join_columns,
        bad_data_action,
        criticality
    )

    new_test = spark.createDataframe(data=[data], schema=schema)
    tests.write.option("url", jdbc_url).mode("append").mssql(test_table)

    return test_id