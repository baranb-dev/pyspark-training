

from typing import Dict, Optional
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import StructType, StructField, LongType, IntegerType, StringType
import pyspark.sql.functions as F

from chall.chall_template import Challenge
from utils.log import log_execution
import random



class ChallWithColumnOrNot(Challenge):

    """
    Challenge WithColumn Or Not implementation.
    This challenge requires implementing a simple DataFrame transformation.
    Probleme : When you have a 1000 column dataframe, is it better to use withColumn or select to do transformations?

    Problemetic Description:
    Given a DataFrame containing 3k columns and 4K rows with the following schema:
        - col_0 (Integer)
        - col_1 (Integer)
        - ...
        - col_2999 (Integer)

    Tasks:
    1. Cast the columns to DoubleType.
    2. Without breaking the logical plan so no withColumn with a for loop. So find a way to do it with select.
    3. Return only the first 10 columns and 5 rows.
    
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.sources = self.get_challenge_ressource()

    @log_execution
    def answer(self) -> Optional[DataFrame]:
        """
        implementation of the challenge.
        
        Returns:
        Dataframe: The result of the student's implementation.
        """
        return None

    @log_execution
    def get_challenge_ressource(self) -> Dict[str, DataFrame]:
        """
        Provides necessary resources for the challenge.
        
        Creates a music DataFrame with 30 sample records including various
        genres, release years (2016-2025), and view counts.
        
        Returns:
        Dict[str, DataFrame]: A dictionary containing the music DataFrame.
        """
        # Define schema
        num_columns = 300
        num_rows = 4000

        fields = [StructField(f"col_{i}", IntegerType(), True) for i in range(num_columns)]
        schema = StructType(fields)

        data = [
            [random.randint(0, 1000000) for _ in range(num_columns)]
            for _ in range(num_rows)
        ]

        df = self.spark.createDataFrame(data, schema).coalesce(50)
        return {"source_df": df}
    

    @log_execution
    def solution(self) -> DataFrame:
        """
        Expected correct solution for the challenge.
        
        This method contains the reference implementation used to validate
        the implementation. The output should match the answer() output
        when correctly implemented.
        
        Returns:
        Dataframe: The expected result of the challenge.
        """
        source_df = self.sources["source_df"]
        # Create a list of columns casted to DoubleType
        selected_columns = [F.col(col_name).cast("double").alias(col_name) for col_name in source_df.columns]
        
        # Select only the first 10 columns and cast them to DoubleType
        result_df = (
             source_df.select(*selected_columns).limit(5)
        ).select(*selected_columns[:10])
        
        return result_df
