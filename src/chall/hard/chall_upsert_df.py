
from chall.chall_template import Challenge

from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from typing import Dict, Optional

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from utils.log import log_execution
from datetime import date
from utils.chall_utils import create_simple_df # type: ignore

class ChallUpsertDf(Challenge):
    """
    Challenge Upsert DataFrame implementation.
    This challenge requires implementing a more complex DataFrame transformation.
    Probleme : Two dataframe manipulations with upsert logic.

    Problemetic Description:
    Given two DataFrames with the same schema:
    1. r_paper_df with the following schema:
        - r_paper_id (String)
        - r_paper_name (String)
        - r_paper_year (Integer)
        - r_paper_author_id (Integer)
        - r_paper_publish_status (String)
        - r_paper_update_date (Date)



        
    Tasks:
    1. Write a answer that upserts records from the second DataFrame into the first DataFrame based on the r_paper_id. 
    2. If a record with the same r_paper_id exists in both DataFrames, update the existing record in 
    the first DataFrame with the values from the second DataFrame.
    
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.sources = self.get_challenge_ressource() # type: ignore

    @log_execution
    def answer(self) -> Optional[DataFrame]:
        """
        implementation of the challenge.
        
        Returns:
        Dataframe: The result of the student's implementation.
        """
        # Student's code goes here
        return None

    @log_execution
    def get_challenge_ressource(self) -> Dict[str, DataFrame]:
        """
        Provides necessary resources for the challenge.
        Returns:
        Dict[str, DataFrame]: A dictionary containing the required DataFrames.
        """
        # r_paper_df: 10 initial records with update dates
        r_paper_df = self.spark.createDataFrame([
            ("P001", "Machine Learning Basics", 2020, "A001", "published", "2023-01-01"),
            ("P002", "Deep Learning Applications", 2021, "A002", "draft", "2023-01-02"),
            ("P003", "Natural Language Processing", 2019, "A003", "published", "2023-01-03"),
            ("P004", "Computer Vision Techniques", 2022, "A004", "archived", "2023-01-04"),
            ("P005", "Reinforcement Learning", 2021, "A005", "published", "2023-01-05"),
            ("P006", "Data Mining Methods", 2020, "A006", "draft", "2023-01-06"),
            ("P007", "Big Data Analytics", 2023, "A007", "published", "2023-01-07"),
            ("P008", "Cloud Computing", 2022, "A008", "archived", "2023-01-08"),
            ("P009", "Cybersecurity Fundamentals", 2021, "A009", "published", "2023-01-09"),
            ("P010", "Quantum Computing", 2023, "A010", "draft", "2023-01-10"),
        ], [
            "r_paper_id", "r_paper_name", "r_paper_year",
            "r_paper_author_id", "r_paper_publish_status", "r_paper_update_date"
        ])

        # r_paper_updates_df: 5 records to upsert (some overlap, some new) with later update dates
        r_paper_updates_df = self.spark.createDataFrame([
            ("P003", "Natural Language Processing - 2nd Ed", 2024, "A003", "published", "2024-02-01"),  # update
            ("P005", "Reinforcement Learning Advanced", 2022, "A005", "published", "2024-02-02"),      # update
            ("P011", "Edge Computing", 2024, "A011", "draft", "2024-02-03"),                           # new
            ("P002", "Deep Learning Applications", 2022, "A002", "published", "2024-02-04"),           # update
            ("P012", "Federated Learning", 2023, "A012", "draft", "2024-02-05"),                       # new
        ], [
            "r_paper_id", "r_paper_name", "r_paper_year",
            "r_paper_author_id", "r_paper_publish_status", "r_paper_update_date"
        ])

        return {
            "r_paper_df": r_paper_df,
            "r_paper_updates_df": r_paper_updates_df
        }
    
    @log_execution
    def solution(self) -> DataFrame:
        """
        Provides the solution for the challenge.
        
        Returns:
        Dataframe: The result of the provided solution.
        """

        r_paper_df = self.sources["r_paper_df"]
        r_paper_updates_df = self.sources["r_paper_updates_df"]

        # Perform the upsert operation
        combined_df = r_paper_df.unionByName(r_paper_updates_df)
        window_spec = Window.partitionBy("r_paper_id").orderBy(F.desc("r_paper_update_date"))
        result_df = combined_df.withColumn(
            "rank", F.row_number().over(window_spec)
        ).filter(F.col("rank") == 1).drop("rank")   


        return result_df    
    