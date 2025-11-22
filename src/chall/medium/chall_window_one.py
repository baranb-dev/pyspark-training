
from chall.chall_template import Challenge

from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from typing import Dict, Optional

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from utils.log import log_execution

class ChallWindowOne(Challenge):
    """
    Challenge Window Medium implementation.
    This challenge requires implementing a more complex DataFrame transformation.
    Probleme : Multiple dataframe manipulations with window functions.

    Problemetic Description:
    Given two DataFrames:
    1. r_paper_df with the following schema:
        - r_paper_id (String)
        - r_paper_name (String)
        - r_paper_year (Integer)

    2. r_authors DataFrame with the following schema:
        - r_paper_id (String)
        - author_id (String)
        - name (String)

        
    Tasks:
    1. Write a answer that combines the DataFames 
    and assigns a unique row number to each author and is partitioned by their research paper ID. 
    2. Don't forget to add the paper name to the final dataframe.
    
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

        r_paper_df = self.spark.createDataFrame([
            ("P001", "Machine Learning Basics", 2020),
            ("P002", "Deep Learning Applications", 2021),
            ("P003", "Natural Language Processing", 2019),
            ("P004", "Computer Vision Techniques", 2022),
            ("P005", "Reinforcement Learning", 2021),
            ("P005", "Data Mining Methods", 2020),
            ("P005", "Big Data Analytics", 2023),
            ("P003", "Cloud Computing", 2022),
            ("P003", "Cybersecurity Fundamentals", 2021),
            ("P002", "Quantum Computing", 2023)
        ], ["r_paper_id", "r_paper_name", "r_paper_year"])

        r_authors_df = self.spark.createDataFrame([
            ("P001", "A001", "Alice Johnson"),
            ("P001", "A002", "Bob Smith"),
            ("P002", "A001", "Alice Johnson"),
            ("P003", "A003", "Charlie Brown"),
            ("P004", "A002", "Bob Smith"),
            ("P005", "A004", "Diana Prince"),
        ], ["r_paper_id", "author_id", "name"])

        return {
            "r_paper_df": r_paper_df,
            "r_authors_df": r_authors_df
        }
        

    @log_execution
    def solution(self) -> DataFrame:
        """
        Provides the solution for the challenge.
        
        Returns:
        Dataframe: The result of the provided solution.
        """

        r_paper_df = self.sources["r_paper_df"]
        r_authors_df = self.sources["r_authors_df"]

        # Join r_authors_df with r_paper_df to include paper names
        r_authors_df = r_authors_df.join(
            r_paper_df.select("r_paper_id", "r_paper_name"),
            on="r_paper_id",
            how="left"
        )


        # Define a window partitioned by r_paper_id and ordered by author_id
        window_spec = Window.partitionBy("r_paper_id").orderBy("author_id")

        # Add a row number column
        result_df = r_authors_df.withColumn("row_number", F.row_number().over(window_spec))

        return result_df    
    