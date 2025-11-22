from chall.chall_template import Challenge

from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from typing import Dict, Optional

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from utils.log import log_execution
from datetime import date
from utils.chall_utils import create_simple_df # type: ignore

class ChallUpsertDfV2(Challenge):
    """
    Challenge Upsert v2 DataFrame implementation.
    This challenge requires implementing a more complex DataFrame transformation.
    Probleme : Two dataframe manipulations with upsert logic.

    Problemetic Description:
    Given two DataFrames with schema but not the same shcema:
    1. r_paper_df with the following schema:
        - r_paper_id (String)
        - r_paper_name (String)
        - r_paper_year (Integer)
        - r_paper_author_id (Integer)
        - r_paper_publish_status (String)
    2. r_paper_updates_df
        - r_paper_id (String)
        - r_paper_name (String)
        - r_paper_info (String)
        - r_paper_publish_status (String)


        
    Tasks:
    1. Write a answer that upserts records from the second DataFrame into the first DataFrame based on the r_paper_id. 
    2. If a record with the same r_paper_id exists in both DataFrames, update the existing record in 
    the first DataFrame with the values from the second DataFrame or insert it. 

This taks is very difficult because the two dataframe do not have the same schema.
    
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
        # r_paper_df: 10 initial records
        r_paper_df = self.spark.createDataFrame([
            ("P001", "Machine Learning Basics", 2020, "A001", "published"),
            ("P002", "Deep Learning Applications", 2021, "A002", "draft"),
            ("P003", "Natural Language Processing", 2019, "A003", "published"),
            ("P004", "Computer Vision Techniques", 2022, "A004", "archived"),
            ("P005", "Reinforcement Learning", 2021, "A005", "published"),
            ("P006", "Data Mining Methods", 2020, "A006", "draft"),
            ("P007", "Big Data Analytics", 2023, "A007", "published"),
            ("P008", "Cloud Computing", 2022, "A008", "archived"),
            ("P009", "Cybersecurity Fundamentals", 2021, "A009", "published"),
            ("P010", "Quantum Computing", 2023, "A010", "draft"),
        ], [
            "r_paper_id", "r_paper_name", "r_paper_year",
            "r_paper_author_id", "r_paper_publish_status"
        ])

        # r_paper_updates_df: 5 records to upsert (some overlap, some new)
        r_paper_updates_df = self.spark.createDataFrame([
            ("P003", "Natural Language Processing - 2nd Ed", "Updated NLP content", "published"),  # update
            ("P005", "Reinforcement Learning Advanced", "Advanced RL info", "published"),         # update
            ("P011", "Edge Computing", "Edge computing intro", "draft"),                          # new
            ("P002", "Deep Learning Applications", "DL Apps info", "published"),                  # update
            ("P012", "Federated Learning", "Federated learning basics", "draft"),                 # new
        ], [
            "r_paper_id", "r_paper_name", "r_paper_info", "r_paper_publish_status"
        ])

        return {
            "r_paper_df": r_paper_df,
            "r_paper_updates_df": r_paper_updates_df
        }
    
    @log_execution
    def solution(self) -> DataFrame:
        """
        Performs an upsert operation merging r_paper_updates_df into
        r_paper_df based on 'r_paper_id'.

        For overlapping IDs, values from r_paper_updates_df replace those
        in r_paper_df. New records in r_paper_updates_df are appended.
        Uses FULL OUTER JOIN to preserve both existing and new records.

        Algorithm:
        1. Identify common and new columns between DataFrames.
        2. Rename common columns in updates DataFrame to avoid ambiguity.
        3. Perform FULL OUTER JOIN on r_paper_id.
        4. Apply all column replacements in a single select() operation
        using coalesce for efficiency.
        5. Retain original column order with new columns appended.

        Returns:
            DataFrame: The upserted DataFrame with merged and updated
                    records from both DataFrames.
        """
        r_paper_df = self.sources["r_paper_df"]
        r_paper_updates_df = self.sources["r_paper_updates_df"]

        # Identify common columns (excluding join key)
        common_cols = (
            set(r_paper_df.columns) &
            set(r_paper_updates_df.columns) -
            {"r_paper_id"}
        )

        # Identify new columns from updates not in original DataFrame
        new_cols = (
            set(r_paper_updates_df.columns) -
            set(r_paper_df.columns) -
            {"r_paper_id"}
        )

        # Rename common columns in updates to avoid ambiguity during join
        r_paper_updates_renamed = r_paper_updates_df.select(
            "r_paper_id",
            *[
            F.col(col).alias(f"{col}_update") if col in common_cols else F.col(col)
            for col in r_paper_updates_df.columns if col != "r_paper_id"
            ]
        )

        # Perform FULL OUTER JOIN to include both existing and new records
        result = r_paper_df.join(
            r_paper_updates_renamed,
            on="r_paper_id",
            how="full"
        )

        # Apply all column replacements in a single select() operation
        # This is more efficient than multiple withColumn() calls
        select_cols = [
            F.coalesce(
                F.col(f"{col}_update"),
                F.col(col)
            ).alias(col) if col in common_cols else F.col(col)
            for col in r_paper_df.columns
        ] + [F.col(new_col) for new_col in new_cols]

        result = result.select(*select_cols)

        return result
