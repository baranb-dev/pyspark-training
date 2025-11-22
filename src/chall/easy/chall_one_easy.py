"""
Challenge One Easy: Music DataFrame Operations.

This challenge involves working with a music dataset containing information
about songs, their genres, release years, durations, and view counts.
"""

from typing import Dict, Optional
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import StructType, StructField, LongType, IntegerType, StringType
import pyspark.sql.functions as F

from chall.chall_template import Challenge
from utils.log import log_execution # type: ignore


class ChallengeOneEasy(Challenge):
    """
    Challenge One Easy implementation.
    This challenge requires implementing a simple DataFrame transformation.
    Probleme : Conditional logic and datetime operations.

    Problemetic Description:
    Given a DataFrame containing music data with the following schema:
        - music_id (Integer)
        - title (String)
        - genre (String)
        - release_year (Integer)
        - duration (Integer, in seconds)
        - view_count (Integer) 

    Tasks:
    1. Filter the DataFrame to include only songs released after 2020 ( included ).
    2. Filter with music with more than 1,000,000 views.
    
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
        schema = StructType([
            StructField("music_id", IntegerType(), False),
            StructField("title", StringType(), False),
            StructField("genre", StringType(), False),
            StructField("release_year", IntegerType(), False),
            StructField("duration", IntegerType(), False),
            StructField("view_count", LongType(), False)
        ])
        
        # Sample data with 30 records
        data = [
            (1, "Blinding Lights", "Pop", 2019, 200, 2500000000),
            (2, "Shape of You", "Pop", 2017, 233, 5800000000),
            (3, "Someone Like You", "Pop", 2016, 285, 3200000000),
            (4, "Rockstar", "Hip-Hop", 2017, 218, 2100000000),
            (5, "God's Plan", "Hip-Hop", 2018, 198, 3400000000),
            (6, "HUMBLE.", "Hip-Hop", 2017, 177, 1800000000),
            (7, "Bohemian Rhapsody", "Rock", 2016, 354, 1900000000),
            (8, "Stairway to Heaven", "Rock", 2018, 482, 450000000),
            (9, "Hotel California", "Rock", 2019, 391, 380000000),
            (10, "Despacito", "Latin", 2017, 228, 8100000000),
            (11, "Bailando", "Latin", 2016, 240, 3100000000),
            (12, "Mi Gente", "Latin", 2017, 189, 2900000000),
            (13, "Bad Guy", "Pop", 2019, 194, 1400000000),
            (14, "Dance Monkey", "Pop", 2019, 209, 3100000000),
            (15, "Levitating", "Pop", 2020, 203, 1200000000),
            (16, "Peaches", "Pop", 2021, 198, 950000000),
            (17, "Stay", "Pop", 2021, 141, 1600000000),
            (18, "Heat Waves", "Indie", 2020, 238, 1800000000),
            (19, "Sweater Weather", "Indie", 2016, 240, 1100000000),
            (20, "Mr. Brightside", "Indie", 2019, 222, 890000000),
            (21, "Montero", "Hip-Hop", 2021, 137, 780000000),
            (22, "Industry Baby", "Hip-Hop", 2021, 212, 920000000),
            (23, "Save Your Tears", "Pop", 2020, 215, 1500000000),
            (24, "Flowers", "Pop", 2023, 200, 1400000000),
            (25, "As It Was", "Pop", 2022, 167, 1800000000),
            (26, "Anti-Hero", "Pop", 2022, 200, 1100000000),
            (27, "Calm Down", "Afrobeat", 2022, 239, 890000000),
            (28, "Unholy", "Pop", 2022, 156, 760000000),
            (29, "Cruel Summer", "Pop", 2024, 178, 920000000),
            (30, "vampire", "Pop", 2023, 219, 650000000)
        ]
        
        # Create DataFrame
        music_df = self.spark.createDataFrame(data, schema).coalesce(1) # type: ignore
        
        return {"music_df": music_df}
    
    @log_execution
    def solution(self) -> DataFrame:
        """
        Expected correct solution for the challenge.
        
        Returns:
        DataFrame: The expected result of the challenge.
        """
        current_year = 2025 # type: ignore
        return self.sources["music_df"].where( # type: ignore
            (F.col("release_year") >= 2020)
            & (F.col("view_count") > 1_000_000)
        ).coalesce(1)
