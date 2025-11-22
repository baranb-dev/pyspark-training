


import pyspark.sql.functions as F
from typing import Dict, Optional
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

from chall.chall_template import Challenge
from utils.log import log_execution

class ChallengeOneHard(Challenge):
    """
    Challenge One Hard implementation.
    This challenge requires implementing a complex DataFrame transformation.
    Problem: Pivot function, Complex Join and Aggregations.

    Problematic Description:
    Given two DataFrames:
    1. properties_df with the following schema:
        - property_id (Integer)
        - landlord_id (Integer)
        - property_type (String)
        - rent (Float)
        - square_feet (Integer)
        - city (String)

    2. landlords_df DataFrame with the following schema:
        - landlord_id (Integer)
        - f_name (String)
        - l_name (String)
        - email (String)
        - phone (String)

    Tasks:
    1. We need to summarize infromation about landlords and their properties. So create a Dataframe that contains for each landlord:
        - landlord_id
        - full_name ( concatenation of f_name and l_name with a space in between )
        - total_income ( sum of rent owned by the landlord )
    This DataFrame should then be joined with the Landlords DataFrame to get the full name of each landlord, 
    and the rental income for each landlord should be calculated by summing the rental income for each property.
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
        # Student's code goes here
        return None

    @log_execution
    def get_challenge_ressource(self) -> Dict[str, DataFrame]:
        """
        Provides necessary resources for the challenge.
        
        Returns:
        Dict[str, DataFrame]: A dictionary containing the required DataFrames.
        """
        data_properties = [
            (1, 101, "Apartment", 1200.0, 850, "New York"),
            (2, 101, "Condo", 1500.0, 900, "New York"),
            (3, 102, "House", 2000.0, 1200, "Boston"),
            (4, 102, "Apartment", 1100.0, 800, "Boston"),
            (5, 103, "Townhouse", 1800.0, 1100, "Chicago"),
            (6, 103, "Apartment", 1300.0, 950, "Chicago"),
            (7, 104, "Condo", 1700.0, 1000, "San Francisco"),
            (8, 104, "House", 2500.0, 1400, "San Francisco"),
            (9, 101, "Apartment", 1250.0, 870, "New York"),
            (10, 102, "Townhouse", 2100.0, 1250, "Boston"),
        ]
        schema_properties = [
            "property_id", "landlord_id", "property_type", "rent", "square_feet", "city"
        ]
        properties_df = self.spark.createDataFrame(data_properties, schema=schema_properties)

        data_landlords = [
            (101, "Alice", "Smith", "alice.smith@email.com", "555-1111"),
            (102, "Bob", "Johnson", "bob.johnson@email.com", "555-2222"),
            (103, "Carol", "Williams", "carol.williams@email.com", "555-3333"),
            (104, "David", "Brown", "david.brown@email.com", "555-4444"),
        ]
        schema_landlords = [
            "landlord_id", "f_name", "l_name", "email", "phone"
        ]
        landlords_df = self.spark.createDataFrame(data_landlords, schema=schema_landlords)

        return {
            "properties_df": properties_df,
            "landlords_df": landlords_df
        }
        

    @log_execution
    def solution(self) -> DataFrame:
        # Pivot the Properties DataFrame
        properties_pivot_df = (
            self.sources["properties_df"].groupBy("landlord_id")
            .pivot("property_type")
            .agg(F.sum("rent"))
        )

        # Join with the Landlords DataFrame
        rental_income_df = properties_pivot_df.join(
            self.sources["landlords_df"], "landlord_id"
        ).select(
            "landlord_id",
            F.concat(
                F.col("f_name"),
                F.lit(" "),
                F.col("l_name"),
            ).alias("landlord_name"),
            (
                F.coalesce( F.col("Apartment"), F.lit(0))
                + F.coalesce(F.col("Condo"), F.lit(0))
                + F.coalesce(F.col("House"), F.lit(0))
            )
            .cast("float")
            .alias("total_rental_income"),
        )

        # Sort by landlord_id
        rental_income_df = rental_income_df.sort(
            "landlord_id"
        )

        return rental_income_df

