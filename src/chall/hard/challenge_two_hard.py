


import pyspark.sql.functions as F
from typing import Dict, Optional
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.window import Window as W

from chall.chall_template import Challenge
from utils.log import log_execution

class ChallengeTwoHard(Challenge):
    """
    Challenge Two Hard implementation.
    This challenge requires implementing a complex DataFrame transformation.
    Problem: Windows function, join and ranking.

    Problematic Description:
    Given two DataFrames in get source function:
    1. products_df with the following schema:
        - product_id (Integer)
        - category (String)
        - product_name (String) 

    2. sales_df DataFrame with the following schema:
        - sale_id (Integer)
        - product_id (Integer)
        - quantity (Integer)
        - revenue (Float)


    Tasks:
    Write a function that returns the top 3 selling products in each product category based on the revenue generated, 
    without any gaps in the ranking sequence.

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
        data_products = [
            # Electronics
            (1, "Electronics", "Smartphone"),
            (2, "Electronics", "Laptop"),
            (11, "Electronics", "Tablet"),
            (12, "Electronics", "Smartwatch"),
            # Home
            (3, "Home", "Vacuum Cleaner"),
            (4, "Home", "Blender"),
            (13, "Home", "Microwave"),
            (14, "Home", "Coffee Maker"),
            # Sports
            (5, "Sports", "Bicycle"),
            (6, "Sports", "Tennis Racket"),
            (15, "Sports", "Football"),
            (16, "Sports", "Basketball"),
            # Books
            (7, "Books", "Novel"),
            (8, "Books", "Comics"),
            (17, "Books", "Biography"),
            (18, "Books", "Science Fiction"),
            # Toys
            (9, "Toys", "Action Figure"),
            (10, "Toys", "Puzzle"),
            (19, "Toys", "Board Game"),
            (20, "Toys", "Doll"),
        ]

        data_sales = [
            # Electronics (products 1,2,11,12) - each sold 1-5 times
            (1, 1, 2, 1200.0),
            (2, 1, 1, 600.0),
            (3, 1, 1, 600.0),
            (4, 1, 1, 600.0),
            (5, 2, 1, 1500.0),
            (6, 2, 3, 4500.0),
            (7, 2, 1, 1500.0),
            (8, 11, 2, 800.0),
            (9, 11, 1, 400.0),
            (10, 11, 1, 400.0),
            (11, 12, 1, 300.0),
            (12, 12, 1, 300.0),
            (13, 12, 1, 300.0),
            (14, 12, 1, 300.0),
            (15, 12, 1, 300.0),
            # Home (products 3,4,13,14)
            (16, 3, 2, 400.0),
            (17, 3, 1, 200.0),
            (18, 3, 1, 200.0),
            (19, 4, 1, 100.0),
            (20, 4, 2, 200.0),
            (21, 13, 1, 250.0),
            (22, 13, 1, 250.0),
            (23, 13, 1, 250.0),
            (24, 14, 1, 150.0),
            (25, 14, 1, 150.0),
            (26, 14, 1, 150.0),
            (27, 14, 1, 150.0),
            # Sports (products 5,6,15,16)
            (28, 5, 1, 800.0),
            (29, 5, 2, 1600.0),
            (30, 5, 1, 800.0),
            (31, 6, 1, 120.0),
            (32, 6, 2, 240.0),
            (33, 15, 1, 100.0),
            (34, 15, 1, 100.0),
            (35, 15, 1, 100.0),
            (36, 16, 1, 90.0),
            (37, 16, 1, 90.0),
            (38, 16, 1, 90.0),
            (39, 16, 1, 90.0),
            (40, 16, 1, 90.0),
            # Books (products 7,8,17,18)
            (41, 7, 3, 45.0),
            (42, 7, 2, 30.0),
            (43, 8, 1, 15.0),
            (44, 8, 2, 30.0),
            (45, 17, 1, 25.0),
            (46, 17, 1, 25.0),
            (47, 17, 1, 25.0),
            (48, 18, 1, 40.0),
            (49, 18, 1, 40.0),
            (50, 18, 1, 40.0),
            (51, 18, 1, 40.0),
            # Toys (products 9,10,19,20)
            (52, 9, 2, 50.0),
            (53, 9, 1, 25.0),
            (54, 10, 1, 10.0),
            (55, 10, 2, 20.0),
            (56, 19, 1, 60.0),
            (57, 19, 1, 60.0),
            (58, 19, 1, 60.0),
            (59, 20, 1, 35.0),
            (60, 20, 1, 35.0),
            (61, 20, 1, 35.0),
            (62, 20, 1, 35.0),
            (63, 20, 1, 35.0),
        ]

        products_df = self.spark.createDataFrame(
            data_products, ["product_id", "category", "product_name"]
        )
        sales_df = self.spark.createDataFrame(
            data_sales, ["sale_id", "product_id", "quantity", "revenue"]
        )

        return {
            "products_df": products_df,
            "sales_df": sales_df,
        }   

    @log_execution
    def solution(self) -> DataFrame:

        sales_agg = self.sources["sales_df"] \
            .groupBy("product_id") \
            .agg(F.sum("revenue").alias("total_revenue"))
        
        products_with_revenue = self.sources["products_df"] \
            .join(sales_agg, "product_id", "inner")
        
        w = W.partitionBy("category").orderBy(F.desc("total_revenue"))

        ranked_products = products_with_revenue \
            .select (
                "*",
                F.row_number().over(w).alias("rank")
            )

        top_3_products_per_category = ranked_products \
            .where(F.col("rank") <= 3)
        
        return top_3_products_per_category 


