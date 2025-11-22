
from chall.chall_template import Challenge

from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from typing import Dict, Optional

from utils.log import log_execution


class ChallengeOneMedium(Challenge):
    """
    Challenge One Medium implementation.
    This challenge requires implementing a more complex DataFrame transformation.
    Probleme : String manipulation and joins.

    Problemetic Description:
    Given three DataFrames:
    1. customers_df with the following schema:
        - customer_id (Integer)
        - f_name (String)
        - l_name (String)
        - email (String)

    2. Orders DataFrame with the following schema:
        - order_id (Integer)
        - customer_id (Integer)
        - order_date (String, format 'yyyy-MM-dd')
        - product_id (Integer)

    3. Products DataFrame with the following schema:
        - product_id (Integer)
        - product_name (String)
        - category (String)

    Tasks:
    1. Join the three DataFrames to create a consolidated view of customers and their orders.
    2. For each customer merge f_name and l_name into a single full_name column ( sep " " ).
    
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
        Dict[Dataframe]: A dictionary containing challenge resources.
        """
        # Code to create and return the required DataFrames
        data_customers = [
            (1, "Alice", "Smith", "alice.smith@email.com"),
            (2, "Bob", "Johnson", "bob.johnson@email.com"),
            (3, "Charlie", "Williams", "charlie.williams@email.com"),
            (4, "Diana", "Brown", "diana.brown@email.com"),
            (5, "Evan", "Jones", "evan.jones@email.com"),
        ]
        customers_df = self.spark.createDataFrame(
            data_customers, ["customer_id", "f_name", "l_name", "email"]
        )

        data_orders = [
            (101, 1, "2024-06-01", 1001),
            (102, 2, "2024-06-02", 1002),
            (103, 3, "2024-06-03", 1003),
            (104, 4, "2024-06-04", 1004),
            (105, 5, "2024-06-05", 1005),
        ]
        orders_df = self.spark.createDataFrame(
            data_orders, ["order_id", "customer_id", "order_date", "product_id"]
        )

        data_products = [
            (1001, "Laptop", "Electronics"),
            (1002, "Book", "Education"),
            (1003, "Headphones", "Electronics"),
            (1004, "Coffee Mug", "Kitchen"),
            (1005, "Backpack", "Accessories"),
        ]
        products_df = self.spark.createDataFrame(
            data_products, ["product_id", "product_name", "category"]
        )

        return {
            "customers": customers_df,
            "orders": orders_df,
            "products": products_df,
        }
    
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
        customers_df = self.sources["customers"]
        orders_df = self.sources["orders"]
        products_df = self.sources["products"]

        # Join customers with orders
        cust_orders_df = customers_df.join(
            orders_df,
            "customer_id",
            "inner"
        )

        # Join the result with products
        full_df = cust_orders_df.join(
            products_df,
            "product_id",
            "inner"
        )

        # Create full_name column
        from pyspark.sql import functions as F
        result_df = full_df.select(
            "customer_id", "email", "order_id", "order_date", "product_name", "category",
            F.concat_ws(" ", F.col("f_name"), F.col("l_name")).alias("full_name")
        )

        return result_df.coalesce(1)