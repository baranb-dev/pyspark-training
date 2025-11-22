from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

def create_simple_df(spark: SparkSession) -> DataFrame:
    """
    Create a simple DataFrame with two typed columns: 'name' (str) and 'age' (int).

    Parameters:
    spark (SparkSession): The active Spark session.

    Returns:
    DataFrame: A DataFrame with sample data and specified schema.
    """
    schema = StructType([
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True)
    ])

    data = [
        ("Alice", 30),
        ("Bob", 25)
    ]

    return spark.createDataFrame(data, schema)

