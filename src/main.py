from utils.prepare_challenge import get_challenge
from utils.log import log_execution
from pyspark.sql import SparkSession

if __name__ == "__main__":

    spark = SparkSession.builder \
        .appName("PySpark Training Challenges") \
        .master("local[3]") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .config("spark.sql.shuffle.partitions", 3) \
        .getOrCreate()    

    for chall in get_challenge(spark):
        print("**********************************************************************")
        result_df = chall.answer()
        solution_df = chall.solution()
        
        if chall.validate():
            print(f"*********** Challenge {chall.__class__.__name__} passed validation!**************")
        else:
            print("Your challenge result:")
            if result_df is not None:
                result_df.show(truncate=False)
                print("Expected solution result:")
                solution_df.show(truncate=False)
            else:
                print("No result returned from answer().")

         
        print("**********************************************************************")


