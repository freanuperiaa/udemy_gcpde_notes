from pyspark.sql import SparkSession
import sys

def main(gcs_path):
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("Spark SQL Function Example") \
        .getOrCreate()

    # Read the CSV file into a DataFrame
    df = spark.read.option("header", "true").csv(gcs_path)

    # Create a temporary view
    df.createOrReplaceTempView("data")

    
    result = spark.sql("SELECT * FROM data WHERE age > 30")

    # Show the result
    result.show()
    
    # Stop the Spark session
    spark.stop()

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: spark_sql_script.py <gcs_path>")
        sys.exit(-1)
    
    gcs_path = sys.argv[1]
    main(gcs_path)
