import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id, count, regexp_replace, explode
from helpers import *

def create_spark_session():
    """
    Description:
        This function createa a new Spark session
    
    Argments:
        None
    
    Returns:
        A Spark session created
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()

    return spark

def run_sample_queries(spark):
    """
    Description:
        This function runs sample queries against the parquet files
    
    Argments:
        None
    
    Returns:
        None
    """
    stations = spark.read.parquet(stations_parquet)
    population = spark.read.parquet(population_parquet)
    timetables = spark.read.parquet(timetables_parquet)
    passengers = spark.read.parquet(passengers_parquet)
    
    stations.createOrReplaceTempView("stations")
    population.createOrReplaceTempView("population")
    timetables.createOrReplaceTempView("timetables")
    passengers.createOrReplaceTempView("passengers")

    results1 = spark.sql("""
        SELECT
            population.area_code,
            population.area_name,
            count(*) n_stations
        FROM population
        LEFT JOIN stations ON stations.area_code = population.area_code
        GROUP BY population.area_code, population.area_name
        ORDER BY n_stations DESC
        LIMIT 10
    """)

    results1.show()

    results2 = spark.sql("""
        WITH population_summary AS (
          SELECT
              area_code,
              area_name,
              daytime_population - population AS population_diff
          FROM population
          ORDER BY population_diff DESC
          LIMIT 10
        )
        SELECT
          population_summary.area_code,
          population_summary.area_name,
          population_diff,
          SUM(passengers)
        FROM population_summary
        LEFT JOIN stations ON population_summary.area_code = stations.area_code
        LEFT JOIN passengers ON stations.survey_id = passengers.survey_id
        WHERE passengers.year = '2021'
        GROUP BY population_summary.area_code, population_summary.area_name, population_diff
        ORDER BY population_summary.population_diff DESC
    """)
    results2.show()
    
def main():
    """
    Description:
        The main function.
        This creates a new Spark session, and calls subsequent functions to process the transportation data.
    
    Argments:
        None
    
    Returns:
        None
    """
    # Create a Spark session
    spark = create_spark_session()

    run_sample_queries(spark)

if __name__ == "__main__":
    main()

