import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, count, regexp_replace, explode
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

def process_station_data(spark):
    """
    Description:
        This function processes log data and creates the following parquet files:
        - station
    
    Argments:
        spark: Spark session object
    
    Returns:
        None
    """
    # Read Toyko Metro station data
    df_station_metro = spark.read.json(station_metro_file)
    df_station_metro = df_station_metro.select(
        col("@id"),
        col("dc:title"),
        col("odpt:railway"),
        col("odpt:stationCode"),
        col("odpt:passengerSurvey"),
        col("owl:sameAs")
    )

    # Read Toei station data
    df_station_toei = spark.read.json(station_toei_file)
    df_station_toei = df_station_toei.select(
        col("@id"),
        col("dc:title"),
        col("odpt:railway"),
        col("odpt:stationCode"),
        col("odpt:passengerSurvey"),
        col("owl:sameAs")
    )

    # Concat Tokyo Metro and Toei station data
    df_station = df_station_metro.unionByName(df_station_toei)
    print(df_station.count())
    
    # Read station address data
    df_station_address = spark.read.option("header",True).csv(station_address_file)
    df_station_address = df_station_address.withColumn("post", regexp_replace("post", "-", ""))
    df_station_address.show()

    # Read zip code data
    df_zip = spark.read.option("header",True).csv(zip_code_file)
    df_zip.show()

    # Join station data with the address and zip code data
    stations_table = df_station.join(df_station_address, (df_station['dc:title'] == df_station_address["station_name"]))
    stations_table = stations_table.join(df_zip, (stations_table['post'] == df_zip["zip_code"]))
    stations_table = stations_table.select(
        col("@id").alias("id"),
        col("owl:sameAs").alias("station_id"),
        col("dc:title").alias("station_name"),        
        "zip_code",
        "area_code",
        col("odpt:stationCode").alias("station_code"),
        explode("odpt:passengerSurvey").alias("survey_id"),
    )
    
    stations_table.printSchema()
    stations_table.show()
    print(stations_table.count())

    # Write data into parquet files
    stations_table.write.mode("overwrite").parquet(stations_parquet)

    
def process_timetable_data(spark):
    """
    Description:
        This function processes log data and creates the following parquet files:
        - timetable
    
    Argments:
        spark: Spark session object
    
    Returns:
        None
    """

    # Read Tokyo Metro timetable data
    df_timetable_metro = spark.read.json(timetable_metro_file, multiLine=True)
    df_timetable_metro = df_timetable_metro.select(
                    col("@id").alias("id"),
                    col("owl:sameAs").alias("timetable_id"),
                    col("odpt:railDirection").alias("direction"),
                    col("odpt:railway").alias("railway"),
                    col("odpt:station").alias("station"),
                    col("data.odpt:departureTime").alias("departure_time"),
                    col("data.odpt:trainType").alias("train_type"),
                    explode("odpt:stationTimetableObject").alias("data")
    )
    df_timetable_metro = df_timetable_metro.select(
                    "id",
                    "timetable_id",
                    "direction",
                    "railway",
                    "station",
                    "departure_time",
                    "train_type"
    )

    # Read Toei timetable data
    df_timetable_toei = spark.read.json(timetable_toei_file, multiLine=True)
    df_timetable_toei = df_timetable_toei.select(
                    col("@id").alias("id"),
                    col("owl:sameAs").alias("timetable_id"),
                    col("odpt:railDirection").alias("direction"),
                    col("odpt:railway").alias("railway"),
                    col("odpt:station").alias("station"),
                    col("data.odpt:departureTime").alias("departure_time"),
                    col("data.odpt:trainType").alias("train_type"),
                    explode("odpt:stationTimetableObject").alias("data")
    )
    df_timetable_toei = df_timetable_toei.select(
                    "id",
                    "timetable_id",
                    "direction",
                    "railway",
                    "station",
                    "departure_time",
                    "train_type"
    )
    
    # Concat Tokyo Metro and Toei station data
    df_timetable = df_timetable_metro.unionByName(df_timetable_toei)

    df_timetable.printSchema()
    df_timetable.show()
    print(df_timetable.count())

    # Write data into parquet files
    df_timetable.write.mode("overwrite").parquet(timetables_parquet)


def process_population_data(spark):
    """
    Description:
        This function processes log data and creates the following parquet files:
        - Population
    
    Argments:
        spark: Spark session object
    
    Returns:
        None
    """
    
    # Parse zip code data
    df_zip = spark.read.option("header",True).csv(zip_code_file)
    df_zip.show()
    
    # Parse population data
    df_population = spark.read.option("header",True).csv(population_file)

    df_population = df_population.select(
                    col("地域コード").alias("area_code"),
                    col("地域").alias("area_name_ja"),
                    col("District").alias("area_name"),
                    col("昼間人口 Daytime population／平成27年 2015").alias("daytime_population"),
                    col("昼間人口 Daytime population／平成27年 2015／男 Male").alias("daytime_population_male"),
                    col("昼間人口 Daytime population／平成27年 2015／女 Female").alias("daytime_population_female"),
                    col("常住人口 De jure population 1)／平成27年 2015").alias("population"),
                    col("常住人口 De jure population 1)／平成27年 2015／男 Male").alias("population_male"),
                    col("常住人口 De jure population 1)／平成27年 2015／女 Female").alias("population_female")
                    )

    df_population.show()
    df_population.printSchema()
    
    # write Population table to parquet files
    df_population.write.mode("overwrite").parquet(population_parquet)

    
def process_survey_data(spark):
    """
    Description:
        This function processes log data and creates the following parquet files:
        - PassengerSurvey
    
    Argments:
        spark: Spark session object
    
    Returns:
        None
    """
    # Read Tokyo Metro survey data
    df_passengers_metro = spark.read.json(survey_metro_file)
    df_passengers_metro = df_passengers_metro.select(
                col("@id").alias("id"),
                col("owl:sameAs").alias("survey_id"),
                col("data")["odpt:surveyYear"].alias("year"),
                col("data")["odpt:passengerJourneys"].alias("passengers"),
                explode("odpt:passengerSurveyObject").alias("data")
                )

    # Read Toei survey data
    df_passengers_toei = spark.read.json(survey_toei_file)
    df_passengers_toei = df_passengers_toei.select(
                col("@id").alias("id"),
                col("owl:sameAs").alias("survey_id"),
                col("data")["odpt:surveyYear"].alias("year"),
                col("data")["odpt:passengerJourneys"].alias("passengers"),
                explode("odpt:passengerSurveyObject").alias("data")
                )

    # Concat Tokyo Metro and Toei survey data
    df_passengers = df_passengers_metro.unionByName(df_passengers_toei)
    
    df_passengers.show()
    df_passengers.printSchema()

    # Write data into parquet files
    df_passengers.write.mode("overwrite").partitionBy('year').parquet(passengers_parquet)
    

def data_check(spark):
    """
    Description:
        The data check function.
        This runs data check queries against parquet files based on the definitions in helpers

    Argments:
        spark: Spark session object
    
    Returns:
        None
    """
    for check in checks:
        test_sql = check['test_sql']
        expected_result = check['expected_result']
        comparison = check['comparison']
        table = check['table']
        target = check['target']
        parquet_file = check['parquet']

        parquet_data = spark.read.parquet(parquet_file)
        parquet_data.createOrReplaceTempView(table)
        result = spark.sql(test_sql).first()[target]

        if comparison == '=':
            if result != expected_result:
                raise ValueError(f"Check failed: {result} {comparison} {expected_result}")
        if comparison == '!=':
            if result == expected_result:
                raise ValueError(f"Check failed: {result} {comparison} {expected_result}")
        if comparison == '>':
            if result <= expected_result:
                raise ValueError(f"Check failed: {result} {comparison} {expected_result}")
        if comparison == '<':
            if result <= expected_result:
                raise ValueError(f"Check failed: {result} {comparison} {expected_result}")
    
    
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
    
    # Process each data
    process_station_data(spark)
    process_timetable_data(spark)
    process_population_data(spark)
    process_survey_data(spark)

    # Run data check
    data_check(spark)

if __name__ == "__main__":
    main()

