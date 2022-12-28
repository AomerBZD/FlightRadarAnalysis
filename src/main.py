
"""
main.py
~~~~
This Python module contains Apache Spark ETL jobs Analyzing
FlightRadar24 Data by answering the following questions:
- Q1: What is the company with the most active flights in the world ?
- Q2: By continent, what are the companies with the most regional active flights
      (airports of Origin & Destination within the same continent) ?
- Q3: World-wide, Which active flight has the longest route ?
- Q4: By continent, what is the average route distance ?
       (flight localization by airport of origin)
- Q5.1: Which leading airplane manufacturer has the most active flights in the world ?
- Q5.2: By continent, what is the most frequent airplane model ?
        (airplane localization by airport of origin)
- Q6: By company registration country, what are the tops 3 airplanes model flying ?
- Q7.1: By continent, what airport is the most popular destination ?
- Q7.2: What airport airport has the greatest inbound/outbound flights difference ?
        (positive or negative)
- Q8: By continent, what is the average active flight speed ?
      (flight localization by airport of origin)
"""

import math
from typing import Optional
# Pyspark
from pyspark.sql import Row
from pyspark.sql.window import Window
from pyspark.sql.functions import col, split, rank, avg

# flight radar API
from FlightRadar24.api import FlightRadar24API
from FlightRadar24.flight import Flight

# Spark session management
from utils.spark import start_spark


def main():
    """Main ETL script definition.
    """
    # start Spark application and get Spark session, logger and config
    spark, log, config = start_spark(
        app_name='FR24-App',
        files=['etl_config.json'])

    # log that main ETL job is starting
    log.warn('etl_job is up-and-running')

    # execute ETL pipeline
    data = extract_data(spark, log, config)
    data_transformed = transform_data(data, log)
    load_data(data_transformed, log, config)

    # log the success and terminate Spark application
    log.warn('etl_job is finished')
    spark.stop()


def flatten_dict(data: dict, parent_key: str = '', sep: str = '_') -> dict:
    """this function flatten all nested fields of a dict recursively
    :param parent_key: name of the parent key
    :param sep: the separator to use between sub-keys
    :return: flattened dict
    """
    items = []
    for key, value in data.items():
        new_key = parent_key + sep + key if parent_key else key
        if isinstance(value, dict):
            items.extend(flatten_dict(value, new_key, sep=sep).items())
        else:
            items.append((new_key, value))
    return dict(items)


def _recursive_cast(obj: dict, type_, default_) -> dict:
    """dict recursive cast
    :param type_: the type used for cast
    :param default_: when cast is not working use this value
    :return: dict with all leaves casted into type_ type
    """
    for key, value in obj.items():
        if isinstance(value, dict):
            obj[key] = _recursive_cast(value, type_, default_)
        elif isinstance(value, list):
            obj[key] = [_recursive_cast(_, type_, default_) for _ in value]
        else:
            try:
                obj[key] = type_(value)
            except (ValueError, TypeError):
                obj[key] = default_
    return obj


def flight_details_cleaning(flight_detail: dict) -> dict:
    """Get flight detailed information and clean it
    :param flight_detail: detailed flight data
    :return: final detailed flight data
    """
    # retreive historical and images data
    flight_detail.pop("aircraft_history")
    flight_detail.pop("aircraft_images")

    # remove temporarly time details information
    time_details = flight_detail.pop("time_details")
    trail = flight_detail.pop('trail')

    # convert to string all remaining fields
    flight_detail = _recursive_cast(flight_detail, str, "N/A")

    # time details linearization
    time_details = _recursive_cast(flatten_dict(time_details, "time_details"), int, 0)
    flight_detail.update(time_details)

    # trail data conversion
    trail_keys = ["alt", "hd", "lat", "lng", "spd", "ts"]
    flight_detail.update({"trail_" + k: [_[k] for _ in trail] for k in trail_keys})

    return flight_detail


def get_flight_details(flight: Flight, login: Optional[str], password: Optional[str]) -> dict:
    """Get flight detailed information
    :param flight: retreived flight data from fr_api
    :return: A dict of detailed information of flight_id using FlightRadarAPI
    """
    fr_api = FlightRadar24API()
    if login and password:
        fr_api.login(login, password)

    details = fr_api.get_flight_details(flight.id)

    if not isinstance(details, dict):
        # payment needed to retreive these data
        # TODO: looking for workarround
        return {}

    flight.set_flight_details(details)
    flight_detail_entry = flight_details_cleaning(flight.__dict__)

    return flight_detail_entry


def extract_data(spark, log, config):
    """Load data from FlightRadar24
    :param spark: Spark session object.
    :return: Spark DataFrame.
    """
    # create API
    fr_api = FlightRadar24API()
    if "extract.frApi.login" in config and "extract.frApi.password" in config:
        log.info("logging into FlighRadar24API")
        fr_api.login(config["extract.frApi.login"], config["extract.frApi.password"])

    flights_rdd = spark.sparkContext \
            .parallelize(fr_api.get_flights()) \
            .map(lambda _: get_flight_details(_,
                                              config.get("extract.frApi.login", None),
                                              config.get("extract.frApi.password", None))) \
            .filter(bool)
    return spark.createDataFrame(flights_rdd).persist()


def transform_data(flights_details_df, log):
    """Transform original dataset.
    :param df: Input DataFrame.
    :param config: Parameters
    :return: Transformed DataFrames as a dict.
    """
    jobs_result = {}
    # ----------------------------- Job 1 ------------------------------------------
    log.info("# J1: What is the company with the most active flights in the world ?")
    jobs_result["J1"] = flights_details_df \
            .where(flights_details_df.on_ground == '0') \
            .groupby(flights_details_df.airline_icao)\
            .count()\
            .orderBy(col("count").desc())\
            .limit(1)
    

    # ----------------------------- Job 2 ------------------------------------------
    log.info("# J2:  By continent, what are the companies with the most regional active flights "
             "(airports of Origin & Destination within the same continent) ?")
    jobs_result["J2"] = flights_details_df \
            .where(flights_details_df.on_ground == '0') \
            .withColumn(
                'destination_airport_continent',
                split(flights_details_df['destination_airport_timezone_name'], '/').getItem(0)
            ) \
            .withColumn(
                'origin_airport_continent',
                split(flights_details_df['origin_airport_timezone_name'], '/').getItem(0)
            ) \
            .groupby(
                col('origin_airport_continent'),
                col('destination_airport_continent'),
                col('airline_icao')
            ) \
            .count() \
            .orderBy(col("count").desc())

    # ----------------------------- Job 3 ------------------------------------------
    log.info("# J3: World-wide, Which active flight has the longest route ?")
    def flight_set_route_distance(flight_detail):
        def distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
            # The math module contains a function named
            # radians which converts from degrees to radians.
            lon1 = math.radians(lon1)
            lon2 = math.radians(lon2)
            lat1 = math.radians(lat1)
            lat2 = math.radians(lat2)
            # Haversine formula
            dlon = lon2 - lon1
            dlat = lat2 - lat1
            a = math.sin(dlat / 2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2)**2
            c = 2 * math.asin(math.sqrt(a))
            # Radius of earth in kilometers. Use 3956 for miles
            radius = 6371
            # calculate the result
            return c * radius
        # compute and add a new column to current flight details
        trail = list(zip(flight_detail['trail_lat'], flight_detail['trail_lng']))
        trail = [_ for _ in trail if _[0] is not None and _[1] is not None]
        route_distance = sum(distance(beg[0], beg[1], end[0], end[1])
                             for beg, end in zip(trail, trail[1:]))
        return Row(**flight_detail.asDict(), route_distance=float(route_distance))

    longest_route_flight = flights_details_df \
            .select(
                "airline_name",
                "aircraft_model",
                "origin_airport_name",
                "origin_airport_timezone_name",
                "destination_airport_name",
                "destination_airport_timezone_name",
                "on_ground",
                "ground_speed",
                "trail_lat",
                "trail_lng"
            ) \
            .where(flights_details_df.on_ground == '0') \
            .rdd.map(flight_set_route_distance).toDF() \
            .orderBy(col("route_distance").desc()) \
            .drop("trail_lat", "trail_lng") \
            .persist()

    jobs_result["J3"] = longest_route_flight.limit(1)

    # ----------------------------- Job 4 ------------------------------------------
    log.info("# J4: By continent, what is the average route distance ? "
             "(flight localization by airport of origin)")
    jobs_result["J4"] = longest_route_flight \
            .withColumn(
                'origin_airport_continent',
                split(col('origin_airport_timezone_name'), '/').getItem(0)
            ) \
            .groupby(col("origin_airport_continent")) \
            .agg(avg(col("route_distance")))

    # ----------------------------- Job 5.1 -----------------------------------------
    log.info("# J5.1: Which leading airplane manufacturer has the most active "
             "flights in the world ?")
    jobs_result["J5.1"] = flights_details_df \
            .withColumn(
                'aircraft_manufacturer',
                split(flights_details_df['aircraft_model'], ' ').getItem(0)
            )\
            .groupby(col('aircraft_manufacturer')) \
            .count() \
            .orderBy(col("count").desc())
    

    # ----------------------------- Job 5.2 -----------------------------------------
    log.info("# J5.2: By continent, what is the most frequent airplane model ?"
             " (airplane localization by airport of origin)")
    jobs_result["J5.2"] = flights_details_df \
            .groupby(col('aircraft_model')) \
            .count() \
            .orderBy(col("count").desc())

    # ----------------------------- Job 6 ------------------------------------------
    log.info("# J6: By company registration country, what are the tops 3 airplanes model flying ?")
    company_registration_country_col = "origin_airport_country_code"
    airplane_model_col = "aircraft_model"
    win_spec = Window.partitionBy(company_registration_country_col).orderBy(col("count").desc())
    jobs_result["J6"] = flights_details_df \
            .groupby(col(company_registration_country_col), col(airplane_model_col)) \
            .count() \
            .withColumn("rank", rank().over(win_spec))\
            .where(col("rank") <= 3)

    # ----------------------------- Job 7.1 -----------------------------------------
    log.info("# J7.1: By continent, what airport is the most popular destination ?")
    win_spec = Window.partitionBy("destination_airport_continent").orderBy(col("count").desc())
    jobs_result["J7.1"] = flights_details_df \
            .withColumn(
                'destination_airport_continent',
                split(flights_details_df['destination_airport_timezone_name'], '/').getItem(0)
            ) \
            .groupby(col('destination_airport_continent'), col('destination_airport_name')) \
            .count() \
            .withColumn("rank", rank().over(win_spec)) \
            .where(col("rank") == 1)

    # ----------------------------- Job 7.2 -----------------------------------------
    log.info("# J7.2: What airport airport has the greatest inbound/outbound flights difference ?")
    jobs_result["J7.2"] = flights_details_df \
            .withColumnRenamed("destination_airport_name", "airport_name") \
            .groupby(col('airport_name')) \
            .count() \
            .withColumnRenamed("count", "inbound_flights") \
            .join(
                flights_details_df \
                    .withColumnRenamed("origin_airport_name", "airport_name") \
                    .groupby(col('airport_name')) \
                    .count() \
                    .withColumnRenamed("count", "outbound_flights"), \
                "airport_name",
                "inner"
            ) \
            .selectExpr(
                "airport_name",
                "inbound_flights",
                "outbound_flights",
                "(outbound_flights - inbound_flights) as flights_inout_difference") \
            .orderBy(col("flights_inout_difference").desc())

    # ----------------------------- Job 8 ------------------------------------------
    log.info("# J8: By continent, what is the average active flight speed ?"
             " (flight localization by airport of origin)")
    jobs_result["J8"] = longest_route_flight\
            .where(longest_route_flight.on_ground == '0') \
            .withColumn(
                'origin_airport_continent',
                split(col('origin_airport_timezone_name'), '/').getItem(0)
            ) \
            .groupby(col("origin_airport_continent")) \
            .agg(avg(col("ground_speed")))

    return jobs_result


def load_data(transformed_dfs_dict, log, config):
    """Collect data locally and write to CSV.

    :param df: DataFrames to print.
    :return: None
    """

    for job_name, transformed_df in transformed_dfs_dict.items():
        log.info(f"loading " + job_name + " data into csv file")
        transformed_df.show()
        transformed_df \
	     .coalesce(1) \
	     .write \
	     .csv(config.get("load.outputPath/", "output/") + job_name, mode='overwrite')


# entry point for PySpark ETL application
if __name__ == '__main__':
    main()
