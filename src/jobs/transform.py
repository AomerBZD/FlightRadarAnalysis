"""
transform
~~~~~~~
This module contains pyspark functions that transform input dataframe in order
to produce data used later for analysis.
"""
import math
from pyspark.sql import Row
from pyspark.sql.window import Window
from pyspark.sql.functions import col, split, rank, avg


def job1_most_active_flights(flights_details_df):
    """
    :param flights_details_df: Input DataFrame.
    :return: Transformed DataFrames.
    """
    job_result = flights_details_df \
            .where((flights_details_df.on_ground == '0') & \
                   (flights_details_df.airline_icao != 'N/A')) \
            .groupby(flights_details_df.airline_icao)\
            .count()\
            .orderBy(col("count").desc())\
            .limit(1)

    job_result.show()
    return job_result


def job2_by_continent_most_regional_active_flight(flights_details_df):
    """
    :param flights_details_df: Input DataFrame.
    :return: Transformed DataFrames.
    """
    job_result = flights_details_df \
            .where((flights_details_df.on_ground == '0') & \
                   (flights_details_df.airline_icao != 'N/A')) \
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

    job_result.show()
    return job_result


def job3_route_distance_desc(flights_details_df):
    """
    :param flights_details_df: Input DataFrame.
    :return: Transformed DataFrames.
    """
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

    longest_route_flight.show()
    return longest_route_flight


def job4_by_continent_average_route_distance(longest_route_flight):
    """
    :param flights_details_df: Input DataFrame.
    :return: Transformed DataFrames.
    """
    job_result = longest_route_flight \
            .withColumn(
                'origin_airport_continent',
                split(col('origin_airport_timezone_name'), '/').getItem(0)
            ) \
            .groupby(col("origin_airport_continent")) \
            .agg(avg(col("route_distance")))

    job_result.show()
    return job_result


def job5_1_leading_airplane_manufacturer(flights_details_df):
    """
    :param flights_details_df: Input DataFrame.
    :return: Transformed DataFrames.
    """
    job_result = flights_details_df \
            .withColumn(
                'aircraft_manufacturer',
                split(flights_details_df['aircraft_model'], ' ').getItem(0)
            )\
            .groupby(col('aircraft_manufacturer')) \
            .count() \
            .orderBy(col("count").desc())

    job_result.show()
    return job_result


def job5_2_by_continent_most_frequent_airplane_model(flights_details_df):
    """
    :param flights_details_df: Input DataFrame.
    :return: Transformed DataFrames.
    """
    job_result = flights_details_df \
            .groupby(col('aircraft_model')) \
            .count() \
            .orderBy(col("count").desc())

    job_result.show()
    return job_result


def job6_by_company_regist_country_top_airplane_model(flights_details_df):
    """
    :param flights_details_df: Input DataFrame.
    :return: Transformed DataFrames.
    """
    company_registration_country_col = "origin_airport_country_code"
    airplane_model_col = "aircraft_model"

    win_spec = Window.partitionBy(company_registration_country_col).orderBy(col("count").desc())

    job_result = flights_details_df \
            .groupby(col(company_registration_country_col), col(airplane_model_col)) \
            .count() \
            .withColumn("rank", rank().over(win_spec))\
            .where(col("rank") <= 3)

    job_result.show()
    return job_result


def job7_1_by_continent_most_popular_destination(flights_details_df):
    """
    :param flights_details_df: Input DataFrame.
    :return: Transformed DataFrames.
    """
    win_spec = Window.partitionBy("destination_airport_continent").orderBy(col("count").desc())

    job_result = flights_details_df \
            .withColumn(
                'destination_airport_continent',
                split(flights_details_df['destination_airport_timezone_name'], '/').getItem(0)
            ) \
            .groupby(col('destination_airport_continent'), col('destination_airport_name')) \
            .count() \
            .withColumn("rank", rank().over(win_spec)) \
            .where(col("rank") == 1)

    job_result.show()
    return job_result


def job7_2_greatest_inbound_outbount_flights_difference(flights_details_df):
    """
    :param flights_details_df: Input DataFrame.
    :return: Transformed DataFrames.
    """
    job_result = flights_details_df \
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

    job_result.show()
    return job_result


def job8_by_continent_average_active_flight_speed(longest_route_flight):
    """
    :param flights_details_df: Input DataFrame.
    :return: Transformed DataFrames.
    """
    job_result = longest_route_flight \
            .where(longest_route_flight.on_ground == '0') \
            .withColumn(
                'origin_airport_continent',
                split(col('origin_airport_timezone_name'), '/').getItem(0)
            ) \
            .groupby(col("origin_airport_continent")) \
            .agg(avg(col("ground_speed")))

    job_result.show()
    return job_result

