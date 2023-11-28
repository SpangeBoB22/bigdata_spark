from pathlib import Path
from typing import Tuple

import folium
import pandas as pd
from folium.plugins import HeatMap
from pandas import DataFrame as PandasFrame
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import col, avg, dayofweek, date_trunc, lit, floor, mean, month, broadcast, round, hour

NY_CENTER_LOCATION = [40.7128, -74.0060]


class DataExplorer:

    def __init__(self,
                 df_trip: DataFrame,
                 df_fare: DataFrame):
        self._df_trip = df_trip
        self._df_fare = df_fare

    @staticmethod
    def print_essential_data(df: DataFrame) -> None:
        """
        Prints the essential data into the console.

        :param df: the dataframe instance
        :return: None
        """
        print(f'Number of rows: {df.count()}')
        print(f'Number of columns: {len(df.columns)}')
        print(f'Columns list: {df.columns}')
        df.printSchema()

    def explore_payment_type(self) -> Tuple[PandasFrame, PandasFrame]:
        """
        Forms diagrams of distribution of payment types and amount of payment depending on its type.

        :return: Pandas dataframes with aggregated data the data can be used for a visualization.
        """
        # Aggregation by type of payment and average total cost of travel
        payment_total_agg = self._df_fare.groupBy("payment_type").agg(avg("total_amount").alias("avg_total")).toPandas()

        # Aggregation to count the number of trips by payment type
        payment_count_agg = self._df_fare.groupBy("payment_type").count().toPandas()

        return payment_total_agg, payment_count_agg

    def trips_days_and_month(self) -> Tuple[PandasFrame, PandasFrame]:
        """
        Forms the dependence of the number of trips on the day of the week and months of the year.

        :return: Pandas dataframes with aggregated data the data can be used for a visualization.
        """
        df_trip_with_month = self._df_trip.withColumn("month", month("pickup_datetime"))

        # Grouping and counting the number of trips by month
        trips_by_month = df_trip_with_month.groupBy("month").count().orderBy("month").toPandas()

        # Retrieve day of week and date from pickup_datetime
        df_trip_with_dayofweek = self._df_trip.withColumn("day_of_week", dayofweek("pickup_datetime")) \
            .withColumn("date", date_trunc("day", "pickup_datetime"))

        # Grouping by day of the week and calculating total trips and unique dates
        trips_by_dayofweek = df_trip_with_dayofweek.groupBy("day_of_week") \
            .agg(F.count("*").alias("total_trips"),
                 F.countDistinct("date").alias("unique_dates"))

        # Calculation of the average number of trips for each day of the week and explicit sorting by day_of_week
        avg_trips_by_dayofweek = trips_by_dayofweek.withColumn("avg_trips", col("total_trips") / col("unique_dates")) \
            .orderBy("day_of_week").toPandas()

        return trips_by_month, avg_trips_by_dayofweek

    def travel_time_on_distance(self) -> PandasFrame:
        """
        Calculates the dependence of the average travel time on the distance of the trip.

        :return: Pandas dataframe with aggregated data the data can be used for a visualization.
        """
        # Filtering by number of passengers
        df_filtered_passengers = self._df_trip.filter(self._df_trip.passenger_count <= 9)

        # Calculation of quartiles and IQR for trip_time_in_secs and trip_distance
        quantiles_time = df_filtered_passengers.approxQuantile("trip_time_in_secs",
                                                               [0.25, 0.75],
                                                               0.05)
        quantiles_distance = df_filtered_passengers.approxQuantile("trip_distance",
                                                                   [0.25, 0.75],
                                                                   0.05)

        IQR_time = quantiles_time[1] - quantiles_time[0]
        IQR_distance = quantiles_distance[1] - quantiles_distance[0]

        lower_bound_time = quantiles_time[0] - 1.5 * IQR_time
        upper_bound_time = quantiles_time[1] + 1.5 * IQR_time

        lower_bound_distance = quantiles_distance[0] - 1.5 * IQR_distance
        upper_bound_distance = quantiles_distance[1] + 1.5 * IQR_distance

        # Outliers filtration
        df_clean = df_filtered_passengers.filter(
            (col("trip_time_in_secs") >= lit(lower_bound_time)) &
            (col("trip_time_in_secs") <= lit(upper_bound_time)) &
            (col("trip_distance") >= lit(lower_bound_distance)) &
            (col("trip_distance") <= lit(upper_bound_distance))
        )

        # Creating distance intervals in 0.1 mile increments
        df_with_intervals = df_clean.withColumn("distance_interval",
                                                floor(df_clean["trip_distance"] * 10) / 10)

        # Grouping data by distance intervals and averaging travel time
        avg_data = df_with_intervals.groupBy("distance_interval").agg(
            mean("trip_time_in_secs").alias("avg_time")).orderBy("distance_interval")

        # Data collection for visualization
        result = avg_data.collect()

        # Splitting the data for plotting the graph
        return pd.DataFrame({'distance': [row['distance_interval'] for row in result],
                             'avg_time': [row['avg_time'] for row in result]})

    def make_heat_maps(self,
                       path_to_pickup_map: str | Path,
                       path_to_drop_off_map: str | Path) -> None:
        """
        Plots heatmaps of pickups and dropoffs locations. Saves them into the HTML's.

        :param path_to_pickup_map: saving path for pickups heatmap
        :param path_to_drop_off_map: saving path for dropoffs heatmap
        :return: None
        """
        # Rounding coordinates to a certain number of decimal places
        precision = 3
        df = self._df_trip.withColumn("pickup_latitude_round",
                                      round(self._df_trip["pickup_latitude"],
                                            precision)) \
            .withColumn("pickup_longitude_round",
                        round(self._df_trip["pickup_longitude"],
                              precision)) \
            .withColumn("dropoff_latitude_round",
                        round(self._df_trip["dropoff_latitude"],
                              precision)) \
            .withColumn("dropoff_longitude_round",
                        round(self._df_trip["dropoff_longitude"],
                              precision))

        df_filtered = df.filter(
            (df.dropoff_latitude.isNotNull()) &
            (df.dropoff_longitude.isNotNull()) &
            (df.dropoff_latitude != 0) &
            (df.dropoff_longitude != 0)
        )

        # Data aggregation
        pickup_data = df_filtered.groupBy("pickup_latitude_round",
                                          "pickup_longitude_round").count()
        dropoff_data = df_filtered.groupBy("dropoff_latitude_round",
                                           "dropoff_longitude_round").count()

        # Data collection for heat maps
        pickup_points = pickup_data.collect()
        dropoff_points = dropoff_data.collect()

        # Preparation of data for heat maps
        pickup_map_data = [[row['pickup_latitude_round'],
                            row['pickup_longitude_round'],
                            row['count']] for row in
                           pickup_points]
        dropoff_map_data = [[row['dropoff_latitude_round'],
                             row['dropoff_longitude_round'],
                             row['count']] for row in
                            dropoff_points]

        # Creating base maps of New York City
        map_pickup = folium.Map(location=NY_CENTER_LOCATION, zoom_start=11)
        map_dropoff = folium.Map(location=NY_CENTER_LOCATION, zoom_start=11)

        # Adding heat maps
        HeatMap(pickup_map_data).add_to(map_pickup)
        HeatMap(dropoff_map_data).add_to(map_dropoff)

        # Saving maps
        map_pickup.save(path_to_pickup_map)
        map_dropoff.save(path_to_drop_off_map)

    def fare_from_distance(self) -> PandasFrame:
        """
        Builds the dataframe, describing the dependence of the average cost of a trip on distance.

        :return: the Pandas dataframe which can be used for visualization.
        """
        if self._df_trip.count() < self._df_fare.count():
            joined_df = self._df_fare.join(broadcast(self._df_trip), "medallion")
        else:
            joined_df = self._df_trip.join(broadcast(self._df_fare), "medallion")

        fare_distance_df = joined_df.groupBy(round("trip_distance", 1)).agg(
            mean("fare_amount").alias("avg_fare")).orderBy("round(trip_distance, 1)")

        return fare_distance_df.toPandas()

    def average_speed_from_day_hour(self) -> PandasFrame:
        """
        The method calculates the average time spent per mile of travel and determines its dependence on the hour (time of day).

        :return: the Pandas dataframe which can be used for visualization
        """
        df_filtered = self._df_trip.filter(col("trip_time_in_secs") <= 7200)
        df = df_filtered.withColumn("hour_of_day", hour(col("pickup_datetime")))
        df = df.withColumn("time_per_mile", col("trip_time_in_secs") / col("trip_distance"))
        hourly_avg = df.groupBy("hour_of_day").agg(mean("time_per_mile").alias("avg_time_per_mile"))
        hourly_avg = hourly_avg.orderBy("hour_of_day")

        return hourly_avg.toPandas()

