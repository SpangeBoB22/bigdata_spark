from unittest import TestCase

from app.data_reader import DataReader
from app.eda import DataExplorer

from app.constants import *

import matplotlib.pyplot as plt
import pathlib

DROPOFF_MAP_PATH = './drop_off.html'
PICKUP_MAP_PATH = './pickup.html'


class TestDataExploration(TestCase):

    def setUp(self):
        self._df_trip = DataReader.read_parquet('F:/parquet_sample/trip')
        self._df_fare = DataReader.read_parquet('F:/parquet_sample/fare')
        self._data_explorer = DataExplorer(df_trip=self._df_trip,
                                           df_fare=self._df_fare)

        self._trip_january = DataReader.read_the_data('F:/spark_data/data/trip_data_1.csv',
                                                      schema=TRIP_SCHEMA)
        self._fare_january = DataReader.read_the_data('F:/spark_data/fare/trip_fare_1.csv',
                                                      schema=FARE_SCHEMA)
        self._short_data_explorer = DataExplorer(df_trip=self._trip_january,
                                                 df_fare=self._fare_january)

    def test_essentials(self):
        DataExplorer.print_essential_data(self._df_trip)
        DataExplorer.print_essential_data(self._df_fare)

    def test_explore_payment_type(self):
        payment_total_agg, payment_count_agg = self._data_explorer.explore_payment_type()
        self.assertLess(0, payment_total_agg.shape[0])
        self.assertLess(0, payment_count_agg.shape[0])

        fig, axes = plt.subplots(1, 2, figsize=(14, 6))

        # Average cost per trip graph
        axes[0].bar(payment_total_agg['payment_type'], payment_total_agg['avg_total'])
        axes[0].set_title('Average cost of a trip by type of payment')
        axes[0].set_xlabel('Type of payment')
        axes[0].set_ylabel('Average cost of travel')

        # Pie chart of the distribution of the number of trips
        axes[1].pie(payment_count_agg['count'], labels=payment_count_agg['payment_type'], autopct='%1.1f%%')
        axes[1].set_title('Distribution of number of trips by type of payment')

        plt.tight_layout()
        plt.show()

    def test_trip_by_periods(self):
        trips_by_month, avg_trips_by_dayofweek = self._data_explorer.trips_days_and_month()
        self.assertLess(0, trips_by_month.shape[0])
        self.assertLess(0, avg_trips_by_dayofweek.shape[0])

        _, ax = plt.subplots(1, 2, figsize=(16, 5))

        ax[0].plot(trips_by_month['month'], trips_by_month['count'])
        ax[0].set_title('Number of trips by month')
        ax[0].set_xlabel('Month')
        ax[0].set_ylabel('Number of trips')
        ax[0].set_xticks(trips_by_month['month'])

        plt.bar(avg_trips_by_dayofweek['day_of_week'], avg_trips_by_dayofweek['avg_trips'])
        ax[1].set_title('Number of trips by day of the week')
        ax[1].set_xlabel('Day of the week')
        ax[1].set_ylabel('Number of trips')
        ax[1].set_xticks(avg_trips_by_dayofweek['day_of_week'], ['Mo', 'Tu', 'We', 'Th', 'Fr', 'Sa', 'Su'])

        plt.show()

    def test_travel_time_on_distance(self):
        dependence_df = self._data_explorer.travel_time_on_distance()
        self.assertLess(0, dependence_df.shape[0])

        _, ax = plt.subplots(1, 1, figsize=(12, 6))
        ax.plot(dependence_df['distance'], dependence_df['avg_time'], marker='o')
        ax.set_title('Dependence of travel time on distance')
        ax.set_xlabel('Travel distance (miles)')
        ax.set_ylabel('Trip duration (seconds)')
        plt.show()

    def test_make_heatmaps(self):
        self._data_explorer.make_heat_maps(PICKUP_MAP_PATH,
                                           DROPOFF_MAP_PATH)
        self.assertTrue(pathlib.Path(PICKUP_MAP_PATH))
        self.assertTrue(pathlib.Path(DROPOFF_MAP_PATH))

    def test_fare_from_distance(self):
        pd_df = self._short_data_explorer.fare_from_distance()
        self.assertLess(0, pd_df.shape[0])

        # Sorting data by distance
        pd_df = pd_df.sort_values(by="round(trip_distance, 1)")

        # Calculation of moving average
        window_size = 5
        pd_df["avg_fare_smooth"] = pd_df["avg_fare"].rolling(window=window_size).mean()

        # Graphing
        plt.figure(figsize=(10, 6))
        plt.plot(pd_df["round(trip_distance, 1)"], pd_df["avg_fare"], marker='o', alpha=0.3, label='Raw Data')
        plt.plot(pd_df["round(trip_distance, 1)"], pd_df["avg_fare_smooth"], marker='o', color='red',
                 label='Smoothed Data')
        plt.title('Average Fare vs. Trip Distance (Smoothed)')
        plt.xlabel('Trip Distance (Rounded)')
        plt.ylabel('Average Fare ($)')
        plt.legend()
        plt.grid(True)
        plt.show()

    def test_avg_time_from_dat_time(self):
        hourly_avg_pd = self._data_explorer.average_speed_from_day_hour()

        plt.figure(figsize=(12, 6))
        plt.plot(hourly_avg_pd['hour_of_day'], hourly_avg_pd['avg_time_per_mile'], marker='o')
        plt.title('Average Time per Mile vs. Hour of Day')
        plt.xlabel('Hour of Day')
        plt.ylabel('Average Time per Mile (seconds)')
        plt.grid(True)
        plt.show()




