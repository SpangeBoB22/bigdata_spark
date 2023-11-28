NYC Taxi dataset - exploration with Spark
=========================================

The project is an introduction to analyzing data with Spark. It uses PySpark and the necessary Hadoop libraries to increase the speed of data processing.

**The project:**

- reading csv format and writing to parquet for further work
- printing basic characteristics of the dataset to the console
- data exploration using Spark capabilities.


**The following dependencies are investigated as part of data exploration:**

- Distribution of number of trips by payment type and average cost of a trip depending on payment types
- Dependence of the number of trips on the day of the week and month of the year
- Dependence of travel time on travel distance
- heat maps of the number of trips (overlaid on a geographical map)
- Dependence of trip cost on distance
- average time to travel 1 mile depending on the time of day

.. toctree::
   :maxdepth: 2

   structure_and_usage
   api_reference

