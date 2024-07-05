import os
from itertools import combinations,permutations
from collections import Counter
from dotenv import load_dotenv
from pyspark import RDD, SparkContext
from pyspark.sql import DataFrame, SparkSession

### install dependency ###
# pip install python-dotenv
# pip install pyspark # make sure you have jdk installed
#####################################

### please update your relative path while running your code ###
temp_airline_textfile = r"/Users/raemonx/Desktop/COMP_6231/assignment_4/flights_data.txt"
temp_airline_csvfile = r"/Users/raemonx/Desktop/COMP_6231/assignment_4/flights_data_reduced.csv"
default_spark_context = "local[*]"  # only update if you need
#######################################


### please don't update these lines ###
load_dotenv()
airline_textfile = os.getenv("AIRLINE_TXT_FILE", temp_airline_textfile)
airline_csvfile = os.getenv("AIRLINE_CSV_FILE", temp_airline_csvfile)
spark_context = os.getenv("SPARK_CONTEXT", default_spark_context)
#######################################


def co_occurring_airline_pairs_by_origin(flights_data: RDD) -> RDD:
    """
    Takes an RDD that represents the contents of the flights_data from txt file. Performs a series of MapReduce operations via PySpark
    to calculate the number of co-occurring airlines with same origin airports operating on the same date, determine count of such occurrences pairwise.
    Returns the results as an RDD sorted by the airline pairs alphabetically ascending (by first and then second value in the pair) with the counts in descending order.
    :param flights_dat: RDD object of the contents of flights_data
    :return: RDD of pairs of airline and the number of occurrences
        Example output:     [((Airline_A,Airline_B),3),
                                ((Airline_A,Airline_C),2),
                                ((Airline_B,Airline_C),1)]
    """
    #take the flight data and split
    #Output Type: tuple
    #Output Key-Value Pair Type: There's no explicit key-value pair here; it's transforming each line into a tuple.
    flights_data = flights_data.map(lambda line: tuple(line.split(',')))

    #Output Type: tuple
    #Output Key-Value Pair Type: ((str, str, str), int)
    airline = flights_data.map(lambda x: ((x[0], x[2], x[1]), 1))

    #Output Key - Value Pair Type: ((str, str, str), int)
    airline_counts = airline.reduceByKey(lambda x, y: x + y)

    #Output Key-Value Pair Type: ((str, str), (str, int))
    airline_pairs = airline_counts.map(lambda x: ((x[0][0], x[0][1]), (x[0][2], x[1])))

    #Output Key-Value Pair Type: ((str, str), Iterable)
    airline_pairs_group = airline_pairs.groupByKey()

    #Output Key-Value Pair Type: ((str, str), (str, str))
    airline_pairs_permute = airline_pairs_group.flatMap(lambda x: permutations(sorted(list(x[1])), 2))

    #Output Key-Value Pair Type: ((str, str), int)
    airline_pairs_min = airline_pairs_permute.map(lambda x: ((x[0][0], x[1][0]), min(x[0][1], x[1][1])))

    #Output Key-Value Pair Type: ((str, str), int)
    airline_pairs_unique = airline_pairs_min.filter(lambda x: x[0][0] < x[0][1])

    #Output Key-Value Pair Type: ((str, str), int)
    counts = airline_pairs_unique.reduceByKey(lambda c, d: c + d)

    return counts
def air_flights_most_canceled_flights(flights: DataFrame) -> str:
    """
    Takes the flight data as a DataFrame and finds the airline that had the most canceled flights on Sep. 2021
    :param flights: Spark DataFrame of the flights CSV file.
    :return: The name of the airline with most canceled flights on Sep. 2021.
    """
    canceled_flights = flights.filter(flights["Cancelled"] == "True")
    canceled_flights_september = canceled_flights.filter(canceled_flights["Month"] == 9)
    canceled_flights_september_2021 = canceled_flights_september.filter(canceled_flights_september["Year"] == 2021)

    airline_most_canceled = canceled_flights_september_2021.groupBy("Airline").count()
    airline_most_canceled = airline_most_canceled.orderBy("count", ascending=False)
    answer = airline_most_canceled.first()
    return answer["Airline"]

def air_flights_diverted_flights(flights: DataFrame) -> int:
    """
    Takes the flight data as a DataFrame and calculates the number of flights that were diverted in the period of
    20-30 Nov. 2021.
    :param flights: Spark DataFrame of the flights CSV file.
    :return: The number of diverted flights between 20-30 Nov. 2021.
    """
    diverted_flights = flights.filter(flights["Diverted"] == "True")
    diverted_flights_day = diverted_flights.filter((diverted_flights["DayofMonth"] >= 20) & (diverted_flights["DayofMonth"] <= 30))
    diverted_flights_month = diverted_flights_day.filter(diverted_flights["Month"] == 11)
    diverted_flights_year = diverted_flights_month.filter(diverted_flights["Year"] == 2021)
    diverted_flights_count = diverted_flights_year.count()

    return diverted_flights_count


def air_flights_avg_airtime(flights: DataFrame) -> float:
    """
    Takes the flight data as a DataFrame and calculates the average airtime of the flights from Nashville, TN to
    Chicago, IL.
    :param flights: Spark DataFrame of the flights CSV file.
    :return: The average airtime of the flights from Nashville, TN to
    Chicago, IL.
    """

    Nashville_flights = flights.filter(flights['OriginCityName'] == 'Nashville, TN')
    Nashville_to_Chicago_flights = Nashville_flights.filter(Nashville_flights['DestCityName'] == 'Chicago, IL')
    Nashville_to_ORD_flights_2021 = Nashville_to_Chicago_flights.filter(Nashville_to_Chicago_flights['Year'] == 2021)
    select_avg_airtime = Nashville_to_ORD_flights_2021.select('AirTime')
    avg_airtime = select_avg_airtime.agg({'AirTime': 'avg'}).collect()[0][0]
    return avg_airtime


def air_flights_missing_departure_time(flights: DataFrame) -> int:
    """
    Takes the flight data as a DataFrame and find the number of unique dates where the departure time (DepTime) is
    missing.
    :param flights: Spark DataFrame of the flights CSV file.
    :return: the number of unique dates where DepTime is missing.
    """
    missing_depttime = flights.filter(flights["DepTime"].isNull())
    unique_missing = missing_depttime.select("Year", "Month", "DayofMonth")
    unique_missing_count = unique_missing.distinct().count()

    return unique_missing_count


def main():
    # initialize SparkContext and SparkSession
    sc = SparkContext(spark_context)
    spark = SparkSession.builder.getOrCreate()

    print("########################## Problem 1 ########################")
    # problem 1: co-occurring operating flights with Spark and MapReduce
    # read the file
    flights_data = sc.textFile(airline_textfile)
    sorted_airline_pairs = co_occurring_airline_pairs_by_origin(flights_data)
    sorted_airline_pairs.persist()
    for pair, count in sorted_airline_pairs.take(10):
        print(f"{pair}: {count}")

    print("########################## Problem 2 ########################")
    # problem 2: PySpark DataFrame operations
    # read the file
    flights = spark.read.csv(airline_csvfile, header=True, inferSchema=True)
    print(
        "Q1:",
        air_flights_most_canceled_flights(flights),
        "had the most canceled flights in September 2021.",
    )
    print(
        "Q2:",
        air_flights_diverted_flights(flights),
        "flights were diverted between the period of 20th-30th " "November 2021.",
    )
    print(
        "Q3:",
        air_flights_avg_airtime(flights),
        "is the average airtime for flights that were flying from "
        "Nashville to Chicago.",
    )
    print(
        "Q4:",
        air_flights_missing_departure_time(flights),
        "unique dates where departure time (DepTime) was " "not recorded.",
    )


if __name__ == "__main__":
    main()
