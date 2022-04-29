from datetime import datetime
from functools import lru_cache

import pandas as pd
import numpy as np
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import SparkSession
from sklearn.linear_model import LinearRegression


@lru_cache(maxsize=None)
def get_spark(name="covid") -> SparkSession:
    return SparkSession.builder.appName(name).getOrCreate()


def read_data(path="data/covid/usa_covid_cases.json", prefix='_'):
    """
    Reads json data and converts columns starting with `prefix` to int.
    Data from bigquery-public-data:covid19_usafacts confirmed_cases, and deaths
    """
    df = get_spark().read.json(path)
    date_columns = [c for c in df.columns if c.startswith(prefix)]
    other_columns = list(set(df.columns) - set(date_columns))
    columns = other_columns + [F.col(c).cast("int").alias(c) for c in date_columns]
    return df.select(*columns)


def read_data_slow_withColumn(path="data/covid/usa_covid_cases.json"):
    """
    Reads json data and converts columns starting with _ to int.
    Very slow because of the use of withColumn.
    Compare to read_data
    """
    df = get_spark().read.json(path)
    for c in [c for c in df.columns if c.startswith("_")]:
        df = df.withColumn(c, F.col(c).cast("int").alias(c))
    return df


def normalize_covid(df, prefix='_'):
    """
    Normalizes covid DataFrames of bigquery-public-data:covid19_usafacts confirmed_cases, and deaths.
    Converts many date columns to rows of dates with their associated count values.
    """
    date_columns = [c for c in df.columns if c.startswith(prefix)]
    other_columns = list(set(df.columns) - set(date_columns))
    # Transform columns of dates into rows of dates.
    # Date columns headings are converted to a literal column array.
    # Date values are placed into an array
    # The dates and values are zipped and then exploded into multiple rows.
    # https://stackoverflow.com/questions/41027315/pyspark-split-multiple-array-columns-into-rows
    columns = other_columns + [F.explode(F.arrays_zip(F.array([F.lit(c[1:]) for c in date_columns]).alias('date'),
                                                      F.array(*date_columns).alias('counts'))).alias('dc')]
    df = df.select(*columns)
    # Get the date and column from the zipped structured. Convert the date from string to date.
    columns = other_columns + [F.to_date(df.dc.date, 'yyyy_MM_dd').alias('date'), df.dc.counts.alias('counts')]
    return df.select(*columns)


def columns_as_dates(df):
    return [datetime.strptime(c[1:], '%Y_%m_%d') for c in df.columns if c.startswith("_")]


# TODO complete this code
def create_dataset():
    """
    Prepare the given data for analysis.
    :return: one `DataFrame` with all the data.
    """
    # Read the data files, normalize each data (normalize_covid), and rename each counts
    cases = read_data("data/covid/usa_covid_cases.json")
    cases = normalize_covid(cases).withColumnRenamed('counts', 'cases')
    deaths = read_data("data/covid/usa_covid_deaths.json")
    deaths = normalize_covid(deaths).withColumnRenamed('counts', 'deaths')
    # Join into one DataFrame
    df = cases.join(deaths, ['county_fips_code', 'state', 'date'])
    # Drop duplicate columns
    return df.drop(deaths.state_fips_code).drop(deaths.county_name)


# Better way to convert to timestamp?
# https://stackoverflow.com/questions/11865458/how-to-get-unix-timestamp-from-numpy-datetime64
def date_to_timestamp(dates: pd.Series) -> pd.Series:
    """
    Convert from pandas Series of PySpark date format to days since unix, January 1, 1970 (midnight UTC/GMT)
    """
    # Timestamp produces dtype: datetime64[ns]
    return dates.apply(lambda v: pd.Timestamp(v)).astype('int64') / 1e9 / 86400


# TODO Like Listing 9.4
# This can and should be done without a UDF; however, this demonstrates simple UDF.
@F.pandas_udf(T.DoubleType())
def death_case_ratio(cases : pd.Series , deaths:pd.Series)->pd.Series:
    """Returns the deaths / cases as ratio."""
    return deaths/cases


# TODO Like Listing 9.8
@F.pandas_udf(T.DoubleType())
def daily_rate_of_change(dates: pd.Series, cases: pd.Series)->float():
    # date_to_timestamp
    dates = date_to_timestamp(dates)
    return LinearRegression().fit(X=dates.astype(int).values.reshape(-1, 1), y=cases).coef_[0]



def main():
    show_num = 5
    data = create_dataset()
    # Partition data by code, and within those partitions, sort by date (ascending)
    df = data.repartition('county_fips_code').sortWithinPartitions('date')
    # See what it looks like
    print("Partitioned and sorted data")
    df.where("county_fips_code = 13071").show()

    # TODO Apply simple UDF, like Listing 9.5
    # TODO Show the state, county_name, and county_fips_code in all output
    print("Death case ratio, worst cases")
    # TODO df.select(...)
    (df
     .where(F.col("county_fips_code") == '26005')
     .withColumn("dr_ratio", F.when(F.col("cases") != 0, death_case_ratio("cases", "deaths")).otherwise(np.inf))
     .select("state", "county_name", "county_fips_code", "dr_ratio")
     .orderBy("dr_ratio", ascending=False)
     ).show(show_num)

    # Local pandas data for testing & debugging
    pdf = data.select('date', 'cases', 'deaths').where("county_fips_code = 13071").toPandas()
    print("Sample daily case rate change")
    daily_rate_of_change.func(pdf['date'], pdf['cases'])

    # TODO Like Listing 9.9
    print("Daily rate of change for cases")
    # TODO df.groupBy(...)
    (df
     .groupBy("state", "county_name", "county_fips_code")
     .agg(daily_rate_of_change("date", "cases").alias("rt_case_chg"))
     ).show(show_num)

    print("Least rate of change")
    # TODO
    (df
     .groupBy("state", "county_name", "county_fips_code")
     .agg(daily_rate_of_change("date", "cases").alias("rt_case_chg"))
     .orderBy("rt_case_chg", ascending=True)
     ).show(show_num)

    print("Most rate of change")
    # TODO
    (df
     .groupBy("state", "county_name", "county_fips_code")
     .agg(daily_rate_of_change("date", "cases").alias("rt_case_chg"))
     .orderBy("rt_case_chg", ascending=False)
     ).show(show_num)

    print("Daily rate of change for deaths")
    # TODO df.groupBy(...)

(df
     .groupBy("state", "county_name", "county_fips_code")
     .agg(daily_rate_of_change("date", "deaths").alias("rt_death_chg"))
     ).show(show_num)

    print("Least rate of change")
    # TODO
    (df
     .groupBy("state", "county_name", "county_fips_code")
     .agg(daily_rate_of_change("date", "deaths").alias("rt_death_chg"))
     .orderBy("rt_death_chg", ascending=True)
     ).show(show_num)

    print("Most rate of change")
    # TODO
    (df
     .groupBy("state", "county_name", "county_fips_code")
     .agg(daily_rate_of_change("date", "deaths").alias("rt_death_chg"))
     .orderBy("rt_death_chg", ascending=False)
     ).show(show_num)


if __name__ == '__main__':
    main()
