# Databricks notebook source
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
import pyspark.sql.functions as F


# COMMAND ----------

# MAGIC %sh
# MAGIC ls

# COMMAND ----------

def get_spark() -> SparkSession:
    return SparkSession.builder.appName("word_freqs").getOrCreate()
spark=get_spark()
spark.version

# COMMAND ----------

Items_list=[['Banana',2,1.74],
    ["Apple", 4, 2.04],
    ["Carrot", 1, 1.09],
    ["Cake", 1, 10.99]
   ]

# COMMAND ----------

df_items=spark.createDataFrame(Items_list,["Items","Quantity","Price"])

# COMMAND ----------

df_items=df_items.select(*df_items.columns)
df_items.where(f.col("items")=="Banana").count()
df_items.show()

# COMMAND ----------

df=df_items.select(f.array([f.lit(col) for col in df_items.columns]).alias("a"),f.array("items","Quantity","Price").alias("b"))
df.show()

# COMMAND ----------

df1=df.select(f.map_from_arrays("a","b").alias("arraymap"))
display(df1)

# COMMAND ----------

df1.write.json("tetestprep")


# COMMAND ----------

# MAGIC %sh
# MAGIC ls

# COMMAND ----------

df1.select("arraymap.Items").show()
df2=df1.withColumn("structcol",f.col("arraymap").cast("struct"))

# COMMAND ----------

df2.show()

# COMMAND ----------



# COMMAND ----------

df=df_items.select(f.array("Items","Quantity").alias("I-Q"),"Price")
df.show()

# COMMAND ----------



# COMMAND ----------

df.select(f.size("I-Q"),"Price").show()

# COMMAND ----------

df.select("I-Q","Price",f.array([f.lit(c) for c  in df_items.columns]).alias("Columnarray")).show(10)

# COMMAND ----------

df1=df_items.select(f.array("Items","Quantity","Price").alias("values"),f.array([f.lit(c) for c  in df_items.columns]).alias("keys"))
df1.show()

# COMMAND ----------

df2=df1.select(f.arrays_zip("values","keys").alias("arrayzip"))
display(df2)


# COMMAND ----------

df2=df1.select(f.map_from_arrays("keys","values").alias("store"))

# COMMAND ----------

df2.select("store.Quantity").show()
#-col col Quantity

# COMMAND ----------

df_items.printSchema()

# COMMAND ----------

  #/dbfs/FileStore/Periodic_Table_Of_Elements.csv\
elements = spark.read.csv("/FileStore/Periodic_Table_Of_Elements.csv",
header=True,
inferSchema=True,
)
display(elements)

# COMMAND ----------

(elements
         .filter(f.col("Phase")=="liq")
         .groupBy("period")
         .agg({"Period":"count"})
         .select("Period","count(Period)")
).show(12)

# COMMAND ----------

elements.createOrReplaceTempView("elements")
elements.createOrReplaceTempView("DBTABLE")

# COMMAND ----------

# %sql
# -- spark.catalog.listTables()
# -- select Period from elements
# -- select period, count(*) from elements where phase='liq' group by period
spark.sql("select period, count(*) from elements where phase='liq' group by period").show(5)

# COMMAND ----------

# MAGIC %sql
# MAGIC select period, count(*) from elements where phase="liq" group by period

# COMMAND ----------

# spark.catalog.dropTempView("elements")
spark.catalog.listTables()

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended dbtable

# COMMAND ----------

collection = [1, "two", 3.0, ("four", 4), {"five": 5}]

# COMMAND ----------

sc = spark.sparkContext

# COMMAND ----------

collection_rdd = sc.parallelize(collection)

# COMMAND ----------

print(collection_rdd.collect())
print(collection_rdd.getNumPartitions())

# COMMAND ----------

def add_one(value):
    try:
        return  value+1
    except:
        return Value

# COMMAND ----------

collection_rdd = collection_rdd.map(add_one)

# COMMAND ----------

print(collection_rdd.collect())

# COMMAND ----------

collection_rdd = collection_rdd.filter(
lambda elem: isinstance(elem, (float, int))
)
print(collection_rdd.collect())

# COMMAND ----------

a_rdd = sc.parallelize([0, 1, None, [], 0.0],2)
data=a_rdd.filter(lambda x: x).collect()
# data.map(lambda x: x^2).collect()

# COMMAND ----------

fraction=[[x,y]  for x in range(1,100) for y in range(1,100)]
# print(fraction)

# COMMAND ----------

df=spark.createDataFrame(fraction,["numerator","denominator"])
df.show(2)

# COMMAND ----------

df=df.select(f.array(f.col("numerator"),f.col("denominator")).alias("fraction"))

# COMMAND ----------

df.show(2)


# COMMAND ----------

from typing import Tuple,Optional
from fractions import Fraction
Frac=Tuple[int,int]

# COMMAND ----------

def py_reduce_fraction(frac: Frac) -> Optional[Frac]:
    """Reduce a fraction represented as a 2-tuple of integers."""
    num, denom = frac
    if denom:
        answer = Fraction(num, denom)
        return answer.numerator, answer.denominator
    return None


# COMMAND ----------

import pyspark.sql.types as T 
@f.udf(T.IntegerType())
def naive_udf(t: \) -> str:
    return answer * 3.14159

# COMMAND ----------

df = spark.createDataFrame([[1], [2], [3]], schema=["column"])
df.rdd
print(df.rdd.collect())

# COMMAND ----------

from pyspark.sql import SparkSession
spark = SparkSession.builder.config(
"spark.jars.packages",
"com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.19.1",
).getOrCreate()

# COMMAND ----------

# MAGIC %sh
# MAGIC pyspark --jars spark-bigquery-latest.jar
# MAGIC spark-submit --jars spark-bigquery-latest.jar my_job_file.py

# COMMAND ----------

df=spark.read.parquet("/FileStore/tables/*.parquet")
df.show(2)

# COMMAND ----------

import pandas as pd
import pyspark.sql.types as T
import  pyspark.sql.functions as F
# @f.pandas_udf(T.DoubleType())
def f_to_c(degrees: pd.Series) -> pd.Series:   #Series to series
    """Transforms Farhenheit to Celcius."""
    return (degrees - 32) * 5 / 9

# COMMAND ----------

f_to_c=F.pandas_udf(f_to_c,T.DoubleType())

# COMMAND ----------

df.select("temp",f_to_c("temp").alias("temp_c")).show(1)

# COMMAND ----------

from time import sleep
from typing import Iterator
@F.pandas_udf(T.DoubleType())
def f_to_c2(degrees: Iterator[pd.Series]) -> Iterator[pd.Series]: #Iterator to Iterators
    """Transforms Farhenheit to Celcius."""
    sleep(5)
    for batch in degrees:
        yield (batch - 32) * 5 / 9

# COMMAND ----------

df.select("temp",f_to_c2("temp").alias("temp_c")).show(2)

# COMMAND ----------

from typing import Tuple
@F.pandas_udf(T.DateType())
#Iterator of multiples series to iterator of series
def create_date(year_mo_da: Iterator[Tuple[pd.Series, pd.Series, pd.Series]]) -> Iterator[pd.Series]: 
    """Merges three cols (representing Y-M-D of a date) into a Date col."""
    for (year, mo, da) in year_mo_da:
        yield pd.to_datetime(pd.DataFrame(dict(year=year, month=mo, day=da)))

# COMMAND ----------

df.select("year","mo","da",create_date(F.col("year"), F.col("mo"), F.col("da")).alias("date")).distinct().show(10)

# COMMAND ----------

#Group-Map Returns Datat frame
def scale_temperature(temp_by_day: pd.DataFrame) -> pd.DataFrame:
    """Returns a simple normalization of the temperature for a site.
    If the temperature is constant for the whole window, defaults to 0.5."""
    temp = temp_by_day.temp
    answer = temp_by_day[["stn", "year", "mo", "da", "temp"]]
    if temp.min() == temp.max():
        return answer.assign(temp_norm=0.5)
    return answer.assign(temp_norm=(temp - temp.min()) / (temp.max() - temp.min()))
  
    

# COMMAND ----------

# Parametriazation:
dbutils.widgets.removeAll()
dbutils.widgets.text("InputPath","/FileStore/","EnterInputPath")
dbutils.widgets.text("InputFileName","Periodic_Table_Of_Elements.csv","EnterInputFileName")
# dbutils.widgets.text("OutputFileName","/FileStore/tables/","EnterOutputFileName")
# dbutils.widgets.text("TargetTable","/FileStore/tables/","EnterInputFileName")

# COMMAND ----------

inpPath=getArgument("InputPath")
inpFileName=getArgument("InputFileName")
print(inpPath)
print(inpFileName)

# COMMAND ----------

import pathlib
def getdf(path,FileName):
    path=path+FileName
    fileextension=pathlib.Path(path).suffix
    if fileextension==".csv" :
        df=spark.read.csv(path)
    elif fileextension==".json" :
        df=spark.read.json(path)
    elif fileextension==".parquet" :
        df=spark.read.parquet(path)  
    return df 

# COMMAND ----------

df=getdf(getArgument("InputPath"),getArgument("InputFileName"))
# display(df)
for col in df.columns:
    df=df.withColumnRenamed(col,col[1:])
df.printSchema()
df.write.saveAsTable("Temptable")

# COMMAND ----------

spark.catalog.listTables()
# describe Temp
# import pyspark.sql as s


# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended Temptable

# COMMAND ----------

gsod=spark.read.parquet("/FileStore/tables/*.parquet")
# gsod.printSchema()

# COMMAND ----------

gsod.groupBy("year").agg(f.min("temp").alias("temp")).join(gsod,on=["year","temp"]).select("stn", "year", "mo", "da","temp").show()

# COMMAND ----------

#using windows
from pyspark.sql.window import Window
each_year = Window.partitionBy("year")
gsod.select("stn", "year", "mo", "da",f.min("temp").over(each_year).alias("cold_temp")).where(f.col("temp")==f.col("cold_temp")).orderBy("temp").distinct().show()

# COMMAND ----------

(gsod
     .withColumn("Coldest_Temp",f.min("temp").over(each_year))
     .filter(f.col("Coldest_Temp")==f.col("temp"))
     .select("year", "mo", "da", "stn", "temp")
     .orderBy("year","mo","da")
     .show()
)

# COMMAND ----------

each_day = Window.partitionBy("year", "mo", "da")
(gsod
     .withColumn("max",f.max("temp").over(each_day))
     .filter(f.col("max")==f.col("temp"))
     .select("year", "mo", "da", "stn", "temp")
     .orderBy("year", "mo", "da")
     .show()
)

# COMMAND ----------

# nonconsecutive Rank - if the order by values has same vallue then it will give same ranking and misses the consecutive rank and jumps to next rank like 1,1,3. Rank 2 willl be missed
temp_per_month_asc = Window.partitionBy("mo").orderBy("count_temp")
gsod_light=spark.read.parquet("/FileStore/tables/gsod_light.parquet")
gsod_light.show()

# COMMAND ----------

gsod_light.select("year", "mo", "da", "stn", "temp",f.rank().over(temp_per_month_asc).alias("rank")).show()

# COMMAND ----------

#Avoiding gaps in ranking using dense_rank()- consecutive 1,1,2
gsod.select("stn","year","mo","da",f.dense_rank().over(temp_per_month_asc).alias("rank")).show()

# COMMAND ----------

#Percent_rank--scoring and ranking -(#records with a lower values than current record)/(# of records in the window -1)
temp_each_year=Window.partitionBy("Year").orderBy("temp")
gsod_light.select("stn","year","mo","da","temp","count_temp",f.percent_rank().over(temp_each_year).alias("rank_temp")).show()

# COMMAND ----------

#CREATING BUCKETS BASED ON RANKS, USING NTILE()
gsod_light.withColumn("rank_tile",f.ntile(2).over(temp_each_year)).show()

# COMMAND ----------

#Numbering records within each window partition using row_number()
gsod_light.withColumn("rownum",f.row_number().over(temp_each_year)).show()


# COMMAND ----------

#Creating a window with a descending-ordered column
temp_per_month_desc = Window.partitionBy("mo").orderBy(f.col("count_temp").desc())          

# COMMAND ----------

gsod_light.withColumn("rownum",f.row_number().over(temp_per_month_desc)).show()   

# COMMAND ----------

#lag()
temp_each_year=Window.partitionBy("year").orderBy(f.col("temp").desc())
gsod_light.withColumn("lag1",f.lag("temp").over(temp_each_year)).withColumn("lag2",f.lag("temp",2).over(temp_each_year)).show()

# COMMAND ----------

#cume_dist()
gsod_light.withColumn("cumm_distribution",f.cume_dist().over(temp_each_year)).show()

# COMMAND ----------

#Ordering a window and the computation of the average
not_ordered = Window.partitionBy("year")
ordered = not_ordered.orderBy("temp")
# gsod_light.withColumn("avg_NO", f.avg("temp").over(not_ordered)).show()
# gsod_light.withColumn("avg_O", f.avg("temp").over(ordered)).show()
gsod_light.withColumn("avg_NO", f.avg("temp").over(not_ordered)).withColumn("avg_O", f.avg("temp").over(ordered)).show()

# COMMAND ----------

#This window is unbounded: everyrecord, from the first to the last, is in the
not_ordered = Window.partitionBy("year").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
#This window is growing to the left: window.every record up to the current row value is included in a window.
ordered = not_ordered.orderBy("temp").rangeBetween(Window.unboundedPreceding, Window.currentRow)

# COMMAND ----------

#Creating a date column to apply range window on#
gsod_light_p = (gsod_light
                .withColumn("year", f.lit(2019))
                .withColumn("dt",f.to_date(f.concat_ws("-", f.col("year"), f.col("mo"), f.col("da"))),)
                .withColumn("dt_num", f.unix_timestamp("dt"))
               )
gsod_light_p.show()

# COMMAND ----------

#Computing the average temperature for a 60-day sliding window
ONE_MONTH_ISH = 30 * 60 * 60 * 24 # or 2_592_000 seconds
one_month_ish_before_and_after = Window.partitionBy("year").orderBy("dt_num").rangeBetween(-ONE_MONTH_ISH, ONE_MONTH_ISH)
gsod_light_p.withColumn("avg_count", f.avg("count_temp").over(one_month_ish_before_and_after)).show()

# COMMAND ----------

gsod_light_q=gsod_light_p.withColumn("ord",f.lit(10)).filter(f.col("temp")!=21.3)
gsod_light_q.withColumn("count", f.count("ord").over(Window.partitionBy().orderBy("ord").rowsBetween(-2, 2))).show()
gsod_light_q.withColumn("count", f.count("ord").over(Window.partitionBy().orderBy("ord").rangeBetween(-2, 2))).show()

# COMMAND ----------

each_year = Window.partitionBy("year")
(gsod
.withColumn("min_temp", f.min("temp").over(each_year))
.where("temp = min_temp")
.select("year", "mo", "da", "stn", "temp")
.orderBy("year", "mo", "da")
.show())

# COMMAND ----------

One_Week_ish=7*24*60*60
one_week_ish_before_and_after = (Window.partitionBy("mo").orderBy("dt_num").rangeBetween(-One_Week_ish, One_Week_ish))
(gsod
     .withColumn("year", f.lit(2019))
     .withColumn("dt",f.to_date(f.concat_ws("-", f.col("year"), f.col("mo"), f.col("da"))),)
     .withColumn("dt_num", f.unix_timestamp("dt"))
     .withColumn("IshOT",f.when(f.col("temp")==f.max("temp").over(one_week_ish_before_and_after),True).otherwise(False))
).show()

# COMMAND ----------

from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("word_freqs").getOrCreate()
sc=spark.sparkContext

# COMMAND ----------

data="""{"a":1,"b":2,"c":3}|{"a":1,"b":2,"c":3}|{"a":1,"b","c":3}""".split("|")
rdd_data=sc.parallelize(data)    
curreptdf=spark.read.json(rdd_data)
display(curreptdf)


# COMMAND ----------

curreptdf.where(f.col('a').isNotNull()).count()

# COMMAND ----------

data="""{"a":1,"b":2,"c":3}|{"a":1,"b":2,"c":3}|{"a":1,"b","c":3}""".split("|")
rdd_data=sc.parallelize(data)    
curreptdf=spark.read.option("mode","DROPMALFORMED").json(rdd_data)
display(curreptdf)

# COMMAND ----------

data="""{"a":1,"b":2,"c":3}|{"a":1,"b":2,"c":3}|{"a":1,"b","c":3}""".split("|")
rdd_data=sc.parallelize(data)    
curreptdf=spark.read.option("mode","FAILFAST").json(rdd_data)
display(curreptdf)

# COMMAND ----------

mybadrecords="/FileStore/tables"
data="""{"a":1,"b":2,"c":3}|{"a":1,"b":2,"c":3}|{"a":1,"b","c":3}""".split("|")
rdd_data=sc.parallelize(data)    
curreptdf=spark.read.option("badRecordPath",mybadrecords).json(rdd_data)
display(curreptdf)

# COMMAND ----------

# MAGIC %fs mounts

# COMMAND ----------

#Reading Streams
datapath="streaming path"
df=spark.readStream.opttion("maxFilesPerTrigger",1).json(datapath)


# COMMAND ----------

df=spark.createDataFrame([{"one": 1, "two": [1,2,3]}])
df.printSchema()

# COMMAND ----------

exo2_2_df = spark.createDataFrame([["test", "more test", 10_000_000_000]], ["one", "two", "three"])
exo2_2_df.printSchema()

# COMMAND ----------

exo2_3_df = spark.read.text("./data/gutenberg_books/1342-0.txt").select(length(col("value")).alias("number_of_char"))

# COMMAND ----------

from pyspark.sql.functions import col, greatest
exo2_4_df = spark.createDataFrame([["key", 10_000, 20_000]], ["key", "value1", "value2"])


# COMMAND ----------

exo2_4_mod = exo2_4_df.select(greatest(col("value1"), col("value2")).alias("maximum_value")).select("maximum_value")

# COMMAND ----------

# words_withlengthmorethan3 = words_clean.where(length("word") > 3)
# words_withlengthmorethan3.show()
#

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, explode, lower, regexp_extract,length
spark = SparkSession.builder.getOrCreate()
book = spark.read.text("/FileStore/tables/1342_0.txt")
lines = book.select(split(book.value, " ").alias("line"))
words = lines.select(explode(col("line")).alias("word"))
words_lower = words.select(lower(col("word")).alias("word_lower"))
words_clean = words_lower.select(
regexp_extract(col("word_lower"), "[a-z]*", 0).alias("word"))
words_nonull = words_clean.where(col("word") != "")
# words_withoutis = words_nonull.where(col("word") != "is").show()
# words_withoutis.show()
# words_withlengthmorethan3 = words_nonull.where(length("word") > 3)
# words_withlengthmorethan3.show()
words_nonull.where(~col("word").isin(["no", "is", "the", "if"])).show()


# COMMAND ----------

import pyspark.sql.functions as f
results = (
    spark.read.text("/FileStore/tables/1342_0.txt")
    .select(f.split(f.col("value"), " ").alias("line"))
    .select(f.explode(f.col("line")).alias("word"))
    .select(f.lower(f.col("word")).alias("word"))
    .select(f.regexp_extract(f.col("word"), "[a-z']*", 0).alias("word"))
    .where(f.col("word") != "")
    .groupby(f.col("word"))
    .count()
    .filter(f.col("count")==1)
)

results.show(2)


# COMMAND ----------

def scale_temperature(temp_by_day: pd.DataFrame) -> pd.DataFrame:
    """Returns a simple normalization of the temperature for a site.
    If the temperature is constant for the whole window, defaults to 0.5."""
    temp = temp_by_day.temp
    answer = temp_by_day[["stn", "year", "mo", "da", "temp"]]
    if temp.min() == temp.max():
        return answer.assign(temp_norm=0.5)
    return answer.assign(temp_norm=(temp - temp.min()) / (temp.max() - temp.min()))

# COMMAND ----------

def scale_temperature(temp_by_day: pd.DataFrame) -> pd.DataFrame:
    """Returns a simple normalization of the temperature for a site.
    If the temperature is constant for the whole window, defaults to 0.5."""
    temp = temp_by_day.temp
    answer = temp_by_day[["stn", "year", "mo", "da", "temp"]]
    if temp.min() == temp.max():
        return answer.assign(temp_norm=0.5)
    return answer.assign(temp_norm=(temp - temp.min()) / (temp.max() - temp.min()))

# COMMAND ----------

gsod=spark.read.parquet("/FileStore/tables/*.parquet")
# gsod.printSchema()
# gsod.show(2)

# COMMAND ----------

from pyspark.sql.functions import col, length
# The `length` function returns the number of characters in a string column.
# exo2_3_df = spark.read.text("/FileStore/tables/1342_0.txt").select(length(col("value")).alias("number_of_char")).show(5)


# COMMAND ----------

book = spark.read.text("/FileStore/tables/1342_0.txt")
lines = book.select(split(book.value, " ").alias("line"))
words = lines.select(explode(col("line")).alias("word"))

# COMMAND ----------

import pyspark.sql.functions as F
import pyspark.sql.types as T
import pandas as pd

def scale_temperature2(temp_by_day: pd.DataFrame) -> pd.DataFrame:
    """Returns a simple normalization of the temperature for a site.
    If the temperature is constant for the whole window, defaults to 0.5."""
    def f_to_c(temp):
        temp=(temp - 32) * 5 / 9
        return temp
    temp = f_to_c(temp_by_day.temp)
    answer = temp_by_day[["stn", "year", "mo", "da", "temp"]]
    if temp.min() == temp.max():
        return answer.assign(temp_norm=0.5)
    return answer.assign(temp_norm=(temp - temp.min()) / (temp.max() - temp.min()))
gsod=spark.read.parquet("/FileStore/tables/*.parquet")
gsod_map=(gsod
          .groupBy("stn","year","mo")
          .applyInPandas(scale_temperature2,schema=("stn string, year string, mo string,da string, temp double,temp_norm double"))
         )
gsod_map.show(5)


# COMMAND ----------

gsod=spark.read.parquet("/FileStore/tables/*.parquet")
gsod_map=gsod.groupBy("stn","year","mo").applyInPandas(scale_temperature,schema=("stn string, year string, mo string, "
"da string, temp double,temp_norm double"))
# gsod_map.show(5)


# COMMAND ----------

exo2_2_df = spark.createDataFrame([["test", "more test", 10_000_000_000]], ["one", "two", "three"])
# exo2_2_df.printSchema()
count=0
for col in exo2_2_df.dtypes:
    if col[1] != "string":
        count=count+1
# print("number of non string columns is/are {} ".format(count))

# COMMAND ----------

exo2_1_df = spark.createDataFrame(data=[ [[1, 2, 3, 4, 5]], [[5, 6, 7, 8, 9, 10]]], schema=["numbers"])
exo2_1_df=exo2_1_df.select(f.explode("numbers").alias("numbers"))
print("count is {}".format(exo2_1_df.count()))
exo2_1_df.show()

# COMMAND ----------

collection = [1, "two", 3.0, ("four", 4), {"five": 5}]
sc = spark.sparkContext
collection_rdd = sc.parallelize(collection)
print(collection_rdd.collect())
print(collection_rdd.getNumPartitions())

# COMMAND ----------

df_items.createOrReplaceTempView()

# COMMAND ----------

data=range(1,100000000)
rdd=sc.parallelize(data,4)
# rdd.collect()
data_rdd=rdd.map(lambda x:x*x)
data_rdd.collect()


# COMMAND ----------

# spark.conf
# 
data=range(1,10)
rdd=sc.parallelize(data)
# rdd.collect()
data_rdd=rdd.map(lambda x:x*x)
data_rdd.collect()
print(df.rdd.getNumPartitions())

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions","3")
df = spark.range(0,20)
print(df.rdd.getNumPartitions())

# COMMAND ----------

rdd1 = spark.sparkContext.parallelize((0,300000), 3)
rdd1.getNumPartitions()

# COMMAND ----------

import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('SparkByExamples.com') \
        .master("local[5]").getOrCreate()

df=spark.range(0,20)
print(df.rdd.getNumPartitions())

df.write.mode("overwrite").csv("/FileStore/tables/partition.csv")


# COMMAND ----------

df2 = df.repartition(6)
print(df2.rdd.getNumPartitions())

# COMMAND ----------


df3 = df.coalesce(2)
print(df3.rdd.getNumPartitions())


# COMMAND ----------

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
    cases = read_data("/FileStore/tables/usa_covid_cases.json")
    cases = normalize_covid(cases).withColumnRenamed('counts', 'cases')
    deaths = read_data("/FileStore/tables/usa_covid_deaths.json")
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


# if __name__ == '__main__':
#     main()


# COMMAND ----------



# COMMAND ----------

data="""{"a":1,"b":2,"c":3}|{"a":1,"b":2,"c":3}|{"a":1,"b","c":3}""".split("|")
rdd_data=sc.parallelize(data)    
curreptdf=spark.read.option("mode","DROPMALFORMED").json(rdd_data)
display(curreptdf)

# COMMAND ----------

 data = create_dataset()
data.groupBy('state').agg(F.sum("cases").alias("cases")).show()

# COMMAND ----------

!pip install opendatasets

# COMMAND ----------

import opendatasets as od

# COMMAND ----------

data=range(1,10)
rdd=sc.parallelize(data)
K=rdd.collect()
# data_rdd=rdd.map(lambda x:x*x)
# data_rdd.collect()
# print(rdd.getNumPartitions())
type(K)

# COMMAND ----------


