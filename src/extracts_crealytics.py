import glob
from pyspark.sql import SparkSession
import pyspark.pandas as pd
from deep_translator import GoogleTranslator
import pandas as ps
from pyspark.sql.functions import trim,upper,lower,to_date,to_timestamp,transform,split,col
from pyspark.sql.functions import dayofmonth,month,hour
from pyspark.sql.types import StringType,IntegerType
from pyspark.sql.functions import udf,col
from pathlib import Path


# creating spark session app
spark=SparkSession\
           .builder\
           .appName("SparkSQLTransformApp")\
           .getOrCreate()


EXTRACTSPATH = Path.cwd().parent/"EXTRACTS_RAW/"

STAGINGPATH = Path.cwd().parent/"STAGING_LAKE"

df = spark.read.format("excel").option("header","true").load(str(EXTRACTSPATH))

print(df.show())