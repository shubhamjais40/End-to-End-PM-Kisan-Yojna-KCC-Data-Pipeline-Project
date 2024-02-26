from pyspark.sql import SparkSession
#import pyspark.pandas as pd
#import pandas as ps
from pyspark.sql.functions import trim,upper,lower,to_date,to_timestamp,transform,split,col,substring
from pyspark.sql.functions import dayofmonth,month,hour
from pyspark.sql.types import StringType,IntegerType
from pyspark.sql.functions import sum,avg,max,count
from pyspark.sql.functions import udf,col
from fastavro import reader
from pathlib import Path
import logging


log = logging.getLogger('test')
log.setLevel(logging.DEBUG)

def avro_reader(filename):
    with open(filename, 'rb') as fo:
        avro_reader = reader(fo)
        records = [r for r in avro_reader]
        return records


sc=SparkSession\
           .builder\
           .appName("SparkSQLDenoramlisedApp")\
           .getOrCreate()



# Input Source &  output file definition

FILE_TO_TRANSFORM = "kcc.parquet"

STAGING_PATH = Path.cwd().parent.joinpath("STAGING_LAKE",FILE_TO_TRANSFORM)
#print(STAGING_PATH)

OUTPUT = Path.cwd().parent.joinpath("DOWNSTREAM_READY_EXTRACTS")

PATH_TO_BLOCKS_DF = Path.cwd().parent.joinpath('ARCHIVE','blocks_coordinates.csv')

PATH_TO_CALENDER = Path.cwd().parent.joinpath('ARCHIVE','calender_range_20230601_20231231.avro')


if STAGING_PATH.exists():
    print("staging path exists")
else:
    log.warning("Staging path not exists,need to create")


#Reading kcc Data from source

kccDF = sc.read.parquet(str(STAGING_PATH)).select('Sector','Category','Crops','QueryType','StateName','DistrictName','BlockName','QueryText','kccEng','createdTime','convertedDate','createdHour')
print(kccDF.printSchema())




#Checking kccDF if any timestamp is null ,Block if null ,if  Crop, QueryText is null,
null_count = kccDF.where(kccDF.createdTime.isNull() | kccDF.BlockName.isNull() | kccDF.Crops.isNull()).count()

if null_count == 0:
    log.debug("No Null Values in either Data Entities")
else:
    log.warning("Null Values found ")
    raise Exception("{null_count} suspense records found") 




blockDF = sc.read.format('csv').option('inferSchema',True).option('header',True).load(str(PATH_TO_BLOCKS_DF))

#Renaming BlockName to avoid ambiguous conlficts between kccDF & blockDF 
blockDF = blockDF.withColumnRenamed("BlockName","blocks")
#removing duplicates
blockDF_unique = blockDF.dropDuplicates(['blocks'])

#avoid if file appended with header:true while generating blocks coordinates
blockDFValid = blockDF_unique.where('blocks !="BlockName"')
#blockDF.printSchema()

#blockDFValid.count()

#check if any block name is null
#blockDFValid.where(blockDFValid.blocks.isNull()).collect()
from pyspark_assert import assert_frame_equal

try:
    assert_frame_equal(blockDF, blockDFValid)
except Exception as e:
    logging.warning('Could contain duplicate values or heading appended multiple Times')
    logging.warning(e)
    
finally:
    log.info('\n blockDFValid is cleaned ')


log.debug("Reading Calender Data from Archive")

try:
    calender_dic = avro_reader(str(PATH_TO_CALENDER))
    calenderDF = sc.createDataFrame(calender_dic).drop_duplicates(['date'])
    calenderDF.printSchema()
    log.debug(calenderDF.count())
except Exception as e:
    log.error(e)

calenderDF = calenderDF.withColumn("datePart",substring(col("date"),1,10))
print(calenderDF.show(5))


log.debug("Merging all three DataFrames")

mergedKccDF = kccDF.join(blockDFValid,kccDF.BlockName ==  lower(blockDFValid.blocks),"left")\
        .join(calenderDF,kccDF.convertedDate.cast(StringType()) == calenderDF.datePart,"left")

#mergedKccDF.printSchema()
log.debug("Selecting relevant columns from merged dataframe")

kccMergedDF = mergedKccDF.select('Sector','Category','Crops','QueryType','QueryText','kccEng','StateName','DistrictName','blocks','latitude','longitude','convertedDate','day','month','MonthName','quarter','weekName','weekDay','year')

print(kccMergedDF.printSchema())

log.info(kccMergedDF.show(5))

log.debug("Writing dataframe to parquet file..")

kccMergedDF.write.partitionBy('year','MonthName').mode('errorifexists').parquet(str(OUTPUT))

log.info("DownStream ready to export file saved successfully..")


