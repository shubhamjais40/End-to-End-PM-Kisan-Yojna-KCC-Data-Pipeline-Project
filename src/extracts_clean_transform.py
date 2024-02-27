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



spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")
spark.conf.set("spark.sql.execution.arrow.pyspark.selfDestruct.enabled", "true")

EXTRACTSPATH = Path.cwd().parent/"EXTRACTS_RAW/"

STAGINGPATH = Path.cwd().parent/"STAGING_LAKE"

kccFrame = spark.read.format("excel").option("header","true").load(str(EXTRACTSPATH))
# defining path for extracts raw files and output staging lake

#EXTRACTSPATH = Path.cwd().parent/"EXTRACTS_RAW"/"*.xlsx"

STAGINGPATH = Path.cwd().parent/"STAGING_LAKE"

# extract_list = glob.glob(str(EXTRACTSPATH))



# frame=pd.DataFrame([])
# #frame.info()

# for chunks in extract_list:
#     df=ps.read_excel(chunks,index_col=None)
#     frame=frame._append(df)
#     print(f'{chunks} appended...')

# frame.drop_duplicates()

# kccFrame=spark.createDataFrame(frame)

kccFrame1=kccFrame.withColumn("kcc",split(col("KccAns"),"\n",2).getItem(0))
kccFrame1=kccFrame1.withColumn("Crops",split(col("Crop"),"\(",2).getItem(0))


columns_list = kccFrame1.columns

# trim all spaces and converting all values to lowercase using loop over each column
for key in columns_list:
    kccFrame1=kccFrame1.withColumn(key,lower((key)))
    kccFrame1=kccFrame1.withColumn(key,trim(key))
    print(key+"..Done")

# transforming CreatedOn column to seperate createdate and createdtime
kccFrame1 = kccFrame1.withColumn("createdDate",split('CreatedOn','t',-1)\
               .getItem(0))\
                .withColumn("createdTime",split('CreatedOn','t',-1)\
                .getItem(1))\
                #.take(4)

kccFrame1=kccFrame1.withColumn("convertedDate",to_date(col("createdDate")))

kccFrame1=kccFrame1.withColumn("createdMonth",month('convertedDate'))\
                    .withColumn("createdDay",dayofmonth('convertedDate'))\
                    .withColumn("createdHour",hour('createdTime'))



def convertTextEng(kcc:str):
    return GoogleTranslator(source='te', target='en').translate(kcc)


convertToEng = udf(convertTextEng,StringType()) 


kccFrame1 = kccFrame1.withColumn('kccEng',convertToEng('kcc'))

kccFrame1 = kccFrame1.drop("Season","KccAns","CreatedOn","createdDate","Crop")

kccFrame1.write.partitionBy("createdMonth").mode("overwrite").parquet(str(STAGINGPATH)+"/kcctest.parquet")

print("Transformation completed successfully..")
