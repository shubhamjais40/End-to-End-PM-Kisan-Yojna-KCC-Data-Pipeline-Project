import glob
import pyspark.pandas as ps
import pandas as pd
from deep_translator import GoogleTranslator
from pyspark.sql.types import StringType,IntegerType

from pyspark.sql import SparkSession
from pyspark.sql.functions import transform,split,col,trim,upper,lower,udf
from pyspark.sql.functions import to_date,to_timestamp,dayofmonth,month,hour

spark=SparkSession\
           .builder\
           .appName("SparkSQLExampleApp")\
           .getOrCreate()


spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")
spark.conf.set("spark.sql.execution.arrow.pyspark.selfDestruct.enabled", "true")


filepath = "C:\\Users\\cvb\\Documents\\automation_python\\PM kisan call center query project\\project\\extracts"
extract_list = glob.glob(filepath+"\\*.xlsx")
#extract_list

frame=pd.DataFrame([])
#frame.info()

for chunks in extract_list:
    df=ps.read_excel(chunks,index_col=None)
    frame=frame.append(df)
    print(f'{chunks} appended...')

kccFrame=spark.createDataFrame(frame)

kccFrame1=kccFrame.withColumn("kcc",split(col("KccAns"),"\n",2).getItem(0))
kccFrame1=kccFrame1.withColumn("Crops",split(col("Crop"),"\(",2).getItem(0))
# kccFrame1.select("Crops","kcc").explain()

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

kccFrame1 = kccFrame1.drop("Season","KccAns","CreatedOn","createdDate")

kccFrame1.write.partitionBy("createdMonth").mode("overwrite").parquet("kccRawAppended.parquet")