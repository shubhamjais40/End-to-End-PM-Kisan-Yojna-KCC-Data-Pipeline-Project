from pyspark.sql import SparkSession
import pyspark.pandas as ps
import pandas as pd
import glob


spark=SparkSession\
           .builder\
           .appName("SparkSQLExampleApp")\
           .getOrCreate()

filepath = "C:\\Users\\cvb\\Documents\\automation_python\\PM kisan call center query project\\project\\extracts"
extract_list = glob.glob(filepath+"\\*.xlsx")

frame=pd.DataFrame([])

for chunks in extract_list:
    df=pd.read_excel(chunks,index_col=None,engine='openpyxl')
    frame=frame._append(df)
    print(f'{chunks} appended...')


kccFrame=spark.createDataFrame(frame)

print(kccFrame.schema)


spark.stop()