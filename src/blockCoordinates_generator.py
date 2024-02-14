import os
import pandas as pd
from utils import get_coordinates


os.chdir("..\extracts")
extracts_list=os.listdir()

frame=[]
for file in extracts_list:
    frame.append(pd.read_excel(file,usecols='J'))

BlockFrame=pd.concat(frame)
BlockFrame=BlockFrame.drop_duplicates(ignore_index=True)
BlockFrame['BlockName']=BlockFrame['BlockName'].str.strip()
BlockFrame=BlockFrame[BlockFrame['BlockName']!='0']

BlockFrame["coordinates"]=BlockFrame['BlockName'].apply(get_coordinates)

blocks=[BlockFrame['BlockName']]
final_Block_points=pd.DataFrame(BlockFrame.coordinates.tolist(),columns=['latitude','longitude'],index=blocks)
os.chdir(r"..\archive")
final_Block_points.to_csv(r"blocks_coordinates.csv",mode='a')