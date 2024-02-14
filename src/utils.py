import fastavro
import pandas as pd
from geopy.geocoders import Nominatim
from datetime import datetime

def avro_reader(filename):
    with open(filename, 'rb') as fo:
        avro_reader = reader(fo)
        records = [r for r in avro_reader]
        return records


def avro_writer(schema,dataDic_to_Write,filename_out):

    def encode_datetime_as_string(data, *args):
        return datetime.isoformat(data)

    fastavro.write.LOGICAL_WRITERS["string-datetime2"] = encode_datetime_as_string
    
    parsed_schema=fastavro.parse_schema(schema)
    records = dataDic_to_Write
    with open(filename_out, 'wb') as output:
        fastavro.writer(output, parsed_schema, records)
        
    return None


def calenderGenerater(start:str,end:str,freq:str):
    dummy = pd.Series(pd.date_range(start=start, end=end,freq=freq))
    calender = pd.DataFrame(data={'date':dummy,'day':dummy.dt.day,'weekday':dummy.dt.dayofweek,'month':dummy.dt.month,'quarter':dummy.dt.quarter},index=None)
    
    week = {0:'Monday',1:'Tuesday',2:'Wednesday',3:'Thursday',4:'Friday',5:'Saturday',6:'Sunday'}
    mon = {1:'January',2:'Febuary',3:'March',4:'April',5:'May',6:'June',7:'July',8:'August',9:'September',10:'October',11:'November',12:'December'}
    
    monthMap = pd.DataFrame(data=[mon.keys(),mon.values()]).T
    monthMap.rename(columns={0:'MonthNum',1:'MonthName'},inplace=True)

    weekmap = pd.DataFrame(data=[week.keys(),week.values()]).T
    weekmap = weekmap.rename(columns={0:'weekNum',1:'weekName'})
    
    calender = calender.merge(weekmap,how='left',left_on='weekday',right_on='weekNum',suffixes=('', ''))
    calender = calender.merge(monthMap,how = 'left', left_on = 'month',right_on = 'MonthNum',suffixes=('',''))
    
    calender = calender.drop(columns=['weekNum','MonthNum'])
    calender_rec = calender.to_dict('records')

    return calender_rec


def get_coordinates(city_name):
    geolocator = Nominatim(user_agent="blockName")  # Replace "your_app_name" with your actual application name or identifier
    location = geolocator.geocode(city_name)

    if location:
        latitude, longitude = location.latitude, location.longitude
        return latitude, longitude
    else:
        return "N.A","N.A"
    
