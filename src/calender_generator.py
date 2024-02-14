
from utils import avro_writer,calenderGenerater
import argparse

#for parsing start date and end date with frequency of calender generation
parser=argparse.ArgumentParser()
parser.add_argument("start")
parser.add_argument("end")
parser.add_argument("freq")
args=parser.parse_args()

start_date = args.start
end_date = args.end
freq = args.freq.upper()

#schema for avro writer   
schema = {'doc': 'calender records',
          'name': 'calender',
          'namespace': 'test',
          'type': 'record',
          'fields': [
        {'name': 'date', 'type': [ "null",{"type": "string","logicalType": "datetime2",}]},
        {'name': 'day', 'type': 'int'},
        {'name': 'weekday', 'type': 'int'},
        {'name': 'month', 'type': 'int'},
        {'name': 'quarter', 'type': 'int'},
        {'name': 'weekName', 'type': 'string'},
        {'name': 'MonthName', 'type': 'string'}
    ]
}
    
calender_rec_dict = calenderGenerater(start_date,end_date,freq)

from_date = start_date.replace('/','')
to_date = end_date.replace('/','')
avro_writer(schema, calender_rec_dict, f'../archive/calender_range_{from_date}_{to_date}.avro')
print(f"Calender generated for date range {from_date} - {to_date} freq {freq}")
    


