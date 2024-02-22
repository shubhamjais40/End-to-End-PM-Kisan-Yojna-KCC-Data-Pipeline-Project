import requests
import pandas as pd
import os
from pathlib import Path

class PMKisan_Extracts:
    def __init__(self,output_dir='EXTRACTS_RAW'):
        self.HEADERS = {'User-Agent': 'ReqBin Python Client/1.0'}
        self.URL = "https://dackkms.gov.in/Account/API/kKMS_QueryData.aspx?StateCD=01&DistrictCd=0104"
        self.OUTPUTPATH = Path.cwd().parent.joinpath(output_dir)
    
    def test_api_conn(self,month,year):
        payload={'Month':f'{month}','Year':f'{year}'}
        response=requests.get(url=self.URL,params=payload,headers=self.HEADERS)
        return response.url,response.status_code,response.json()["Response"]
    
    def extract_monthly_data(self,month,year):
        payload={'Month':f'{month}','Year':f'{year}'}
        getResponse=requests.get(url=self.URL,params=payload,headers=self.HEADERS)
        dataTable=pd.DataFrame(getResponse.json()["data"])
        return dataTable
    
    def saveMonthTableToxls(self,datatable,filepath:str):
        datatable.to_excel(filepath+".xlsx",engine='openpyxl',index=False)
        return "File saved successfully"

    def kisan_monthly_extracts(self,month,year):
        data = self.extract_monthly_data(month,year)
        self.saveMonthTableToxls(data,str(self.OUTPUTPATH)+"//"+f'{year}'+'_'+f'{month}'+"extracts")
        return 0

    def kisan_backlog_extracts(self,startMonth,endMonth,year):
        for month in range(startMonth,endMonth+1):
            data=self.extract_monthly_data(month,year)
            self.saveMonthTableToxls(data,str(self.OUTPUTPATH)+"//"+f'{year}'+'_'+f'{month}'+"extracts")
        return 0

    def clear_extractsDir():
        pass
    
