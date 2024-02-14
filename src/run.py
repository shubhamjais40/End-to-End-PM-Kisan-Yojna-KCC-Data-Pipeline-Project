import datetime as dt
from extract import PMKisan_Extracts

ke=PMKisan_Extracts()

ke.kisan_backlog_extracts(6,12,2023)

#df=ke.extract_monthly_data(10,2023)
#ke.save_tableToXlsFile(df,)