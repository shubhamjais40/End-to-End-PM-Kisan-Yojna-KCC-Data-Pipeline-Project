from extract import PMKisan_Extracts
import datetime as dt
import argparse
#import logging

#Modifying Logging configuration
#logging.basicConfig(filename='extracts_runner.log',level=logging.DEBUG)

parser = argparse.ArgumentParser(description = 'Take parameter as type of extracts needed and object params')
parser.add_argument('extracts_type',type=str,help='Adhoc OR Monthly')
parser.add_argument('-m','--month',type=int,help="Enter valid month like:'MM' of year")
parser.add_argument('-y','--year',type=int,help="Enter valid year like:'YYYY'")
parser.add_argument('-sm','--startmonth',type=int,help="Enter valid start month like:'MM' of year")
parser.add_argument('-em','--endmonth',type=int,help="Enter valid end month like:'MM' of year")
parser.add_argument('-out','--outputdir',default='TESTDIR',type=str,help="Enter valid directory Name to ingesting Extracts")

#logging.DEBUG("Waiting for Extracts Type to perform extracts generation")
args = parser.parse_args()
EXTRACTS_TYPE = args.extracts_type.upper()
VALID_EXTRACTS_INPUT = ['ADHOC','MONTHLY']

#logging.debug("Validating Extracts as per Input")
if EXTRACTS_TYPE in VALID_EXTRACTS_INPUT:
    print(f"Extracts_Type -> {EXTRACTS_TYPE}")
else:
    print("Please enter valid Extracts Type from options-> [ADHOC | MONTHLY]")


# Conditional Run for input reuqirements

if EXTRACTS_TYPE == 'MONTHLY':
    print("Monthly Extracts will run")
    MONTH = args.month
    YEAR = args.year
    OUTDIR = args.outputdir.upper()
    print(f"Pulling DataSet for params {MONTH},{YEAR},{OUTDIR} job..")
    monObj = PMKisan_Extracts(OUTDIR)
    monObj.kisan_monthly_extracts(MONTH,YEAR)
    print(f"Successfully ingested Extracts for {YEAR}-{MONTH} in {OUTDIR}!!")

if EXTRACTS_TYPE == 'ADHOC':
    print("Adhoc Extracts will run")
    ST_MONTH=args.startmonth
    EN_MONTH = args.endmonth
    YEAR_ADHOC = args.year
    OUTDIR = args.outputdir.upper()
    print(f"{ST_MONTH},{EN_MONTH},{YEAR_ADHOC},{OUTDIR}")
    adhocobj=PMKisan_Extracts(OUTDIR)
    print(adhocobj.OUTPUTPATH)
    adhocobj.kisan_backlog_extracts(ST_MONTH,EN_MONTH,YEAR_ADHOC)
    print(f"Successfully ingested ADHOC Extracts for Year:{YEAR_ADHOC}-Month:{ST_MONTH} to Month:{EN_MONTH} in {OUTDIR}!!")

#ke=PMKisan_Extracts("EXTRACTS_RAW")

# #ke.kisan_backlog_extracts(6,12,2023)
# print(ke.test_api_conn(1,2023))
# ke.kisan_monthly_extracts(1,2024)
# #ke.save_tableToXlsFile(df,)

