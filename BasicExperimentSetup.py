#imports
import sys
import time
import re
import pandas as pd
import datetime as dt
from dask.distributed import Client, performance_report

#CONSTANTS
DEBUG = False
DISTRIBUTED = False
TIMEFORMAT = "%a-%d-%m-%y %H:%M:%S"
DATA_FILE = './data/NYC_taxi_trips/yellow_tripdata_2014-01.parquet' # Base_File


#---------------------------------------------------------------------------------------------------------------------------
#CLI styling: turn a string into a colored string FROM https://stackoverflow.com/questions/287871/how-do-i-print-colored-text-to-the-terminal
#---------------------------------------------------------------------------------------------------------------------------
#Available colors
cli_styling = {'HEADER' : '\033[95m',
    'OKBLUE' : '\033[94m',
    'OKCYAN' : '\033[96m',
    'OKGREEN' : '\033[92m',
    'WARNING' : '\033[93m',
    'FAIL' : '\033[91m',
    'ENDC' : '\033[0m',
    'BOLD' : '\033[1m',
    'UNDERLINE' : '\033[4m'}

#cli colors to be used
YELLOW_CLI = 'WARNING'
GREEN_CLI = 'OKGREEN'
RED_CLI = 'FAIL'

#function to call to style CLI text
def cli_style(text, type): 
    return f"{cli_styling[type]}{text}{cli_styling['ENDC']}"
#-------------------------------------------------------------------------------------------------------------------------


def get_time(): #get a string timestamp in the specified TIMEFORMAT
    return dt.datetime.now().strftime(TIMEFORMAT)


#-------------------------------------------------------------------------------------------------------------------------
#Main code
#-------------------------------------------------------------------------------------------------------------------------
def main():
    pattern = 'data:.*'
    regex = re.compile(pattern)
    if len(list(filter(regex.match, sys.argv))) == 1:
        DATA_FILE = list(filter(regex.match, sys.argv))[0][len(pattern) - 2:]
        print(cli_style(f"{get_time()}||READING IN DATA",GREEN_CLI))
        try:
            data = pd.read_parquet(DATA_FILE)
        except (FileNotFoundError,FileExistsError) as e:
            print(cli_style(f"{get_time()}||DATA FILE NOT FOUND",RED_CLI))
            print(cli_style(f"{get_time()}||STOPPING EXECUTION",RED_CLI))
            return
        except:
            print(cli_style(f"{get_time()}||AN {e} ERROR OCCURRED",RED_CLI))
            print(cli_style(f"{get_time()}||STOPPING EXECUTION",RED_CLI))
            return
        
    else:
        print(cli_style(f"{get_time()}||INCORRECT NUMBER OF DATA FILES PROVIDED",RED_CLI))
        print(cli_style(f"{get_time()}||STOPPING EXECUTION",RED_CLI))
        return

    if 'dbg' in sys.argv: 
        DEBUG = True
        print(cli_style(f"{get_time()}||EXECUTING EXPERIMENT IN DEBUG MODE",YELLOW_CLI))
    if 'dstr' in sys.argv: 
        DISTRIBUTED = True
        print(cli_style(f"{get_time()}||EXECUTING EXPERIMENT DISTRIBUTED",GREEN_CLI))
        print(cli_style(f"{get_time()}||DISTRIBUTED MODE NOT YET IMPLEMENTED",RED_CLI))
    else:
        print(cli_style(f"{get_time()}||EXECUTING EXPERIMENT LOCALLY",GREEN_CLI))
        client = Client()
        print(cli_style(f"{get_time()}||DASHBOARD AVAILABLE AT:{client.dashboard_link}",GREEN_CLI))
    

if __name__ == '__main__':        
    main()