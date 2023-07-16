#Data Engineering
import pandas as pd
import glob
from datetime import datetime

csvfile="/Users/duygukahraman/Python_Projects/pythonresources/exchange_rates_1.csv"
targetfile="transformed_data.csv"

def extract():
    extracted_data=pd.DataFrame(columns=['rates'])
    
    for csvfile in glob.glob("*.csv"):
        extracted_data=extracted_data.append(extract_from_csv(csvfile),ignore_index=True)
        
    return extracted_data


def extract_from_csv(file_to_process):
    dataframe=pd.read_csv(file_to_process)
    return dataframe

def transform(data):
    data.rename(columns = {'Unnamed: 0':'names'}, inplace = True)
    return data
            
def load(targetfile,data_to_load):
    data_to_load.to_csv(targetfile)

def log(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second
    now = datetime.now() # get current timestamp
    timestamp = now.strftime(timestamp_format)
    with open("logfile.txt","a") as f:
        f.write(timestamp + ',' + message + '\n')  
        
          
log("ETL Job Started")   
log("extract process started")
result=extract()
result
log("extract process ended")
log("transform process started")
transform(result)
result
log("transform process ended")
log("load process started")
load(targetfile,result)
log("load process ended")
log("ETL Job Ended")










#csv_data=pd.read_csv('/Users/duygukahraman/Python_Projects/pythonresources/exchange_rates_1.csv')
#GBP_value=csv_data.iloc[49]
#print(GBP_value);

