import glob
import pandas as pd
from datetime import datetime
import time

def extract_from_json(file_to_process):
    dataframe = pd.read_json(file_to_process)
    return dataframe

def extract():
    data = "bank_market_cap_1.json"
    extracted_data = extract_from_json(data)
    return extracted_data

def Load(df):
    df.to_csv("bank_market_cap_gbp.csv",index=False)
def Log(strings):
    end = time.time()
    with open("log.txt",'a') as f:
        d = str(end)+" "+ strings+"\n"
        print(d)
        f.write(d)
    f.close()
def transform(path):
    df = pd.read_json(path)
    df = df.rename(columns={'Market Cap (US$ Billion)': "Market Cap (GBP$ Billion)"})
    df.loc[:,'Market Cap (GBP$ Billion)']= round(df.iloc[:,-1]*0.7323984208000001,3)
    return df


Log("ETL Job Started")
Log("Extract phase Started")
df_exchange_rates = pd.read_csv("exchange_rates.csv",index_col=0)
dataframe = extract()
exchange_rate = df_exchange_rates.loc['GBP','Rates']

Log("Extract phase Ended")

Log("Transform phase Started")


path = "bank_market_cap_1.json"
df = transform(path)
Log("Transform phase Ended")


Log("Load phase Started")
Load(df)
Log("Load phase Ended")

print("GBP exchange rates: ",df_exchange_rates.loc['GBP','Rates'])
print(dataframe.head(5))
print(df.head(5))