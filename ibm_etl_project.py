



import pandas as pd
from datetime import datetime

columns = ['Name','Market Cap (US$ Billion)']
marketCap_file = 'bank_market_cap_1.json'
exchange_rate_file = 'exchange_rates_1.csv'
logfile = "bank_market_cap_logfile.txt"
loadfile = "bank_market_cap_gbp.csv"


def extract_from_json(file_to_process):
    dataframe = pd.read_json(file_to_process)
    return dataframe


def extract(fileName):
    extracted_data = pd.DataFrame(extract_from_json(fileName),columns= columns)
    return extracted_data

ratesGbp = pd.read_csv(exchange_rate_file, index_col=0)
exchange_rate = ratesGbp.at['GBP','rates']
print(exchange_rate)


def transform(data):

    data['Market Cap (US$ Billion)'] = data['Market Cap (US$ Billion)'].apply(lambda x: x* exchange_rate)
    data['Market Cap (US$ Billion)'] = data['Market Cap (US$ Billion)'].round(decimals = 2)
    data.rename(columns={'Market Cap (US$ Billion)':'Market Cap (GBP$ Billion)'},inplace=True)
    return data

def load(targetfile,data_to_load):
    data_to_load.to_csv(targetfile)


def log(message):
    timestamp_format = '%H:%M:%S-%h-%d-%Y'  # Hour-Minute-Second-MonthName-Day-Year
    now = datetime.now()  # get current timestamp
    timestamp = now.strftime(timestamp_format)
    with open("bank_market_cap_logfile.txt", "a") as f:
        f.write(timestamp + ',' + message + '\n')
# Write your code here
log("ETL Job Started")

log("Extract phase Started")
extracted_data = extract(marketCap_file)


log("Transform phase Started")

transformed_data = transform(extracted_data)
print(transformed_data.head(5))

log("Transform phase Ended")

log("Load phase Started")
load(loadfile,transformed_data)

log("Load phase Ended")

log("ETL Job Ended")
