from bs4 import BeautifulSoup
import html5lib
import requests
import pandas as pd


url = "https://api.apilayer.com/exchangerates_data/latest?base=EUR&apikey=qkrJ9dQLves7k0bXjHvO80s3BOOIR8mD"

response = requests.get(url)


dataframe = pd.DataFrame(response.json())


# Drop unnescessary columns
dataframe.drop(columns=["success", "timestamp", "base", "date"], inplace=True)
print(dataframe)

dataframe.to_csv("exchange_rates_1.csv")