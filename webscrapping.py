from bs4 import BeautifulSoup
import requests
import pandas as pd

#Write your code here
response = requests.get("https://en.wikipedia.org/wiki/List_of_largest_banks")
html_data = response.content


soup = BeautifulSoup(html_data, "html5lib")


data = pd.DataFrame(columns=["Name", "Market Cap (US$ Billion)"])
for row in soup.find_all('tbody')[3].find_all('tr'):
    col = row.find_all('td')
    if col:
        bank_name = col[1].find_all("a")[1].text
        market_cap = float(col[2].text)
        data = data.append({"Name": bank_name,
                            "Market Cap (US$ Billion)": market_cap},
                           ignore_index=True)

print(data.head())

data.to_json("bank_market_cap_1.json")
