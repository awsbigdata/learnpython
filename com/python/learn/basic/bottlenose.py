import requests
import json
url = "https://yahoo-finance15.p.rapidapi.com/api/yahoo/qu/quote/HDFCBANK.NS,TCS.NS"

headers = {
    'x-rapidapi-key': "df27a77ec4mshc95a798c15cc04bp1f5654jsne511d2b53144",
    'x-rapidapi-host': "yahoo-finance15.p.rapidapi.com"
    }

response = requests.request("GET", url, headers=headers)

print(json.loads(response.text))

