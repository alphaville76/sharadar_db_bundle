import random
import requests
import pandas as pd
import sharadar.loaders.constant as k


def random_user_agent():
    return random.choice(k.USER_AGENTS)

head = {
            "User-Agent": random_user_agent(),
            "X-Requested-With": "XMLHttpRequest",
            "Accept": "text/html",
            "Accept-Encoding": "gzip, deflate",
            "Connection": "keep-alive",
        }

# U.S. ISM Manufacturing Purchasing Managers Index (PMI)
# https://www.investing.com/economic-calendar/ism-manufacturing-pmi-173
url = 'https://sbcharts.investing.com/events_charts/us/173.json'
response = requests.get(url, headers=head)
if response.status_code != 200:
    raise ConnectionError(
        f"ERR#0015: error {response.status_code}, try again later."
    )

data = response.json()["data"]
df = pd.DataFrame(data, columns=['timestamp', 'value', 'forecast'])
df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms', utc=True)
df.set_index('timestamp', inplace=True)
df.drop(columns=['forecast'], inplace=True)

print(df.sort_index(ascending=False).head())