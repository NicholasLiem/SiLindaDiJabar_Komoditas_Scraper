import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import csv
import os

class Scraper:
    def __init__(self):
        self.BASE_URL = "https://svc-silinda.jabarprov.go.id/api/api/graphic_data"
        self.headers = ["commodity_name", "location_id", "commodity_id", "legend", "value", "time", "date"]
        self.session = requests.Session()
        retries = Retry(total=5, backoff_factor=1, status_forcelist=[502, 503, 504])
        self.session.mount('https://', HTTPAdapter(max_retries=retries))

    def fetch_data(self, commodity_id, location_id):
        try:
            response = self.session.get(
                f"{self.BASE_URL}/{commodity_id}/{location_id}/day/price/2021-01-01%20%20/2024-04-07/0/market/-/eceran/null"
            )
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            print(f"Request failed: {e}")
            return None

    def scrap_and_write_csv(self, max_location=396):
        os.makedirs('results_csv', exist_ok=True)

        for commodity_id in range(2, 108):
            filename = f"commodity_{commodity_id}.csv"
            
            with open(f'results_csv/{filename}', mode='w', newline='', encoding='utf-8') as file:
                writer = csv.DictWriter(file, fieldnames=self.headers)
                writer.writeheader()

                for location_id in range(1, max_location + 1):
                    data = self.fetch_data(commodity_id, location_id)
                    if data and 'data' in data:
                        for item in data['data']:
                            if not isinstance(item.get("result"), list):
                                continue
                            for result in item.get("result"):
                                row = {
                                    "commodity_name": item.get("commodity_name", f"Commodity {commodity_id}"),
                                    "location_id": item.get("location_id", ""),
                                    "commodity_id": commodity_id,
                                    "legend": item.get("legend", ""),
                                    **{k: v for k, v in result.items() if k in self.headers}
                                }
                                writer.writerow(row)
                    else:
                        print(f"Failed to fetch or parse data for commodity ID {commodity_id} at location {location_id}")



scraper = Scraper()
scraper.scrap_and_write_csv()
