import aiohttp
import asyncio
import csv
import os
from concurrent.futures import ThreadPoolExecutor

class AsyncScraper:
    BASE_URL = "https://svc-silinda.jabarprov.go.id/api/api/graphic_data"
    HEADERS = ["commodity_name", "location_id", "commodity_id", "legend", "value", "time", "date"]

    async def fetch_data(self, session, commodity_id, location_id):
        url = f"{self.BASE_URL}/{commodity_id}/{location_id}/day/price/2021-01-01%20%20/2024-04-07/0/market/-/eceran/null"
        try:
            async with session.get(url) as response:
                if response.status >= 500:
                    print(f"Internal Server Error for commodity {commodity_id} at location {location_id}, skipping...")
                    return None
                response.raise_for_status()
                return await response.json()
        except aiohttp.ClientError as e:
            print(f"Request failed for commodity {commodity_id} at location {location_id}: {e}")
            return None

    async def scrap_and_write_csv(self, max_location=396):
        os.makedirs('results_csv', exist_ok=True)
        async with aiohttp.ClientSession() as session:
            tasks = [self.process_commodity(session, commodity_id, max_location) for commodity_id in range(1, 108)]
            await asyncio.gather(*tasks)

    async def process_commodity(self, session, commodity_id, max_location):
        filename = f"results_week_csv/commodity_{commodity_id}.csv"
        with open(filename, mode='w', newline='', encoding='utf-8') as file:
            writer = csv.DictWriter(file, fieldnames=self.HEADERS)
            writer.writeheader()
            with ThreadPoolExecutor() as executor:
                loop = asyncio.get_running_loop()
                for location_id in range(1, max_location + 1):
                    data = await self.fetch_data(session, commodity_id, location_id)
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
                                    **{k: v for k, v in result.items() if k in self.HEADERS}
                                }
                                await loop.run_in_executor(executor, writer.writerow, row)

if __name__ == "__main__":
    scraper = AsyncScraper()
    asyncio.run(scraper.scrap_and_write_csv())
