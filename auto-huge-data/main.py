import asyncio
import os
import logging
import time
import csv
from datetime import datetime
from curl_cffi.requests import AsyncSession, Response
from rich.logging import RichHandler
from pymongo import MongoClient
from asyncio.windows_events import WindowsSelectorEventLoopPolicy

PROXY = "stickyproxy"

MONGO_HOST = "192.168.68.52"
MONGO_PORT = 32769
MONGO_DBNAME = "scrapeditems"
MONGO_COLS = "products"

mongo = MongoClient(host=MONGO_HOST, port=MONGO_PORT)
db = mongo[MONGO_DBNAME]
collection = db[MONGO_COLS]

# optional
start_time = time.time()
logging.basicConfig(
    format="{asctime} - {levelname} - {message}",
    style="{",
    datefmt="%Y-%m-d %H:%M",
    level=logging.INFO,
    handlers=[RichHandler()],
)

log = logging.getLogger("rich")

# mandatory

with open("urls.csv", "r") as f:
    reader = csv.reader(f)
    urls = [url[0] for url in reader]


async def run() -> list[Response]:
    async with AsyncSession() as session:
        proxy = os.getenv(PROXY)
        if proxy is not None:
            log.info("proxy from ENV")
            session.proxies = {"http": proxy, "https": proxy}
        else:
            log.warning("no proxy found continuing without.")

        tasks = []

        for url in urls:
            task = session.get(url)
            tasks.append(task)

        result = await asyncio.gather(*tasks)

    return result


def main():
    asyncio.set_event_loop_policy(WindowsSelectorEventLoopPolicy())
    data = asyncio.run(run())
    failed = []
    results = []

    for response in data:
        if response.status_code != 200:
            log.warning(f"failed on {response.url} with code {response.status_code}")
            failed.append(response.url)
        else:
            results.append(
                {"url": response.url, "date": datetime.now(), "html": response.text}
            )
    inserted = collection.insert_many(results)
    log.info(inserted)

    print("\n--- %s seconds ---" % (time.time() - start_time))


if __name__ == "__main__":
    main()
