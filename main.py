# -*- coding: utf-8 -*-
# @Author: Bi Ying
# @Date:   2023-05-22 14:32:17
# @Last Modified by:   Bi Ying
# @Last Modified time: 2023-05-22 17:39:33
import re
import shutil
import zipfile
import asyncio
from pathlib import Path
from datetime import datetime

import httpx
import pandas as pd
from bs4 import BeautifulSoup


# Better to set a cookie to avoid being blocked
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36",
    # "Cookie": "",
}
BATCH_SIZE = 3
SLEEP_INTERVAL = 3
ERROR_SLEEP_INTERVAL = 10
PROXIES = {
    "http://": "http://127.0.0.1:7890",
    "https://": "http://127.0.0.1:7890",
}
START_YEAR = 2005

index_extractor = re.compile(r"news/(\d+)")
columns = ["index", "url", "title", "author", "timestamp", "content", "datetime"]


async def crawl_single_month_articles_urls(url: str):
    print("Crawling", url)
    try_times = 0
    async with httpx.AsyncClient(proxies=PROXIES, http2=True, headers=HEADERS) as client:
        while try_times < 3:
            try:
                response = await client.get(url, headers=HEADERS)
                soup = BeautifulSoup(response.text, "lxml")
                articles = soup.select(".standard-box.standard-list a")
                return [article["href"] for article in articles]
            except Exception as e:
                print(f"Error: {e} {url}")
                try_times += 1
                await asyncio.sleep(ERROR_SLEEP_INTERVAL)
    return []


async def crawl_articles_urls(output_file: str = "articles_urls.txt"):
    current_year = datetime.now().year
    years = list(range(START_YEAR, current_year + 1))
    months = [
        "january",
        "february",
        "march",
        "april",
        "may",
        "june",
        "july",
        "august",
        "september",
        "october",
        "november",
        "december",
    ]
    articles_urls = []
    years_months_pairs = [(year, month) for year in years for month in months]
    articles_list_batches = [
        years_months_pairs[i : i + BATCH_SIZE] for i in range(0, len(years_months_pairs), BATCH_SIZE)
    ]
    print("Batches count:", len(articles_list_batches))
    for articles_list_batch in articles_list_batches:
        tasks = [
            crawl_single_month_articles_urls(f"https://www.hltv.org/news/archive/{year}/{month}")
            for year, month in articles_list_batch
        ]
        results = await asyncio.gather(*tasks)
        for result in results:
            articles_urls.extend(result)
        await asyncio.sleep(SLEEP_INTERVAL)

    print("Articles count:", len(articles_urls))

    with open(output_file, "w") as f:
        f.write("\n".join(articles_urls))


async def crawl_single_article(url: str):
    if not url.startswith("https://www.hltv.org"):
        url = "https://www.hltv.org" + url
    print("Crawling", url)
    try_times = 0
    async with httpx.AsyncClient(proxies=PROXIES, http2=True, headers=HEADERS) as client:
        while try_times < 3:
            content = None
            try:
                response = await client.get(url)
                content = response.text
                soup = BeautifulSoup(response.text, "lxml")
                title = soup.select_one(".headline").text
                author = soup.select_one(".article-info .author").text
                timestamp = soup.select_one(".article-info .date")["data-unix"]
                content = soup.select_one(".newsdsl").text
                return {
                    "index": index_extractor.search(url).group(1),
                    "url": url,
                    "title": title,
                    "author": author,
                    "timestamp": timestamp,
                    "datetime": datetime.fromtimestamp(int(timestamp) // 1000),
                    "content": content.strip(),
                }
            except Exception as e:
                print(url, e)
                print(f"{url} content: {content}")
                try_times += 1
                await asyncio.sleep(ERROR_SLEEP_INTERVAL)


async def crawl_articles_content(
    urls_file: str = "articles_urls.txt",
    output_file: str = "articles_data.csv",
):
    with open(urls_file, "r") as f:
        articles_urls = f.read().splitlines()

    if Path(output_file).exists():
        df = pd.read_csv(output_file)
        done_urls = df["url"].tolist()
    else:
        df = pd.DataFrame(columns=columns)
        done_urls = []

    articles_batches = []
    batch_index = 0
    current_batch = []
    for url in articles_urls:
        if not url.startswith("https://www.hltv.org"):
            url = "https://www.hltv.org" + url
        if url not in done_urls:
            print("Adding", url, "to batch. Batch index:", batch_index)
            current_batch.append(url)
            if len(current_batch) == BATCH_SIZE:
                articles_batches.append(current_batch)
                current_batch = []
                batch_index += 1

    print("Batches count:", len(articles_batches))
    for articles_batch in articles_batches:
        tasks = [crawl_single_article(url) for url in articles_batch]
        results = await asyncio.gather(*tasks)

        new_df = pd.DataFrame([result for result in results if result is not None])
        if not new_df.empty:
            df = pd.concat([df, new_df], ignore_index=True)
        else:
            print("None data:", articles_batch)
        await asyncio.sleep(SLEEP_INTERVAL)

    df["datetime"] = pd.to_datetime(df["datetime"])
    df = df.sort_values(by=["datetime"])
    df.to_csv(output_file, index=False)


def write_articles_to_file(data_file: str = "articles_data.csv", output_folder: str = "./"):
    temp_articles_folder = Path(output_folder) / "temp_articles"
    if not temp_articles_folder.exists():
        temp_articles_folder.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(data_file, parse_dates=["datetime"])

    for _, row in df.iterrows():
        title = re.sub(r"[^\w\-_\. ]", "_", row["title"])
        datetime = row["datetime"].strftime("%Y-%m-%d-%H-%M-%S")
        file_name = f"{datetime}_{title}.txt"
        file_path = temp_articles_folder / file_name
        with open(file_path, "w") as f:
            f.write(f"# {row['title']}")
            f.write("\n\n")
            f.write(f"Author: {row['author']}")
            f.write("\n\n")
            f.write(f"Datetime: {row['datetime']}")
            f.write("\n\n")
            f.write(str(row["content"]).strip())

    with zipfile.ZipFile(Path(output_folder) / "articles.zip", "w", zipfile.ZIP_DEFLATED) as zipf:
        for file in temp_articles_folder.iterdir():
            zipf.write(file, file.name)

    shutil.rmtree(temp_articles_folder)


async def main():
    await crawl_articles_urls()
    await crawl_articles_content()


asyncio.run(main())
write_articles_to_file()
