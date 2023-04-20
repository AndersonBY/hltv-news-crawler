# -*- coding: utf-8 -*-
# @Author: Bi Ying
# @Date:   2023-02-13 14:54:28
# @Last Modified by:   Bi Ying
# @Last Modified time: 2023-04-20 21:51:16
import csv
import asyncio
from pathlib import Path
from datetime import datetime

import httpx
from bs4 import BeautifulSoup


HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36"
}
BATCH_SIZE = 3
PROXY = "http://127.0.0.1:7890"


async def crawl_single_month_articles_urls(url: str):
    print("Crawling", url)
    try_times = 0
    async with httpx.AsyncClient(proxies=PROXY, http2=True, headers=HEADERS) as client:
        while try_times < 3:
            try:
                response = await client.get(url, headers=HEADERS)
                soup = BeautifulSoup(response.text, "lxml")
                articles = soup.select(".standard-box.standard-list a")
                return [article["href"] for article in articles]
            except Exception as e:
                print(f"Error: {e} {url}")
                try_times += 1
                await asyncio.sleep(10)
    return []


async def crawl_articles_urls(output_file: str = "articles_urls.txt"):
    current_year = datetime.now().year
    years = list(range(2005, current_year + 1))
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
        await asyncio.sleep(1)

    print("Articles count:", len(articles_urls))

    with open(output_file, "w") as f:
        f.write("\n".join(articles_urls))


async def crawl_single_article(url: str):
    print("Crawling", url)
    try_times = 0
    async with httpx.AsyncClient(proxies=PROXY, http2=True, headers=HEADERS) as client:
        while try_times < 3:
            try:
                response = await client.get(url)
                soup = BeautifulSoup(response.text, "lxml")
                title = soup.select_one(".headline").text
                author = soup.select_one(".article-info .author").text
                timestamp = soup.select_one(".article-info .date")["data-unix"]
                content = soup.select_one(".newsdsl").text
                return {
                    "url": url,
                    "title": title,
                    "author": author,
                    "timestamp": timestamp,
                    "content": content,
                }
            except Exception as e:
                print(url, e)
                try_times += 1
                await asyncio.sleep(10)


async def crawl_articles_content(
    urls_file: str = "articles_urls.txt",
    output_file: str = "articles_data.csv",
):
    with open(urls_file, "r") as f:
        articles_urls = f.read().splitlines()

    if Path(output_file).exists():
        need_write_header = False
        with open(output_file, newline="", encoding="utf8") as csvfile:
            reader = csv.DictReader(csvfile)
            done_urls = [row["url"] for row in reader]
    else:
        need_write_header = True
        done_urls = []
    print(done_urls)

    if need_write_header:
        with open(output_file, "a", newline="", encoding="utf8") as csvfile:
            fieldnames = ["url", "title", "author", "timestamp", "content"]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

    articles_batches = [
        articles_urls[i : i + BATCH_SIZE]
        for i in range(0, len(articles_urls), BATCH_SIZE)
        if articles_urls[i] not in done_urls
    ]
    print("Batches count:", len(articles_batches))
    for articles_batch in articles_batches:
        tasks = [crawl_single_article(url) for url in articles_batch]
        results = await asyncio.gather(*tasks)

        with open(output_file, "a", newline="", encoding="utf8") as csvfile:
            fieldnames = ["url", "title", "author", "timestamp", "content"]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            for result in results:
                if result is not None:
                    writer.writerow(result)
                else:
                    print("None data:", articles_batch)
        await asyncio.sleep(4)


async def main():
    await crawl_articles_urls()
    await crawl_articles_content()


asyncio.run(main())
