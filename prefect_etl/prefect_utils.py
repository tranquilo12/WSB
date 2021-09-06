import asyncio
import requests
import pandas as pd
from wsb import Gather
from utils import batch
from tqdm.auto import tqdm
from aiohttp import ClientSession
from aiolimiter import AsyncLimiter
from requests.exceptions import HTTPError
from urllib.parse import urlparse, urlencode

########################################################################################################################

asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
limiter = AsyncLimiter(max_rate=1, time_period=2)
gather_wsb = Gather()

########################################################################################################################


def get_submission_search_urls(start_date: str, end_date: str) -> list:
    base_url = "https://api.pushshift.io/reddit/search/submission/?"
    # submission_status_df = gather_wsb.get_submission_status_mat_view(local=True)

    date_range = [x for x in pd.date_range(start=start_date, end=end_date)]

    # For missing_start_end_dates
    missing_dates = list(
        set(date_range)
    )

    missing_start_end_dates = [
        (
            int((x - pd.Timedelta(days=1)).timestamp()),
            int((x + pd.Timedelta(days=1)).timestamp()),
        )
        for x in missing_dates
    ]

    # Make all params
    all_urls = []
    for start, end in missing_start_end_dates:
        parsed = urlparse(base_url)
        params = {
            "subreddit": "wallstreetbets",
            "before": str(end),
            "after": str(start),
            "sort_type": "num_comments",
            "sort": "desc",
            "limit": "1000",
        }
        params = urlencode(params)
        parsed = parsed._replace(query=params)
        all_urls.append(parsed.geturl())

    return all_urls


def get_comments_ids_search_urls(start_date: str, end_date: str) -> list:
    (
        submission_comments_status_df,
        _,
    ) = gather_wsb.get_submission_comments_status_mat_view(local=True)

    submission_ids = submission_comments_status_df.loc[
                     start_date:end_date, "submission_id"
                     ].values.tolist()

    search_comments_base_url = "https://api.pushshift.io/reddit/submission/comment_ids"
    all_urls = []

    for submission_id in submission_ids:
        all_urls.append(f"{search_comments_base_url}/{submission_id}")

    return all_urls


########################################################################################################################


async def download(url, session) -> dict:
    async with limiter:
        try:
            response = await session.request(url=url, method="GET")
            response.raise_for_status()
            response_json = await response.json()
        except HTTPError as http_err:
            print(f"HTTP error occurred: {http_err}")
            response_json = None
        except Exception as err:
            response_json = None
            print(f"An error occurred: {err}")
    return response_json


async def extract_submissions(start_date: str, end_date: str) -> list:
    urls = get_submission_search_urls(start_date=start_date, end_date=end_date)
    cols = ["created_utc", "id", "author", "url", "title", "selftext", "stickied"]
    all_results = []
    async with ClientSession() as session:
        tasks = [asyncio.create_task(download(url, session)) for url in urls]

        for future in tqdm(asyncio.as_completed(tasks), total=len(urls)):
            results = await future

            if results:
                results = results["data"]

                for result in results:
                    r = {
                        col: (result[col] if col in result.keys() else None)
                        for col in cols
                    }
                    all_results.append(r)
    return all_results


async def extract_comments_ids_and_make_urls(start_date: str, end_date: str) -> list:
    search_comments_base_url = "https://api.pushshift.io/reddit/comment/search"

    comments_ids_urls = get_comments_ids_search_urls(start_date=start_date, end_date=end_date)
    all_urls = []

    async with ClientSession() as session:
        tasks = [
            asyncio.create_task(download(url=url, session=session)) for url in comments_ids_urls
        ]

        for future in tqdm(asyncio.as_completed(tasks), total=len(comments_ids_urls), desc="Searching for comments, within submissions"):
            results = await future

            if results:
                results = results["data"]

                for id_batch in batch(results, n=100):
                    ids = ','.join(id_batch)
                    all_urls.append(f"{search_comments_base_url}?ids={ids}")

    return all_urls


async def extract_comments(urls: list) -> list:
    cols = [
        "created_utc",
        "retrieved_on",
        "id",
        "parent_id",
        "link_id",
        "author",
        "submission_id",
        "body",
        "subreddit",
    ]
    all_results = []
    async with ClientSession() as session:
        tasks = [
            asyncio.create_task(download(url=url, session=session)) for url in urls
        ]

        for future in tqdm(asyncio.as_completed(tasks), total=len(urls)):
            results = await future

            if results:
                results = results["data"]

                for result in results:
                    r = {
                        col: (result[col] if col in result.keys() else None)
                        for col in cols
                    }
                    all_results.append(r)
    return all_results


########################################################################################################################
