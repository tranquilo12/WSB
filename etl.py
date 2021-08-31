import os
import sys
import asyncio
import prefect
import requests
import datetime
import pandas as pd
from wsb import Gather
from tqdm.auto import tqdm
from prefect import task, Flow
from aiohttp import ClientSession
from aiolimiter import AsyncLimiter
from requests.exceptions import HTTPError
from dask.distributed import Client, as_completed
from urllib.parse import urlparse, urlencode, parse_qsl
from prefect.tasks.postgres.postgres import PostgresExecute, PostgresExecuteMany

asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
limiter = AsyncLimiter(max_rate=1, time_period=2)
gather_wsb = Gather()


def get_submission_search_urls() -> list:
    base_url = "https://api.pushshift.io/reddit/search/submission/?"
    submission_status_df = gather_wsb.get_submission_status_mat_view(local=True)

    # We're only interested in things from 2019-Jun
    start_date = "2019-06-01"
    date_range = [x for x in pd.date_range(start=start_date, end="2021-08-29")]

    dates_in_submissions = submission_status_df.loc[
        submission_status_df["date"] > "2019-06-01", "date"
    ].values

    # For missing_start_end_dates
    missing_dates = list(
        set(date_range) - set([pd.to_datetime(x) for x in dates_in_submissions])
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
            "limit": "1000"
        }
        params = urlencode(params)
        parsed = parsed._replace(query=params)
        all_urls.append(parsed.geturl())

    return all_urls


def get_comments_search_urls() -> list:
    base_url = "https://api.pushshift.io/reddit/search/comments/?"
    submission_status_df = gather_wsb.get_submission_comments_status_mat_view(local=True)

    # We're only interested in things from 2019-Jun
    start_date = "2019-06-01"
    date_range = [x for x in pd.date_range(start=start_date, end="2021-08-29")]

    dates_in_submissions = submission_status_df.loc[
        submission_status_df["date"] > "2019-06-01", "date"
    ].values

    # For missing_start_end_dates
    missing_dates = list(
        set(date_range) - set([pd.to_datetime(x) for x in dates_in_submissions])
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
        }
        params = urlencode(params)
        parsed = parsed._replace(query=params)
        all_urls.append(parsed.geturl())

    return all_urls


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


async def extract_submissions() -> list:
    urls = get_submission_search_urls()
    cols = ["created_utc", "id", "author", "url", "title", "selftext", "stickied"]
    all_results = []
    async with ClientSession() as session:
        tasks = [asyncio.create_task(download(url, session)) for url in urls]
        for future in tqdm(asyncio.as_completed(tasks), total=len(urls)):
            results = await future

            if results:
                results = results["data"]

                for result in results:
                    r = {col: (result[col] if col in result.keys() else None) for col in cols}
                    all_results.append(r)

    return all_results


async def extract_submissions_wrapper() -> list:
    return await extract_submissions()


@task
def insert_submissions(submissions_list: list):
    gather_wsb.insert_submissions(submissions=submissions_list, local=True)


@task
def insert_submissions_2(submissions_dict_list: list):
    cols = ",".join(gather_wsb.submission_cols)
    PostgresExecuteMany(
        db_name=gather_wsb.config["POSTGRESLOCAL"]["database"],
        user=gather_wsb.config["POSTGRESLOCAL"]["username"],
        host=gather_wsb.config["POSTGRESLOCAL"]["host"],
        port=gather_wsb.config["POSTGRESLOCAL"]["port"],
        query=f"INSERT INTO submissions({cols}) VALUES (%s);",
        data=submissions_dict_list,
        commit=True
    )


@task
def refresh_submission_status_mat_view():
    PostgresExecute(
        db_name=gather_wsb.config["POSTGRESLOCAL"]["database"],
        user=gather_wsb.config["POSTGRESLOCAL"]["username"],
        host=gather_wsb.config["POSTGRESLOCAL"]["host"],
        port=gather_wsb.config["POSTGRESLOCAL"]["port"],
        query="REFRESH MATERIALIZED VIEW submission_status;",
        commit=True
    )


with Flow("UpdateSubAndRefreshSubView") as flow:
    submissions = asyncio.run(extract_submissions_wrapper())
    insert_submissions_2(submissions_dict_list=submissions)
    refresh_submission_status_mat_view()


# flow.register(project_name="UpdateSubProject")

if __name__ == "__main__":
    flow.run()
