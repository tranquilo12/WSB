import asyncio
import gzip
import io
import json
import urllib.parse
from urllib.parse import urlparse, urlencode

import boto3
import pandas as pd
from aiohttp import ClientSession
from aiolimiter import AsyncLimiter
from requests.exceptions import HTTPError
from tqdm.auto import tqdm

from utils import batch
from wsb import Gather

########################################################################################################################

asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
limiter = AsyncLimiter(max_rate=1, time_period=3)
gather_wsb = Gather()

########################################################################################################################


def get_submission_search_urls(start_date: str, end_date: str) -> list:
    base_url = "https://api.pushshift.io/reddit/search/submission/?"
    # submission_status_df = gather_wsb.get_submission_status_mat_view(local=True)

    date_range = [x for x in pd.date_range(start=start_date, end=end_date)]

    # For missing_start_end_dates
    missing_dates = list(set(date_range))

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


async def download(
    url,
    session,
    return_sub_id_from_url: bool = False,
    return_sub_id_from_dict: bool = False,
) -> dict:

    if return_sub_id_from_dict:
        sub_id, url = list(url.keys())[0], list(url.values())[0]
    elif return_sub_id_from_url:
        sub_id = url.split("/")[-1]
    else:
        sub_id = None

    async with limiter:
        try:
            response = await session.request(url=url, method="GET", timeout=600)
            response.raise_for_status()
            response_json = await response.json()
            response_json["submission_id"] = sub_id
        except HTTPError as herr:
            response_json = herr.response.json()
            print(f"An error occurred: {herr}")
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


def uri_validate(x):
    try:
        valid_result = urllib.parse.urlparse(x)
        assert all(
            [valid_result.scheme, valid_result.netloc]
        ), "Please validate `comment_ids_urls` passed as list."
    except:
        return False


async def make_urls_using_sub_id_for_comments(
    start_date: str, end_date: str, comments_ids_urls: list = None
) -> list:
    search_comments_base_url = "https://api.pushshift.io/reddit/comment/search"

    if comments_ids_urls is None:
        comments_ids_urls = get_comments_ids_search_urls(
            start_date=start_date, end_date=end_date
        )
    else:
        if uri_validate(comments_ids_urls[0]):
            pass
        else:
            return [None]

    all_urls = []

    async with ClientSession() as session:
        tasks = [
            asyncio.create_task(
                download(url=url, session=session, return_sub_id_from_url=True)
            )
            for url in comments_ids_urls
        ]

        for future in tqdm(
            asyncio.as_completed(tasks),
            total=len(comments_ids_urls),
            desc="Searching for comments ids within submissions",
        ):
            results = await future

            if results and ("data" in results.keys()):
                sub_id = results["submission_id"]
                results = results["data"]

                for id_batch in batch(results, n=300):
                    ids = ",".join(id_batch)
                    all_urls.append({sub_id: f"{search_comments_base_url}?ids={ids}"})

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
            asyncio.create_task(
                download(url=url, session=session, return_sub_id_from_dict=True)
            )
            for url in urls
        ]

        for future in tqdm(
            asyncio.as_completed(tasks),
            total=len(urls),
            desc="Downloading comments in batches...",
        ):
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


def upload_json_gz_to_s3(key: str, obj: list):
    s3_client = boto3.client("s3")
    bytes_data = io.BytesIO()
    with gzip.GzipFile(fileobj=bytes_data, mode="wb") as bf:
        with io.TextIOWrapper(bf, encoding="utf-8") as wrapper:
            wrapper.write(json.dumps(obj, ensure_ascii=False, default=None))
    bytes_data.seek(0)
    s3_client.upload_fileobj(
        bytes_data,
        "polygonio-dumps",
        key,
    )


def download_json_gz_from_s3(key):
    s3_client = boto3.client("s3")
    response = s3_client.get_object(Bucket="polygonio-dumps", Key=key)
    content = response["Body"].read()
    with gzip.GzipFile(fileobj=io.BytesIO(content), mode="rb") as fh:
        return json.load(fh)


########################################################################################################################
if __name__ == "__main__":
    sdate, edate = "2021-07-01", "2021-07-31"
    loop = asyncio.get_event_loop()

    comments_urls_with_sub_ids = loop.run_until_complete(
        make_urls_using_sub_id_for_comments(start_date=sdate, end_date=edate)
    )

    comments_list = loop.run_until_complete(
        extract_comments(urls=comments_urls_with_sub_ids)
    )

    upload_key = f"comments_list_from_{sdate}_{edate}.json.gzip"
    print(f"- Uploading to S3://polygonio-dumps/{upload_key}")
    upload_json_gz_to_s3(key=upload_key, obj=comments_list)
    print("- Done")
