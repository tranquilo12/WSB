import asyncio
import logging
from datetime import datetime, timedelta
from typing import Optional, Any

import nest_asyncio
import pandas as pd
from pmaw import PushshiftAPI
from psycopg2.extras import execute_values
from tqdm.auto import tqdm

from prefect_utils import (
    make_urls_using_sub_id_for_comments,
    extract_submissions,
    extract_comments,
    Gather,
)

nest_asyncio.apply()
gwsb = Gather()
praw_client = gwsb.get_praw_client()

logger = logging.getLogger("psaw")
handler = logging.StreamHandler()
handler.setLevel(logging.DEBUG)
logger.addHandler(handler)


def extract_submissions_wrapper(start_date: str, end_date: str) -> list:
    """
    Wrapper function to extract submissions from reddit API.
    :param start_date: str (YYYY-MM-DD)
    :param end_date: str (YYYY-MM-DD)
    """
    all_results = asyncio.run(
        extract_submissions(start_date=start_date, end_date=end_date)
    )
    return all_results


def extract_comments_wrapper(urls: list) -> list:
    """
    Wrapper function to extract comments from reddit API.
    :param urls: list of urls
    """
    all_results = asyncio.run(extract_comments(urls=urls))
    return all_results


def insert_submissions(dict_list: list) -> None:
    """
    Insert submissions to db.
    :params dict_list: list of dicts that contain submissions
    """
    gather_wsb = Gather()
    gather_wsb.insert_submissions(submissions=dict_list, local=True)


def insert_comments(comments_dict_list: list) -> None:
    """
    Insert comments to db.
    :param comments_dict_list: list of dicts that contain comments
    """
    gather_wsb = Gather()
    gather_wsb.insert_comments(comments=comments_dict_list, local=True)


def refresh_submission_status_mat_view() -> None:
    """
    Refresh Submission Status materialized view.
    """
    gather_wsb = Gather()
    gather_wsb.refresh_submission_status_mat_view(local=True)


def refresh_submission_comments_status_mat_view() -> None:
    """
    Refresh Submission Comments Status materialized view.
    """
    gather_wsb = Gather()
    gather_wsb.refresh_submission_comments_status_mat_view(local=True)


def extract_comments_ids_and_make_urls_wrapper(start_date: str, end_date: str) -> list:
    """
    Extract comments ids and make urls.
    :param start_date: str (YYYY-MM-DD)
    :param end_date: str (YYYY-MM-DD)
    """
    all_urls = asyncio.run(
        make_urls_using_sub_id_for_comments(start_date=start_date, end_date=end_date)
    )
    return all_urls


def update_submissions_praw(
    gather_wsb: Gather,
    api: PushshiftAPI,
    lookback_days: int,
    return_submissions: bool = False,
) -> list[dict[str, Optional[Any]]]:
    """
    Update submissions in praw.
    :param gather_wsb: Gather
    :param api: PushshiftAPI
    :param lookback_days: int
    :param return_submissions: bool
    """
    after = int((datetime.today() - timedelta(days=lookback_days)).timestamp())
    before = int(datetime.today().timestamp())
    params = {
        "after": after,
        "before": before,
        "subreddit": "wallstreetbets",
        "filter": [
            "created_utc",
            "id",
            "author",
            "url",
            "title",
            "selftext",
            "stickied",
        ],
        "limit": 100000,
    }

    print("-- Downloading...")
    url_results = api.search_submissions(**params)
    recent_submissions_as_objs = [res for res in url_results]
    print(f"-- Length of submissions downloaded: {len(recent_submissions_as_objs)}")

    # Two different dicts created, one for my information (that can be returned) and another that can be inserted.
    all_recent_submissions = [
        {
            "created_utc": submission["created_utc"],
            "id": submission["id"],
            "author": submission["author"].__dict__["name"]
            if submission["author"]
            else None,
            "url": submission["url"],
            "title": submission["title"],
            "num_comments": submission["num_comments"],
            "selftext": submission["selftext"],
            "stickied": submission["stickied"],
        }
        for submission in recent_submissions_as_objs
    ]

    all_recent_submissions_to_be_inserted = [
        {
            "created_utc": submission["created_utc"],
            "id": submission["id"],
            "author": submission["author"].__dict__["name"]
            if submission["author"]
            else None,
            "url": submission["url"],
            "title": submission["title"],
            "selftext": submission["selftext"],
            "stickied": submission["stickied"],
        }
        for submission in recent_submissions_as_objs
    ]

    print("-- Inserting all submissions...")
    gather_wsb.insert_submissions(
        submissions=all_recent_submissions_to_be_inserted, local=True
    )
    print("-- Done...")

    if return_submissions:
        return all_recent_submissions
    else:
        pass


def clean_comments(all_comments_within_submission: list) -> list:
    """
    A function that cleans comments from psaw.
    :param all_comments_within_submission: list of comments
    """
    comments_as_tuples = []
    for comment in tqdm(all_comments_within_submission):
        if not isinstance(comment, dict):
            comment = comment.__dict__

        cols = [
            "created_utc",
            "id",
            "parent_id",
            "link_id",
            "author",
            "body",
            "subreddit",
        ]
        comment = {col: comment[col] for col in cols}
        comment["submission_id"] = comment["link_id"].split("_")[1]

        # All the authors, converted from the author object to a string.
        if comment["author"]:
            comment["author"] = (
                comment["author"].__dict__["name"]
                if comment["author"] is not None
                else None
            )

        # for all the comments, we just have "wallstreetbets" as the subreddit
        if comment["subreddit"]:
            comment["subreddit"] = "wallstreetbets"

        # easier to insert when they're tuples
        comments_as_tuples.append(tuple(v for v in comment.values()))
    return comments_as_tuples


def update_comments_praw(
    all_recent_submissions: list,
    gather_wsb: Gather,
    api: PushshiftAPI,
) -> None:
    """
    A function to update all comments, taking the submissions from all_recent_submissions_df
    :param all_recent_submissions: list
    :param gather_wsb: Gather
    :param api: PushshiftApi
    """
    print("-- Converting submissions list to pd.DataFrame...")
    all_recent_submissions = pd.DataFrame.from_records(all_recent_submissions)
    all_recent_submissions["date"] = all_recent_submissions["created_utc"].apply(
        lambda x: pd.to_datetime(int(x), unit="s")
    )

    print("-- Identifying submissions with comments...")
    all_recent_submissions = all_recent_submissions.set_index("date")
    all_recent_submissions = all_recent_submissions.sort_values(
        by="num_comments", ascending=False
    )
    all_submissions_with_comments = all_recent_submissions.loc[
        all_recent_submissions["num_comments"] > 0, "id"
    ].values

    print("-- Downloading comments...")
    comments_as_tuples = []
    for submission_id in tqdm(all_submissions_with_comments):
        all_comments_within_submission = list(
            api.search_comments(
                subreddit="wallstreetbets", link_id=submission_id, limit=10000000
            )
        )
        cleaned_comments = clean_comments(
            all_comments_within_submission=all_comments_within_submission
        )
        comments_as_tuples += cleaned_comments

    print(f"-- Length of comments downloaded: {len(comments_as_tuples)}")
    insert_query = f"INSERT INTO comments(created_utc, id, parent_id, link_id, author, submission_id, body, subreddit) VALUES %s ON CONFLICT (created_utc, id) DO NOTHING;"
    with gather_wsb.get_psycopg2_conn(local=True) as conn:
        with conn.cursor() as cur:
            execute_values(cur, insert_query, comments_as_tuples)
            conn.commit()
    print("-- Done...")
