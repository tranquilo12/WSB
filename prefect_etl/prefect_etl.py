import sys
import asyncio
import prefect
from prefect import task, Parameter

sys.path.append("C:/Users/SHIRAM/Documents/WSB")
from wsb import Gather

try:
    from prefect_etl.prefect_utils import extract_comments_ids_and_make_urls
    from prefect_etl.prefect_utils import extract_submissions, extract_comments
except ModuleNotFoundError as e:
    from prefect_utils import extract_comments_ids_and_make_urls
    from prefect_utils import extract_submissions, extract_comments

gather_wsb = Gather()


@task(name="Extract Submission wrapper")
def extract_submissions_wrapper(start_date: str, end_date: str) -> list:
    all_results = asyncio.run(
        extract_submissions(start_date=start_date, end_date=end_date)
    )
    return all_results


@task(name="Extract Comments wrapper")
def extract_comments_wrapper(urls: list) -> list:
    all_results = asyncio.run(extract_comments(urls=urls))
    return all_results


@task(name="Insert Submissions to db")
def insert_submissions(dict_list: list) -> None:
    gather_wsb.insert_submissions(submissions=dict_list, local=True)


@task(name="Insert Comments to db")
def insert_comments(comments_dict_list: list) -> None:
    gather_wsb.insert_comments(comments=comments_dict_list, local=True)


@task(
    name="Refresh Submission Status materialized view",
)
def refresh_submission_status_mat_view() -> None:
    gather_wsb.refresh_submission_status_mat_view(local=True)


@task(
    name="Refresh Submission Comments Status materialized view",
)
def refresh_submission_comments_status_mat_view() -> None:
    gather_wsb.refresh_submission_comments_status_mat_view(local=True)


@task(name="Extract Comments Ids and make urls")
def extract_comments_ids_and_make_urls_wrapper(start_date: str, end_date: str) -> prefect.Task:
    all_urls = asyncio.run(extract_comments_ids_and_make_urls(start_date=start_date, end_date=end_date))
    return all_urls
