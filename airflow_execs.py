import click

from airflow_utils import (
    extract_comments_ids_and_make_urls_wrapper,
    extract_comments_wrapper,
    extract_submissions_wrapper,
    refresh_submission_comments_status_mat_view,
    refresh_submission_status_mat_view,
    insert_submissions,
    insert_comments,
    update_submissions_praw,
    update_comments_praw,
)

from prefect_utils import Gather
from pmaw import PushshiftAPI


@click.command()
@click.option("--start_date", required=True, help="YYYY-MM-DD", type=str)
@click.option("--end_date", required=True, help="YYYY-MM-DD", type=str)
@click.option(
    "--update_sub_stage_1", required=True, help="Either 0/1 or True/False", type=bool
)
@click.option(
    "--update_sub_stage_2", required=True, help="Either 0/1 or True/False", type=bool
)
@click.option(
    "--update_com_stage_1", required=True, help="Either 0/1 or True/False", type=bool
)
@click.option(
    "--update_com_stage_2", required=True, help="Either 0/1 or True/False", type=bool
)
def executor(start_date, end_date, update_sub_stage_1, update_sub_stage_2, update_com_stage_1, update_com_stage_2):
    """
    Executor for the Airflow DAG.
    :param start_date: str (YYYY-MM-DD), start date of the range
    :param end_date: str (YYYY-MM-DD), end date of the range
    :param update_sub_stage_1: bool, whether to update submissions, using the usual pushshift API method
    :param update_sub_stage_2: bool, whether to update submissions, using the pmaw method
    :param update_com_stage_1: bool, whether to update comments, using the usual pushshift API method
    :param update_com_stage_2: bool, whether to update comments, using the pmaw method
    """
    gather_wsb = Gather()
    reddit_api = gather_wsb.get_praw_client()

    if update_sub_stage_1:
        print("--- Updating submissions: Stage 1---")
        all_results = extract_submissions_wrapper(
            start_date=start_date, end_date=end_date
        )
        insert_submissions(dict_list=all_results)

        print("-- Refreshing submission status mat view --")
        refresh_submission_status_mat_view()

    if update_sub_stage_2:
        print("--- Updating submissions: Stage 2---")
        all_recent_submissions = update_submissions_praw(
            gather_wsb=gather_wsb,
            api=PushshiftAPI(praw=reddit_api),
            lookback_days=5,
            return_submissions=True
        )

        print("-- Refreshing submission status mat view --")
        refresh_submission_status_mat_view()

    if update_com_stage_1:
        comment_urls = extract_comments_ids_and_make_urls_wrapper(
            start_date=start_date, end_date=end_date
        )
        all_results = extract_comments_wrapper(urls=comment_urls)
        insert_comments(comments_dict_list=all_results)
        refresh_submission_comments_status_mat_view()

    if update_com_stage_2:
        print("--- Updating comments: Stage 2---")
        assert "all_recent_submissions" in locals(), "Please make sure you have updated submissions first, using stage 2"
        update_comments_praw(
            all_recent_submissions=all_recent_submissions,
            gather_wsb=gather_wsb,
            api=PushshiftAPI(praw=reddit_api),
        )
        refresh_submission_comments_status_mat_view()


if __name__ == "__main__":
    executor()
