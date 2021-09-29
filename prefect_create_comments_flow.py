import datetime

import click
from prefect import Flow
from prefect.core import Parameter

from prefect_etl import insert_comments, extract_comments_wrapper, refresh_submission_comments_status_mat_view, \
    extract_comments_ids_and_make_urls_wrapper


def create_flow():
    with Flow("UpdateAndRefreshComments") as flow:
        s_date = Parameter(name="start_date", default="2021-08-01", required=True)
        e_date = Parameter(name="end_date", default="2021-09-01", required=True)
        all_comments_urls = extract_comments_ids_and_make_urls_wrapper(start_date=s_date, end_date=e_date)
        submissions_comments_dict_list = extract_comments_wrapper(urls=all_comments_urls)
        comments_inserted = insert_comments(comments_dict_list=submissions_comments_dict_list)
        refresh_submission_comments_status_mat_view(upstream_tasks=[comments_inserted])
    return flow


def register_flow():
    flow = create_flow()
    flow.register(project_name="UpdateWSB")


def visualize_flow():
    flow = create_flow()
    flow.visualize()


def run_flow(start_date, end_date):
    fmt = "%Y-%m-%d"
    start_date = datetime.datetime.strptime(start_date, fmt).strftime(fmt)
    end_date = datetime.datetime.strptime(end_date, fmt).strftime(fmt)

    flow = create_flow()
    flow.run(parameters={"start_date": start_date, "end_date": end_date})


@click.command()
@click.option("--register", default=0)
@click.option("--visualize", default=0)
@click.option("--start_date", default=None)
@click.option("--end_date", default=None)
def main(register, visualize, start_date, end_date):
    if register:
        register_flow()

    if visualize:
        visualize_flow()

    if (start_date is not None) and (end_date is not None):
        run_flow(start_date=start_date, end_date=end_date)


if __name__ == "__main__":
    main()