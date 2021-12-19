import json
import os
import platform
import time
from io import StringIO

import numpy as np
import pandas as pd
import psycopg2
import requests
from psycopg2.extras import Json
from tqdm.auto import tqdm

from db import DB
from misc.reddit_logger import root_logger
from utils import batch

MINUTE = 60


class Gather(DB):
    def __init__(self):
        super(Gather, self).__init__()

        self.MINUTE = 60
        self.logger = root_logger
        self.system_type = platform.system()
        self.pushshift_api_client = self.get_pushshift_client()

        self.prepared_statement_filepath = os.path.join(
            self.curdir_fullpath, "prepared_statements.csv"
        )

        self.pushshift_comment_ids_endpoint = (
            "https://api.pushshift.io/reddit/submission/comment_ids"
        )

        self.pushshift_submissions_endpoint = (
            "https://api.pushshift.io/reddit/search/submission/?"
        )

    def get_submissions_to_backfill(
        self, dates_absent: list, stickied: bool = True
    ) -> list:
        before_after_dates = [
            (
                int((x - pd.Timedelta(days=1)).timestamp()),
                int((x + pd.Timedelta(days=1)).timestamp()),
            )
            for x in dates_absent
        ]

        # for all dates absent, find out if there are more stickied submissions, and merge them back into stickied_df at the end
        all_params = [
            {
                "subreddit": "wallstreetbets",
                "stickied": "true" if stickied is True else "false",
                "before": x[1],
                "after": x[0],
            }
            for x in before_after_dates
        ]

        all_backfilled_submissions = []
        for param in tqdm(all_params):
            response = requests.get(self.pushshift_submissions_endpoint, params=param)

            if response.status_code == 200:
                data = response.json()
                if "data" in data.keys():
                    all_backfilled_submissions.append(data["data"])

            time.sleep(1)

        return all_backfilled_submissions

    @staticmethod
    def get_all_daily_discussion_thread_sub_ids_pmaw(
        last_n_days: int = 30, after_n_days: int = None
    ) -> list:
        endpoint = "https://api.pushshift.io/reddit/search/submission/?"
        if after_n_days:
            params = {
                "q": "Daily Discussion Thread",
                "subreddit": "wallstreetbets",
                "before": f"{last_n_days}",
                "after": f"{after_n_days}",
            }
        else:
            params = {
                "q": "Daily Discussion Thread",
                "subreddit": "wallstreetbets",
                "before": f"{last_n_days}",
            }

        res = requests.get(endpoint, params=params)

        if res.status_code == 200:
            results = res.json()

            all_submissions = results["data"]

            all_daily_discussion_thread_sub_ids = []
            for sub in all_submissions:
                if "link_flair_text" in sub.keys():
                    if sub["link_flair_text"] == "Daily Discussion":
                        all_daily_discussion_thread_sub_ids.append(sub)

            return all_daily_discussion_thread_sub_ids

    def insert_comments_from_praw(self, comment: dict) -> None:
        insert_query = f"""INSERT INTO wsb_comments({','.join(comment.keys())}) VALUES %s ON CONFLICT (id) DO NOTHING;"""
        with self.get_psycopg2_conn() as conn:
            with conn.cursor() as cur:
                insert_query_2 = cur.mogrify(insert_query, tuple(comment.values()))
                print(insert_query_2)
                cur.execute(insert_query_2)
                conn.commit()

    def insert_submissions_from_pmaw(self, all_subreddit_submissions: list) -> None:
        all_mogrified_comments, un_mogrified_comments = [], []

        with self.get_psycopg2_conn() as conn:
            with conn.cursor() as cur:
                for submission in tqdm(all_subreddit_submissions):
                    pop_keys = [
                        key
                        for key in submission.keys()
                        if key not in self.submission_cols
                    ]
                    for key in pop_keys:
                        _ = submission.pop(key)

                    for key, val in submission.items():
                        if key in [
                            "all_awardings",
                            "awarders",
                            "author_flair_richtext",
                            "gildings",
                            "link_flair_richtext",
                            "treatment_tags",
                            "collections",
                            "crosspost_parent_list",
                            "poll_data",
                        ]:
                            if isinstance(submission[key], dict) or isinstance(
                                submission[key], list
                            ):
                                submission[key] = Json(json.dumps(val))
                    try:
                        cols_str = ",".join(submission.keys())
                        values_str = ("%s," * len(submission)).rstrip(",")
                        insert_query = f"""INSERT INTO wsb_submissions ({cols_str}) VALUES ({values_str}) ON CONFLICT (id) DO NOTHING;"""

                        mogrified_comment = cur.mogrify(
                            insert_query, list(submission.values())
                        )
                        all_mogrified_comments.append(mogrified_comment)

                    except (TypeError, psycopg2.ProgrammingError) as _:
                        un_mogrified_comments.append(submission)

        all_joined_comments = b" ".join(all_mogrified_comments)

        with self.get_psycopg2_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(all_joined_comments)
            conn.commit()

    def get_daily_discussion_thread_submissions_pmaw(self, limit: int = 10000) -> list:
        posts = self.pushshift_api_client.search_submissions(
            q="Daily Discussion Thread", subreddit="wallstreetbets", limit=limit
        )
        submissions = [s for s in posts]
        return submissions

    def get_all_comment_ids_from_submission_ids_pmaw(
        self, submission_comment_ids: dict
    ) -> list:
        all_sub_id_with_comments = []
        for sub_id, comments in tqdm(submission_comment_ids.items()):
            comments_arr = self.pushshift_api_client.search_comments(ids=comments)
            for c in comments_arr:
                c["submission_id"] = sub_id
                all_sub_id_with_comments.append(c)
        return all_sub_id_with_comments

    def get_all_comments_from_pmaw_single_thread(
        self, submissions: list = None, before: int = 30, after: int = 100
    ) -> list:
        if isinstance(submissions, list) and len(submissions) == 0:
            submissions = self.get_all_daily_discussion_thread_sub_ids_pmaw(
                last_n_days=before, after_n_days=after
            )
        elif submissions is None:
            submissions = self.get_all_daily_discussion_thread_sub_ids_pmaw(
                last_n_days=before, after_n_days=after
            )

        all_results = []
        for sub in tqdm(submissions):
            url = f"{self.pushshift_comment_ids_endpoint}/{sub['id']}"
            res = requests.get(url)

            if res.status_code == 200:
                all_results.append(
                    {"submission_id": sub["id"], "comment_ids": res.json()["data"]}
                )

        return all_results

    @staticmethod
    def filter_all_submissions_for_correct_link_flair_text(
        submissions: list,
        link_flair_text: str = "Daily Discussion",
    ) -> tuple:
        exceptions = []
        all_dates_gathered = []
        for submission in submissions:
            if "link_flair_text" in list(submission.keys()):
                if submission["link_flair_text"] == link_flair_text:
                    try:
                        date_info = pd.to_datetime(
                            "-".join(
                                submission["full_link"]
                                .split("/")[-2]
                                .split("_")[-4:][1:]
                            ),
                            format="%B-%d-%Y",
                        )
                        it = {"date": date_info, "submission_links": []}
                        all_dates_gathered.append(it)
                    except (TypeError, ValueError) as _:
                        exceptions.append(submission)
                else:
                    pass
            else:
                pass

        return all_dates_gathered, exceptions

    @staticmethod
    def match_correct_submission_links_with_their_dates(
        all_dates_gathered: list, submissions: list
    ) -> tuple:
        final_dates_gathered = []
        exceptions = []
        for submission in tqdm(submissions):
            if "link_flair_text" in submission.keys():
                if submission["link_flair_text"] == "Daily Discussion":
                    try:
                        date_info = pd.to_datetime(
                            "-".join(
                                submission["full_link"]
                                .split("/")[-2]
                                .split("_")[-4:][1:]
                            ),
                            format="%B-%d-%Y",
                        )
                        for ele in all_dates_gathered:
                            if ele["date"] == date_info:
                                ele["submission_links"].append(submission["full_link"])
                                final_dates_gathered.append(ele)

                    except (TypeError, ValueError, AssertionError) as _:
                        exceptions.append(submission)
                else:
                    pass
            else:
                pass
        return final_dates_gathered, exceptions

    def compare_submission_dates_in_pmaw_to_dates_in_db(
        self, final_dates_gathered: list
    ) -> tuple[list, pd.DataFrame]:
        all_submissions_meta_df = (
            pd.DataFrame(final_dates_gathered)
            .drop_duplicates(subset=["date"], inplace=False)
            .reset_index(drop=True)
            .set_index("date")
            .explode("submission_links")
            .reset_index()
        )

        all_submissions_meta_df.columns = ["date", "links_in_reddit"]

        # Check if these are present in the database, and how many comments do each of these submissions have?
        with self.get_psycopg2_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT DISTINCT id, full_link FROM wsb_submissions WHERE link_flair_text = 'Daily Discussion';"
                )
                sub_in_db_df = pd.DataFrame(
                    cur.fetchall(), columns=["id", "links_in_db"]
                )

        merged_df = pd.merge(
            left=all_submissions_meta_df,
            right=sub_in_db_df,
            left_on="links_in_reddit",
            right_on="links_in_db",
            how="outer",
        )

        in_db_possible_backup_needed_df = merged_df[~merged_df["date"].isna()].copy()
        in_db_possible_backup_needed_df = in_db_possible_backup_needed_df.sort_values(
            by="date"
        )
        in_db_possible_backup_needed_df = in_db_possible_backup_needed_df.reset_index(
            drop=True
        )

        # Grab the submissions that are not there in db and push them
        submission_links_not_uploaded = set(
            in_db_possible_backup_needed_df["links_in_reddit"].values.tolist()
        ) - set(in_db_possible_backup_needed_df["links_in_db"].values.tolist())

        submission_ids = [ele.split("/")[6] for ele in submission_links_not_uploaded]
        in_db_not_daily_subs_df = merged_df[merged_df["date"].isna()].copy()

        return submission_ids, in_db_not_daily_subs_df

    def get_all_comment_ids_for_given_submission_ids_in_db(
        self, submission_ids: list
    ) -> list:
        with self.get_psycopg2_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""SELECT submission_id, id as comment_id from comments;"""
                )
                submission_comments_df = pd.DataFrame(
                    cur.fetchall(), columns=["submission_id", "comment_id"]
                )

        comment_ids_present = submission_comments_df.loc[
            submission_comments_df.submission_id.isin(submission_ids), "comment_id"
        ].values.tolist()
        return comment_ids_present

    def find_missing_comment_ids_from_pmaw(
        self,
        submission_ids_to_be_refreshed: list = None,
        comment_ids_present: list = None,
    ) -> tuple[list, list]:
        """
        Get all the backfill submissions for some submissions that might be incomplete.
        :return:
        """

        all_comments, exceptions = [], []

        for submission_id in tqdm(submission_ids_to_be_refreshed):
            comments = self.pushshift_api_client.search_submission_comment_ids(
                ids=[submission_id], safe_exit=True, memsafe=True
            )

            submission_id_comments = {
                "submission_id": submission_id,
                "comments": [
                    comment
                    for comment in comments
                    if comment not in comment_ids_present
                ],
            }

            if len(submission_id_comments["comments"]) > 0:

                comments_arr = self.pushshift_api_client.search_comments(
                    ids=submission_id_comments["comments"]
                )
                for c in comments_arr:
                    c["submission_id"] = submission_id_comments["submission_id"]
                    all_comments.append(c)

        return all_comments, exceptions

    def insert_all_comments_to_db_using_df_pmaw(self, df: pd.DataFrame) -> None:
        """
        For each comment in all_comments... insert that into the database.
        :param df: a list of dicts, [{... all dict properties of the wsb_comments table}]
        :return: None
        """

        print("Formatting into correct format...")
        to_insert_comments = []

        comment = df.iloc[
            0,
        ].to_dict()

        comment_copy = comment.copy()
        to_pop = [
            key for key in list(comment_copy.keys()) if key not in self.comments_cols
        ]
        for key in to_pop:
            _ = comment.pop(key)

        # comment = {
        #     "subreddit_name_prefixed" if k == "subreddit" else k: v
        #     for k, v in comment.items()
        # }

        for i in tqdm(range(len(df))):
            comment = df.iloc[
                i,
            ].to_dict()
            comment_copy = comment.copy()
            comment["subreddit_name_prefixed"] = f"r/{comment['subreddit']}"
            to_pop = [
                key
                for key in list(comment_copy.keys())
                if key not in self.comments_cols
            ]

            for key in to_pop:
                _ = comment.pop(key)

            for key, val in comment.items():
                if key in [
                    "awarders",
                    "user_reports",
                    "all_awardings",
                    "report_reasons",
                    "gildings",
                    "author_flair_richtext",
                    "treatment_tags",
                    "mod_reports",
                ]:
                    comment[key] = Json(json.dumps(val))

                if isinstance(comment[key], np.bool_):
                    comment[key] = bool(val)

                if isinstance(comment[key], np.int64):
                    comment[key] = int(val)

            if "edited" in comment.keys() and isinstance(comment["edited"], bool):
                comment["edited"] = int(0)

            if "author_patreon_flair" in comment.keys() and isinstance(
                comment["author_patreon_flair"], bool
            ):
                comment["author_patreon_flair"] = ""

            if "author_cakeday" in comment.keys() and isinstance(
                comment["author_cakeday"], np.float
            ):
                comment["author_cakeday"] = bool(0)

            if "author_premium" in comment.keys() and isinstance(
                comment["author_premium"], float
            ):
                comment["author_premium"] = bool(0)

            if "can_mod_post" in comment.keys() and isinstance(
                comment["can_mod_post"], float
            ):
                comment["can_mod_post"] = bool(0)

            to_insert_comments.append(tuple(comment.values()))

            if (i % 1000000 == 0) & (i > 0):

                df_ = pd.DataFrame.from_records(
                    to_insert_comments, columns=list(comment.keys())
                )

                for key in [
                    "author_flair_richtext",
                    "gildings",
                    "all_awardings",
                    "awarders",
                    "treatment_tags",
                ]:
                    if key in df_.columns.tolist():
                        df_ = df_.drop(columns=[key])

                if "locked" in df_.columns:
                    df_.loc[:, "locked"] = df_["locked"].astype(bool)

                if "total_awards_received" in df_.columns:
                    df_.loc[:, "total_awards_received"] = (
                        df_["total_awards_received"].fillna(0.0).astype(int)
                    )

                df_.loc[:, "edited"] = df_["edited"].astype(int)

                text_stream: StringIO = StringIO()
                df_.to_csv(path_or_buf=text_stream, header=True, index=False)
                text_stream.seek(0)

                print("-- COPY-ing into the db...")
                focused_cols = ",".join(df_.columns.tolist())
                with self.get_psycopg2_conn() as conn:
                    with conn.cursor() as cur:
                        try:
                            cur.copy_expert(
                                sql=f"""COPY wsb_comments ({focused_cols}) FROM STDIN WITH (FORMAT CSV, DELIMITER ',', HEADER);""",
                                file=text_stream,
                            )
                            conn.commit()
                        except psycopg2.ProgrammingError as e:
                            print(e)
                print("-- Done.")

                print("-- Vacuuming wsb_comments...")
                self.execute_command_in_db(command_str="VACUUM wsb_comments;")
                print("-- Done.")

    def insert_all_comments_to_db_using_list_pmaw(
        self, all_comments_list: list, skip_iterations: int = None
    ) -> None:
        """
        For each comment in all_comments... insert that into the database.
        :param all_comments_list: A list of dicts, [{... all dict properties of the wsb_comments table}]
        :param skip_iterations: If some iterations have already been completed, then just skip past those and move on..
        :return: None
        """

        print("formatting into correct format...")
        comments_cols = []
        to_insert_comments = []

        # make a reasonable batch len
        batch_len = 50000

        # there are too many comments, batch them
        batched_comments = batch(all_comments_list, n=batch_len)
        if skip_iterations is not None:
            skip_iterations = int(skip_iterations)
            print(f"-- skipping iterations: {skip_iterations}")
            for _ in tqdm(range(skip_iterations), total=skip_iterations):
                _ = next(batched_comments)

        # we need to pre-calculate the total number of batches here
        total_batches = (
            int(len(all_comments_list) // batch_len) - skip_iterations
            if skip_iterations is not None
            else int(len(all_comments_list) // batch_len)
        )

        j = 0
        for bc in tqdm(batched_comments, total=total_batches):
            j += 1

            for comment in tqdm(bc, total=batch_len, leave=False):
                # we need to remove some keys from comment, but cant pop keys in a loop within a dict, so a workaround
                # is used, by using comment_copy.
                comment_copy = comment.copy()
                to_pop = [
                    key
                    for key in list(comment_copy.keys())
                    if key not in self.comments_cols
                ]
                for key in to_pop:
                    _ = comment.pop(key)

                comment = {
                    "subreddit_name_prefixed" if k == "subreddit" else k: v
                    for k, v in comment.items()
                }
                comment["subreddit_name_prefixed"] = f"r/wallstreetbets"

                for key, val in comment.items():
                    if key in [
                        "awarders",
                        "user_reports",
                        "all_awardings",
                        "report_reasons",
                        "gildings",
                        "author_flair_richtext",
                        "treatment_tags",
                        "mod_reports",
                    ]:
                        # comment[key] = json(json.dumps(val, default=vars))
                        comment[key] = json.dumps(val, default=vars)

                    if isinstance(comment[key], np.bool_):
                        comment[key] = bool(val)

                    if isinstance(comment[key], np.int64):
                        comment[key] = int(val)

                if "edited" in comment.keys() and isinstance(comment["edited"], bool):
                    comment["edited"] = int(0)

                if "author_patreon_flair" in comment.keys() and isinstance(
                    comment["author_patreon_flair"], bool
                ):
                    comment["author_patreon_flair"] = ""

                if "author_cakeday" in comment.keys() and isinstance(
                    comment["author_cakeday"], np.float
                ):
                    comment["author_cakeday"] = bool(0)

                if "author_premium" in comment.keys() and isinstance(
                    comment["author_premium"], float
                ):
                    comment["author_premium"] = bool(0)

                if "can_mod_post" in comment.keys() and isinstance(
                    comment["can_mod_post"], float
                ):
                    comment["can_mod_post"] = bool(0)

                if len(comments_cols) == 0:
                    comments_cols = list(comment.keys())

                to_insert_comments.append(comment)

            # turn into dataframe to make things easier downstream.
            df_ = pd.dataframe.from_dict(to_insert_comments)

            text_stream = StringIO()
            for key in ["author_flair_richtext", "gildings", "all_awardings"]:
                if key in df_.columns.tolist():
                    df_ = df_.drop(columns=[key])

            if "locked" in df_.columns:
                df_.loc[:, "locked"] = df_["locked"].astype(bool)

            if "total_awards_received" in df_.columns:
                df_.loc[:, "total_awards_received"] = (
                    df_["total_awards_received"].fillna(0.0).astype(int)
                )

            df_.loc[:, "edited"] = df_["edited"].astype(int)
            df_.to_csv(text_stream, header=True, index=False)
            text_stream.seek(0)

            print("-- copy-ing into the db...")
            focused_cols = ",".join(df_.columns.tolist())
            with self.get_psycopg2_conn() as conn:
                with conn.cursor() as cur:
                    try:
                        cur.copy_expert(
                            sql=f"""copy comments({focused_cols}) from stdin with (format csv, delimiter ',', header);""",
                            file=text_stream,
                        )
                        conn.commit()
                    except psycopg2.ProgrammingError as e:
                        print(e)
            print("-- done.")

    def backfill_comments(self) -> None:
        self.logger.info(
            "-- 1/9 Fetching 'Daily Discussions Thread' submissions from pmaw ..."
        )
        all_submissions_from_praw = self.get_daily_discussion_thread_submissions_pmaw()

        self.logger.info("-- 2/9 Updating wsb_submissions table...")
        self.update_wsb_submissions_table(submissions=all_submissions_from_praw)

        self.logger.info("-- 3/9 Fetching distinct wsb_submissions ids...")
        with self.get_psycopg2_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT DISTINCT(id) FROM wsb_submissions;")
                submission_ids_present = [i[0] for i in cur.fetchall()]

        self.logger.info(
            "-- 4/9 Filtering all submissions for correct link flair text ..."
        )
        (
            all_dates_gathered,
            submission_exceptions_1,
        ) = self.filter_all_submissions_for_correct_link_flair_text(
            submissions=all_submissions_from_praw
        )

        self.logger.info(
            "-- 5/9 Matching correct submission links with their dates ..."
        )
        (
            final_dates_gathered,
            submission_exceptions_2,
        ) = self.match_correct_submission_links_with_their_dates(
            all_dates_gathered, submissions=all_submissions_from_praw
        )

        self.logger.info("-- 6/9 Comparing submission dates in pmaw to dates in db...")
        (
            submission_ids_to_be_refreshed,
            in_db_not_daily_sub_df,
        ) = self.compare_submission_dates_in_pmaw_to_dates_in_db(
            final_dates_gathered=final_dates_gathered
        )

        if len(submission_ids_to_be_refreshed) > 0:

            self.logger.info(
                "-- 7/9 Getting all comment_ids for given submission ids..."
            )
            comment_ids_present = (
                self.get_all_comment_ids_for_given_submission_ids_in_db(
                    submission_ids=submission_ids_present
                )
            )

            self.logger.info("-- 8/9 Finding mission comment ids from pmaw..")
            all_comments, comments_exceptions = self.find_missing_comment_ids_from_pmaw(
                submission_ids_to_be_refreshed=submission_ids_to_be_refreshed,
                comment_ids_present=comment_ids_present,
            )

            self.logger.info("-- 9/9 Inserting all comments to db...")
            self.insert_all_comments_to_db_pmaw(df=pd.DataFrame(all_comments))
        else:
            self.logger.info("-- No submission ids to be refreshed...")
