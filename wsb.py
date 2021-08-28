import ast
import csv
import os
import json
import time
import praw
import pmaw
import click
import redis
import random
import psycopg2
import istarmap
import platform
import datetime
import requests
import sqlalchemy
import numpy as np
import configparser
import pandas as pd
from utils import batch
from io import StringIO
from tqdm.auto import tqdm
from reddit_logger import root_logger
from subprocess import PIPE, Popen, STDOUT
from backoff import on_exception, expo
from ratelimit import limits, RateLimitException
from psycopg2.extras import Json, execute_values
from multiprocessing import Pool, Manager, cpu_count

MINUTE = 60


class Gather:
    def __init__(self):
        self.pmaw_api = pmaw.PushshiftAPI()
        self.MINUTE = 60

        self.curdir_fullpath = os.path.dirname(os.path.realpath(__file__))
        self.config = configparser.ConfigParser()
        self.config.read(os.path.join(self.curdir_fullpath, "config.ini"))
        self.logger = root_logger
        self.db_conn_str = self.get_db_conn_str()
        self.system_type = platform.system()
        self.prepared_statement_filepath = os.path.join(
            self.curdir_fullpath, "prepared_statements.csv"
        )
        self.psql_command = self.get_psql_command()
        self.pushshift_comment_ids_endpoint = (
            "https://api.pushshift.io/reddit/submission/comment_ids"
        )
        self.redis_conn = self.get_redis_conn()

    @staticmethod
    def get_redis_conn():
        r = redis.Redis(host="localhost", port=6379, db=0)
        return r

    def get_db_conn_str(self):
        username = self.config["POSTGRES"]["username"]
        password = self.config["POSTGRES"]["password"]
        host = self.config["POSTGRES"]["host"]
        port = self.config["POSTGRES"]["port"]
        database = self.config["POSTGRES"]["database"]
        db_conn_str = f"postgresql://{username}:{password}@{host}:{port}/{database}"
        return db_conn_str

    def get_praw_client(self):
        """
            Get the praw reddit client.
            :return: The praw.Reddit instance.
            """
        return praw.Reddit(
            client_id=self.config["REDDIT"]["clientId"],
            client_secret=self.config["REDDIT"]["clientSecret"],
            redirect="http://localhost:8080",
            username=self.config["REDDIT"]["username"],
            password=self.config["REDDIT"]["password"],
            user_agent=self.config["REDDIT"]["useragent"],
        )

    def get_psycopg2_conn(self) -> psycopg2.connect:
        """
        Get the psycopg2 conn module.
        :return: the psycopg2.connect module
        """
        return psycopg2.connect(
            dbname=self.config["POSTGRES"]["database"],
            port=self.config["POSTGRES"]["port"],
            host=self.config["POSTGRES"]["host"],
            password=self.config["POSTGRES"]["password"],
            user=self.config["POSTGRES"]["username"],
        )

    def get_sqlalchemy_engine(self, autocommit: bool = True) -> sqlalchemy.engine:
        """
        Get the sqlalchemy engine for the same pg database
        :param: autocommit: Do you just want to test the connection or autocommit ?
        :return: the sqlalchemy engine
        """
        conn_str = f"postgresql://{self.config['POSTGRES']['username']}:{self.config['POSTGRES']['password']}@{self.config['POSTGRES']['host']}:{self.config['POSTGRES']['port']}/{self.config['POSTGRES']['database']}"
        return sqlalchemy.create_engine(conn_str).execution_options(
            autocommit=autocommit
        )

    def get_psql_command(self):
        if self.system_type == "Darwin":
            # psql = r"/usr/lib/postgresql/13/psql"
            psql = r"/usr/local/bin/psql"
            return psql

        elif self.system_type == "Windows":
            psql = r"C:\\Program Files\\PostgreSQL\\12\\bin\\psql.exe"
            return psql

    @staticmethod
    def get_all_daily_discussion_thread_sub_ids_pmaw(
        last_n_days: int = 30, after_n_days: int = None
    ):
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

    def insert_comments_from_praw(self, comment) -> None:
        insert_query = f"""INSERT INTO wsb_comments({','.join(comment.keys())}) VALUES %s ON CONFLICT (id) DO NOTHING;"""
        with self.get_psycopg2_conn() as conn:
            with conn.cursor() as cur:
                insert_query_2 = cur.mogrify(insert_query, tuple(comment.values()))
                print(insert_query_2)
                cur.execute(insert_query_2)
                conn.commit()

    def insert_submissions_from_pmaw(self, all_subreddit_submissions: list):
        all_mogrified_comments, un_mogrified_comments = [], []

        wsb_submission_cols = self.get_table_cols_names(table_name="wsb_submissions")

        with self.get_psycopg2_conn() as conn:
            with conn.cursor() as cur:
                for submission in tqdm(all_subreddit_submissions):
                    pop_keys = [
                        key
                        for key in submission.keys()
                        if key not in wsb_submission_cols
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

                    except (TypeError, psycopg2.ProgrammingError) as e:
                        un_mogrified_comments.append(submission)

        all_joined_comments = b" ".join(all_mogrified_comments)

        with self.get_psycopg2_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(all_joined_comments)
            conn.commit()

    def create_wsb_comments_table(self):
        """
        Create the comments table that will be populated.
        :return: None
        """
        CREATE_WSB_COMMENTS_TABLE_QUERY = """CREATE TABLE IF NOT EXISTS wsb_comments
            (
                total_awards_received           int,
                approved_at_utc                 int,
                comment_type                    int,
                awarders                        jsonb,
                mod_reason_by                   text,
                banned_by                       text,
                ups                             int,
                author_flair_type               text,
                removal_reason                  text,
                link_id                         text,
                author_flair_template_id        text,
                likes                           int,
                user_reports                    jsonb,
                saved                           bool,
                id                              text,
                retrieved_on                    int,
                banned_at_utc                   int,
                mod_reason_title                text,
                gilded                          int,
                archived                        bool,
                no_follow                       bool,
                author                          text,
                can_mod_post                    bool,
                send_replies                    bool,
                parent_id                       text,
                score                           int,
                author_fullname                 text,
                report_reasons                  jsonb,
                approved_by                     text,
                all_awardings                   jsonb,
                submission_id                   text,
                author_cakeday                  bool,
                subreddit_id                    text,
                body                            text,
                edited                          int,
                downs                           int,
                author_flair_css_class          text,
                is_submitter                    bool,
                collapsed                       bool,
                author_flair_richtext           jsonb,
                author_patreon_flair            text,
                body_html                       text,
                gildings                        jsonb,
                collapsed_reason                text,
                associated_award                text,
                stickied                        bool,
                author_premium                  bool,
                subreddit_type                  text,
                can_gild                        bool,
                top_awarded_type                text,
                author_flair_text_color         text,
                score_hidden                    bool,
                permalink                       text,
                num_reports                     int,
                locked                          bool,
                name                            text,
                created                         int,
                author_flair_text               text,
                treatment_tags                  jsonb,
                created_utc                     int,
                subreddit_name_prefixed         text,
                controversiality                int,
                depth                           int,
                author_flair_background_color   text,
                collapsed_because_crowd_control bool,
                mod_reports                     jsonb,
                mod_note                        text,
                distinguished                   text
            );"""

        CREATE_WSB_INDEX_QUERY = (
            """CREATE UNIQUE INDEX wsb_comment_uid ON wsb_comments(id);"""
        )

        CREATE_WSB_SUBMISSION_QUERIES = """
            CREATE TABLE IF NOT EXISTS wsb_submissions 
            (
                all_awardings                 jsonb,
                allow_live_comments           bool,
                author                        text,
                author_flair_css_class        text,
                author_flair_richtext         jsonb,
                author_flair_text             text,
                author_flair_type             text,
                author_flair_background_color text,
                author_flair_template_id      text,
                author_flair_text_color       text,
                author_fullname               text,
                author_patreon_flair          bool,
                author_premium                bool,
                awarders                      jsonb,
                banned_by                     text,
                can_mod_post                  bool,
                collections                   jsonb,
                contest_mode                  bool,
                created_utc                   bigint,
                edited                        bigint,
                crosspost_parent              text,
                crosspost_parent_list         jsonb,
                domain                        text,
                full_link                     text,
                gildings                      jsonb,
                id                            text,
                is_crosspostable              bool,
                is_meta                       bool,
                is_original_content           bool,
                is_reddit_media_domain        bool,
                is_robot_indexable            bool,
                is_self                       bool,
                is_video                      bool,
                link_flair_background_color   text,
                link_flair_css_class          text,
                link_flair_richtext           jsonb,
                link_flair_template_id        text,
                link_flair_text               text,
                link_flair_text_color         text,
                link_flair_type               text,
                locked                        bool,
                media_only                    bool,
                no_follow                     bool,
                num_comments                  int,
                num_crossposts                int,
                over_18                       bool,
                parent_whitelist_status       text,
                permalink                     text,
                pinned                        bool,
                poll_data                     jsonb,
                post_hint                     text,
                pwls                          int,
                removed_by_category           text,
                retrieved_on                  bigint,
                score                         int,
                selftext                      text,
                send_replies                  bool,
                spoiler                       bool,
                stickied                      bool,
                subreddit                     text,
                subreddit_id                  text,
                subreddit_subscribers         bigint,
                subreddit_type                text,
                subreddit_sort                text,
                suggested_sort                text,
                thumbnail                     text,
                title                         text,
                total_awards_received         int,
                treatment_tags                jsonb,
                upvote_ratio                  float,
                url                           text,
                whitelist_status              text,
                wls                           int
            );
            CREATE UNIQUE INDEX wsb_submissions_uid ON wsb_submissions (id);
        """

        with self.get_psycopg2_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(CREATE_WSB_COMMENTS_TABLE_QUERY)
                cur.execute(CREATE_WSB_INDEX_QUERY)
                cur.execute(CREATE_WSB_SUBMISSION_QUERIES)
            conn.commit()

    def create_wsb_comments_status_table(self):
        print("-- Creating wsb_comments_status table...")
        query = """CREATE TABLE IF NOT EXISTS wsb_comments_status(insert_date timestamp without time zone, submission_ids_completed text[]);"""
        with self.get_psycopg2_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(query)
            conn.commit()
        print("-- Done")

    def update_wsb_comments_materialized_views(self):
        print("-- Updating materialized view wsb_submission_comments...")
        query_1 = """CREATE MATERIALIZED VIEW wsb_submission_comments AS SELECT CURRENT_DATE, t.submission_id, array_agg(t.id) as comment_ids FROM wsb_comments t group by t.submission_id;"""
        query_2 = """CREATE UNIQUE INDEX wsb_submission_comments_rel_uindex ON wsb_submission_comments (submission_id);"""
        with self.get_psycopg2_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(query_1)
                cur.execute(query_2)
            conn.commit()
        print("-- Done")

    def update_wsb_comments_status_table(self):
        print("-- Updating wsb_comments_status table...")
        query = """
        INSERT INTO wsb_comments_status 
        SELECT CURRENT_DATE, array_agg(DISTINCT t.submission_id) AS submission_ids_completed 
        FROM wsb_comments t ON CONFLICT (insert_date) 
        DO UPDATE SET submission_ids_completed = excluded.submission_ids_completed; 
        """

        with self.get_psycopg2_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(query)
            conn.commit()

        print("-- Done.")

    def get_table_unique_col_values(self, table_name: str, colname: str):
        QUERY = f"""SELECT DISTINCT (t.{colname}) FROM {table_name} t;"""
        with self.get_psycopg2_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(QUERY)
                res = cur.fetchall()
        res = [r[0] for r in res]
        return res

    def get_table_cols_names(self, table_name: str):
        with self.get_psycopg2_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""SELECT column_name FROM information_schema.columns WHERE table_schema = 'public' AND table_name = '{table_name}';"""
                )
                wsb_submission_cols = cur.fetchall()

        wsb_submission_cols = [i[0] for i in wsb_submission_cols]
        return wsb_submission_cols

    def get_daily_discussion_thread_submissions_pmaw(self, limit: int = 10000) -> list:
        posts = self.pmaw_api.search_submissions(
            q="Daily Discussion Thread", subreddit="wallstreetbets", limit=limit
        )
        submissions = [s for s in posts]
        return submissions

    def update_wsb_submissions_table(self, submissions: list = None) -> None:
        if submissions is None:
            submissions = self.get_daily_discussion_thread_submissions_pmaw()
        else:
            pass

        sub_ids_found_in_db = self.get_table_unique_col_values(
            table_name="wsb_submissions", colname="id"
        )

        submissions_to_insert = []

        for sub in submissions:
            if "link_flair_text" in sub.keys():
                if sub["link_flair_text"] == "Daily Discussion":
                    if sub["id"] not in sub_ids_found_in_db:
                        submissions_to_insert.append(sub)
                    else:
                        pass
                else:
                    pass
            else:
                pass

        if len(submissions_to_insert) > 0:
            self.insert_submissions_from_pmaw(
                all_subreddit_submissions=submissions_to_insert
            )

    def get_all_comment_ids_from_submission_ids_pmaw(
        self, submission_comment_ids: dict
    ) -> list:
        all_sub_id_with_comments = []
        for sub_id, comments in tqdm(submission_comment_ids.items()):
            comments_arr = self.pmaw_api.search_comments(ids=comments)
            for c in comments_arr:
                c["submission_id"] = sub_id
                all_sub_id_with_comments.append(c)
        return all_sub_id_with_comments

    def get_all_comments_from_pmaw_single_thread(
        self, submissions: list = None, before: int = 30, after: int = 100
    ):
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
        submissions: list, link_flair_text: str = "Daily Discussion",
    ) -> [list, list]:
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
    ) -> [list, list]:
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
    ) -> [list, pd.DataFrame]:
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
                # query = cur.mogrify(
                #     # f"""SELECT DISTINCT(id) FROM polygonio.public.wsb_comments t WHERE t.submission_id = ANY (%s);""",
                #     [submission_ids],
                # )
                # comment_ids_present = [ele[0] for ele in cur.fetchall()]
                cur.execute(
                    f"""SELECT submission_id, id as comment_id from wsb_comments;"""
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
    ) -> [list, list]:
        """
        Get all the backfill submissions for some submissions that might be incomplete.
        :return:
        """

        all_comments, exceptions = [], []

        for submission_id in tqdm(submission_ids_to_be_refreshed):
            comments = self.pmaw_api.search_submission_comment_ids(
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

                comments_arr = self.pmaw_api.search_comments(
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

        comment = df.iloc[0,].to_dict()

        wsb_comments_cols = self.get_table_cols_names(table_name="wsb_comments")

        comment_copy = comment.copy()
        to_pop = [
            key for key in list(comment_copy.keys()) if key not in wsb_comments_cols
        ]
        for key in to_pop:
            _ = comment.pop(key)

        comment = {
            "subreddit_name_prefixed" if k == "subreddit" else k: v
            for k, v in comment.items()
        }

        for i in tqdm(range(len(df))):
            comment = df.iloc[i,].to_dict()
            comment_copy = comment.copy()
            comment["subreddit_name_prefixed"] = f"r/{comment['subreddit']}"
            to_pop = [
                key for key in list(comment_copy.keys()) if key not in wsb_comments_cols
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

                for key in ["author_flair_richtext", "gildings", "all_awardings", "awarders", "treatment_tags"]:
                    if key in df_.columns.tolist():
                        df_ = df_.drop(columns=[key])

                if "locked" in df_.columns:
                    df_.loc[:, "locked"] = df_["locked"].astype(bool)

                if "total_awards_received" in df_.columns:
                    df_.loc[:, "total_awards_received"] = (
                        df_["total_awards_received"].fillna(0.0).astype(int)
                    )

                df_.loc[:, "edited"] = df_["edited"].astype(int)

                text_stream = StringIO()
                df_.to_csv(text_stream, header=True, index=False)
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
        wsb_comments_cols = self.get_table_cols_names(table_name="wsb_comments")

        # make a reasonable batch len
        batch_len = 50000

        # there are too many comments, batch them
        batched_comments = batch(all_comments_list, n=batch_len)
        if skip_iterations is not none:
            skip_iterations = int(skip_iterations)
            print(f"-- skipping iterations: {skip_iterations}")
            for i in tqdm(range(skip_iterations), total=skip_iterations):
                _ = next(batched_comments)

        # we need to pre-calculate the total number of batches here
        total_batches = (
            int(len(all_comments_list) // batch_len) - skip_iterations
            if skip_iterations is not none
            else int(len(all_comments_list) // batch_len)
        )

        j = 0
        for bc in tqdm(batched_comments, total=total_batches):
            j += 1

            for comment in tqdm(bc, total=batch_len, leave=false):
                # we need to remove some keys from comment, but cant pop keys in a loop within a dict, so a workaround
                # is used, by using comment_copy.
                comment_copy = comment.copy()
                to_pop = [
                    key
                    for key in list(comment_copy.keys())
                    if key not in wsb_comments_cols
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

            text_stream = stringio()
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
            df_.to_csv(text_stream, header=true, index=false)
            text_stream.seek(0)

            # self.execute_command_in_db(
            #     command_str="create table if not exists wsb_comments_temp as table wsb_comments with no data;"
            # )

            print("-- copy-ing into the db...")
            focused_cols = ",".join(df_.columns.tolist())
            with self.get_psycopg2_conn() as conn:
                with conn.cursor() as cur:
                    try:
                        cur.copy_expert(
                            sql=f"""copy wsb_comments({focused_cols}) from stdin with (format csv, delimiter ',', header);""",
                            file=text_stream,
                        )
                        # execute_values(cur, insert_query, to_insert_comments)
                        conn.commit()
                    except psycopg2.programmingerror as e:
                        print(e)
            print("-- done.")

            # if j % 10 == 0:
            #     focused_cols = ",".join(list(comment.keys()))
            #     print("-- Inserting from wsb_comments_temp to wsb_comments...")
            #     self.execute_command_in_db(
            #         # command_str=f"INSERT INTO wsb_comments({focused_cols}) SELECT {focused_cols} from wsb_comments_temp ON CONFLICT (id) DO NOTHING ;"
            #     command_str = f"INSERT INTO wsb_comments({focused_cols}) SELECT {focused_cols} from wsb_comments_temp;"
            #     )
            #     print("-- Done.")
            #
            #     print("-- Truncating wsb_comments_temp...")
            #     self.execute_command_in_db(command_str="TRUNCATE wsb_comments_temp;")
            #     print("-- Done.")

    def backfill_comments(self):
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
            comment_ids_present = self.get_all_comment_ids_for_given_submission_ids_in_db(
                submission_ids=submission_ids_present
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

    def execute_command_in_db(self, command_str: str):
        command = [
            self.psql_command,
            "--command",
            command_str,
            "-d",
            self.db_conn_str,
        ]
        process = Popen(command, stdout=PIPE, stderr=STDOUT, shell=True)
        stdout = process.communicate()[0].decode("utf-8").strip()
        return stdout

    def copy_from_local_to_db(self):
        command = [
            self.psql_command,
            "--command",
            f"\copy wsb_comments_tickers FROM '{self.prepared_statement_filepath}' WITH (FORMAT csv, header);",
            "-d",
            self.db_conn_str,
        ]
        process = Popen(command, stdout=PIPE, stderr=STDOUT, shell=True)
        stdout = process.communicate()[0].decode("utf-8").strip()
        return stdout

    def update_wsb_comments_analytics(self):
        # check if the table needs to be created from scratch

        self.logger.info("-- 1/9 Checking if we are to create from scratch...")
        with self.get_psycopg2_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema='public' AND table_name='wsb_comments_tickers');"""
                )
                dont_create_from_scratch = cur.fetchall()[0][0]

        self.logger.info(
            f"-- 2/9 Updating from scratch: {not dont_create_from_scratch}"
        )
        # update_from_scratch = False if the table doesn't exist, but that's the opposite of what we want.
        # We WANT a table there, so if there's no table there, you'll have to start from scratch.

        if not dont_create_from_scratch:
            self.logger.info(
                "-- 3/9 Dropping wsb_comments_analytics_tickers_found and creating it again but empty..."
            )
            create_tickers_found_table = f"DROP TABLE IF EXISTS wsb_comments_tickers; CREATE TABLE IF NOT EXISTS wsb_comments_tickers (comment_id text, tickers text);"
            o1 = self.execute_command_in_db(command_str=create_tickers_found_table)
            print(o1)

            self.logger.info(
                "-- 4/9 Copying data from local to wsb_comments_analytics_tickers_found..."
            )
            o2 = self.copy_from_local_to_db()
            print(o2)

            self.logger.info(
                "-- 5/9 Altering wsb_comments_analytics_tickers_found with column type tickers text to text[]..."
            )
            alter_tickers_found_table = """ALTER TABLE wsb_comments_tickers ALTER COLUMN tickers TYPE text[] USING string_to_array(tickers, ', ');"""
            o3 = self.execute_command_in_db(command_str=alter_tickers_found_table)
            print(o3)

        self.logger.info(
            "-- 6/9 Insert the new diff data into wsb_comments_analytics, without the ticker information..."
        )
        insert_new_diff_data_into_wsb_comments_analytics = """INSERT INTO wsb_comments_analytics(created_utc, submission_id, parent_id, comment_id, author, body) SELECT to_timestamp(created_utc) as created_utc, submission_id, parent_id, id as comment_id, author, body FROM wsb_comments WHERE id = ANY (SELECT l.id FROM wsb_comments l LEFT JOIN wsb_comments_analytics i ON i.comment_id = l.id WHERE i.comment_id IS NULL);"""
        o35 = self.execute_command_in_db(
            command_str=insert_new_diff_data_into_wsb_comments_analytics
        )
        print(o35)

        self.logger.info(
            "-- 7/9 Dropping table wsb_comments_analytics_temp and creating it again..."
        )
        drop_and_create_analytics_temp = """DROP table if exists wsb_comments_analytics_temp; CREATE TABLE wsb_comments_analytics_temp AS (SELECT t1.created_utc, t1.submission_id, t1.parent_id, t1.comment_id, t1.author, t1.body, t2.tickers FROM wsb_comments_analytics t1 LEFT JOIN wsb_comments_tickers t2 on t1.comment_id = t2.comment_id);"""
        o4 = self.execute_command_in_db(command_str=drop_and_create_analytics_temp)
        print(o4)

        self.logger.info(
            "-- 8/9 Truncating/Dropping wsb_comments_analytics, and renaming wsb_comments_analytics_temp tp wsb_comments_analytics..."
        )
        drop_and_rename_wsb_comments_analytics = """TRUNCATE wsb_comments_analytics; DROP TABLE wsb_comments_analytics; ALTER TABLE wsb_comments_analytics_temp RENAME TO wsb_comments_analytics;"""
        o5 = self.execute_command_in_db(
            command_str=drop_and_rename_wsb_comments_analytics
        )
        print(o5)

        self.logger.info("-- 9/9 Creating hyper table...")
        create_hypertable = """SELECT create_hypertable('wsb_comments_analytics', 'created_utc', 'comment_id', number_partitions := 10, migrate_data := TRUE);"""
        o6 = self.execute_command_in_db(command_str=create_hypertable)
        print(o6)

    def brute_force_backfill_comments(self, before: int, after: int, debug: int = 0):

        if debug:
            df_json = self.redis_conn.get("df_json")

            if df_json is None:
                submission_ids_to_be_refreshed = self.get_all_comments_from_pmaw_single_thread(
                    before=before, after=after
                )

                all_comments = []
                for ele in tqdm(submission_ids_to_be_refreshed):
                    comments_arr = self.pmaw_api.search_comments(ids=ele["comment_ids"])

                    for c in comments_arr:
                        c["submission_id"] = ele["submission_id"]
                        all_comments.append(c)

                df = pd.DataFrame(all_comments)
                self.redis_conn.set(name="df_json", value=df.to_json())
            else:
                df = pd.read_json(df_json)
        else:
            submission_ids_to_be_refreshed = self.get_all_comments_from_pmaw_single_thread(
                before=before, after=after
            )

            all_comments = []
            for ele in tqdm(submission_ids_to_be_refreshed):
                comments_arr = self.pmaw_api.search_comments(ids=ele["comment_ids"])

                for c in comments_arr:
                    c["submission_id"] = ele["submission_id"]
                    all_comments.append(c)

            df = pd.DataFrame(all_comments)

        self.logger.info("-- Inserting all comments to db...")
        self.insert_all_comments_to_db_pmaw(df=df)


@click.command()
@click.option("--after", "-a", help="%Y-%m-%d formatted string")
@click.option("--before", "-b", help="%Y-%m-%d formatted string")
@click.option("--debug", "-d", help="if debug, will turn on redis interaction...")
def main(after, before, debug):

    print("-- Loading data...")
    with open("all_comments_arr_2021_08_17.json", "r") as f:
        all_comments_list = json.load(f)
    print("-- Done...")

    gather = Gather()
    gather.insert_all_comments_to_db_using_list_pmaw(
        all_comments_list=all_comments_list, skip_iterations=94
    )

    # after = datetime.datetime.strptime(after, "%Y-%m-%d").timestamp()
    # before = datetime.datetime.strptime(before, "%Y-%m-%d").timestamp()
    # g.brute_force_backfill_comments(after=int(after), before=int(before), debug=debug)


if __name__ == "__main__":
    main()

    # submission_ids_to_be_refreshed = g.get_all_comments_from_pmaw_single_thread()
    #
    # all_comments = []
    # for ele in tqdm(submission_ids_to_be_refreshed):
    #     comments_arr = g.pmaw_api.search_comments(
    #         ids=ele["comment_ids"]
    #     )
    #
    #     for c in comments_arr:
    #         c["submission_id"] = ele["submission_id"]
    #         all_comments.append(c)

    # g.logger.info("-- Finding mission comment ids from pmaw..")
    # all_comments, comments_exceptions = g.find_missing_comment_ids_from_pmaw(
    #     submission_ids_to_be_refreshed=submission_ids_to_be_refreshed,
    #     comment_ids_present=[],
    # )

    # g.logger.info("-- Inserting all comments to db...")
    # g.insert_all_comments_to_db_pmaw(df=pd.DataFrame(all_comments))

    # print("-- updating updating wsb_submissions...")
    # g.update_wsb_submissions_table(all_submission_from_praw)

    # g.backfill_comments()
    # g.update_wsb_comments_analytics()
