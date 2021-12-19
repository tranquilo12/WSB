import os
from subprocess import PIPE, Popen, STDOUT
from typing import List

import numpy as np
import pandas as pd
from psycopg2.extras import execute_values

from connections import Connections


class DB(Connections):
    def __init__(self):
        super(DB, self).__init__()
        self.comments_cols = [
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
        self.submission_cols = [
            "created_utc",
            "id",
            "author",
            "url",
            "title",
            "selftext",
            "stickied",
        ]

    def copy_from_local_to_db(self, table_name: str, filepath: str) -> str:
        if os.path.isfile(filepath):
            command = [
                self.get_psql_command(),
                "--command",
                f"\copy {table_name} FROM '{filepath}' WITH (FORMAT csv, header);",
                "-d",
                self.get_db_conn_str(),
            ]
            process = Popen(command, stdout=PIPE, stderr=STDOUT, shell=True)
            stdout = process.communicate()[0].decode("utf-8").strip()
        else:
            stdout = f"Error: File not found in location: {filepath}"
        return stdout

    def execute_command_in_db(self, command_str: str, local: bool = True) -> str:
        command = [
            self.get_psql_command(),
            "--command",
            command_str,
            "-d",
            self.get_db_conn_str(local=local),
        ]
        process = Popen(command, stdout=PIPE, stderr=STDOUT, shell=False)
        stdout = process.communicate()[0].decode("utf-8").strip()
        return stdout

    def create_hypertable(self, table_name: str) -> str:
        command = f"""SELECT create_hypertable('{table_name}', 'created_utc', 'id', number_partitions:=10, chunk_time_interval:=86400, migrate_data:=TRUE)"""
        stdout = self.execute_command_in_db(command_str=command)
        return stdout

    def create_submission_comments_status_mat_view(self) -> str:
        command = f"""CREATE MATERIALIZED VIEW submission_comments_status AS
                      SELECT to_timestamp(created_utc)::date as "date", submission_id, array_agg(DISTINCT id) as "all_comments_found"
                      FROM comments
                      GROUP BY to_timestamp(created_utc)::date, submission_id;"""
        stdout = self.execute_command_in_db(command_str=command)
        return stdout

    def get_submission_comments_status_mat_view(
        self, local: bool = False
    ) -> List[pd.DataFrame]:

        df = pd.read_sql_table(
            table_name="submission_comments_status",
            con=self.get_sqlalchemy_engine(local=local),
        )

        df = df.set_index("date")

        df.loc[:, "comments_len"] = df["all_comments_found"].apply(
            lambda x: len(np.unique(x))
        )

        df2 = pd.DataFrame(
            df.groupby([df.index]).apply(lambda x: x["comments_len"].sum()),
            columns=["submission_comments_count"],
        ).reset_index()

        return [df, df2]

    def refresh_submission_comments_status_mat_view(self, local: bool = True) -> str:
        command = "REFRESH MATERIALIZED VIEW submission_comments_status;"
        stdout = self.execute_command_in_db(command_str=command, local=local)
        print("-- Refreshed submission_comments_status materialized view...")
        return stdout

    def create_submission_status_mat_view(self, local: bool = True) -> str:
        command = f"""CREATE MATERIALIZED VIEW submission_status AS
                      SELECT to_timestamp(created_utc)::date as "date", array_agg(DISTINCT id) as "all_submissions_found"
                      FROM submissions
                      GROUP BY to_timestamp(created_utc)::date;"""
        stdout = self.execute_command_in_db(command_str=command, local=local)
        return stdout

    def get_submission_status_mat_view(self, local: bool = True) -> pd.DataFrame:
        df = pd.read_sql_table(
            table_name="submission_status", con=self.get_sqlalchemy_engine(local=local)
        )

        df["submissions_count"] = df.loc[:, "all_submissions_found"].apply(
            lambda x: len(x)
        )
        return df

    def refresh_submission_status_mat_view(self, local: bool = True) -> str:
        command = "REFRESH MATERIALIZED VIEW submission_status;"
        stdout = self.execute_command_in_db(command_str=command, local=local)
        print("-- Refreshed submission_status materialized view...")
        return stdout

    def get_unique_column_values_from_table(
        self, table_name: str, col_name: str, local: bool = True
    ) -> list:
        query = f"""SELECT DISTINCT (t.{col_name}) FROM {table_name} t;"""
        with self.get_psycopg2_conn(local=local) as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                res = cur.fetchall()
        res = [r[0] for r in res]
        return res

    def insert_comments(self, comments: list, local: bool = True) -> None:
        comments_as_tuples = []
        for comment in comments:
            comments_as_tuples.append(tuple(v for v in comment.values()))
        del comments

        insert_query = f'INSERT INTO comments({",".join(self.comments_cols)}) VALUES %s ON CONFLICT (created_utc, id) DO NOTHING;'
        with self.get_psycopg2_conn(local=local) as conn:
            with conn.cursor() as cur:
                execute_values(cur, insert_query, comments_as_tuples)
                conn.commit()
        print("-- Inserted comments...")

    def insert_submissions(self, submissions: list, local: bool = True) -> None:
        submissions_as_tuples = []
        for submission in submissions:
            submissions_as_tuples.append(tuple([v for v in submission.values()]))
        del submissions

        insert_query = f"INSERT INTO submissions({','.join(self.submission_cols)}) VALUES %s ON CONFLICT (created_utc, id) DO NOTHING;"
        with self.get_psycopg2_conn(local=local) as conn:
            with conn.cursor() as cur:
                execute_values(cur, insert_query, submissions_as_tuples)
                conn.commit()
        print("-- Inserted submissions")
