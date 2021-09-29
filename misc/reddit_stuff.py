import configparser
import json
import os
import platform
import random
import time
from functools import wraps
from multiprocessing import Pool, Manager, cpu_count
from subprocess import PIPE, Popen, STDOUT

import click
import numpy as np
import pandas as pd
import pmaw
import praw
import psycopg2
import requests
import sqlalchemy
from backoff import on_exception, expo
from psycopg2.extras import Json, execute_values
from ratelimit import limits, RateLimitException
from tqdm.auto import tqdm

MINUTE = 60
config = configparser.ConfigParser()
config.read("config.ini")

pmaw_api = pmaw.PushshiftAPI()


def retry(exception_to_check, tries=4, delay=3, backoff=2, logger=None):
    """Retry calling the decorated function using an exponential backoff.

    :param exception_to_check: the exception to check. may be a tuple of
        exceptions to check
    :type exception_to_check: Exception or tuple
    :param tries: number of times to try (not retry) before giving up
    :type tries: int
    :param delay: initial delay between retries in seconds
    :type delay: int
    :param backoff: backoff multiplier e.g. value of 2 will double the delay
        each retry
    :type backoff: int
    :param logger: logger to use. If None, print
    :type logger: logging.Logger instance
    """

    def deco_retry(f):
        @wraps(f)
        def f_retry(*args, **kwargs):
            mtries, mdelay = tries, delay
            while mtries > 1:
                try:
                    return f(*args, **kwargs)
                except exception_to_check as e:
                    msg = "%s, Retrying in %d seconds..." % (str(e), mdelay)
                    if logger:
                        logger.warning(msg)
                    else:
                        print(msg)
                    time.sleep(mdelay)
                    mtries -= 1
                    mdelay *= backoff
            return f(*args, **kwargs)

        return f_retry  # true decorator

    return deco_retry


def get_reddit_client_praw():
    """
    Get the praw reddit client.
    :return: The praw.Reddit instance.
    """
    return praw.Reddit(
        client_id=config["REDDIT"]["clientId"],
        client_secret=config["REDDIT"]["clientSecret"],
        redirect="http://localhost:8080",
        username=config["REDDIT"]["username"],
        password=config["REDDIT"]["password"],
        user_agent=config["REDDIT"]["useragent"],
    )


def get_conn() -> psycopg2.connect:
    """
    Get the psycopg2 conn module.
    :return: the psycopg2.connect module
    """
    return psycopg2.connect(
        dbname=config["POSTGRES"]["database"],
        port=config["POSTGRES"]["port"],
        host=config["POSTGRES"]["host"],
        password=config["POSTGRES"]["password"],
        user=config["POSTGRES"]["username"],
    )


def get_sqlalchemy_engine(autocommit: bool = True) -> sqlalchemy.engine:
    """
    Get the sqlalchemy engine for the same pg database
    :param: autocommit: Do you just want to test the connection or autocommit ?
    :return: the sqlalchemy engine
    """
    conn_str = f"postgresql://{config['POSTGRES']['username']}:{config['POSTGRES']['password']}@{config['POSTGRES']['host']}:{config['POSTGRES']['port']}/{config['POSTGRES']['database']}"
    return sqlalchemy.create_engine(conn_str).execution_options(autocommit=autocommit)


def insert_submission_praw(submission) -> None:
    insert_query = f"""INSERT INTO wsb_comments({','.join(submission.keys())}) VALUES %s ON CONFLICT (id) DO NOTHING;"""

    with get_conn() as conn:
        with conn.cursor() as cur:
            insert_query_2 = cur.mogrify(insert_query, tuple(submission.values()))
            print(insert_query_2)
            cur.execute(insert_query_2)
        conn.commit()


def get_wsb_submissions_cols():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
            SELECT column_name
      FROM information_schema.columns
     WHERE table_schema = 'public'
       AND table_name   = 'wsb_submissions';
       """
            )
            wsb_submission_cols = cur.fetchall()

    wsb_submission_cols = [i[0] for i in wsb_submission_cols]
    return wsb_submission_cols


def insert_submissions_pmaw(all_subreddit_submissions: list):
    all_mogrified_comments = []
    un_mogrified_comments = []

    wsb_submission_cols = get_wsb_submissions_cols()

    with get_conn() as conn:
        with conn.cursor() as cur:

            for submission in tqdm(all_subreddit_submissions):
                pop_keys = [
                    key for key in submission.keys() if key not in wsb_submission_cols
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

                except (TypeError, psycopg2.errors.ProgrammingError) as e:
                    un_mogrified_comments.append(submission)

    all_joined_comments = b" ".join(all_mogrified_comments)

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(all_joined_comments)


def create_wsb_comments_table():
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

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(CREATE_WSB_COMMENTS_TABLE_QUERY)
            cur.execute(CREATE_WSB_INDEX_QUERY)
            cur.execute(CREATE_WSB_SUBMISSION_QUERIES)
        conn.commit()


def update_wsb_comments_materialized_views():
    print("-- Updating materialized view wsb_submission_comments...")
    query_1 = """CREATE MATERIALIZED VIEW wsb_submission_comments AS SELECT CURRENT_DATE, t.submission_id, array_agg(t.id) as comment_ids FROM wsb_comments t group by t.submission_id;"""
    query_2 = """CREATE UNIQUE INDEX wsb_submission_comments_rel_uindex ON wsb_submission_comments (submission_id);"""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(query_1)
            cur.execute(query_2)
        conn.commit()
    print("-- Done")


def create_wsb_comments_status_table():
    print("-- Creating wsb_comments_status table...")
    query = """CREATE TABLE IF NOT EXISTS wsb_comments_status(insert_date timestamp without time zone, submission_ids_completed text[]);"""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(query)
        conn.commit()
    print("-- Done")


def update_wsb_comments_status_table():
    print("-- Updating wsb_comments_status table...")
    query = """
    INSERT INTO wsb_comments_status 
    SELECT CURRENT_DATE, array_agg(DISTINCT t.submission_id) AS submission_ids_completed 
    FROM wsb_comments t ON CONFLICT (insert_date) 
    DO UPDATE SET submission_ids_completed = excluded.submission_ids_completed; 
    """

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(query)
        conn.commit()

    print("-- Done.")


def clean_comment_praw(comment: praw.Reddit.comment):
    return {
        key: val
        for key, val in vars(comment).items()
        if key
        not in ["_replies", "_submission", "_reddit", "subreddit", "author", "_fetched"]
    }


def get_unique_col_values_from_wsb_comments(colname: str):
    QUERY = f"""SELECT DISTINCT (t.{colname}) FROM wsb_comments t;"""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(QUERY)
            res = cur.fetchall()
    res = [r[0] for r in res]
    return res


def get_all_daily_submissions_praw(reddit: praw.Reddit):
    subreddit = reddit.subreddit("wallstreetbets")

    june_2021_titles = {}
    for submission in subreddit.search(
        f"and title:'Daily Discussion Thread for June 2021'"
    ):
        june_2021_titles[submission.id] = submission.title

    june_2021_titles = {
        k: v for k, v in june_2021_titles.items() if "Agricultural" not in v
    }

    june_2021_titles["nqi9f6"] = "Daily Discussion Thread for June 02, 2021"
    june_2021_titles["nr9r9t"] = "Daily Discussion Thread for June 03, 2021"

    other_titles = {}
    for submission in subreddit.search(f"and title:'Daily Discussion Thread'"):
        other_titles[submission.id] = submission.title

    other_titles = {k: v for k, v in other_titles.items() if "Agricultural" not in v}
    other_titles.update(june_2021_titles)
    return other_titles


def traverse_comments_praw(comment: praw.Reddit.comment, submission_id: str):
    """

    :param comment:
    :param submission_id:
    :return:
    """
    if hasattr(comment, "comments"):
        for cc in tqdm(comment.comments(), leave=False):
            traverse_comments_praw(comment=cc, submission_id=submission_id)
    else:
        cleaned_comment = clean_comment_praw(comment=comment)
        cleaned_comment["submission_id"] = submission_id
        for key, val in cleaned_comment.items():
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
                cleaned_comment[key] = Json(json.dumps(val))
            if isinstance(cleaned_comment["edited"], bool):
                cleaned_comment["edited"] = 0

        cols_str = ",".join(cleaned_comment.keys())
        values_str = ("%s," * len(cleaned_comment)).rstrip(",")
        insert_query = f"""INSERT INTO wsb_comments ({cols_str}) VALUES ({values_str}) ON CONFLICT (id) DO NOTHING;"""

        with get_conn() as conn:
            with conn.cursor() as cur:
                try:
                    mogrified_query = cur.mogrify(
                        insert_query, tuple(cleaned_comment.values())
                    )
                    cur.execute(mogrified_query)
                except psycopg2.ProgrammingError as e:
                    print(cleaned_comment["id"], e)


@on_exception(expo, RateLimitException, max_tries=8)
@limits(calls=1, period=MINUTE)
def call_pushshift_api(url) -> requests.Response:
    """
    A method to control the rate limit of the api calls
    :param url: the pushshift api url
    :return: the api response
    """
    response = requests.get(url)
    if response.status_code != 200:
        raise Exception("API response: {}".format(response.status_code))
    return response


def call_pushshift_api_securely(listManager, url):
    """
    Use multiprocessing to push values to a listManager, and return that.
    :param listManager: the multiprocessing.Manager object
    :param url: the pushShift api object
    :return: None, this just populates the listManager
    """
    time.sleep(1)
    info = None
    resolved = False
    submission_id = url.split("/")[-1]

    try:
        while not resolved:
            res = None
            tooManyCalls = False

            try:
                res = call_pushshift_api(url)
                if res == "Not Found":
                    resolved = True
                    break
            except Exception as e:
                print(e)
                if e == "too many calls":
                    tooManyCalls = True

            if tooManyCalls:
                time.sleep(60)

            elif res.status_code < 300:
                info = res.json()
                resolved = True

            elif res.status_code == 429:
                print(res.status_code)
                time.sleep(60)
            else:
                print(res.status_code)
                sleep_val = random.randint(1, 10)
                time.sleep(sleep_val)

    except Exception as e:
        print(e)

    finally:
        if info is not None:
            listManager.append({"submission_id": submission_id, "comments": info})
            time.sleep(0.5)
            return


def get_all_possible_existing_comments_from_db():
    # create the pool
    manager = Manager()

    # Need a manager to help get the values async, the values will be updated after join
    listManager = manager.list()

    all_subs_in_db = get_unique_col_values_from_wsb_comments(colname="submission_id")
    pushshift_endpoint = "https://api.pushshift.io/reddit/submission/comment_ids"
    args = [[listManager, f"{pushshift_endpoint}/{sub}"] for sub in all_subs_in_db]

    workers = max(cpu_count() - 4, 1)

    pool = Pool(workers)

    try:
        for _ in tqdm(pool.istarmap(call_pushshift_api_securely, args)):
            pass
        pool.close()
        pool.join()
    finally:
        pool.close()
        pool.join()

    df_ = pd.DataFrame(list(listManager))
    return df_


def get_all_sub_ids_pmaw(
    pmaw_api_instance: pmaw.PushshiftAPI, subreddit: str, q: str, limit: int
) -> list:
    """
    For all the subreddits available, give a query in there as well... what do you want to see in the submission
    :param pmaw_api_instance: the API instance
    :param subreddit: the subreddit string
    :param q: the query param
    :param limit: the amount of posts that will be pulled down
    :return:
    """
    posts = pmaw_api_instance.search_submissions(q=q, subreddit=subreddit, limit=limit)
    posts = [p for p in posts]
    insert_submissions_pmaw(all_subreddit_submissions=posts)
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT DISTINCT (id) FROM wsb_submissions;")
            sub_ids = cur.fetchall()
            sub_ids = [sub_id[0] for sub_id in sub_ids]
    return sub_ids


def get_all_comments_from_ids_pmaw(
    pmaw_api_instance: pmaw.PushshiftAPI, submission_comment_ids: dict
) -> list:
    """
    For all the sub_ids that have been provided to this func, we can now pull their comments down from pmaw.
    :param pmaw_api_instance: The pmaw_api instance
    :param submission_comment_ids: a dict that looks like {"submission_id": 121213231, "comments": [2312, 123123, 123]}
    :return: all the comments found associated with each submission id
    """
    all_comments = []
    for sub_id, comments in tqdm(submission_comment_ids.items()):
        comments_arr = pmaw_api_instance.search_comments(ids=comments)
        for c in comments_arr:
            c["submission_id"] = sub_id
            all_comments.append(c)
    return all_comments


def insert_all_comments_to_db_pmaw(df: pd.DataFrame) -> None:
    """
    For each comment in all_comments... insert that into the database.
    :param df: a list of dicts, [{... all dict properties of the wsb_comments table}]
    :return: None
    """

    print("Formatting into correct format...")
    to_insert_comments = []

    comment = df.iloc[0,].to_dict()

    wsb_comments_cols = get_wsb_submissions_cols()

    comment_copy = comment.copy()
    to_pop = [key for key in list(comment_copy.keys()) if key not in wsb_comments_cols]
    for key in to_pop:
        _ = comment.pop(key)

    comment = {
        "subreddit_name_prefixed" if k == "subreddit" else k: v
        for k, v in comment.items()
    }

    insert_query = f"INSERT INTO wsb_comments ({','.join(comment.keys())}) VALUES %s ON CONFLICT (id) DO NOTHING;"

    for i in tqdm(range(len(df))):
        comment = df.iloc[i,].to_dict()
        comment_copy = comment.copy()
        to_pop = [key for key in list(comment_copy.keys()) if key not in wsb_comments_cols]

        for key in to_pop:
            _ = comment.pop(key)

        comment["subreddit_name_prefixed"] = f"r/{comment['subreddit']}"
        _ = comment.pop("subreddit")

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
            comment["edited"] = 0

        if "author_patreon_flair" in comment.keys() and isinstance(
            comment["author_patreon_flair"], bool
        ):
            comment["author_patreon_flair"] = ""

        if "author_cakeday" in comment.keys() and isinstance(
            comment["author_cakeday"], np.float
        ):
            comment["author_cakeday"] = bool(0)

        if "author_premium" in comment.keys() and isinstance(
            comment["author_premium"], np.float
        ):
            comment["author_premium"] = bool(0)

        if "can_mod_post" in comment.keys() and isinstance(
            comment["can_mod_post"], np.float
        ):
            comment["can_mod_post"] = bool(0)

        insert_query = f"INSERT INTO wsb_comments ({','.join(comment.keys())}) VALUES %s ON CONFLICT (id) DO NOTHING;"

        to_insert_comments.append(tuple(comment.values()))

    print("-- Inserting into the db...")
    with get_conn() as conn:
        with conn.cursor() as cur:
            try:
                execute_values(cur, insert_query, to_insert_comments)
                conn.commit()
            except psycopg2.errors.ProgrammingError as e:
                print(e)
    print("-- Done.")


def get_all_submission_ids_to_be_refreshed() -> list:
    print(
        "-- Get all submission_ids_completed from wsb_comments_status for latest date..."
    )
    # determine existing comments that have already been backfilled
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT t.submission_ids_completed FROM wsb_comments_status t ORDER BY t.insert_date DESC LIMIT 1;"
            )
            all_submission_ids_completed = np.unique(cur.fetchall()[0][0]).tolist()
    print("-- Done.")

    # we may have some submission ids that have not been entirely backfilled yet.
    print("-- Get all existing submission_ids...")
    all_existing_submission_ids = get_unique_col_values_from_wsb_comments(
        colname="submission_id"
    )
    print("-- Done.")

    # what about all the comments that can be drawn from praw?
    print(
        "-- Fetching all submission ids from pmaw for wallstreetbets, pushing it to db and fetching unique sub ids..."
    )
    all_subreddit_submissions = get_all_sub_ids_pmaw(
        pmaw_api, subreddit="wallstreetbets", q="Daily Discussion Thread", limit=100000
    )
    print("-- Done.")

    # if they dont exist in all_submission_ids_completed, then just push it all into a list of non-completed items.
    print("-- Cleaning up...")
    incomplete_submission_ids = [
        i
        for i in all_existing_submission_ids + all_subreddit_submissions
        if i not in all_submission_ids_completed
    ]
    print("-- Done.")

    return incomplete_submission_ids


def get_all_incomplete_submissions_comment_ids(
    incomplete_submission_ids: list,
) -> None:
    print("-- Get all incomplete submission ids from pmaw...")
    # fetch all comment ids for these submission ids
    for submission_id in tqdm(incomplete_submission_ids):
        comments = pmaw_api.search_submission_comment_ids(
            ids=[submission_id], safe_exit=True, memsafe=True
        )

        submission_id_comments = {
            "submission_id": submission_id,
            "comments": [comment for comment in comments],
        }

        all_comments = []
        comments_arr = pmaw_api.search_comments(ids=submission_id_comments["comments"])
        for c in comments_arr:
            c["submission_id"] = submission_id_comments["submission_id"]
            all_comments.append(c)

        comments_df = pd.DataFrame(all_comments)
        insert_all_comments_to_db_pmaw(df=comments_df)
        update_wsb_comments_status_table()


def copy_from_local_to_db():
    if os.name == "posix":
        filepath = r"c:/Users/SHIRAM/Documents/Numerai/prepared_statements.csv"
        psql = r"/usr/lib/postgresql/13/psql"
    elif os.name == "win":
        filepath = r"C:\\Users\\SHIRAM\\Documents\\Numerai\\prepared_statements.csv"
        psql = r"C:\\Program Files\\PostgreSQL\\12\\bin\\psql.exe"
    else:
        filepath = r"C:\\Users\\SHIRAM\\Documents\\Numerai\\prepared_statements.csv"
        psql = r"C:\\Program Files\\PostgreSQL\\12\\bin\\psql.exe"

    db_conn = f"postgresql://{config['POSTGRES']['username']}:{config['POSTGRES']['password']}@{config['POSTGRES']['host']}:{config['POSTGRES']['port']}/{config['POSTGRES']['database']}"

    command = [
        psql,
        "--command",
        f"\copy wsb_comments_tickers FROM '{filepath}' WITH (FORMAT csv, header);",
        "-d",
        db_conn,
    ]
    process = Popen(command, stdout=PIPE, stderr=STDOUT, shell=True)
    stdout = process.communicate()[0].decode("utf-8").strip()
    return stdout


def execute_command_in_db(command_str: str):
    if platform.system() == "Darwin":
        psql = r"/usr/local/bin/psql"
    elif os.name == "win":
        psql = r"C:\\Program Files\\PostgreSQL\\12\\bin\\psql.exe"
    else:
        psql = r"C:\\Program Files\\PostgreSQL\\12\\bin\\psql.exe"

    db_conn = f"postgresql://{config['POSTGRES']['username']}:{config['POSTGRES']['password']}@{config['POSTGRES']['host']}:{config['POSTGRES']['port']}/{config['POSTGRES']['database']}"

    command = [
        psql,
        "--command",
        command_str,
        "-d",
        db_conn,
    ]
    process = Popen(command, stdout=PIPE, stderr=STDOUT, shell=True)
    stdout = process.communicate()[0].decode("utf-8").strip()
    return stdout


def update_wsb_comments_analytics(create_from_scratch: bool = False):
    if create_from_scratch:
        print(
            "-- Dropping wsb_comments_analytics_tickers_found and creating it again but empty..."
        )
        create_tickers_found_table = f"DROP TABLE IF EXISTS wsb_comments_tickers; CREATE TABLE IF NOT EXISTS wsb_comments_tickers (comment_id text, tickers text);"
        output_1 = execute_command_in_db(command_str=create_tickers_found_table)
        print(output_1)

        print("-- Copying data from local to wsb_comments_analytics_tickers_found...")
        output_2 = copy_from_local_to_db()
        print(output_2)

        print(
            "-- Altering wsb_comments_analytics_tickers_found with column type tickers text to text[]..."
        )
        alter_tickers_found_table = """ALTER TABLE wsb_comments_tickers ALTER COLUMN tickers TYPE text[] USING string_to_array(tickers, ', ');"""
        output_3 = execute_command_in_db(command_str=alter_tickers_found_table)
        print(output_3)

    print(
        "-- Insert the new diff data into wsb_comments_analytics, without the ticker information..."
    )
    insert_new_diff_data_into_wsb_comments_analytics = """INSERT INTO wsb_comments_analytics(created_utc, submission_id, parent_id, comment_id, author, body) SELECT to_timestamp(created_utc) as created_utc, submission_id, parent_id, id as comment_id, author, body FROM wsb_comments ON CONFLICT DO NOTHING;"""
    output_35 = execute_command_in_db(
        command_str=insert_new_diff_data_into_wsb_comments_analytics
    )
    print(output_35)

    print("-- Dropping table wsb_comments_analytics_temp and creating it again...")
    drop_and_create_analytics_temp = """DROP table if exists wsb_comments_analytics_temp; CREATE TABLE wsb_comments_analytics_temp AS (SELECT t1.created_utc, t1.submission_id, t1.parent_id, t1.comment_id, t1.author, t1.body, t2.tickers FROM wsb_comments_analytics t1 LEFT JOIN wsb_comments_tickers t2 on t1.comment_id = t2.comment_id);"""
    output_4 = execute_command_in_db(command_str=drop_and_create_analytics_temp)
    print(output_4)

    print(
        "-- Truncating/Dropping wsb_comments_analytics, and renaming wsb_comments_analytics_temp tp wsb_comments_analytics..."
    )
    drop_and_rename_wsb_comments_analytics = """TRUNCATE wsb_comments_analytics; DROP TABLE wsb_comments_analytics; ALTER TABLE wsb_comments_analytics_temp RENAME TO wsb_comments_analytics;"""
    output_5 = execute_command_in_db(command_str=drop_and_rename_wsb_comments_analytics)
    print(output_5)

    print("-- Creating hyper table...")
    create_hypertable = """SELECT create_hypertable('wsb_comments_analytics', 'created_utc', 'comment_id', number_partitions := 10, migrate_data := TRUE);"""
    output_6 = execute_command_in_db(command_str=create_hypertable)
    print(output_6)

    # print("-- Dropping wsb_comments_analytics_tickers_found...")
    # drop_analytics_tickers_found = (
    #     """DROP TABLE IF EXISTS wsb_comments_analytics_tickers_found;"""
    # )
    # output_7 = execute_command_in_db(command_str=drop_analytics_tickers_found)
    # print(output_7)


def backfill_submissions(capture_exceptions: bool, to_be_refreshed: list = None):
    """
    Get all the backfill submissions for some submissions that might be incomplete.
    :return:
    """
    if to_be_refreshed is None:
        to_be_refreshed = get_all_submission_ids_to_be_refreshed()
    else:
        pass

    all_exceptions = []

    for submission_id in tqdm(to_be_refreshed):
        comments = pmaw_api.search_submission_comment_ids(
            ids=[submission_id], safe_exit=True, memsafe=True
        )

        submission_id_comments = {
            "submission_id": submission_id,
            "comments": [comment for comment in comments],
        }

        all_comments = []
        comments_arr = pmaw_api.search_comments(ids=submission_id_comments["comments"])
        for c in comments_arr:
            c["submission_id"] = submission_id_comments["submission_id"]
            all_comments.append(c)

        comments_df = pd.DataFrame(all_comments)

        if len(comments_df) > 0:
            try:
                insert_all_comments_to_db_pmaw(df=comments_df)
                update_wsb_comments_status_table()
            except:
                if capture_exceptions:
                    all_exceptions.append(comments_df)
                else:
                    pass

    if len(all_exceptions) > 0:
        return all_exceptions
    else:
        return None


@retry(AssertionError, tries=4, delay=5, backoff=5, logger=None)
def gather_historical_data():
    r_client = get_reddit_client_praw()
    daily_submissions = get_all_daily_submissions_praw(reddit=r_client)
    ignore_submissions = get_unique_col_values_from_wsb_comments(
        colname="submission_id"
    )

    for sub_id, sub_title in tqdm(
        daily_submissions.items(), desc="For each submission..."
    ):
        if sub_id not in ignore_submissions:
            submission = r_client.submission(id=sub_id)

            for comment in tqdm(submission.comments, leave=False):
                traverse_comments_praw(comment=comment, submission_id=sub_id)
        else:
            print(f"Skipping submission_id: {sub_id}")


@retry(AssertionError, tries=4, delay=5, backoff=5, logger=None)
def gather_live_data():
    r_client = get_reddit_client_praw()
    for submission in r_client.subreddit("wallstreetbets").stream.submissions():
        for comment in tqdm(submission.comments, leave=False):
            traverse_comments_praw(comment=comment, submission_id=submission.id)


@click.command()
@click.option(
    "--historical",
    default=0,
    help="Just get all the daily submissions historical data from wsb",
)
@click.option("--live", default=0, help="Just get all the live stream data from wsb")
@click.option(
    "--backfill", default=0, help="Backfill existing comments from the database"
)
@click.option(
    "--update", default=0, help="Will update the wsb_comments_analytics table..."
)
def hello(historical, live, backfill, update):
    if historical:
        gather_historical_data()
    if live:
        gather_live_data()
    if backfill:
        all_exceptions = backfill_submissions(capture_exceptions=True)
        all_exceptions = pd.concat(all_exceptions)
        all_exceptions.to_csv("all_exceptions.csv")
    if update:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema='public' AND table_name='wsb_comments_tickers');"""
                )
                create_from_scratch = cur.fetchall()[0][0]

        print(f"-- Updating from scratch: {not create_from_scratch}")
        # update_from_scratch = False if the table doesn't exist, but that's the opposite of what we want.
        # We WANT a table there, so if there's no table there, you'll have to start from scratch.
        update_wsb_comments_analytics(create_from_scratch=not create_from_scratch)


if __name__ == "__main__":
    hello()
