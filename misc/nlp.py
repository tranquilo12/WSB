import re

import numpy as np
import pandas as pd
import spacy
from joblib import Parallel, delayed
from tqdm.auto import tqdm

from generate_urls import get_all_tickers
from reddit_stuff import get_conn


def get_wsb_comments_analytics():
    # Get data from the comments db
    with get_conn() as conn:
        with conn.cursor() as cur:
            query = "SELECT created_utc, comment_id, body FROM wsb_comments_analytics;"
            cur.execute(query)
            res = cur.fetchall()

    wsb_comments_analytics_df = pd.DataFrame.from_records(res)
    wsb_comments_analytics_df.columns = ["datetime", "comment_id", "body"]
    return wsb_comments_analytics_df


def chunker(iterable, total_length, chunksize):
    return (
        iterable[pos : pos + chunksize] for pos in range(0, total_length, chunksize)
    )


def flatten(list_of_lists):
    "Flatten a list of lists to a combined list"
    return [item for sublist in list_of_lists for item in sublist]


def clean_text(text):
    text = re.sub(r"\s+", " ", text)
    text = text.replace(":", "").replace(";", "").lower()
    return text


def tokenize(doc):
    token_list = [[str(token).lower(), token.pos_] for token in doc]
    return token_list


def complex_tkr_condition(tkr, tokenized_keys, tokenized_value):
    if (tkr not in ["HAS", "A", "FOR", "NEW", "ME"]) and (tkr in tokenized_keys):
        if (tkr in comment_split) and (tokenized_value in ["PROPN", "NOUN"]):
            return tkr


def process_chunk(nlp, texts, tickers):
    processed_pipe = []
    texts = [clean_text(text) for text in texts]

    for doc in nlp.pipe(texts, batch_size=20):
        tokenized_doc = tokenize(doc)
        tokenized_arr = np.array(tokenized_doc)
        tickers_found = [
            tkr
            for tkr in tickers
            if complex_tkr_condition(tkr, list(np.array(tokenized_doc)[:, 0]), list(tokenized_arr[np.where(tokenized_arr == tkr)]))
        ]
        processed_pipe.append(tickers_found)
    return processed_pipe


def process_one(nlp, text, tickers):
    doc = nlp(clean_text(text))
    tokenized_doc = tokenize(doc)
    tokenized_arr = np.array(tokenized_doc)
    tickers_found = []
    for tkr in tickers:
        if len(tokenized_doc) > 0:
            if complex_tkr_condition(tkr, list(np.array(tokenized_doc)[:, 0]),
                                     list(tokenized_arr[np.where(tokenized_arr == tkr)])):
                tickers_found.append(tkr)
    return tickers_found


def parallel_nlp(nlp, texts, tickers, total_len, chunksize=100):
    executor = Parallel(n_jobs=7, backend="multiprocessing", prefer="threads")
    tasks = (
        delayed(process_chunk)(nlp, chunk, tickers)
        for chunk in tqdm(chunker(texts, total_len, chunksize=chunksize))
    )
    result = executor(tasks)
    return flatten(result)


if __name__ == "__main__":

    tqdm.pandas()

    print("-- Loading spacy...")
    nlp = spacy.load("en_core_web_sm")

    print("-- Getting all active tickers")
    tickers = get_all_tickers(active="true")

    tickers_only = [ticker["ticker"].lower() for ticker in tickers]

    print("-- Fetching wsb_comment_analytics...")
    wsb_comments_analytics_df = get_wsb_comments_analytics()

    print("-- Filtering for body only...")
    nan_cond = ~wsb_comments_analytics_df["body"].isna()
    texts = wsb_comments_analytics_df.loc[nan_cond, "body"]

    # print("-- Parallel exec...")
    # wsb_comments_analytics_df["preprocessed_parallel"] = parallel_nlp(
    #     nlp=nlp,
    #     texts=texts,
    #     tickers=tickers,
    #     total_len=len(wsb_comments_analytics_df),
    #     chunksize=1000,
    # )

    print("-- Linear exec...")
    wsb_comments_analytics_df.loc[nan_cond, "preprocessed_parallel"] = \
        wsb_comments_analytics_df.loc[nan_cond, "body"].progress_apply(lambda x: process_one(nlp=nlp, text=x, tickers=tickers))

    print("Writing to csv..")
    wsb_comments_analytics_df.to_csv("wsb_comments_analytics_processed.csv")
