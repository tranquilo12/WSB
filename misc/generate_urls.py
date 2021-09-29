import configparser
from io import StringIO
from typing import List

import boto3
import pandas as pd
import requests

config = configparser.ConfigParser()
config.read("config.ini")


def get_boto3_s3():
    """
    Get the boto3 conn, read all the config.
    :return: The boto3 conn
    """
    s3 = boto3.resource("s3")
    return s3


# Get all tickers:
def get_all_tickers(active: str = "true",) -> list:
    """

    :param active:
    :return:
    """
    base = "https://api.polygon.io/v3/reference/tickers"
    apiKey = config["POLYGONIO"]["apikey"]
    params = {
        "apiKey": apiKey,
        "limit": 1000,
        "order": "asc",
        "sort": "ticker",
        "active": active,
    }

    all_tickers_results = []
    response = requests.get(base, params=params)

    if response.status_code == 200:
        all_tickers_results = get_all_tickers_nested(
            resp=response, key=apiKey
        )

    return all_tickers_results


# Get all tickers:
def get_all_tickers_nested(
    resp: requests.Response,  key: str
) -> List[dict]:
    """
    :param key: 
    :param resp
    :return: a list containing results from that response
    """

    tickers = []
    next_url = "next"
    if (resp.status_code == 200) and (next_url is not None) and (next_url != ""):
        response_json = resp.json()

        # get the first result ever from this response
        tickers = response_json["results"]

        if ("next_url" in response_json.keys()) and (response_json["next_url"] != ""):
            next_url = response_json["next_url"]
            next_response = requests.get(next_url, params={"apiKey": key})
            next_tickers_results = get_all_tickers_nested(
                resp=next_response, key=key
            )

            if not isinstance(next_tickers_results, list):
                pass
            else:
                tickers += next_tickers_results

    return tickers


# Aggs: /v2/aggs/ticker/{stocksTicker}/range/{multiplier}/{timespan}/{from}/{to}
# def generate_aggs_requests():

if __name__ == "__main__":

    print("Grabbing all active tickers...")
    all_active_tickers: List[dict] = get_all_tickers(active="true")
    active_df = pd.DataFrame.from_dict(all_active_tickers)
    print("Done..")

    print("Grabbing all inactive tickers...")
    all_inactive_tickers: List[dict] = get_all_tickers(active="false")
    inactive_df = pd.DataFrame.from_dict(all_inactive_tickers)
    print("Done..")

    print("Appending both dfs...")
    df = pd.concat([active_df, inactive_df])
    csv_buffer = StringIO()
    df.to_csv(path_or_buf=csv_buffer, sep="\t")
    print("Done..")

    # Write to s3
    print("Creating s3 resource and uploading to s3...")
    s3 = get_boto3_s3()
    s3.Bucket("polygonio-all").put_object(
        Key="tickers.tape", Body=csv_buffer.getvalue()
    )
    print("Done..")
