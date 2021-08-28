import json
import rejson
import datetime
import pandas as pd

date_format = "%Y-%m-%d"


class RedisJsonDecoder(json.JSONDecoder):
    def decode(self, s, *args, **kwargs):
        if isinstance(s, bytes):
            s = s.decode("UTF-8")
        return super(RedisJsonDecoder, self).decode(s, *args, **kwargs)


def batch(iterable, n=1):
    l = len(iterable)
    for ndx in range(0, l, n):
        yield iterable[ndx : min(ndx + n, l)]


def get_redis_conn() -> rejson.Client:
    """
    Get a simple wrapper to get the redis conn
    This has been slightly modified by the RedisJsonDecode() object from utils
    :return: the redis-json client obj
    """
    return rejson.Client(
        host="localhost",
        port=6379,
        db=0,
        decode_responses=True,
        decoder=RedisJsonDecoder(),
    )


def split_dates(date_str: str) -> list:
    """
    Just a helper function to change something from %Y-%m-%d to %Y, %m, %d.
    :param date_str: The %Y-%m-%d formatted date string.
    :return: the year, mon, day separated.
    """
    try:
        _ = datetime.datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError as _:
        raise ValueError("Please ensure the date is of format:= %Y-%m-%d")

    return [date_str.split("-")[0], date_str.split("-")[1], date_str.split("-")[2]]


def convert_to_datetime(date_str: str) -> datetime.datetime:
    return datetime.datetime.strptime(date_str, "%Y-%m-%d")


def log_return(price: pd.Series):
    return np.log(price).diff()
