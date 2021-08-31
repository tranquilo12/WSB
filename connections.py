import os
import sys
import praw
import pmaw
import psycopg2
import sqlalchemy
import configparser


class Connections:
    def __init__(self):
        self.curdir_fullpath = os.path.dirname(os.path.realpath(__file__))
        self.config = configparser.ConfigParser()
        self.config.read(os.path.join(self.curdir_fullpath, "config.ini"))

    def get_redis_conn(self):
        return redis.Redis(
            host=self.config["REDIS"]["host"],
            port=self.config["REDIS"]["port"],
            db=self.config["REDIS"]["db"],
        )

    def get_db_conn_str(self, local: bool = False):
        if local:
            section = "POSTGRESLOCAL"
        else:
            section = "POSTGRES"

        username = self.config[section]["username"]
        password = self.config[section]["password"]
        host = self.config[section]["host"]
        port = self.config[section]["port"]
        database = self.config[section]["database"]
        return f"postgresql://{username}:{password}@{host}:{port}/{database}"

    def get_sqlalchemy_engine(self, autocommit: bool = True, local: bool = False):
        conn_str = self.get_db_conn_str(local=local)
        return sqlalchemy.create_engine(conn_str).execution_options(
            autocommit=autocommit
        )

    def get_psycopg2_conn(self, local: bool = False):
        if local:
            section = "POSTGRESLOCAL"
        else:
            section = "POSTGRES"
        return psycopg2.connect(
            dbname=self.config[section]["database"],
            port=self.config[section]["port"],
            host=self.config[section]["host"],
            password=self.config[section]["password"],
            user=self.config[section]["username"],
        )

    def get_psql_command(self):
        if self.system_type == "Darwin":
            psql = r"/usr/local/bin/psql"
            return psql

        elif self.system_type == "Windows":
            psql = r"C:\\Program Files\\PostgreSQL\\12\\bin\\psql.exe"
            return psql

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

    @staticmethod
    def get_pushshift_client():
        return pmaw.PushshiftAPI()
