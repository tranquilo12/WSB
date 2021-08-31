import datetime
import pandas as pd
from wsb import Gather
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from utils import add_time_series_slider

gather = Gather()


st.set_page_config(layout="wide")
st.title("WSB Status")

# SUBMISSION SECTION
submission_status_df = gather.get_submission_status_mat_view(local=True)

row1_1, row1_2 = st.beta_columns((1, 1))

with row1_1:
    rolling_days = st.slider("Submission: Select rolling window length", 0, 100)
    submission_status_df.loc[:, "submissions_count_rolling_avg"] = (
        submission_status_df["submissions_count"].rolling(rolling_days).mean()
    )
    submission_status_df.loc[:, "submissions_count_rolling_std"] = (
        submission_status_df["submissions_count"].rolling(rolling_days).std()
    )

with row1_2:
    year_start = st.slider("Select start year", 2019, 2021)
    submission_status_df = submission_status_df[
        submission_status_df["date"].dt.year >= year_start
    ]

st.header("Submission Status")

fig = go.Figure(
    [
        go.Scatter(
            x=submission_status_df["date"],
            y=submission_status_df["submissions_count"],
            mode="lines",
            name="Submissions count",
        ),
        go.Scatter(
            x=submission_status_df["date"],
            y=submission_status_df["submissions_count_rolling_avg"],
            mode="lines",
            name=f"Rolling mean {rolling_days} days",
        ),
        go.Scatter(
            x=submission_status_df["date"],
            y=submission_status_df["submissions_count_rolling_std"],
            mode="lines",
            name=f"Rolling std {rolling_days} days",
        ),
    ]
)

fig = add_time_series_slider(fig=fig)
st.plotly_chart(fig, use_container_width=True)

# COMMENTS SECTION
[
    submission_comments_status_df,
    submission_comments_count_df,
] = gather.get_submission_comments_status_mat_view(local=True)

row2_1, row2_2 = st.beta_columns((1, 1))

with row2_1:
    rolling_days_comments = st.slider("Comments: Select rolling window length", 0, 100)
    submission_comments_count_df.loc[:, "submissions_count_rolling_avg"] = (
        submission_comments_count_df["submission_comments_count"]
        .rolling(rolling_days_comments)
        .mean()
    )
    submission_comments_count_df.loc[:, "submissions_count_rolling_std"] = (
        submission_comments_count_df["submission_comments_count"]
        .rolling(rolling_days_comments)
        .std()
    )

with row2_2:
    year_start_comments = st.slider("Comments: Select start year", 2019, 2021)
    submission_comments_count_df = submission_comments_count_df[
        submission_comments_count_df["date"].dt.year >= year_start_comments
    ]

st.header("Submission -> Comments status")

fig2 = go.Figure(
    [
        go.Scatter(
            x=submission_comments_count_df["date"],
            y=submission_comments_count_df["submission_comments_count"],
            mode="lines",
            name="Submissions comments count",
        ),
        go.Scatter(
            x=submission_comments_count_df["date"],
            y=submission_comments_count_df["submissions_count_rolling_avg"],
            mode="lines",
            name=f"Rolling mean {rolling_days_comments} days",
        ),
        go.Scatter(
            x=submission_comments_count_df["date"],
            y=submission_comments_count_df["submissions_count_rolling_std"],
            mode="lines",
            name=f"Rolling std {rolling_days_comments} days",
        ),
    ]
)

fig2 = add_time_series_slider(fig=fig2)
st.plotly_chart(fig2, use_container_width=True)
