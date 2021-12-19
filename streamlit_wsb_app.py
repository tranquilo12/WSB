import plotly.graph_objects as go
import streamlit as st
from prefect import Client

from prefect_etl import Gather
from utils import add_time_series_slider

gather = Gather()
prefect_client = Client()

st.set_page_config(layout="wide")
st.title("WSB Status")

# SUBMISSION SECTION ############################################################################################
st.header("Submission Status")

submission_status_df = gather.get_submission_status_mat_view(local=True)
submission_status_df = submission_status_df.sort_values(by=["date"])

expander_1 = st.expander(label="Submissions options", expanded=False)
with expander_1:
    # (
    #     expander_row1_1,
    #     expander_row1_2,
    #     expander_row1_3,
    #     expander_row1_4,
    # ) = st.columns((1, 1, 1, 1))

    # with expander_row1_1:
    st.markdown(
        """<style> div.stSlider { margin-left:5px; padding-left:15px } </style>""",
        unsafe_allow_html=True,
    )

    rolling_days = st.slider("Submission: Select rolling window length", 0, 100)

    submission_status_df.loc[:, "submissions_count_rolling_avg"] = (
        submission_status_df["submissions_count"].rolling(rolling_days).mean()
    )

    submission_status_df.loc[:, "submissions_count_rolling_std"] = (
        submission_status_df["submissions_count"].rolling(rolling_days).std()
    )

    # with expander_row1_2:
    #     backfill_start_date = st.date_input(
    #         label="Backfill Start Date",
    #         min_value=submission_status_df["date"].min(),
    #         # max_value=submission_status_df["date"].max(),
    #     ).strftime("%Y-%m-%d")

    # with expander_row1_3:
    #     backfill_end_date = st.date_input(
    #         label="Backfill End Date",
    #         min_value=submission_status_df["date"].min(),
    #         # max_value=submission_status_df["date"].max(),
    #     ).strftime("%Y-%m-%d")

    # with expander_row1_4:
    #     # submission_flow_id = "1a06582e-94f7-460c-95e7-d9a1b1eecc3c"
    #     submission_flow_id = gather.get_submissions_flow_id()
    #     st.markdown(
    #         """<style> div.stButton > button:first-child { margin-top: 20px; margin-left:10px } </style>""",
    #         unsafe_allow_html=True,
    #     )
    #     go_button = st.button("Go fetch submissions!")
    #     if go_button:
    #         prefect_client.create_flow_run(
    #             flow_id=submission_flow_id,
    #             parameters={
    #                 "start_date": backfill_start_date,
    #                 "end_date": backfill_end_date,
    #             },
    #         )
    #         go_button = False

# rolling_days = 30
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
st.markdown("---")

# COMMENTS SECTION #####################################################################################################
st.header("Submission -> Comments status")
[
    submission_comments_status_df,
    submission_comments_count_df,
] = gather.get_submission_comments_status_mat_view(local=True)

expander_2 = st.expander(
    label='Comments in Submissions backfill options', expanded=False
)

with expander_2:
    (
        expander_row2_1,
        expander_row2_2,
        expander_row2_3,
        expander_row2_4,
    ) = st.columns((1, 1, 1, 1))

    with expander_row2_1:
        rolling_days_comments = st.slider(
            "Comments: Select rolling window length", 0, 100
        )
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

    with expander_row2_2:
        comments_backfill_start_date = st.date_input(
            label="Backfill Start Date",
            min_value=submission_comments_status_df.index.min(),
            # max_value=submission_comments_status_df.index.max(),
        ).strftime("%Y-%m-%d")

    with expander_row2_3:
        comments_backfill_end_date = st.date_input(
            label="Backfill End Date",
            min_value=submission_comments_status_df.index.min(),
            # max_value=submission_comments_status_df.index.max(),
        ).strftime("%Y-%m-%d")

    with expander_row2_4:
        # refill_comments_flow_id = "2b6c02fa-5ebb-4ed9-9eb0-a5f6aab55c81"
        refill_comments_flow_id = gather.get_comments_flow_id()
        st.markdown(
            """<style> div.stButton > button:first-child { margin-top: 20px; margin-left:10px } </style>""",
            unsafe_allow_html=True,
        )
        go_button_2 = st.button("Go fetch comments!")
        if go_button_2:
            prefect_client.create_flow_run(
                flow_id=refill_comments_flow_id,
                parameters={
                    "start_date": comments_backfill_start_date,
                    "end_date": comments_backfill_end_date,
                },
            )
            go_button_2 = False


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
