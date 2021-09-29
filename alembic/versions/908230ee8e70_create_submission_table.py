"""create submission table

Revision ID: 908230ee8e70
Revises: 
Create Date: 2021-08-28 13:37:52.184783

"""
import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = '908230ee8e70'
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "submissions",
        sa.Column("created_utc", sa.BIGINT),
        sa.Column("id", sa.TEXT),
        sa.Column("author", sa.TEXT),
        sa.Column("url", sa.TEXT),
        sa.Column("title", sa.TEXT),
        sa.Column("selftext", sa.TEXT),
        sa.Column("stickied", sa.TEXT)
    )

    op.create_table(
        "comments",
        sa.Column("created_utc", sa.BIGINT),
        sa.Column("retrieved_on", sa.BIGINT),
        sa.Column("id", sa.TEXT),
        sa.Column("parent_id", sa.TEXT),
        sa.Column("link_id", sa.TEXT),
        sa.Column("author", sa.TEXT),
        sa.Column("submission_id", sa.TEXT),
        sa.Column("body", sa.TEXT),
        sa.Column("subreddit", sa.TEXT),
    )


def downgrade():
    op.drop_table("submissions")
    op.drop_table("comments")
