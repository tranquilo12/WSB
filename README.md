# Retard Strength

## DB notes

### Postgres (within AWS EC2 + TimeScaleDB)

- Tables include: 
  - submissions (hypertable, indexed on id and created_utc)
  - comments (hypertable, indexed on submission_id, id, and created_utc)
  - submission_comments_analytics (materialized view) -> Shows the date, submission_id and all the comments found for
    that submission_id, in `comments`
  - submission_status (materialized view) -> Shows the date, and all submissions found for that date in `submissions`
  
