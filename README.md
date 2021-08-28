# Retard Strength

## DB notes

### Postgres (within AWS EC2 + TimeScaleDB)

- Tables include: 
  - wsb_submissions (hypertable, indexed on id and created_utc)
  - wsb_comments (hypertable, indexed on submission_id, id, and created_utc)
  - wsb_comments_analytics (view)
  - wsb_comments_status
  - wsb_comments_temp
  - wsb_comments_tickers