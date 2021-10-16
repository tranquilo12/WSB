# Retard Strength

## DB notes

### Postgres (within AWS EC2 + TimeScaleDB)

- Tables include: 
  - submissions (hypertable, indexed on id and created_utc).
  - comments (hypertable, indexed on submission_id, id, and created_utc).
  - submission_comments_analytics (materialized view) -> Shows the date, submission_id and all the comments found for
    that submission_id, in `comments`.
  - submission_status (materialized view) -> Shows the date, and all submissions found for that date in `submissions`.

### ETL

- The solution to all the wsb submissions after going through PRAW, `pushshift.io`.
- Via `pushshift.io`, submissions, and their comments are searchable through their endpoints.
- Uses Prefect for executing the ETL pipeline. 
- The first Prefect flow executes the following for updating submissions: ![Updates Submissions](src/UpdateSubmissions.pdf).
- The second Prefect flow executes the following for updating comments, from the submissions found: ![Updates Comments](src/UpdateComments.pdf)

#### Run a prefect flow for submissions

- From scratch, you need to first establish the current computer you're running the flow, as the "server" in the prefect
  flow backend. That's achieved by the command `prefect backend server`.
- Now we need to start the prefect server, and the agent that will control/schedule the automations.
  - Start the prefect server by stating `prefect server start`.
  - Local agents can be started by `prefect agent local start`.
- Create a prefect project, like: `prefect create project <NAME>`.
