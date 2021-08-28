SELECT COUNT(DISTINCT (submission_id))
from wsb_comments;

CREATE TABLE IF NOT EXISTS wsb_comments_status
(
    insert_date timestamp without time zone,
    submission_ids_completed text[]
);

INSERT INTO wsb_comments_status
SELECT CURRENT_DATE, array_agg(t.submission_id) as submission_ids_completed
FROM wsb_comments t;

CREATE MATERIALIZED VIEW wsb_submission_comments AS
SELECT CURRENT_DATE, t.submission_id, array_agg(t.id) as comment_ids
FROM wsb_comments t
group by t.submission_id;

CREATE UNIQUE INDEX wsb_submission_comments_rel_uindex ON wsb_submission_comments (submission_id);

ALTER TABLE wsb_comments
ALTER COLUMN approved_at_utc TYPE BIGINT;

ALTER TABLE wsb_comments
ALTER COLUMN comment_type TYPE BIGINT;

ALTER TABLE wsb_comments
ALTER COLUMN likes TYPE BIGINT;

ALTER TABLE wsb_comments
ALTER COLUMN ups TYPE BIGINT;

ALTER TABLE wsb_comments
ALTER COLUMN banned_at_utc TYPE BIGINT;

ALTER TABLE wsb_comments
ALTER COLUMN gilded TYPE BIGINT;

ALTER TABLE wsb_comments
ALTER COLUMN score TYPE BIGINT;

ALTER TABLE wsb_comments
ALTER COLUMN edited TYPE BIGINT;

ALTER TABLE wsb_comments
ALTER COLUMN downs TYPE BIGINT;

ALTER TABLE wsb_comments
ALTER COLUMN num_reports TYPE BIGINT;

ALTER TABLE wsb_comments
ALTER COLUMN created TYPE BIGINT;

ALTER TABLE wsb_comments
ALTER COLUMN created_utc TYPE BIGINT;

ALTER TABLE wsb_comments
ALTER COLUMN controversiality TYPE BIGINT;

ALTER TABLE wsb_comments
ALTER COLUMN depth TYPE BIGINT;

CREATE TABLE wsb_comments_analytics AS
    SELECT to_timestamp(t.created_utc) as created_utc, t.submission_id, t.parent_id, t.id as comment_id, t.author, t.body, t.likes, t.ups, t.downs, t.controversiality
    FROM wsb_comments t
    ORDER BY created_utc;

CREATE TABLE wsb_comments_analytics (
    created_utc timestamp without time zone,
    submission_id text,
    parent_id text,
    comment_id text,
    author text,
    body text
);

SELECT create_hypertable('wsb_comments_analytics', 'created_utc', 'comment_id', number_partitions := 10);
INSERT INTO wsb_comments_analytics
SELECT to_timestamp(t.created_utc) as created_utc, t.submission_id, t.parent_id, t.id as comment_id, t.author, t.body
FROM wsb_comments t
ORDER BY created_utc;

SELECT min(created_utc), max(created_utc) FROM wsb_comments_analytics;

SELECT string_to_array(body, ' ') AS comm_array
FROM wsb_comments_analytics
WHERE created_utc::date = '2021-06-30';


CREATE OR REPLACE FUNCTION find_tickers_in_comments(tickers text[], commentArr text[])
RETURNS text[] AS $$
DECLARE
    tickersInComments text[];
    t text;
BEGIN
    FOREACH t IN ARRAY tickers
        LOOP
            IF t = any(commentArr) THEN
                tickersInComments := array_append(tickersInComments, t);
            END IF;
        END LOOP;
    RETURN tickersInComments;
end;
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION find_tickers_in_comments_within_date(DateStr text)
RETURNS text[] as
$$
    DECLARE
        tickers text[];
        tickersInComments text[];
        commentArr text[];
        allTickerInComments text[][];
        id text;
    BEGIN
        SELECT array_agg(ticker) FROM ticker_vxes WHERE market = 'stocks' INTO tickers;

        FOR id, commentArr in
            SELECT comment_id, string_to_array(body, ' ')
            FROM wsb_comments_analytics
            WHERE created_utc::date = DateStr::date
        LOOP
            tickersInComments := find_tickers_in_comments(tickers := tickers, commentArr := commentArr);
            allTickerInComments := array_append(allTickerInComments, array[id, tickersInComments]);
        end loop;
    RETURN allTickerInComments;
    end;
$$
LANGUAGE plpgsql;

SELECT * FROM find_tickers_in_comments_within_date('2020-06-30');

DROP TABLE wsb_comments_analytics_temp;
CREATE TABLE wsb_comments_analytics_temp AS
SELECT * FROM wsb_comments_analytics WHERE created_utc::date = '2020-06-30';
SELECT Count(1) FROM wsb_comments;


SELECT count(1) FROM ticker_details WHERE ticker_details is null;
DELETE FROM ticker_details WHERE ticker_details is null;


ALTER TABLE wsb_comments_analytics_tickers_found
ALTER COLUMN tickers TYPE text;

ALTER TABLE wsb_comments_analytics_tickers_found
    ALTER COLUMN tickers TYPE text[] USING string_to_array(tickers, ', ');

VACUUM wsb_comments;
VACUUM wsb_comments_analytics;
VACUUM wsb_comments_status;

-- Stage 1, ensure

-- Stage 2
DROP table if exists wsb_comments_analytics_temp;
CREATE TABLE wsb_comments_analytics_temp AS (
    SELECT t1.created_utc, t1.submission_id, t1.parent_id, t1.comment_id, t1.author, t1.body, t2.tickers FROM wsb_comments_analytics t1
    LEFT JOIN
    wsb_comments_analytics_tickers_found t2 on t1.comment_id = t2.comment_id
);

-- Final stage
TRUNCATE wsb_comments_analytics;
DROP TABLE wsb_comments_analytics;
ALTER TABLE wsb_comments_analytics_temp RENAME TO wsb_comments_analytics;
SELECT create_hypertable('wsb_comments_analytics', 'created_utc', 'comment_id', number_partitions := 10, migrate_data := TRUE);
DROP TABLE IF EXISTS wsb_comments_analytics_tickers_found;

SELECT tbl1.id FROM wsb_comments tbl1 WHERE NOT EXISTS (SELECT FROM wsb_comments_analytics tbl2 WHERE tbl2.comment_id = tbl1.id);

SELECT tb1.id FROM wsb_comments tb1 WHERE (tb1.id) NOT IN (SELECT comment_id FROM wsb_comments_analytics);

SELECT to_timestamp(created_utc), submission_id, id from wsb_comments;

SELECT submission_id, count(comment_id) FROM wsb_comments_analytics
GROUP BY submission_id;

-- EXPLAIN ANALYSE
SELECT tb1.id FROM wsb_comments tb1
LEFT JOIN wsb_comments_analytics tb2 on tb1.id = tb2.comment_id
WHERE tb2.comment_id is NULL;

INSERT INTO wsb_comments SELECT * from wsb_comments_temp;

TRUNCATE TABLE wsb_comments;
DROP TABLE wsb_comments CASCADE ;
ALTER TABLE wsb_comments_temp RENAME TO wsb_comments;


CREATE TABLE IF NOT EXISTS wsb_comments_temp AS TABLE wsb_comments WITH NO DATA;
CREATE TABLE wsb_comments_temp (LIKE wsb_comments INCLUDING ALL);

VACUUM wsb_comments_temp;
VACUUM wsb_comments;

SELECT count(1) FROM wsb_comments_temp;
SELECT reltuples AS estimate FROM pg_class where relname = 'wsb_comments_temp';
SELECT reltuples AS estimate FROM pg_class where relname = 'wsb_comments';


INSERT INTO wsb_comments (SELECT * FROM wsb_comments_temp t WHERE t.submission_id is not Null);

SELECT count(1) FROM wsb_comments t WHERE t.submission_id is NULL;

TRUNCATE wsb_comments_temp ;
TRUNCATE wsb_comments;

INSERT INTO wsb_comments_temp (SELECT * FROM wsb_comments t WHERE t.submission_id is not Null);

SELECT created_utc from wsb_submissions