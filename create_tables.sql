-- Compression types are left out of definitions,
-- assuming that the initial ingest has sufficient data to let Redshift automatically decide them.
-- Otherwise we should define them explicitly.
-- Typically one would use bytedict for categorical or low cardinality fields,
-- lzo for high cardinality string fields,
-- mostly encodings for numerical fields with distributions relatively close to zero.

-- Redshift is for highly structured data,
-- so the embedded JSON and list payloads must be broken out properly into separate tables.
-- This transformation is done in business logic on ingest.

-- sortkeys are generally timestamps except for dimension tables
-- distkeys should be selected on fields which are commonly joined or ALL for certain dimension tables

CREATE SCHEMA sailthru

CREATE TABLE sailthru.blast
(
  day TIMESTAMP NOT NULL sortkey distkey,
  name VARCHAR(256) NOT NULL,
  id   BIGINT NOT NULL
);

CREATE TABLE sailthru.message_blast
(
  profile_id VARCHAR(24) NOT NULL,
  send_time TIMESTAMP NOT NULL sortkey,
  blast_id BIGINT NOT NULL distkey,
  message_id BIGINT NOT NULL,
  device VARCHAR(256),
  message_revenue DECIMAL(10, 2), -- assuming US DOLLARS AND CENTS since currency is not provided as a field, otherwise float may be better
  delivery_status VARCHAR(256), -- assuming non-boolean status
  action_name VARCHAR(32),
  action_ts TIMESTAMP,
  action_url VARCHAR(2048),
);

-- Depending on performance/storage optimization,
-- it could be better to separate data into tables per event type
-- in which case you may have these tables
-- Same goes for message_transactional table

-- CREATE TABLE sailthru.message_blast_open
-- (
--   profile_id VARCHAR(24) NOT NULL,
--   blast_id BIGINT NOT NULL,
--   ts TIMESTAMP NOT NULL,
-- );
--
-- CREATE TABLE sailthru.message_blast_click
-- (
--   profile_id VARCHAR(24) NOT NULL,
--   blast_id BIGINT NOT NULL,
--   ts TIMESTAMP NOT NULL,
--   url VARCHAR(2048)
-- );

CREATE TABLE sailthru.message_transactional
(
  profile_id VARCHAR(24) NOT NULL distkey,
  template VARCHAR(256) NOT NULL,
  send_time TIMESTAMP NOT NULL sortkey,
  id VARCHAR(32) NOT NULL,
  device VARCHAR(256),
  message_revenue DECIMAL(10, 2), -- assuming US DOLLARS AND CENTS since currency is not provided as a field, otherwise float may be better
  delivery_status VARCHAR(256), -- assuming non-boolean status
  action_name VARCHAR(32),
  action_ts TIMESTAMP,
  action_url VARCHAR(2048),
);

CREATE TABLE sailthru.profile
(
  id VARCHAR(24) NOT NULL distkey,
  email VARCHAR(256),
  geo_count INT,
  optout_reason VARCHAR(256),
  optout_time TIMESTAMP,
  signup_time TIMESTAMP,
  create_date TIMESTAMP sortkey,
  total_opens INT,
  total_clicks INT,
  total_unique_opens INT,
  total_unique_clicks INT,
  total_unique_opens INT,
  total_pageviews INT,
  total_messages INT,
  last_open TIMESTAMP,
  last_click TIMESTAMP,
  last_pageview TIMESTAMP,
--   export_ts TIMESTAMP, -- consider export timestamp for profile time series
);

CREATE TABLE sailthru.profile_geo
(
  id VARCHAR(24) NOT NULL sortkey distkey,
  scope VARCHAR(24) NOT NULL,
  label VARCHAR(24) NOT NULL,
  quantity INT NOT NULL
--   export_ts TIMESTAMP, -- consider export timestamp for profile time series
);

CREATE TABLE sailthru.profile_browser
(
  id VARCHAR(24) NOT NULL sortkey distkey,
  browser VARCHAR(24) NOT NULL,
  quantity INT NOT NULL
--   export_ts TIMESTAMP, -- consider export timestamp for profile time series
);

CREATE TABLE sailthru.profile_lists_signup
(
  id VARCHAR(24) NOT NULL sortkey distkey,
  list VARCHAR(256) NOT NULL,
  ts TIMESTAMP NOT NULL
--   export_ts TIMESTAMP, -- consider export timestamp for profile time series
);

CREATE TABLE sailthru.profile_vars
(
  id VARCHAR(24) NOT NULL sortkey distkey,
  var VARCHAR(256) NOT NULL,
  val varchar(32) -- ints or strings in raw data
--   export_ts TIMESTAMP, -- consider export timestamp for profile time series
);