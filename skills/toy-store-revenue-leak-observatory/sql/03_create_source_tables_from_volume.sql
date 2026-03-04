-- Optional: run this only if the raw Maven source tables do not already exist.
-- Assumes the raw CSV files are uploaded to a volume folder such as:
-- /Volumes/chris_demos/demos/ai_data_raw/maven-fuzzy-factory/raw/

CREATE SCHEMA IF NOT EXISTS chris_demos.demos;

CREATE OR REPLACE TABLE chris_demos.demos.website_sessions AS
SELECT *
FROM read_files(
  '/Volumes/chris_demos/demos/ai_data_raw/maven-fuzzy-factory/raw/website_sessions.csv',
  format => 'csv',
  header => true,
  inferSchema => true
);

CREATE OR REPLACE TABLE chris_demos.demos.website_pageviews AS
SELECT *
FROM read_files(
  '/Volumes/chris_demos/demos/ai_data_raw/maven-fuzzy-factory/raw/website_pageviews.csv',
  format => 'csv',
  header => true,
  inferSchema => true
);

CREATE OR REPLACE TABLE chris_demos.demos.orders AS
SELECT *
FROM read_files(
  '/Volumes/chris_demos/demos/ai_data_raw/maven-fuzzy-factory/raw/orders.csv',
  format => 'csv',
  header => true,
  inferSchema => true
);

CREATE OR REPLACE TABLE chris_demos.demos.order_items AS
SELECT *
FROM read_files(
  '/Volumes/chris_demos/demos/ai_data_raw/maven-fuzzy-factory/raw/order_items.csv',
  format => 'csv',
  header => true,
  inferSchema => true
);

CREATE OR REPLACE TABLE chris_demos.demos.order_item_refunds AS
SELECT *
FROM read_files(
  '/Volumes/chris_demos/demos/ai_data_raw/maven-fuzzy-factory/raw/order_item_refunds.csv',
  format => 'csv',
  header => true,
  inferSchema => true
);

CREATE OR REPLACE TABLE chris_demos.demos.products AS
SELECT *
FROM read_files(
  '/Volumes/chris_demos/demos/ai_data_raw/maven-fuzzy-factory/raw/products.csv',
  format => 'csv',
  header => true,
  inferSchema => true
);

CREATE OR REPLACE TABLE chris_demos.demos.maven_fuzzy_factory_data_dictionary AS
SELECT *
FROM read_files(
  '/Volumes/chris_demos/demos/ai_data_raw/maven-fuzzy-factory/raw/maven_fuzzy_factory_data_dictionary.csv',
  format => 'csv',
  header => true,
  inferSchema => true
);
