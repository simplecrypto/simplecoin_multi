DROP DATABASE IF EXISTS simplecoin;
DROP DATABASE IF EXISTS simplecoin_testing;
CREATE USER simplecoin WITH PASSWORD 'testing';
CREATE DATABASE simplecoin;
GRANT ALL PRIVILEGES ON DATABASE simplecoin to simplecoin;
-- Create a testing database to be different than dev
CREATE DATABASE simplecoin_testing;
GRANT ALL PRIVILEGES ON DATABASE simplecoin to simplecoin;
\c simplecoin
CREATE EXTENSION hstore;
\c simplecoin_testing
CREATE EXTENSION hstore;
