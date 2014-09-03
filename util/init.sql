CREATE USER simplecoin WITH PASSWORD 'testing';
-- Production database
DROP DATABASE IF EXISTS simplecoin;
CREATE DATABASE simplecoin;
GRANT ALL PRIVILEGES ON DATABASE simplecoin to simplecoin;
-- Create a testing database to be different than dev
DROP DATABASE IF EXISTS simplecoin_testing;
CREATE DATABASE simplecoin_testing;
GRANT ALL PRIVILEGES ON DATABASE simplecoin_testing to simplecoin;
