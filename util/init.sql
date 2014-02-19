DROP DATABASE IF EXISTS simpledoge;
DROP DATABASE IF EXISTS simpledoge_testing;
CREATE USER simpledoge WITH PASSWORD 'testing';
CREATE DATABASE simpledoge;
GRANT ALL PRIVILEGES ON DATABASE simpledoge to simpledoge;
-- Create a testing database to be different than dev
CREATE DATABASE simpledoge_testing;
GRANT ALL PRIVILEGES ON DATABASE simpledoge to simpledoge;
