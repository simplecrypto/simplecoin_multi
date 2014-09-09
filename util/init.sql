CREATE USER simplemulti WITH PASSWORD 'testing';
CREATE USER simplemulti_ro WITH PASSWORD 'testing';
-- Production database
DROP DATABASE IF EXISTS simplemulti;
CREATE DATABASE simplemulti;
GRANT ALL PRIVILEGES ON DATABASE simplemulti to simplemulti;
GRANT ALL PRIVILEGES ON DATABASE simplemulti to simplemulti_ro;
-- Create a testing database to be different than dev
DROP DATABASE IF EXISTS simplemulti_testing;
CREATE DATABASE simplemulti_testing;
GRANT ALL PRIVILEGES ON DATABASE simplemulti_testing to simplemulti;
GRANT ALL PRIVILEGES ON DATABASE simplemulti_testing to simplemulti_ro;
