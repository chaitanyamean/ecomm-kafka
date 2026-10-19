-- Runs once on first Postgres startup (when the data volume is empty).
-- Creates the three per-service databases.
CREATE DATABASE orders;
CREATE DATABASE inventory;
CREATE DATABASE payment_db;
