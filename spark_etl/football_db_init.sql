-- Create the dedicated analytics database
CREATE DATABASE football_db;

-- Create a schema to separate these layers from the public schema
CREATE SCHEMA football;

CREATE TABLE football.bronze_football_events (
    event_id   VARCHAR(36),
    match_id   VARCHAR(50),
    team       VARCHAR(100),
    player     VARCHAR(100),
    event_type VARCHAR(50),
    timestamp  TIMESTAMP
);

CREATE TABLE football.silver_football_events (
    match_id   VARCHAR(50),
    team       VARCHAR(100),
    event_type VARCHAR(50),
    timestamp  TIMESTAMP
);

CREATE TABLE football.gold_football_stats (
    team         VARCHAR(100) PRIMARY KEY,
    total_goals  INT DEFAULT 0,
    total_fouls  INT DEFAULT 0,
    total_shots  INT DEFAULT 0,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
