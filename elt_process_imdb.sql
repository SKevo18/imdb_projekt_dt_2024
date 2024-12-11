CREATE DATABASE IF NOT EXISTS HEDGEHOG_IMDB;
USE DATABASE HEDGEHOG_IMDB;

CREATE WAREHOUSE IF NOT EXISTS HEDGEHOG_IMDB_WH;
USE WAREHOUSE HEDGEHOG_IMDB_WH;

-- staging
CREATE SCHEMA IF NOT EXISTS HEDGEHOG_IMDB.staging;
CREATE OR REPLACE TABLE staging.title_basics (
    tconst VARCHAR(15) PRIMARY KEY,
    titleType VARCHAR(40),
    primaryTitle VARCHAR(255),
    originalTitle VARCHAR(255),
    isAdult BOOLEAN,
    startYear DATE,
    endYear DATE,
    runtimeMinutes INTEGER,
    genres VARCHAR(255),
    lastUpdate TIMESTAMP
);

CREATE OR REPLACE TABLE staging.title_akas (
    titleId VARCHAR(15) PRIMARY KEY,
    ordering INTEGER,
    title VARCHAR(255),
    region VARCHAR(100),
    language VARCHAR(100),
    types VARCHAR(255),
    attributes VARCHAR(255),
    isOriginalTitle BOOLEAN,
    FOREIGN KEY (titleId) REFERENCES staging.title_basics(tconst)
);

CREATE OR REPLACE TABLE staging.title_crew (
    tconst VARCHAR(15) PRIMARY KEY,
    directors TEXT,
    writers TEXT,
    FOREIGN KEY (tconst) REFERENCES staging.title_basics(tconst)
);

CREATE OR REPLACE TABLE staging.title_episode (
    tconst VARCHAR(15) PRIMARY KEY,
    parentTconst VARCHAR(15),
    seasonNumber INTEGER,
    episodeNumber INTEGER,
    FOREIGN KEY (tconst) REFERENCES staging.title_basics(tconst),
    FOREIGN KEY (parentTconst) REFERENCES staging.title_basics(tconst)
);

CREATE OR REPLACE TABLE staging.title_ratings (
    tconst VARCHAR(15),
    rating FLOAT,
    timestamp TIMESTAMP,
    FOREIGN KEY (tconst) REFERENCES staging.title_basics(tconst)
);

CREATE OR REPLACE TABLE staging.name_basics (
    nconst VARCHAR(15) PRIMARY KEY,
    primaryName VARCHAR(255),
    birthYear INTEGER,
    deathYear INTEGER,
    primaryProfession VARCHAR(255),
    knownForTitles VARCHAR(255)
);

CREATE OR REPLACE TABLE staging.title_principals (
    tconst VARCHAR(15) PRIMARY KEY,
    ordering INTEGER,
    nconst VARCHAR(15),
    category VARCHAR(40),
    job VARCHAR(255),
    characters VARCHAR(255),
    FOREIGN KEY (nconst) REFERENCES staging.name_basics(nconst)
);

CREATE OR REPLACE FILE FORMAT TSV_FORMAT
    TYPE = 'CSV'
    SKIP_HEADER = 1
    FIELD_DELIMITER = '\t' RECORD_DELIMITER = '\n'
    ERROR_ON_COLUMN_COUNT_MISMATCH = TRUE
    NULL_IF = ('\\N');

CREATE STAGE IF NOT EXISTS HEDGEHOG_IMDB.STAGING.IMDB_STAGE;
-- extract/nahrat.sh
LIST @HEDGEHOG_IMDB.STAGING.IMDB_STAGE/;
/*
name	                            size	    md5	                                last_modified
imdb_stage/title.principals.tsv.gz	693215888	46c6714c64061f82db048802cc754c87-83	Fri, 6 Dec 2024 18:41:11 GMT
imdb_stage/title.akas.tsv.gz	    438905200	ff6b9daa81caf8618cd45581e8624e22-53	Fri, 6 Dec 2024 18:08:54 GMT
imdb_stage/name.basics.tsv.gz	    276461856	72c1ac5d83f400ed350e58d7e4136872-33	Fri, 6 Dec 2024 19:08:53 GMT
imdb_stage/title.basics.tsv.gz	    198383712	9c73dce33351e5499f8d8d220a1028d0	Fri, 6 Dec 2024 18:26:55 GMT
imdb_stage/title.crew.tsv.gz	    73586400	4bbc9168a71b7e8a70c41731089dc216	Fri, 6 Dec 2024 18:35:54 GMT
imdb_stage/title.episode.tsv.gz	    47741904	614030ad20eaef6c4bcbb9a11b4a5125	Fri, 6 Dec 2024 18:39:24 GMT
imdb_stage/title.ratings.tsv.gz	    7583120	    883f2bb5e9ca543a35b4d220f8cb036c-2	Fri, 6 Dec 2024 17:45:16 GMT
*/

COPY INTO HEDGEHOG_IMDB.STAGING.title_basics
FROM @HEDGEHOG_IMDB.STAGING.IMDB_STAGE/title.basics.tsv.gz
FILE_FORMAT = TSV_FORMAT
ON_ERROR = 'CONTINUE';
/*
file	                         status	             rows_parsed rows_loaded error_limit errors_seen first_error	                                              first_error_line	first_error_character	first_error_column_name
imdb_stage/title.basics.tsv.gz	PARTIALLY_LOADED	11286007	11285932	11286007	75	        User character length limit (255) exceeded by string '... '	2613256	         18	                    "TITLE_BASICS"["PRIMARYTITLE":3]
*/

COPY INTO HEDGEHOG_IMDB.STAGING.title_akas
FROM @HEDGEHOG_IMDB.STAGING.IMDB_STAGE/title.akas.tsv.gz
FILE_FORMAT = TSV_FORMAT
ON_ERROR = 'CONTINUE';
/*
file	                        status	         rows_parsed rows_loaded error_limit errors_seen first_error	                                                                                                                                                  first_error_line	first_error_character	first_error_column_name
imdb_stage/title.akas.tsv.gz   PARTIALLY_LOADED	50714199	50713875	50714199	324	        User character length limit (255) exceeded by string '...'	397314	         13	                    "TITLE_AKAS"["TITLE":3]
*/

COPY INTO HEDGEHOG_IMDB.STAGING.title_crew
FROM @HEDGEHOG_IMDB.STAGING.IMDB_STAGE/title.crew.tsv.gz
FILE_FORMAT = TSV_FORMAT
ON_ERROR = 'CONTINUE';
/*
file	status	rows_parsed	rows_loaded	 error_limit errors_seen first_error	first_error_line	first_error_character	first_error_column_name
imdb_stage/title.crew.tsv.gz	LOADED	10618821	10618821	1	          0				
*/

COPY INTO HEDGEHOG_IMDB.STAGING.title_episode
FROM @HEDGEHOG_IMDB.STAGING.IMDB_STAGE/title.episode.tsv.gz
FILE_FORMAT = TSV_FORMAT
ON_ERROR = 'CONTINUE';
/*
file	                         status	 rows_parsed rows_loaded	error_limit	errors_seen   first_error   first_error_line   first_error_character   first_error_column_name
imdb_stage/title.episode.tsv.gz	LOADED	8670360	    8670360	       1	       0				
*/

COPY INTO HEDGEHOG_IMDB.STAGING.title_principals
FROM @HEDGEHOG_IMDB.STAGING.IMDB_STAGE/title.principals.tsv.gz
FILE_FORMAT = TSV_FORMAT
ON_ERROR = 'CONTINUE';
/*
file	                             status	             rows_parsed rows_loaded error_limit errors_seen first_error	                                                                                                                                                  first_error_line	first_error_character	first_error_column_name
imdb_stage/title.principals.tsv.gz	PARTIALLY_LOADED	89576330	89576296	89576330	34	        User character length limit (255) exceeded by string '["Self (segment: \"...: \"The Fens,'	64689006	     31	                    "TITLE_PRINCIPALS"["CHARACTERS":6]
*/

COPY INTO HEDGEHOG_IMDB.STAGING.title_ratings
FROM @HEDGEHOG_IMDB.STAGING.IMDB_STAGE/title.ratings.tsv.gz
FILE_FORMAT = TSV_FORMAT
ON_ERROR = 'CONTINUE';
/*
file	                         status	 rows_parsed rows_loaded error_limit	errors_seen	first_error	first_error_line	first_error_character	first_error_column_name
imdb_stage/title.ratings.tsv.gz	LOADED	11370191	    11370191     1	           0				
*/

COPY INTO HEDGEHOG_IMDB.STAGING.name_basics
FROM @HEDGEHOG_IMDB.STAGING.IMDB_STAGE/name.basics.tsv.gz
FILE_FORMAT = TSV_FORMAT
ON_ERROR = 'CONTINUE';
/*
file	                         status	 rows_parsed rows_loaded error_limit	errors_seen	first_error	first_error_line	first_error_character	first_error_column_name
imdb_stage/name.basics.tsv.gz	LOADED	14001033	14001033	1	           0				
*/

-- load - hviezdicova schema
CREATE SCHEMA IF NOT EXISTS HEDGEHOG_IMDB.star;
USE SCHEMA HEDGEHOG_IMDB.star;

CREATE OR REPLACE TABLE dim_date AS
SELECT DISTINCT
    ROW_NUMBER() OVER (ORDER BY timestamp) AS dim_date_id,
    TO_DATE(timestamp) AS date,
    EXTRACT(DAY FROM timestamp) AS day,
    EXTRACT(MONTH FROM timestamp) AS month,
    EXTRACT(YEAR FROM timestamp) AS year,
    EXTRACT(QUARTER FROM timestamp) AS quarter,
    FLOOR(year / 10) * 10 % 100 AS decade,
    FLOOR(year / 100) + 1 AS century,
    TO_CHAR(timestamp, 'Mon') AS monthStr,
    CONCAT(FLOOR(year / 10) * 10 % 100, 's') AS decadeStr,
    CONCAT(FLOOR(year / 100) + 1, '. century') AS centuryStr
FROM staging.title_ratings;

CREATE OR REPLACE TABLE dim_time AS
SELECT DISTINCT
    ROW_NUMBER() OVER (ORDER BY timestamp) AS dim_time_id,
    timestamp,
    EXTRACT(HOUR FROM timestamp) AS hour,
    EXTRACT(MINUTE FROM timestamp) AS minute
FROM staging.title_ratings;

CREATE OR REPLACE TABLE dim_titles AS
SELECT DISTINCT
    ROW_NUMBER() OVER (ORDER BY tb.tconst) AS dim_title_id,
    tb.tconst,
    tb.titleType,
    tb.originalTitle,
    tb.genres,
    CASE
        WHEN tb.isAdult THEN '18+'
        ELSE 'PG'
    END AS rating,
    CASE 
        WHEN te.tconst IS NOT NULL THEN 'S' || te.seasonNumber || 'E' || te.episodeNumber
        ELSE NULL
    END AS episodeTitle,
    CASE 
        WHEN te.tconst IS NOT NULL THEN parent_tb.originalTitle
        ELSE NULL
    END AS seriesTitle
FROM staging.title_basics tb
LEFT JOIN staging.title_episode te ON te.tconst = tb.tconst
LEFT JOIN staging.title_basics parent_tb ON parent_tb.tconst = te.parentTconst
WHERE tb.titleType
    IN ('movie', 'tvSeries') OR
    (tb.titleType = 'tvEpisode' AND te.tconst IS NOT NULL);

CREATE OR REPLACE TABLE dim_names AS
SELECT DISTINCT
    ROW_NUMBER() OVER (ORDER BY nconst) AS dim_name_id,
    nconst,
    primaryName,
    CAST(birthYear AS VARCHAR(5)) AS birthYear,
    CAST(deathYear AS VARCHAR(5)) AS deathYear,
    primaryProfession
FROM staging.name_basics;

CREATE OR REPLACE TABLE dim_akas AS
SELECT DISTINCT
    ROW_NUMBER() OVER (ORDER BY titleId) AS dim_akas_id,
    titleId,
    title,
    region,
    language,
    types
FROM staging.title_akas;

CREATE OR REPLACE TABLE fact_ratings AS
SELECT DISTINCT
    ROW_NUMBER() OVER (ORDER BY ratings.timestamp) AS fact_rating_id,
    ratings.rating,
    staging.title_basics.runtimeMinutes AS titleRuntimeMinutes,
    staging.title_basics.startYear AS titleStartYear,
    staging.title_basics.endYear AS titleEndYear,
    staging.title_episode.episodeNumber,
    staging.title_episode.seasonNumber,
    dim_titles.dim_title_id,
    dim_names.dim_name_id,
    dim_akas.dim_akas_id,
    dim_time.dim_time_id,
    dim_date.dim_date_id
FROM staging.title_ratings ratings
LEFT JOIN staging.title_episode ON ratings.tconst = title_episode.tconst
LEFT JOIN staging.title_principals ON ratings.tconst = title_principals.tconst
LEFT JOIN staging.title_basics ON ratings.tconst = staging.title_basics.tconst
LEFT JOIN dim_titles ON ratings.tconst = dim_titles.tconst
LEFT JOIN dim_names ON title_principals.nconst = dim_names.nconst
LEFT JOIN dim_akas ON ratings.tconst = dim_akas.titleId
LEFT JOIN dim_time ON ratings.timestamp = dim_time.timestamp
LEFT JOIN dim_date ON TO_DATE(ratings.timestamp) = dim_date.date;
