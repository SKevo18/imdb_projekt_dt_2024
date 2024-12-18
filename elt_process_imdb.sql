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
    startDate DATE,
    endDate DATE,
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

-- extract - použil som vlastný skript: extract/nahrat.sh

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

-- load a transform
CREATE SCHEMA IF NOT EXISTS HEDGEHOG_IMDB.star;
USE SCHEMA HEDGEHOG_IMDB.star;

CREATE OR REPLACE TABLE dim_date AS
SELECT
    ROW_NUMBER() OVER (ORDER BY date) AS dim_date_id,
    date,
    EXTRACT(DAY FROM date) AS day,
    EXTRACT(MONTH FROM date) AS month,
    EXTRACT(YEAR FROM date) AS year,
    EXTRACT(QUARTER FROM date) AS quarter,
    FLOOR(EXTRACT(YEAR FROM date) / 10) * 10 % 100 AS decade,
    FLOOR(EXTRACT(YEAR FROM date) / 100) + 1 AS century,
    TO_CHAR(date, 'Mon') AS monthStr,
    CONCAT(FLOOR(EXTRACT(YEAR FROM date) / 10) * 10 % 100, 's') AS decadeStr,
    CONCAT(FLOOR(EXTRACT(YEAR FROM date) / 100) + 1, '. century') AS centuryStr
FROM (
    SELECT DISTINCT TO_DATE(timestamp) AS date FROM staging.title_ratings
    UNION
    SELECT DISTINCT startDate AS date FROM staging.title_basics
    UNION
    SELECT DISTINCT endDate AS date FROM staging.title_basics
);

CREATE OR REPLACE TABLE dim_time AS
SELECT DISTINCT
    ROW_NUMBER() OVER (ORDER BY timestamp) AS dim_time_id,
    TO_TIME(timestamp) AS time,
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

CREATE OR REPLACE TABLE dim_title_names AS
SELECT DISTINCT
    dn.dim_name_id,
    dt.dim_title_id
FROM (
    SELECT nb.nconst, TRIM(title.value) AS tconst
    FROM staging.name_basics nb,
         LATERAL FLATTEN(INPUT => SPLIT(nb.knownForTitles, ',')) title
) par
JOIN dim_names dn ON dn.nconst = par.nconst
JOIN dim_titles dt ON dt.tconst = par.tconst;

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
    CASE 
        WHEN staging.title_basics.titleType = 'tvEpisode' THEN staging.title_episode.episodeNumber
        ELSE NULL
    END AS episodeNumber,
    CASE 
        WHEN staging.title_basics.titleType = 'tvEpisode' THEN staging.title_episode.seasonNumber
        ELSE NULL
    END AS seasonNumber,
    dim_titles.dim_title_id,
    dim_titleStartDate.dim_date_id AS dim_titleStartDate_id,
    dim_titleEndDate.dim_date_id AS dim_titleEndDate_id,
    dim_postedTime.dim_time_id AS dim_postedTime_id,
    dim_postedDate.dim_date_id AS dim_postedDate_id
FROM staging.title_ratings AS ratings
LEFT JOIN staging.title_episode ON ratings.tconst = title_episode.tconst
JOIN staging.title_principals ON ratings.tconst = title_principals.tconst
JOIN staging.title_basics ON ratings.tconst = staging.title_basics.tconst
JOIN dim_titles ON ratings.tconst = dim_titles.tconst
JOIN dim_time dim_postedTime ON TO_TIME(ratings.timestamp) = dim_postedTime.time
JOIN dim_date dim_postedDate ON TO_DATE(ratings.timestamp) = dim_postedDate.date
JOIN dim_date dim_titleStartDate ON staging.title_basics.startDate = dim_titleStartDate.date
LEFT JOIN dim_date dim_titleEndDate ON staging.title_basics.endDate = dim_titleEndDate.date
WHERE staging.title_basics.runtimeMinutes IS NOT NULL;

DROP TABLE IF EXISTS staging.title_basics;
DROP TABLE IF EXISTS staging.title_akas;
DROP TABLE IF EXISTS staging.title_crew;
DROP TABLE IF EXISTS staging.title_episode;
DROP TABLE IF EXISTS staging.title_principals;
DROP TABLE IF EXISTS staging.title_ratings;
DROP TABLE IF EXISTS staging.name_basics;
