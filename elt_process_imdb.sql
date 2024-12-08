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
    startYear INTEGER,
    endYear INTEGER,
    runtimeMinutes INTEGER,
    genres VARCHAR(255)
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
    tconst VARCHAR(15) PRIMARY KEY,
    averageRating FLOAT,
    numVotes INTEGER,
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

CREATE OR REPLACE STAGE HEDGEHOG_IMDB.STAGING.IMDB_STAGE;
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
imdb_stage/title.akas.tsv.gz   PARTIALLY_LOADED	50714199	50713875	50714199	324	        User character length limit (255) exceeded by string 'Vera historia de la primera fundación de Buenos Aires como también de varias navegaciones de muchas p'	397314	         13	                    "TITLE_AKAS"["TITLE":3]
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
imdb_stage/title.principals.tsv.gz	PARTIALLY_LOADED	89576330	89576296	89576330	34	        User character length limit (255) exceeded by string '["Self (segment: \"Busted in Washington Square Park\": narrated by Mike Dreyen) (segment: \"The Fens,'	64689006	     31	                    "TITLE_PRINCIPALS"["CHARACTERS":6]
*/

COPY INTO HEDGEHOG_IMDB.STAGING.title_ratings
FROM @HEDGEHOG_IMDB.STAGING.IMDB_STAGE/title.ratings.tsv.gz
FILE_FORMAT = TSV_FORMAT
ON_ERROR = 'CONTINUE';
/*
file	                         status	 rows_parsed rows_loaded error_limit	errors_seen	first_error	first_error_line	first_error_character	first_error_column_name
imdb_stage/title.ratings.tsv.gz	LOADED	1507795	    1507795     1	           0				
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

CREATE OR REPLACE TABLE dim_year AS
SELECT
    ROW_NUMBER() OVER (ORDER BY year) AS dim_year_id,
    TO_DATE(year || '-01-01') AS date,
    year,
    FLOOR(year / 10) * 10 % 100 AS decade,
    CONCAT(FLOOR(year / 10) * 10 % 100, '. roky') AS decadeStr,
    FLOOR(year / 100) + 1 AS century,
    CONCAT(FLOOR(year / 100) + 1, '. storočie') AS centuryStr
FROM (
    SELECT DISTINCT startYear AS year FROM staging.title_basics WHERE startYear IS NOT NULL
    UNION
    SELECT DISTINCT endYear AS year FROM staging.title_basics WHERE endYear IS NOT NULL
)
ORDER BY year;

CREATE OR REPLACE TABLE dim_titles AS
SELECT DISTINCT
    ROW_NUMBER() OVER (ORDER BY b.tconst) AS dim_title_id,
    b.tconst,
    b.titleType,
    b.primaryTitle,
    b.originalTitle,
    b.genres,
    CASE
        WHEN b.isAdult THEN '18+'
        ELSE 'PG'
    END AS rating
FROM staging.title_basics b;

CREATE OR REPLACE TABLE dim_names AS
SELECT DISTINCT
    ROW_NUMBER() OVER (ORDER BY nb.nconst) AS dim_name_id,
    nb.nconst,
    nb.primaryName,
    CAST(nb.birthYear AS VARCHAR(5)) AS birthYear,
    CAST(nb.deathYear AS VARCHAR(5)) AS deathYear,
    nb.primaryProfession
FROM staging.name_basics nb;

CREATE OR REPLACE TABLE dim_akas AS
SELECT DISTINCT
    ROW_NUMBER() OVER (ORDER BY a.titleId) AS dim_akas_id,
    a.titleId,
    a.title,
    a.region,
    a.language,
    a.types
FROM staging.title_akas a;

CREATE OR REPLACE TABLE fact_titles AS
SELECT
    ROW_NUMBER() OVER (ORDER BY b.tconst) AS fact_title_id,
    r.averageRating,
    r.numVotes,
    b.runtimeMinutes,
    dy_start.dim_year_id AS dim_start_year_id,
    dy_end.dim_year_id AS dim_end_year_id,
    dt.dim_title_id,
    MAX(dn.dim_name_id) AS dim_name_id,
    MAX(da.dim_akas_id) AS dim_akas_id
FROM
    staging.title_basics b
LEFT JOIN staging.title_ratings r ON b.tconst = r.tconst
LEFT JOIN dim_year dy_start ON b.startYear = dy_start.year
LEFT JOIN dim_year dy_end ON b.endYear = dy_end.year
LEFT JOIN dim_titles dt ON b.tconst = dt.tconst
LEFT JOIN staging.title_principals tp ON b.tconst = tp.tconst
LEFT JOIN dim_names dn ON tp.nconst = dn.nconst
LEFT JOIN staging.title_akas a ON b.tconst = a.titleId
LEFT JOIN dim_akas da ON a.titleId = da.titleId
WHERE
    r.averageRating IS NOT NULL AND
    r.numVotes IS NOT NULL AND
    b.runtimeMinutes IS NOT NULL
GROUP BY
    b.tconst, r.averageRating, r.numVotes, b.runtimeMinutes,
    dy_start.dim_year_id, dy_end.dim_year_id, dt.dim_title_id;
