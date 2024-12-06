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
    region VARCHAR(3),
    language VARCHAR(3),
    types VARCHAR(255),
    attributes VARCHAR(255),
    isOriginalTitle BOOLEAN,
    FOREIGN KEY (titleId) REFERENCES staging.title_basics(tconst)
);

CREATE OR REPLACE TABLE staging.title_crew (
    tconst VARCHAR(15) PRIMARY KEY,
    directors VARCHAR(255),
    writers VARCHAR(255),
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

-- load
CREATE OR REPLACE STAGE HEDGEHOG_IMDB.STAGING.IMDB_STAGE;
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

COPY INTO HEDGEHOG_IMDB.STAGING.title_akas
FROM @HEDGEHOG_IMDB.STAGING.IMDB_STAGE/title.akas.tsv.gz
FILE_FORMAT = TSV_FORMAT
ON_ERROR = 'CONTINUE';

COPY INTO HEDGEHOG_IMDB.STAGING.title_basics
FROM @HEDGEHOG_IMDB.STAGING.IMDB_STAGE/title.basics.tsv.gz
FILE_FORMAT = TSV_FORMAT
ON_ERROR = 'CONTINUE';

COPY INTO HEDGEHOG_IMDB.STAGING.title_crew
FROM @HEDGEHOG_IMDB.STAGING.IMDB_STAGE/title.crew.tsv.gz
FILE_FORMAT = TSV_FORMAT
ON_ERROR = 'CONTINUE';

COPY INTO HEDGEHOG_IMDB.STAGING.title_episode
FROM @HEDGEHOG_IMDB.STAGING.IMDB_STAGE/title.episode.tsv.gz
FILE_FORMAT = TSV_FORMAT
ON_ERROR = 'CONTINUE';

COPY INTO HEDGEHOG_IMDB.STAGING.title_principals
FROM @HEDGEHOG_IMDB.STAGING.IMDB_STAGE/title.principals.tsv.gz
FILE_FORMAT = TSV_FORMAT
ON_ERROR = 'CONTINUE';

COPY INTO HEDGEHOG_IMDB.STAGING.title_ratings
FROM @HEDGEHOG_IMDB.STAGING.IMDB_STAGE/title.ratings.tsv.gz
FILE_FORMAT = TSV_FORMAT
ON_ERROR = 'CONTINUE';

COPY INTO HEDGEHOG_IMDB.STAGING.name_basics
FROM @HEDGEHOG_IMDB.STAGING.IMDB_STAGE/name.basics.tsv.gz
FILE_FORMAT = TSV_FORMAT
ON_ERROR = 'CONTINUE';
