-- Súbory, ktoré sú väčšie ako 250 MB musia byť nahraté pomocou PUT cez SnowSQL (webové rozhranie ich neakceptuje)
PUT file://title.akas.tsv.gz @HEDGEHOG_IMDB.STAGING.IMDB_STAGE;
PUT file://title.basics.tsv.gz @HEDGEHOG_IMDB.STAGING.IMDB_STAGE;
PUT file://title.crew.tsv.gz @HEDGEHOG_IMDB.STAGING.IMDB_STAGE;
PUT file://title.episode.tsv.gz @HEDGEHOG_IMDB.STAGING.IMDB_STAGE;
PUT file://title.principals.tsv.gz @HEDGEHOG_IMDB.STAGING.IMDB_STAGE;
PUT file://title.ratings.tsv.gz @HEDGEHOG_IMDB.STAGING.IMDB_STAGE;
PUT file://name.basics.tsv.gz @HEDGEHOG_IMDB.STAGING.IMDB_STAGE;
