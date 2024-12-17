-- Priemerné hodnotenie všetkých titulov podľa roku vydania
SELECT
    ROUND(AVG(fact_ratings.rating), 2) AS "Priemerné hodnotenie",
    dim_titleStartDate.year AS "Rok vydania titulov"
FROM fact_ratings
JOIN dim_titles ON fact_ratings.dim_title_id = dim_titles.dim_title_id
JOIN dim_date dim_postedDate ON fact_ratings.dim_postedDate_id = dim_postedDate.dim_date_id
JOIN dim_date dim_titleStartDate ON fact_ratings.dim_titleStartDate_id = dim_titleStartDate.dim_date_id
WHERE "Rok vydania titulov" <= 2024
GROUP BY "Rok vydania titulov"
ORDER BY "Priemerné hodnotenie" DESC;

-- Aktivita používateľov v priemere počas dňa
SELECT dim_postedTime.hour AS "Hodina",
       ROUND(AVG(za_hodinu), 0) AS "Priemerný počet hodnotení"
FROM (
    SELECT dim_postedTime.hour, COUNT(fact_ratings.rating) AS za_hodinu
    FROM fact_ratings
    JOIN dim_time dim_postedTime ON fact_ratings.dim_postedTime_id = dim_postedTime.dim_time_id
    GROUP BY dim_postedTime.hour, dim_postedTime.minute
) dim_postedTime
GROUP BY hour
ORDER BY hour;

-- Top 50 seriálov v slovenskom znení
SELECT
    dim_titles.originalTitle AS "Názov filmu",
    ROUND(AVG(fact_ratings.rating), 2) AS "Priemerné hodnotenie"
FROM fact_ratings
JOIN dim_titles ON fact_ratings.dim_title_id = dim_titles.dim_title_id
JOIN dim_akas ON dim_titles.tconst = dim_akas.titleId
WHERE
    dim_titles.titleType = 'tvSeries' AND
    dim_akas.region = 'SK' AND
    dim_akas.title = dim_titles.originalTitle
GROUP BY "Názov filmu"
ORDER BY "Priemerné hodnotenie" DESC
LIMIT 50;

-- Hodnotenie seriálov s najväčším počtom epizód
SELECT
    dim_titles.seriesTitle AS "Názov seriálu",
    MAX(fact_ratings.episodeNumber) AS "Počet epizód",
    ROUND(AVG(fact_ratings.rating), 2) AS "Priemerné hodnotenie"
FROM fact_ratings
JOIN dim_titles ON fact_ratings.dim_title_id = dim_titles.dim_title_id
WHERE fact_ratings.episodeNumber IS NOT NULL
GROUP BY "Názov seriálu"
ORDER BY "Počet epizód" DESC, "Priemerné hodnotenie" DESC
LIMIT 50;

-- Herci hrajúci v najviac filmoch
SELECT
    dim_names.nconst AS "nconst",
    dim_names.primaryName AS "Meno",
    ROUND(AVG(fact_ratings.rating), 2) AS "Priemerné hodnotenie titulov",
    COUNT(dim_titles.dim_title_id) AS "Počet titulov"
FROM fact_ratings
JOIN dim_titles ON fact_ratings.dim_title_id = dim_titles.dim_title_id
JOIN dim_title_names ON dim_titles.dim_title_id = dim_title_names.dim_title_id
JOIN dim_names ON dim_title_names.dim_name_id = dim_names.dim_name_id
WHERE
    dim_titles.titleType = 'movie' AND
    dim_names.primaryProfession IN ('actor', 'actress')
GROUP BY "nconst", "Meno"
ORDER BY "Počet titulov" DESC
LIMIT 10;

-- Najdlhší film (v minútach), porovnanie oproti priemernej dĺžke všetkých
SELECT
    dim_titles.originalTitle AS "Názov",
    AVG(fact_ratings.titleRuntimeMinutes) AS "Priemerná dĺžka všetkých",
    MAX(fact_ratings.titleRuntimeMinutes) AS "Dĺžka v minútach",
FROM fact_ratings
JOIN dim_titles ON fact_ratings.dim_title_id = dim_titles.dim_title_id
WHERE dim_titles.titleType = 'movie'
GROUP BY "Názov"
ORDER BY "Dĺžka v minútach" DESC
LIMIT 1;

-- Režiséri s najlepším priemerným hodnotením filmov a počet hlasov
SELECT
    dim_names.primaryName AS "Meno režiséra",
    ROUND(AVG(fact_ratings.rating), 2) AS "Priemerné hodnotenie",
    COUNT(fact_ratings.rating) AS "Celkový počet hlasov" FROM fact_ratings
JOIN dim_titles ON fact_ratings.dim_title_id = dim_titles.dim_title_id
JOIN dim_title_names ON dim_titles.dim_title_id = dim_title_names.dim_title_id
JOIN dim_names ON dim_title_names.dim_name_id = dim_names.dim_name_id
WHERE
    dim_names.primaryProfession LIKE '%director%' AND
    dim_titles.titleType = 'movie'
GROUP BY "Meno režiséra"
ORDER BY "Celkový počet hlasov" DESC, "Priemerné hodnotenie" DESC
LIMIT 10;
