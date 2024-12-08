-- Priemerné hodnotenie titulov podľa dekád
SELECT
    AVG(fact_titles.averageRating) AS "Priemerné hodnotenie",
    CONCAT(dim_year.centuryStr, ' ', dim_year.decadeStr) AS "Obdobie"
FROM fact_titles
JOIN dim_year ON fact_titles.dim_start_year_id = dim_year.dim_year_id
JOIN dim_titles ON fact_titles.dim_title_id = dim_titles.dim_title_id
GROUP BY "Obdobie"
ORDER BY "Priemerné hodnotenie" DESC;

-- Top 10 filmov v slovenskom znení s aspoň 100 000 hodnoteniami
SELECT
    dim_akas.title AS "Názov filmu",
    AVG(fact_titles.averageRating) AS "Priemerné hodnotenie",
FROM fact_titles
JOIN dim_akas ON fact_titles.dim_akas_id = dim_akas.dim_akas_id
JOIN dim_titles ON fact_titles.dim_title_id = dim_titles.dim_title_id
WHERE
    dim_akas.region IN ('sk', 'SK') AND
    (SELECT COUNT(*) FROM dim_akas WHERE dim_akas_id = fact_titles.dim_akas_id) <= 1 AND
    fact_titles.numVotes >= 100000 AND
    dim_titles.titleType = 'movie'
GROUP BY "Názov filmu"
ORDER BY "Priemerné hodnotenie" DESC
LIMIT 10;

