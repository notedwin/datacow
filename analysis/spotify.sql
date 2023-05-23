TRUNCATE spotify_listening

SELECT ts FROM spotify_listening

SELECT month, song
FROM (
    SELECT to_char(ts, 'YYYY-MM') AS month, master_metadata_track_name AS song,
    RANK() OVER (PARTITION BY to_char(ts, 'YYYY-MM') ORDER BY COUNT(*) DESC) AS rank
    FROM spotify_listening
    GROUP BY 1, 2
) subquery
WHERE rank = 1
ORDER BY month;


SELECT DISTINCT(master_metadata_track_name) FROM spotify_listening