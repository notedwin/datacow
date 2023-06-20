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


SELECT CAST(EXTRACT(EPOCH FROM MAX(ts)) AS INT) FROM spotify_listening

UNION ALL
SELECT CAST(EXTRACT(EPOCH FROM CAST(MAX(ts) AS timestamp) AT TIME ZONE 'CST') AS INT) FROM spotify_listening


SELECT track_name, album_name, release_date, 
    popularity, danceability, energy, key, loudness, mode, 
    speechiness, acousticness, instrumentalness, liveness, valence, tempo 
    FROM liked_songs l
    INNER JOIN audio_features a
    ON l.track_uri = a.uri
    LIMIT 100