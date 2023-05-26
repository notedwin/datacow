SELECT *
FROM (
        SELECT master_metadata_track_name AS song,
            spotify_track_uri as track_uri,
			MIN(ts) as min_,
			MAX(ts) as max_,
            count(*) as c
        FROM spotify_listening
-- 		WHERE ts > '2019-01-01'
        GROUP BY 1,
            2
        HAVING COUNT(*) > 15
    ) sp
WHERE sp.track_uri NOT IN (
        SELECT track_uri
        FROM liked_songs
    )
	AND sp.song NOT IN (
	SELECT track_name
	FROM liked_songs
	)
ORDER BY sp.c DESC


SELECT DISTINCT spotify_track_uri as uri FROM spotify_listening WHERE spotify_track_uri IS NOT NULL AND spotify_track_uri NOT IN (SELECT uri FROM audio_features)

SELECT track_name, album_name, release_date, popularity, danceability, energy, key, loudness, mode, speechiness, acousticness, instrumentalness, liveness, valence, tempo FROM liked_songs l
LEFT JOIN audio_features a
ON l.track_uri = a.uri
ORDER BY TEMPO