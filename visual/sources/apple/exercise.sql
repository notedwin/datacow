SELECT startdate::DATE as date,
    COUNT(*) as mins_active
FROM "AppleExerciseTime"
GROUP BY 1