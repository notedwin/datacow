
WITH split AS (
    SELECT
        SUBSTRING(message FROM 1 FOR POSITION('{' IN message) - 1) AS message,
        SUBSTRING(message FROM POSITION('{' IN message)) AS json
    FROM dockerlogs
    WHERE
        message LIKE '%http.log.access.log0%'
), prod as(
SELECT
    -- get the first 24 characters of the message and cast to timestamp
    CAST(SUBSTRING(message FROM 1 FOR 24) AS TIMESTAMP) AS timestamp,
	regexp_replace(json, '("Cf-Visitor"|"Alt-Svc"|"Sec-Ch-Ua"|"Sec-Ch-Ua-Platform"|"Etag"|"If-None-Match"|"Amp-Cache-Transform"):\s*\[[^]]*\]', '\1:[]', 'g')::JSON as log_json
FROM split
)

-- SELECT 
-- 	timestamp,
--     log_json->'request'->>'host',
-- 	log_json->'request'->'headers'->'X-Forwarded-For'->>0
--  FROM prod
--  WHERE log_json->'request'->'headers'->'X-Forwarded-For'->>0 != '73.209.167.249'
 
 
 SELECT DISTINCT
    log_json->'request'->'headers'->'X-Forwarded-For'->>0
 FROM prod
 
 
SELECT
    *
 FROM prod
 INNER JOIN ip_data
  ON log_json->'request'->'headers'->'X-Forwarded-For'->>0 = ip_data.query
 WHERE $__timeFilter(timestamp)