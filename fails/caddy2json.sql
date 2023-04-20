SELECT id,
    message,
    CAST(
        SUBSTRING(
            SUBSTRING(
                message
                FROM 1 FOR POSITION('{' IN message) - 1
            )
            FROM 1 FOR 24
        ) AS TIMESTAMP
    ) AS time_reported,
    regexp_replace(
        SUBSTRING(
            message
            FROM POSITION('{' IN message)
        ),
        '("Cf-Visitor"|"Alt-Svc"|"Sec-Ch-Ua"|"Sec-Ch-Ua-Platform"|"Etag"|"If-None-Match"|"Amp-Cache-Transform"):\s*\[[^]]*\]',
        '\1:[]',
        'g'
    )::JSON AS json_message
FROM dockerlogs
WHERE message LIKE '%%http.log.access.log0%%'