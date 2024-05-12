SELECT 
    *, 
    count_failed_reason * 100 / SUM(count_failed_reason) OVER () AS percentage_failed_reason
FROM
    (SELECT 
        CONCAT(type_name," - ",reason_name) AS failed_reason_name,
        COUNT(DISTINCT log_response_key) AS count_failed_reason
    FROM dim__izi_data__log_response
    WHERE   log_response_key IS NOT NULL AND LOWER(status_name) = 'failed'
            [[AND DATE(log_response_created_datetime) >= DATE({{from}})]]
            [[AND DATE(log_response_created_datetime) <= DATE({{to}})]]
    GROUP BY 1)
ORDER BY 2 DESC;