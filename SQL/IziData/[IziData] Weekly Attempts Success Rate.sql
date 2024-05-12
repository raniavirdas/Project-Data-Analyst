SELECT  DATE_TRUNC(log_response_created_datetime, WEEK(monday)) AS week, 
        COUNT(DISTINCT log_response_key) AS count_attempts, 
        COUNT(DISTINCT CASE WHEN LOWER(status_name) = 'failed' THEN log_response_key END) AS count_failed, 
        COUNT(DISTINCT CASE WHEN LOWER(status_name) = 'success' THEN log_response_key END) AS count_success, 
        COUNT(DISTINCT CASE WHEN LOWER(status_name) = 'success' THEN log_response_key END)*100/COUNT(DISTINCT log_response_key) AS success_rate
FROM    dim__izi_data__log_response
WHERE   log_response_key IS NOT NULL
        [[AND DATE(log_response_created_datetime) >= DATE({{From}})]]
        [[AND DATE(log_response_created_datetime) <= DATE({{To}})]]
GROUP BY 1
ORDER BY 1 ASC;