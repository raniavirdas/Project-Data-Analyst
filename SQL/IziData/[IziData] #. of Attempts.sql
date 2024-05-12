SELECT 
    COUNT(DISTINCT log_response_key) AS count_attempts
FROM dim__izi_data__log_response
WHERE   log_response_key IS NOT NULL
        [[AND DATE(log_response_created_datetime) >= DATE({{from}})]]
        [[AND DATE(log_response_created_datetime) <= DATE({{to}})]]