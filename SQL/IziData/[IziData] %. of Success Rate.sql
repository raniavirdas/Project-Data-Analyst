SELECT 
    COUNT(DISTINCT CASE WHEN LOWER(status_name) = 'success' THEN log_response_key END)/COUNT(DISTINCT log_response_key) AS success_rate
FROM dim__izi_data__log_response
WHERE   log_response_key IS NOT NULL
        [[AND DATE(log_response_created_datetime) >= DATE({{from}})]]
        [[AND DATE(log_response_created_datetime) <= DATE({{to}})]]