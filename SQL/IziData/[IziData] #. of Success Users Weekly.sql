WITH raw_data AS (
    SELECT  *,
            CASE WHEN LOWER(status_name) = 'failed' THEN 1 ELSE 0 END AS fail,
            CASE WHEN LOWER(status_name) = 'success' THEN 1 ELSE 0 END AS success
    FROM dim__izi_data__log_response
    WHERE log_response_key IS NOT NULL
    [[AND DATE(log_response_created_datetime) >= DATE({{From}})]]
    [[AND DATE(log_response_created_datetime) <= DATE({{To}})]]
),

users_category AS (
    SELECT 
        DATE_TRUNC(DATE(log_response_created_datetime), WEEK(monday)) AS week,
        company_id, 
        CASE 
            WHEN (SUM(fail) > 0) AND (SUM(success) > 0) THEN 'failed_then_success'
            WHEN (SUM(fail) = 0) AND (SUM(success) > 0) THEN 'success_only'
            WHEN (SUM(fail) > 0) AND (SUM(success) = 0) THEN 'failed_only' 
        END AS user_category
    FROM 
        raw_data
    GROUP BY 2,1 
)

SELECT  week, 
        COUNT(DISTINCT company_id) AS count_companies, 
        COUNT(DISTINCT CASE WHEN user_category = 'success_only' THEN company_id END) AS success_only, 
        COUNT(DISTINCT CASE WHEN user_category = 'failed_only' THEN company_id END) AS failed_only,
        COUNT(DISTINCT CASE WHEN user_category = 'failed_then_success' THEN company_id END) AS failed_then_success
FROM users_category
GROUP BY 1
ORDER BY 1 asc;