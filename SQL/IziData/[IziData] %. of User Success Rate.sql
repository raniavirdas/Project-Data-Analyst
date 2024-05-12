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
        company_id, 
        SUM(fail) as count_failed, 
        SUM(success) as count_success,
        CASE 
            WHEN (SUM(fail) > 0) AND (SUM(success) > 0) THEN 'Failed Then Success'
            WHEN (SUM(fail) = 0) AND (SUM(success) > 0) THEN 'Success Only'
            WHEN (SUM(fail) > 0) AND (SUM(success) = 0) THEN 'Failed Only' 
        END AS user_category
    FROM 
        raw_data
    GROUP BY 1
)

SELECT 
    user_category, 
    COUNT(DISTINCT company_id) AS count_companies
FROM
    users_category
GROUP BY 1;