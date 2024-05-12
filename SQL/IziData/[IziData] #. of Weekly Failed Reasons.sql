SELECT  week, 
        SUM(fail) as total_failed, 
        SUM(unrecognized_img_reason) AS unrecognized_img_reason, 
        SUM(invalid_selfie_reason) AS invalid_selfie_reason,
        SUM(nik_not_found_reason) AS nik_not_found_reason, 
        SUM(temporary_network_error_reason) AS temporary_network_error_reason
FROM
    (SELECT     DATE_TRUNC(DATE(log_response_created_datetime), WEEK(monday)) AS week, 
                log_response_key, 
                CASE WHEN LOWER(status_name) = 'failed' THEN 1 ELSE 0 END AS fail,
                CASE WHEN LOWER(reason_name) = 'unrecognized_img_reason' THEN 1 ELSE 0 END AS unrecognized_img_reason,
                CASE WHEN LOWER(reason_name) = 'invalid_selfie' THEN 1 ELSE 0 END AS invalid_selfie_reason,
                CASE WHEN LOWER(reason_name) = 'nik_not_found' THEN 1 ELSE 0 END AS nik_not_found_reason,
                CASE WHEN LOWER(reason_name) = 'temporary_network_error' THEN 1 ELSE 0 END AS temporary_network_error_reason
    FROM 
            dim__izi_data__log_response
    WHERE   log_response_key IS NOT NULL 
            AND LOWER(status_name) = 'failed'
            [[AND DATE(log_response_created_datetime) >= DATE({{From}})]]
            [[AND DATE(log_response_created_datetime) <= DATE({{To}})]]
    )
GROUP BY 1
ORDER BY 1 ASC;