WITH companies AS (
    SELECT 
        company_id,
        company_name,
        company_phone,
        company_email,
        company_type_group,
        CASE 
            WHEN lower(company_type_group) IN ('pt', 'cv') THEN upper('pt/cv') 
            WHEN lower(company_type_group) IN ('perorangan') THEN upper('perorangan')
        END AS company_type_category
    FROM data_warehouse_layer.dim__paper__company
    WHERE   
        testing_account_flag = 0
        AND paper_chain_status_name != "paper-chain-inactive"
        AND lower(company_type_group) IN ('perorangan', 'pt', 'cv') 
),

digpay AS (
    SELECT  
        company_id,
        order_id AS external_id,
        digpay_datetime,
        transaction_amount
    FROM data_mart_layer.rpt__raw_data_digital_payments_daily
    WHERE digpay_datetime >= DATETIME('2023-08-01') 
    AND digpay_datetime <= DATETIME('2024-03-31')
)
,

agg_digpay AS (
    SELECT
        company_id,
        DATE_TRUNC(digpay_datetime, month) AS month,
        CASE 
          WHEN SUM(d.transaction_amount) < 100000000 THEN "<100mio"
          WHEN SUM(d.transaction_amount) BETWEEN 100000000 AND 200000000 THEN "100mio to 200mio"
          WHEN SUM(d.transaction_amount) BETWEEN 200000001 AND 500000000 THEN "200mio to 500mio"
          WHEN SUM(d.transaction_amount) BETWEEN 500000001 AND 1000000000 THEN "500mio to 1bio"
          WHEN SUM(d.transaction_amount) > 1000000000 THEN ">1bio"
        END AS tpv_group,
        SUM(d.transaction_amount) AS monthly_tpv
    FROM digpay d
    GROUP BY 1, 2
)
,

raw_data AS (
    SELECT 
        tpv_group,
        company_type_category,
        month,
        SUM(monthly_tpv) AS tpv,
        COUNT(DISTINCT d.company_id) AS user_count,
        SUM(monthly_tpv) / COUNT(DISTINCT d.company_id) AS avg_tpv_per_user
    FROM agg_digpay d
    INNER JOIN companies c ON c.company_id = d.company_id
    GROUP BY 1, 2, 3
)

SELECT 
    tpv_group,
    company_type_category,
    SUM(CASE WHEN month = "2023-08-01" THEN tpv END) AS TPV_Aug23,
    SUM(CASE WHEN month = "2023-09-01" THEN tpv END) AS TPV_Sep23,
    SUM(CASE WHEN month = "2023-10-01" THEN tpv END) AS TPV_Oct23,
    SUM(CASE WHEN month = "2023-11-01" THEN tpv END) AS TPV_Nov23,
    SUM(CASE WHEN month = "2023-12-01" THEN tpv END) AS TPV_Dec23,
    SUM(CASE WHEN month = "2024-01-01" THEN tpv END) AS TPV_Jan24,
    SUM(CASE WHEN month = "2024-02-01" THEN tpv END) AS TPV_Feb24,
    SUM(CASE WHEN month = "2023-08-01" THEN user_count END) AS User_Aug23,
    SUM(CASE WHEN month = "2023-09-01" THEN user_count END) AS User_Sep23,
    SUM(CASE WHEN month = "2023-10-01" THEN user_count END) AS User_Oct23,
    SUM(CASE WHEN month = "2023-11-01" THEN user_count END) AS User_Nov23,
    SUM(CASE WHEN month = "2023-12-01" THEN user_count END) AS User_Dec23,
    SUM(CASE WHEN month = "2024-01-01" THEN user_count END) AS User_Jan24,
    SUM(CASE WHEN month = "2024-02-01" THEN user_count END) AS User_Feb24,
    SUM(CASE WHEN month = "2023-08-01" THEN avg_tpv_per_user END) AS AvgTPVPerUser_Aug23,
    SUM(CASE WHEN month = "2023-09-01" THEN avg_tpv_per_user END) AS AvgTPVPerUser_Sep23,
    SUM(CASE WHEN month = "2023-10-01" THEN avg_tpv_per_user END) AS AvgTPVPerUser_Oct23,
    SUM(CASE WHEN month = "2023-11-01" THEN avg_tpv_per_user END) AS AvgTPVPerUser_Nov23,
    SUM(CASE WHEN month = "2023-12-01" THEN avg_tpv_per_user END) AS AvgTPVPerUser_Dec23,
    SUM(CASE WHEN month = "2024-01-01" THEN avg_tpv_per_user END) AS AvgTPVPerUser_Jan24,
    SUM(CASE WHEN month = "2024-02-01" THEN avg_tpv_per_user END) AS AvgTPVPerUser_Feb24
FROM raw_data AS PivotTable
GROUP BY 1, 2
ORDER BY 1, 2;