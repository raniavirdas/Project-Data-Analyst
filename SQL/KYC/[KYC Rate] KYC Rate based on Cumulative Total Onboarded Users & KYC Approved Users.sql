WITH kyc_docs AS (
  SELECT
        company_id,
        MIN(kyc_approved_at) AS first_kyc_approved_date,
        MAX(kyc_approved_at) AS last_kyc_approved_date,
        DATE_TRUNC(MIN(kyc_approved_at), MONTH) AS month,
        final_status
  FROM
      (
        SELECT
              company_id,
              DATE(updated_at) AS kyc_approved_at,
              final_status
        FROM  kyc_documentation
        WHERE final_status = 'Berhasil'
      )
  GROUP BY  1, 5
),

kyc_informations AS (
    SELECT
          COALESCE(kd.company_id, kyc.company_id) AS company_id,
          COALESCE(first_kyc_approved_date,kyc_updated_date) AS first_kyc_approved_date,
          COALESCE(last_kyc_approved_date,kyc_updated_date) AS last_kyc_approved_date,
          COALESCE(month,DATE_TRUNC(kyc_updated_date,MONTH)) AS month,
          'Berhasil' AS final_status
    FROM kyc_docs kd
    FULL OUTER JOIN 
        (
            SELECT 
                  company_id,
                  DATE(MAX(kyc_updated_datetime)) AS kyc_updated_date
            FROM dim__paper__kyc
            WHERE LOWER(kyc_status_name) = 'validasi_berhasil'
            GROUP BY 1
        ) kyc
    ON kyc.company_id = kd.company_id
),

companies AS (
  SELECT
        company_id,
        company_name,
        business_type_child_name,
        DATE(company_registered_datetime) AS company_registered_date,
        DATE_TRUNC(company_registered_datetime, MONTH) AS company_registered_month,
        first_date_created_digital_payment AS first_digpay_date
  FROM  fact__smb__users
),

agg_month_users AS (
    SELECT 
          company_registered_month,
          COUNT(DISTINCT company_id) AS total_users
    FROM companies
    WHERE business_type_child_name IS NOT NULL
    GROUP BY 1
),

cummulative_month_users AS (
    SELECT
          DATE(company_registered_month) AS month,
          SUM(total_users) OVER (ORDER BY company_registered_month ASC) AS cummulative_total_onboarded_users
    FROM agg_month_users
),

agg_kyc AS (
    SELECT 
          DATE_TRUNC(first_kyc_approved_date, MONTH) AS month,
          COUNT(DISTINCT kyc.company_id) AS kyc_users
    FROM kyc_informations kyc
    LEFT JOIN companies c ON c.company_id = kyc.company_id
    WHERE c.company_id IS NOT NULL
    GROUP BY 1
), 

cummulative_month_kyc AS (
    SELECT
          month,
          SUM(kyc_users) OVER (ORDER BY month ASC) AS cummulative_kyc_users
    FROM agg_kyc
)

SELECT 
      u.month, 
      cummulative_kyc_users,
      cummulative_total_onboarded_users,
      cummulative_kyc_users/cummulative_total_onboarded_users AS kyc_users_rate
FROM 
    cummulative_month_users u 
LEFT JOIN cummulative_month_kyc k 
ON        u.month = k.month
WHERE u.month >= DATE(DATE_TRUNC({{start_date}}, MONTH))
[[AND u.month <= DATE(DATE_TRUNC({{end_date}}, MONTH))]];