WITH kyc_docs AS (
  SELECT
        company_id,
        MIN(kyc_approved_at) AS first_kyc_approved_date,
        MAX(kyc_approved_at) AS last_kyc_approved_date,
        DATE_TRUNC(MIN(kyc_approved_at), month) AS month,
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
  HAVING    first_kyc_approved_date >= "2023-01-01"
),

kyc_informations AS (
    SELECT
          COALESCE(kd.company_id, kyc.company_id) AS company_id,
          COALESCE(first_kyc_approved_date,kyc_updated_date) AS first_kyc_approved_date,
          COALESCE(last_kyc_approved_date,kyc_updated_date) AS last_kyc_approved_date,
          COALESCE(month,DATE_TRUNC(kyc_updated_date,month)) AS month,
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
        DATE_TRUNC(company_registered_datetime, month) AS company_registered_month,
        first_date_created_digital_payment AS first_digpay_date
  FROM  fact__smb__users
),

agg_registered_users AS (
    SELECT 
            DATE(company_registered_month) AS month,
            COUNT(DISTINCT company_id) AS total_users
    FROM companies 
    WHERE company_registered_month >= '2023-01-01' 
    AND business_type_child_name IS NOT NULL
    [[AND DATE(company_registered_date) >= DATE({{start_date}})]]
    [[AND DATE(company_registered_date) <= DATE({{end_date}})]]
    GROUP BY 1
),

agg_kyc AS (
    SELECT 
        DATE_TRUNC(first_kyc_approved_date, month) AS month,
        COUNT(DISTINCT kyc.company_id) AS kyc_users
    FROM kyc_informations kyc
    LEFT JOIN companies c ON c.company_id = kyc.company_id
    WHERE c.company_id IS NOT NULL
  [[AND DATE(first_kyc_approved_date) >= DATE({{start_date}})]]
  [[AND DATE(first_kyc_approved_date) <= DATE({{end_date}})]]
    GROUP BY 1
), 

agg_kyc_rate AS (
    SELECT 
        au.month,
        ak.kyc_users/au.total_users AS kyc_users_rate
    FROM agg_registered_users au
    LEFT JOIN agg_kyc ak ON ak.month = au.month
),

raw_data AS (
    SELECT 
        ak.month,
        SUM(ak.kyc_users) AS num_kyc_users,
        SUM(aru.total_users) AS num_registered_users,
        SUM(akr.kyc_users_rate) AS kyc_users_rate
    FROM agg_registered_users aru 
    LEFT JOIN agg_kyc ak ON aru.month = ak.month
    LEFT JOIN agg_kyc_rate akr ON akr.month = aru.month
    GROUP BY 1
)

SELECT
	*
FROM raw_data
ORDER BY 1;