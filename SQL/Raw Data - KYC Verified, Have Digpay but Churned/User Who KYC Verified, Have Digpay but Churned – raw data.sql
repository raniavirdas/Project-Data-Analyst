--User-user yang register dari 3 bulan terakhir, udah KYC, tapi masih pakai digpay
WITH companies AS (
    SELECT  company_id AS company_id_c,
            company_name,
            company_email,
            company_phone,
            company_registered_datetime,
            user_owner_id,
            user_owner_name,
            user_owner_email,
            user_owner_phone,
            company_type_group
    FROM 
        dim__paper__company
    WHERE LOWER(company_kyc_status_name) = 'validasi_berhasil' -- KYC verified
    AND DATE(company_registered_datetime) >= '2023-11-20' -- 3 bulan yang lalu
    AND LOWER(company_type_group) != 'perorangan'
),

digital_payments AS (
SELECT  rddp.company_id,
        MAX(rddp.digpay_datetime) AS latest_digpay_date,
        COUNT(DISTINCT rddp.order_id) AS last_2weeks_transaction_count
FROM rpt__raw_data_digital_payments_daily AS rddp
WHERE DATE(digpay_datetime) BETWEEN DATE('2024-02-08') AND DATE('2024-02-21')
GROUP BY 1
)

SELECT
        rddp.company_id,
        companies.* EXCEPT (company_id_c),
        latest_digpay_date,
        last_2weeks_transaction_count
FROM digital_payments rddp
INNER JOIN companies
ON rddp.company_id = companies.company_id_c