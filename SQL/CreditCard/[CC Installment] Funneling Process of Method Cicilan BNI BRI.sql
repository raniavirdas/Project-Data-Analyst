WITH dpr AS (
SELECT
      company_id,
      dpr_key AS payment_request_id,
      request_source_name AS payment_request_source_name,
      request_status_name AS payment_request_status_name,
      document_type_name,
      request_created_datetime AS payment_request_created_datetime
FROM  fact__paper__digital_payment_request dpr
WHERE request_created_datetime >= "2023-12-29"
),

dpt AS (
SELECT
      * EXCEPT (rn)
FROM
      (
      SELECT
            company_id,
            payment_request_id,
            external_id,
            transaction_status_note AS payment_transaction_status_name,
            payment_method_name,
            payment_provider_name,
            payment_datetime,
            transaction_created_datetime AS payment_transaction_created_datetime,
            ROW_NUMBER() OVER (PARTITION BY payment_request_id ORDER BY transaction_created_datetime DESC) AS rn
      FROM  fact__paper__digital_payment_transaction dpt
      )
WHERE   rn = 1
),

raw_data AS (
SELECT
      dpr.company_id,
      dpr.payment_request_id,
      dpt.external_id,
      payment_request_status_name,
      payment_transaction_status_name,
      payment_method_name,
      payment_provider_name
FROM dpr
LEFT JOIN dpt ON dpr.payment_request_id = dpt.payment_request_id
)

SELECT
    "PRT Created" AS title,
    COUNT(DISTINCT raw_data.payment_request_id) AS prt_count
FROM raw_data

UNION ALL

SELECT
    "Payment Method Chosen" AS title,
    COUNT(DISTINCT payment_request_id) AS prt_count
FROM raw_data
WHERE external_id IS NOT NULL

UNION ALL

SELECT
    "Payment Method Chosen - All Credit Card" AS title,
    COUNT(DISTINCT payment_request_id) AS prt_count
FROM raw_data
WHERE external_id IS NOT NULL AND LOWER(payment_method_name) LIKE 'credit_card'

UNION ALL

SELECT
    "Payment Method Chosen - All Cicilan" AS title,
    COUNT(DISTINCT payment_request_id) AS prt_count
FROM raw_data
WHERE external_id IS NOT NULL AND LOWER(payment_provider_name) LIKE 'cicilan%' AND LOWER(payment_method_name) LIKE 'credit_card'

UNION ALL

SELECT
    "Payment Method Chosen - Cicilan BNI/BRI" AS title,
    COUNT(DISTINCT payment_request_id) AS prt_count
FROM raw_data
WHERE   external_id IS NOT NULL
        AND LOWER(payment_method_name) LIKE 'credit_card'
        [[AND (
                (LOWER(payment_provider_name) LIKE CONCAT('cicilan_', LOWER({{installment_bank}}), '_%') AND 
                 ({{tenor}} = 'All Tenor' OR 
                  LOWER(payment_provider_name) LIKE CONCAT('%_', {{tenor}}, '_bulan')))
        )]]

UNION ALL

SELECT
    "Payment Method Chosen - Paid Cicilan BNI/BRI" AS title,
    COUNT(DISTINCT payment_request_id) AS prt_count
FROM raw_data
WHERE   external_id IS NOT NULL 
        AND LOWER(payment_method_name) LIKE 'credit_card'
        [[AND (
                (LOWER(payment_provider_name) LIKE CONCAT('cicilan_', LOWER({{installment_bank}}), '_%') AND 
                 ({{tenor}} = 'All Tenor' OR LOWER(payment_provider_name) LIKE CONCAT('%_', {{tenor}}, '_bulan')))
        )]]
        AND payment_transaction_status_name = "PAID"
ORDER BY 2 DESC
