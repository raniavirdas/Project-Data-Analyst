WITH companies AS (
  SELECT
    company_id,
    company_name,
    company_type_group,
    company_size_group,
    business_type_child_name,
    user_owner_registered_platform_name,
    company_registered_datetime,
    package_active_name
  FROM dim__paper__company c
  WHERE paper_chain_status_name != 'paper-chain-inactive'
  AND testing_account_flag = 0
),

company_metasettings AS (
  SELECT
    company_id,
    user_comment_created_datetime,
    CASE
      WHEN corporate_client_flag = 1 OR government_client_flag = 1
      THEN "B2B"
      WHEN corporate_client_flag = 0 AND government_client_flag = 0
      THEN "B2C"
    ELSE '0' END AS metasetting_segment
  FROM dim__paper__user_comment_metasetting
  ORDER BY 3 DESC
),

company_informations AS (
  SELECT
        c.*,
        m.metasetting_segment
  FROM companies c
  LEFT JOIN company_metasettings m
    ON m.company_id = c.company_id
),

expense AS (
  SELECT  
        account_id,
        expense_id,
        company_id,
        expense_name,
        expense_vendor,
        expense_category_name,
        expense_datetime,
        expense_amount,
        expense_note,
        DATE_TRUNC(expense_datetime, WEEK) AS week
  FROM fact__paper__expense
  WHERE expense_datetime >= "2024-01-28" 
        AND expense_datetime < "2024-04-01" 
),

accounts AS (
  SELECT  
        account_id, 
        company_id,
        account_parent_id,
        account_name,
        account_code,
        account_created_datetime
  FROM dim__paper__account a
  WHERE LOWER(account_name) NOT IN ("biaya pencairan digital payment","biaya pembayaran keluar")
),

ranked_expenses AS (
  SELECT
        ci.metasetting_segment,
        e.company_id,
        ci.company_name,
        COUNT(DISTINCT e.expense_id) AS expense_count,
        ROW_NUMBER() OVER (PARTITION BY ci.metasetting_segment ORDER BY COUNT(DISTINCT e.expense_id) DESC) AS expense_rank
  FROM expense e
  INNER JOIN accounts a ON a.account_id = e.account_id
  LEFT JOIN company_informations ci ON ci.company_id = e.company_id
  WHERE ci.metasetting_segment IN ('B2B', 'B2C')
  GROUP BY 1, 2, 3
),

top_expenses AS (
  SELECT
    metasetting_segment,
    company_id,
    company_name,
    expense_count
  FROM ranked_expenses
  WHERE expense_rank <= 30
),

raw_data AS (
    SELECT
        te.metasetting_segment,
        te.company_id,
        ci.company_name,
        COALESCE(ci.package_active_name, "FREE") AS package_active_name,
        COUNT(te.company_id) AS user_count
    FROM top_expenses te
    LEFT JOIN company_informations ci ON ci.company_id = te.company_id
    GROUP BY 1, 2, 3, 4
    ORDER BY 4
)

SELECT
        week,
        COUNT(DISTINCT e.company_id) AS top_company_count
FROM expense e  
INNER JOIN raw_data 
        ON e.company_id = raw_data.company_id
GROUP BY week
ORDER BY 1