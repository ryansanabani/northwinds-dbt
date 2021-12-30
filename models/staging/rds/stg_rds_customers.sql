WITH 
customers AS (
    SELECT * FROM {{source('rds', 'customers')}}
),
companies AS (
    SELECT * FROM {{ref('stg_rds_companies')}}
),
renamed AS (
    SELECT
    company_id,
    concat('rds-', customer_id) AS customer_id,
    SPLIT_PART(contact_name, ' ', 1) AS first_name,
    SPLIT_PART(contact_name, ' ', -1)   AS last_name,
    TRANSLATE(phone, '(, ), -, ., ', '') AS clean_phone,
    CASE WHEN LEN(clean_phone) = 10
    THEN 
    '(' || SUBSTRING(clean_phone, 0, 3) || ') '
    || SUBSTRING(clean_phone, 4, 3) || '-'
    || SUBSTRING(clean_phone, 7, 4)
    ELSE NULL  
    END AS phone
    FROM customers
    JOIN companies ON companies.name = customers.company_name 
),
staged_rds_customers AS (
    SELECT 
    customer_id,
    first_name,
    last_name,
    phone,
    company_id
    FROM renamed
)
SELECT * FROM staged_rds_customers