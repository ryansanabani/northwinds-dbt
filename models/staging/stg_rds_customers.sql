WITH 
customers AS (
    SELECT * FROM {{source('rds', 'customers')}}
),
companies AS (
    SELECT * FROM DBT_JIGSAWLABS_1.STG_RDS_COMPANIES
),
renamed AS (
    SELECT
    company_id,
    concat('rds-', customer_id) AS proper_customer_id,
    SPLIT_PART(contact_name, ' ', 1) AS first_name,
    SPLIT_PART(contact_name, ' ', -1)   AS last_name,
    TRANSLATE(phone, '(, ), -, ., ', '') AS clean_phone,
    CASE WHEN LEN(clean_phone) = 10
    THEN 
    '(' || SUBSTRING(clean_phone, 0, 3) || ') '
    || SUBSTRING(clean_phone, 4, 3) || '-'
    || SUBSTRING(clean_phone, 7, 4)
    ELSE NULL  
    END AS finished_phone
    FROM customers
    JOIN companies ON companies.company_name = customers.company_name 
),
final AS (
    SELECT 
    company_id,
    finished_phone,
    first_name,
    last_name,
    proper_customer_id
    FROM renamed
)
SELECT * FROM final