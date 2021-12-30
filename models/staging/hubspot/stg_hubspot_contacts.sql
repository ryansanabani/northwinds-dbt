--SELECT * FROM "PAGILA_INC"."HUBSPOT"."CONTACTS" LIMIT 10;
WITH source AS (
    SELECT * FROM {{source('hubspot', 'CONTACTS')}}
),
clean_phone_conact AS (
    SELECT 
    CONCAT('hubspot-', hubspot_id) AS contact_id,
    first_name,
    last_name,
    TRANSLATE(phone, '(,),-, ,.', '') AS clean_phone,
    CASE WHEN LEN(clean_phone) = 10 
        THEN         
        '(' || substring(clean_phone, 0, 3) || ') ' 
        || substring(clean_phone, 4, 3) || '-' 
        || substring(clean_phone, 7, 4)
        ELSE NULL 
        END AS phone,
    business_name
FROM source
),
companies AS (
    SELECT * FROM {{ref('stg_hubspot_companies')}}
),
staged_hubspot_contacts AS (
    SELECT 
    contact_id, 
    first_name, 
    last_name, 
    phone, 
    company_id 
    FROM clean_phone_conact
    JOIN companies ON companies.name = clean_phone_conact.business_name
)
SELECT * FROM staged_hubspot_contacts


