WITH 
hubspot_contacts AS(
    SELECT * FROM {{ref('stg_hubspot_contacts')}}   
), 
rds_customers AS (
    SELECT * FROM {{ref('stg_rds_customers')}}
),
merged_contacts AS (
    SELECT 
    first_name, 
    last_name, 
    phone,
    contact_id AS hubspot_contact_id, NULL AS rds_contact_id,
    company_id AS hubspot_company_id, NULL AS rds_company_id
    FROM hubspot_contacts
    UNION ALL 
    SELECT
    first_name,
    last_name,
    phone,
    NULL AS hubspot_contact_id, contact_id AS rds_contact_id,
    NULL AS hubspot_compnay_id, company_id AS rds_company_id
    FROM rds_customers
),
dedupli_contacts AS (
    SELECT
    max(hubspot_contact_id) AS hubspot_contact_id,
    max(rds_contact_id) AS rds_contact_id, 
    first_name, 
    last_name, 
    max(phone) AS phone,
    max(hubspot_company_id) AS hubspot_company_id,
    max(rds_company_id) AS rds_company_id  
    FROM merged_contacts
    GROUP BY first_name, last_name
    ORDER BY rds_contact_id, last_name ASC
)
SELECT 
{{ dbt_utils.surrogate_key(['first_name', 'last_name', 'phone']) }} AS contact_pk,
* 
FROM dedupli_contacts
