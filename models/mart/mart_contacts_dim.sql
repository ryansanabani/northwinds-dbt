{{ config(materialized='table') }}
WITH
int_contacts AS (
    SELECT * FROM {{ ref('int_contacts') }}
),
int_companies AS (
    SELECT * FROM {{ ref('int_companies') }}
),
joined_references AS (  
    SELECT 
    contact_pk,
    first_name,
    last_name,
    phone,
    company_pk
    FROM int_contacts
    JOIN 
    int_companies ON int_companies.hubspot_companies_id = int_contacts.hubspot_company_id
    OR int_companies.rds_companies_id = int_contacts.rds_company_id
)
SELECT * FROM joined_references
