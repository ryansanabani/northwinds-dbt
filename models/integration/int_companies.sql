WITH 
hubspot_companies AS (
    SELECT * FROM {{ref('stg_hubspot_companies')}}
),
rds_companies AS (
    SELECT * FROM {{ref('stg_rds_companies')}}
),
merged_companies AS (
    SELECT
    company_id AS hubspot_companies, NULL AS rds_companies,
    name
    FROM hubspot_companies
    UNION ALL
    SELECT
    NULL AS hubspot_companies, company_id AS rds_companies,
    name
    FROM rds_companies
),
dedupli_companies AS (
    SELECT
    max(hubspot_companies) AS hubspot_companies_id,
    max(rds_companies) AS rds_companies_id,
    name
    FROM merged_companies
    GROUP BY name
    ORDER BY name ASC
),
reintegrate_rds_companies_columns AS (
    SELECT 
    hubspot_companies_id,
    rds_companies_id,
    dedupli_companies.name,
    city,
    postal_code,
    address,
    country
    FROM dedupli_companies
    LEFT OUTER JOIN stg_rds_companies ON stg_rds_companies.company_id = dedupli_companies.rds_companies_id
    ORDER BY rds_companies_id, name ASC
)
SELECT 
{{ dbt_utils.surrogate_key('name') }} AS company_pk,
* 
FROM reintegrate_rds_companies_columns