WITH source AS (
    SELECT * FROM {{source('rds', 'customers')}}
),
staged_rds_companies as (
    SELECT
    concat('rds-', replace(lower(company_name), ' ','-')) AS company_id,
    company_name AS name,
    max(address) AS address,
    max(city) AS city,
    max(postal_code) AS postal_caode,
    max(country) AS country
    FROM source
    GROUP BY name
)
SELECT * FROM staged_rds_companies