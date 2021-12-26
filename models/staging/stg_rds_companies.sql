WITH source AS (
    SELECT * FROM {{source('rds', 'customers')}}
),
renamed as (
    SELECT
    concat('rds-', replace(lower(company_name), ' ','-')) AS company_id,
    company_name,
    max(address) AS addres,
    max(city) AS city,
    max(postal_code) AS postal_caode,
    max(country) AS country
    FROM source
    GROUP BY company_name
)
SELECT * FROM renamed