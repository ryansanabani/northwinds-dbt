WITH source AS (
    SELECT * FROM {{source('hubspot', 'CONTACTS')}}
),
staged_hubspot_companies AS (
    SELECT 
    CONCAT('hubspot-', TRANSLATE(lower(business_name), ' ,', '-')) AS company_id,
    business_name AS name
    FROM source
    GROUP BY name
)
SELECT * FROM staged_hubspot_companies
