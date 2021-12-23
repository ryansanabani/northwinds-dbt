WITH source AS (
    SELECT * FROM PAGILA_INC.NORTHWINDS_RDS_PUBLIC.CUSTOMERS
),
renamed AS (
    SELECT customer_id, country,
    SPLIT_PART(contact_name, ' ', 1) AS first_name,
    SPLIT_PART(contact_name, ' ', -1)   AS last_name
    FROM source
)
SELECT * FROM renamed