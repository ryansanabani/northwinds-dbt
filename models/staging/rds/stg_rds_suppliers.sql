WITH source AS (
    SELECT * FROM {{source('rds', 'suppliers')}}
),
    renamed AS (
        SELECT supplier_id, country, 
        SPLIT_PART(contact_name, ' ', 1) AS first_name, 
        SPLIT_PART(contact_name, ' ', -1) AS last_name,
        address, city, phone, company_name, region, postal_code, fax, contact_title, 
        homepage, _FIVETRAN_DELETED, _FIVETRAN_SYNCED
        FROM source
    ),
    renumbered_cleaned AS (
        SELECT TRANSLATE(phone, '(,),-, ,.', '') AS clean_phone,
        supplier_id, country, first_name, last_name, address, 
        city, phone, company_name, region, postal_code, fax, contact_title, 
        homepage, _FIVETRAN_DELETED, _FIVETRAN_SYNCED
        FROM renamed
    ),
    renumbered_specs AS (
        SELECT 
        LEN(clean_phone) AS phone_length,
        COUNT(*)
        FROM renumbered_cleaned
        GROUP BY phone_length
    ),
    suppliers_staged AS (
        SELECT
        CASE WHEN LEN(clean_phone) = 10 
        THEN         
        '(' || substring(clean_phone, 0, 3) || ') ' 
        || substring(clean_phone, 4, 3) || '-' 
        || substring(clean_phone, 7, 4)
        ELSE NULL 
        END AS proper_phone,
        supplier_id, 
        country, 
        first_name, 
        last_name, 
        address, 
        city, 
        phone, 
        company_name, 
        region, 
        postal_code, 
        fax, 
        contact_title, 
        homepage, 
        _FIVETRAN_DELETED,
        _FIVETRAN_SYNCED
        FROM renumbered_cleaned
    )
SELECT * FROM suppliers_staged