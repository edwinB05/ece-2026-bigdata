with source as (
    select * from {{ source('bronze', 'users') }}
),

first_orders as (
    select
        cast(user_uuid as uuid) as user_id,
        min(cast(date as timestamptz)) as first_order_at
    from {{ source('bronze', 'orders') }}
    group by 1
),

typed as (
    select
        cast(uuid as uuid) as user_id,
        trim(username) as username,
        lower(trim(username)) as username_normalized,
        trim(name) as name,
        upper(trim(sex)) as sex,
        lower(trim(mail)) as email,
        cast(birthdate as date) as birthdate,
        -- The address spans 2 lines: the street, then the city, the state and the zip code
        split_part(address, chr(10), 1) as street,
        split_part(address, chr(10), 2) as address_line_2
    from source
),

validated as (
    select
        t.*,
        -- A birthdate later than the first order of the user is invalid
        not coalesce(t.birthdate > cast(f.first_order_at as date), false) as birthdate_is_valid
    from typed t
    left join first_orders f on t.user_id = f.user_id
)

select
    user_id,
    username,
    username_normalized,
    name,
    sex,
    email,
    -- Replace the invalid birthdates with NULL, the flag keeps track of the original validation
    case when birthdate_is_valid then birthdate end as birthdate,
    birthdate_is_valid,
    street,
    -- Military addresses, such as "DPO AE 12345", have no comma
    nullif(regexp_extract(address_line_2, '^(.+), [A-Z]{2} \d{5}$', 1), '') as city,
    regexp_extract(address_line_2, '([A-Z]{2}) (\d{5})$', 1) as state,
    regexp_extract(address_line_2, '([A-Z]{2}) (\d{5})$', 2) as zip_code
from validated
