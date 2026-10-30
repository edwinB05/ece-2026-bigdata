{{ config(severity='warn') }}

-- The zip code of a user must belong to the range of its state (reference: seeds/state_zip_ranges.csv).
-- The generated addresses combine a random state with a random zip code: the test is a warning.
select
    u.user_id,
    u.state,
    u.zip_code
from {{ ref('stg_users') }} u
join {{ ref('state_zip_ranges') }} r on r.code = u.state
where cast(u.zip_code as integer) not between r.zip_min and r.zip_max
