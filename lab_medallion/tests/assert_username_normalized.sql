-- username_normalized must be lowercase, without leading or trailing whitespace
select user_id, username_normalized
from {{ ref('stg_users') }}
where username_normalized is null
   or username_normalized != lower(trim(username_normalized))
