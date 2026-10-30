-- The quantity of an order must be strictly positive
select order_id, quantity
from {{ ref('stg_orders') }}
where quantity <= 0
