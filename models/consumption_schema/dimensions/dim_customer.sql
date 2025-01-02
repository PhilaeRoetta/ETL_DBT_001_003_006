{{config(
    materialized = 'incremental',
    unique_key = 'CUSTOMER_ID'
)}}
{# Get scope of customers updated #}
with customers_info_updated as (
    SELECT DISTINCT
        CUSTOMER_ID
    FROM
        {{ref("customers")}}
    {% if is_incremental() %}
    WHERE UPDATE_TIME > (SELECT MAX(UPDATE_TIME) FROM {{this}}) 
    {% endif %}
),
{# Get scope of customers who placed orders during last update#}
customer_orders_updated as (
    SELECT DISTINCT
        CUSTOMER_KEY
    FROM
        {{ref("orders")}}
    {% if is_incremental() %}
    WHERE ORDER_TIME > (SELECT MAX(CUSTOMER_LASTORDER) FROM {{this}}) 
    {% endif %}
),

{# Total scope of changing customers#}
customer_updated as(
    SELECT CUSTOMER_ID FROM customers_info_updated
    UNION SELECT CUSTOMER_KEY AS CUSTOMER_ID FROM customer_orders_updated
),

{#Update rows according to previous scopes#}
dim_customer as (
    SELECT
        customers.CUSTOMER_ID,
        customers.CUSTOMER_NAME,
        customers.CUSTOMER_BIRTH,
        orders.CUSTOMER_FIRSTORDER,
        orders.CUSTOMER_LASTORDER,
        COALESCE(orders.CUSTOMER_REVENUE,0) AS CUSTOMER_REVENUE,
        customer.UPDATE_TIME
    FROM
        customers customers
        LEFT JOIN customer_orders orders
        ON customers.CUSTOMER_ID = orders.CUSTOMER_KEY,
        orders_maxtime timing
)

SELECT * FROM dim_customer
