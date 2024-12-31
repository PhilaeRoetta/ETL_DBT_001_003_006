{{ config(
    schema='CURATED',
    materialized = 'incremental',
    unique_key = 'ORDER_ID'
) }}

with orders as (
    select distinct
        {{ dbt_utils.generate_surrogate_key(['ORDER_ID']) }} AS ORDER_KEY,
        ORDER_ID AS ORDER_ID,
        AMOUNT AS ORDER_AMOUNT,
        {{ dbt_utils.generate_surrogate_key(['CUST_NAME']) }} AS CUSTOMER_KEY,
        {{ dbt_utils.generate_surrogate_key(['ITEM_NAME']) }} AS ITEM_KEY,
        ORDER_TIME AS ORDER_TIME
    FROM
        {{source('ETL_LAND','ORD')}}
    {% if is_incremental() %}
    WHERE
        ORDER_TIME > (
            SELECT
                MAX(ORDER_TIME)
            FROM
                {{this}}
        )
    {% endif %}
)

SELECT * FROM orders