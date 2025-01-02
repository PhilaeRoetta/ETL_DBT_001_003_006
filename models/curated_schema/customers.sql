{{config(
    materialized = 'incremental',
    unique_key = 'CUSTOMER_ID'
)}}

with customers as (

    SELECT DISTINCT
        {{ dbt_utils.generate_surrogate_key(['CUST_NAME']) }} AS CUSTOMER_ID,
        CUST_NAME AS CUSTOMER_NAME,
        DOB AS DATE_OF_BIRTH,
        ORDER_TIME AS UPDATE_TIME
    FROM
        {{source("ETL_LAND","ORD")}}
    {%if is_incremental()%}
    WHERE
        ORDER_TIME > (
            SELECT
                MAX(ORDER_TIME)
            FROM
                {{this}}
        )
    {% endif %}
)

SELECT * FROM customers
