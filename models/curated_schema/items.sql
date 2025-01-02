{{ config(
    materialized = "incremental",
    unique_key = "ITEM_ID"
)
}}

with items as (
    SELECT DISTINCT
        {{ dbt_utils.generate_surrogate_key(['ITEM_NAME']) }} AS ITEM_ID,
        ITEM_NAME AS ITEM_NAME,
        ORDER_TIME AS UPDATE_TIME
    FROM
        {{source("ETL_LAND","ORD")}}
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
SELECT * FROM items