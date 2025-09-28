{{ config(
    materialized='incremental',
    unique_key='PART_NO',
    post_hook=[
        "ALTER TABLE {{ this }} SET OPTIONS(description='Data Warehouse table containing all parts inventory data.', labels=[('type','business_vault'),('entity','parts')])"
    ]
) }}

WITH part_info AS (
    SELECT
    hp.PART_ID,
    sp.PART_NAME,
    sp.PART_DESC,
    sc.COUNTRY_NAME,
    AVG(so.UNIT_COST_PRICE) AS AVG_UNIT_COST_PRICE,
    AVG(ss.UNIT_SELL_PRICE) AS AVG_UNIT_SELL_PRICE,
    SUM(so.PURCHASED_QTY) AS TOTAL_PURCHASED_QTY,
    SUM(ss.SOLD_QTY) AS TOTAL_SOLD_QTY
    
    FROM
    {{ ref('hub_part') }} as hp
    JOIN {{ ref('sat_part') }} as sp ON hp.PART_HK = sp.PART_HK
    QUALIFY ROW_NUMBER() OVER (PARTITION BY sp.PART_HK ORDER BY sp.LOAD_DATE DESC) = 1

    JOIN {{ ref('lnk_part_country') }} as lk_p_c ON hp.PART_HK = lk_p_c.PART_HK
    JOIN {{ ref('hub_country') }} hc ON hc.COUNTRY_HK = lk_p_c.COUNTRY_HK
    JOIN {{ ref('sat_country') }} sc ON sc.COUNTRY_HK = hc.COUNTRY_HK

    JOIN {{ ref('lnk_part_order') }} as lk_p_o ON hp.PART_HK = lk_p_o.PART_HK
    JOIN {{ ref('hub_order') }} as ho ON ho.ORDER_HK = lk_p_o.ORDER_HK
    JOIN {{ ref('sat_order') }} as so ON so.ORDER_HK = lk_p_o.ORDER_HK

    JOIN {{ ref('lnk_part_sale') }} as lk_p_s ON hp.PART_HK = lk_p_s.PART_HK
    JOIN {{ ref('hub_sale') }} as hs ON hs.SALE_HK = lk_p_s.SALE_HK
    JOIN {{ ref('sat_sale') }} as ss ON ss.SALE_HK = lk_p_s.SALE_HK

    group by 1,2,3,4

),
inventory_calculation AS (
    SELECT
    PART_ID AS PART_NO,
    PART_NAME,
    PART_DESC AS PART_DESCRIPTION,
    COUNTRY_NAME AS COUNTRY,
    AVG_UNIT_COST_PRICE,
    AVG_UNIT_SELL_PRICE,
    TOTAL_PURCHASED_QTY,
    TOTAL_SOLD_QTY,
    (TOTAL_PURCHASED_QTY - TOTAL_SOLD_QTY) AS AVAILABLE_QTY,
    CASE WHEN (TOTAL_PURCHASED_QTY - TOTAL_SOLD_QTY) < 0
    THEN TRUE 
    ELSE FALSE
    END AS OUT_OF_STOCK_FLG,
    CURRENT_TIMESTAMP() AS CREATED_ON,
    CURRENT_TIMESTAMP() AS LAST_UPDATED_ON
    FROM part_info

)

SELECT * FROM inventory_calculation