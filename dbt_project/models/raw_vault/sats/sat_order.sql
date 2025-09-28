{{ config(
    materialized='incremental',
    unique_key='ORDER_HK',
    post_hook=[
        "ALTER TABLE {{ this }} SET OPTIONS(description='Satellite table for order attributes', labels=[('type','satellite'),('entity','order')])"
    ]
) }}
{%- set source_model = "stg_order" -%}
{%- set src_pk = "ORDER_HK" -%}
{%- set src_hashdiff = "ORDER_HASHDIFF" -%}
{%- set src_payload = [
'PURCHASED_QTY',	
'UNIT_COST_PRICE',	
'CREATE_TS',	
'LAST_UPDT_TS'
] -%}
{%- set src_eff = "EFFECTIVE_FROM" -%}
{%- set src_ldts = "LOAD_DATE" -%}
{%- set src_source = "RECORD_SOURCE" -%}

{{ automate_dv.sat(src_pk=src_pk, src_hashdiff=src_hashdiff,
                   src_payload=src_payload, src_eff=src_eff,
                   src_ldts=src_ldts, src_source=src_source,
                   source_model=source_model) }}