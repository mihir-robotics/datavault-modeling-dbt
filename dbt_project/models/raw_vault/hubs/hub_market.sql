{{ config(
    materialized='incremental',
    unique_key='MARKET_HK',
    post_hook=[
        "ALTER TABLE {{ this }} SET OPTIONS(description='Hub table for market business keys', labels=[('type','hub'),('entity','market')])"
    ]
) }}
{%- set source_model = "stg_market" -%}
{%- set src_pk = "MARKET_HK" -%}
{%- set src_nk = "MARKET_CODE" -%}
{%- set src_ldts = "LOAD_DATE" -%}
{%- set src_source = "RECORD_SOURCE" -%}

{{ automate_dv.hub(src_pk=src_pk, src_nk=src_nk, src_ldts=src_ldts,
                   src_source=src_source, source_model=source_model) }}