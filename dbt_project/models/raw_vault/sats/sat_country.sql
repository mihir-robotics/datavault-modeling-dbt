{{ config(
    materialized='incremental',
    unique_key='COUNTRY_HK',
    post_hook=[
        "ALTER TABLE {{ this }} SET OPTIONS(description='Satellite table for country attributes', labels=[('type','satellite'),('entity','country')])"
    ]
) }}
{%- set source_model = "stg_country" -%}
{%- set src_pk = "COUNTRY_HK" -%}
{%- set src_hashdiff = "COUNTRY_HASHDIFF" -%}
{%- set src_payload = [
'COUNTRY_NAME',		
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