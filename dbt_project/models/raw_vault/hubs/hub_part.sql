{{ config(
    materialized='incremental',
    unique_key='PART_HK',
    post_hook=[
        "ALTER TABLE {{ this }} SET OPTIONS(description='Hub table for part business keys', labels=[('type','hub'),('entity','part')])"
    ]
) }}
{%- set source_model = "stg_part" -%}
{%- set src_pk = "PART_HK" -%}
{%- set src_nk = "PART_ID" -%}
{%- set src_ldts = "LOAD_DATE" -%}
{%- set src_source = "RECORD_SOURCE" -%}

{{ automate_dv.hub(src_pk=src_pk, src_nk=src_nk, src_ldts=src_ldts,
                   src_source=src_source, source_model=source_model) }}