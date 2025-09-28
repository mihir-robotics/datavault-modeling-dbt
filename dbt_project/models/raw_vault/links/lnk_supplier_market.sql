{{ config(
    materialized='incremental',
    unique_key='SUPPLIER_HK',
    post_hook=[
        "ALTER TABLE {{ this }} SET OPTIONS(description='Link table to show relationship', labels=[('type','link')])"
    ]
) }}

{%- set source_model = "stg_supplier" -%}
{%- set src_pk = "SUPPLIER_HK" -%}
{%- set src_fk = ["SUPPLIER_HK", "MARKET_HK"] -%}
{%- set src_ldts = "LOAD_DATE" -%}
{%- set src_source = "RECORD_SOURCE" -%}

{{ automate_dv.link(src_pk=src_pk, src_fk=src_fk, src_ldts=src_ldts,
                    src_source=src_source, source_model=source_model) }}