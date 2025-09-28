{{ config(
    materialized='incremental',
    unique_key='ORDER_HK',
    post_hook=[
        "ALTER TABLE {{ this }} SET OPTIONS(description='Link table to show relationship', labels=[('type','link')])"
    ]
) }}

{%- set source_model = "stg_order" -%}
{%- set src_pk = "ORDER_HK" -%}
{%- set src_fk = ["PART_HK", "ORDER_HK"] -%}
{%- set src_ldts = "LOAD_DATE" -%}
{%- set src_source = "RECORD_SOURCE" -%}

{{ automate_dv.link(src_pk=src_pk, src_fk=src_fk, src_ldts=src_ldts,
                    src_source=src_source, source_model=source_model) }}