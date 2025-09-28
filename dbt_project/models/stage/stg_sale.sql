{{ config(materialized='view') }}
{%- set yaml_metadata -%}
source_model: 
    source: 'RW_SALE'
derived_columns:
  RECORD_SOURCE: "'RW_SALE'"
  LOAD_DATE: 'LAST_UPDT_TS'
  EFFECTIVE_FROM: 'CREATE_TS'
hashed_columns:
    SALE_HK:
      - 'SALE_ID'

    PART_HK:
      - 'PART_ID'

    BUYER_HK:
      - 'BUYER_ID'

    SALE_HASHDIFF:
      is_hashdiff: true
      columns:
        - 'SOLD_QTY'
        - 'UNIT_SELL_PRICE'
        - 'CREATE_TS'
        - 'LAST_UPDT_TS'

{%- endset -%}
{% set metadata_dict = fromyaml(yaml_metadata) %}
{% set source_model = metadata_dict['source_model'] %}
{% set derived_columns = metadata_dict['derived_columns'] %}
{% set hashed_columns = metadata_dict['hashed_columns'] %}
{{ automate_dv.stage(include_source_columns=true,
                     source_model=source_model,
                     derived_columns=derived_columns,
                     hashed_columns=hashed_columns,
                     ranked_columns=none) }}