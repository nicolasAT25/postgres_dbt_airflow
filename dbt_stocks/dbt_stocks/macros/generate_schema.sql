{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}
    {%- set env = env_var('DBT_ENV_NAME') -%}
    {%- if custom_schema_name is none or env == 'prod' -%} {# change to prod if want a custom schema. This applies if dev and prod environments are defined. #}

        {{ default_schema }}

    {%- else -%}

        {{ custom_schema_name | trim }}

    {%- endif -%}

{%- endmacro %}