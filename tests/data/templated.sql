SELECT *
FROM {{ db_name }}.{{ table_name }}
WHERE category IN (
    {%- for v in values %}{{v}}{%- if not loop.last -%},{% endif %}{% endfor -%}
)
