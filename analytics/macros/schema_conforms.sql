{% test schema_conforms(model, types) %}
{% set actual = {} %}
{% set invalid = [] %}
{% if execute %}
  {# DESCRIBE supports three-part external catalog sources as well as native tables. #}
  {% for column in run_query('DESCRIBE ' ~ model).rows %}
    {% do actual.update({column[0]: column[1].upper().split('(')[0]}) %}
  {% endfor %}
  {% for name, allowed in types.items() %}
    {% if actual.get(name, 'MISSING') not in allowed %}
      {% do invalid.append(name ~ ':' ~ actual.get(name, 'MISSING')) %}
    {% endif %}
  {% endfor %}
{% endif %}
select '{{ invalid | join(", ") }}' as invalid_columns
where {{ 'true' if invalid else 'false' }}
{% endtest %}
