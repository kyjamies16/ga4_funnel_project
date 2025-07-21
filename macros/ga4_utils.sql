-- Reusable macro to extract event parameter values from GA4 events.

{% macro get_event_param_value(key, type) %}
  (
    SELECT value.{{ type }}_value
    FROM UNNEST(event_params)
    WHERE key = '{{ key }}'
  )
{% endmacro %}