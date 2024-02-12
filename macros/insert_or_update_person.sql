{% macro insert_or_update_person(
    person_id,
    last_name,
    first_name,
    address,
    city,
    status
) %}

  -- Set target table name (replace with your actual table name)
  {% set target_table = 'persons' %}

  -- Construct INSERT query
  {% set insert_query %}
  INSERT INTO {{ target_table }} (
      PersonID,
      LastName,
      FirstName,
      Address,
      City,
      status
  )
  VALUES (
      {{ person_id }},
      {{ last_name }},
      {{ first_name }},
      {{ address }},
      {{ city }},
      {{ status }}
  );
  {% endset %}

  -- Construct UPDATE query
  {% set update_query %}
  UPDATE {{ target_table }}
  SET
      LastName = {{ last_name }},
      FirstName = {{ first_name }},
      Address = {{ address }},
      City = {{ city }},
      status = {{ status }}
  WHERE PersonID = {{ person_id }};
  {% endset %}

  -- Execute INSERT query first to handle potential inserts
  {% do run_query(insert_query) %}

  -- Then execute UPDATE query to handle updates or inserts if record exists
  {% do run_query(update_query) %}

{% endmacro %}
