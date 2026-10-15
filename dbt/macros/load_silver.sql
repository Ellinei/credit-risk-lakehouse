{#
    Load one ingest_date of Glue's silver Parquet into Delta tables <catalog>.silver.<table>.

        dbt run-operation load_silver --args '{"ingest_date": "2026-10-01"}'
        dbt run-operation load_silver --args '{"ingest_date": "2026-10-01",
                                              "silver_root": "s3://my-lake/silver"}'

    silver_root is s3://<bucket>/silver (external location mode) or
    /Volumes/<catalog>/landing/silver (Volume fallback). Same SQL either way.

    INSERT ... REPLACE WHERE ingest_date = <day> swaps that day's rows atomically. Running
    it twice for the same day gives the same table, which mirrors Glue's dynamic
    partition overwrite on the S3 side. A table with no rows for the day just ends up
    with no rows for the day.

    This is loading, not transformation: columns and types are exactly Glue's output,
    plus _loaded_at for incremental watermarks downstream.
#}
{% macro load_silver(ingest_date, silver_root=none, tables=none) %}
    {% if not ingest_date or not modules.re.match('^\d{4}-\d{2}-\d{2}$', ingest_date | string) %}
        {{ exceptions.raise_compiler_error(
            "load_silver needs ingest_date as YYYY-MM-DD, got: " ~ ingest_date) }}
    {% endif %}
    {% set root = (silver_root or var('silver_root')) | string %}
    {% set root = root.rstrip('/') %}
    {% set schema_name = target.database ~ '.silver' %}
    {% do run_query('create schema if not exists ' ~ schema_name) %}

    {% for table in (tables or var('silver_tables')) %}
        {% set relation = schema_name ~ '.' ~ table %}
        {% set files %}
            read_files(
                '{{ root }}/{{ table }}/',
                format => 'parquet',
                schemaEvolutionMode => 'none'
            )
        {% endset %}
        {% set day_rows %}
            select
                * except (ingest_date),
                cast(ingest_date as date) as ingest_date,
                current_timestamp() as _loaded_at
            from {{ files }}
            where cast(ingest_date as date) = date'{{ ingest_date }}'
        {% endset %}

        {% do run_query('create table if not exists ' ~ relation ~ ' as ' ~ day_rows ~ ' limit 0') %}
        {% do run_query(
            'insert into ' ~ relation
            ~ " replace where ingest_date = date'" ~ ingest_date ~ "' "
            ~ day_rows
        ) %}
        {% set counted = run_query(
            'select count(*) from ' ~ relation
            ~ " where ingest_date = date'" ~ ingest_date ~ "'"
        ) %}
        {{ log('silver.' ~ table ~ ': ' ~ counted.columns[0].values()[0]
               ~ ' rows for ingest_date ' ~ ingest_date, info=True) }}
    {% endfor %}
{% endmacro %}
