SELECT
    table_schema,
    table_name,
    LISTAGG(column_name, ', ') WITHIN GROUP (ORDER BY ordinal_position) AS all_columns
FROM
    information_schema.columns
WHERE
    table_schema IN ('your_schema1', 'your_schema2') -- Add more schema names if needed
    AND table_name IN ('your_table1', 'your_table2') -- Add more table names if needed
    AND column_name NOT IN (
        SELECT
            attname
        FROM
            pg_attribute AS attr
            JOIN pg_class AS cls ON attr.attrelid = cls.oid
            JOIN pg_namespace AS ns ON cls.relnamespace = ns.oid
        WHERE
            ns.nspname IN ('your_schema1', 'your_schema2') -- Add more schema names if needed
            AND cls.relname IN ('your_table1', 'your_table2') -- Add more table names if needed
            AND attnum > 0
            AND attidentity = 'd'
    )
GROUP BY
    table_schema,
    table_name;
