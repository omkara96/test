SELECT
    table_schema,
    table_name,
    STRING_AGG(column_name, ', ' ORDER BY ordinal_position) AS all_columns
FROM
    information_schema.columns
WHERE
    table_schema IN ('your_schema1', 'your_schema2') -- Add more schema names if needed
    AND table_name IN ('your_table1', 'your_table2') -- Add more table names if needed
    AND column_name NOT IN (
        SELECT
            identity_column
        FROM
            pg_table_def
        WHERE
            schemaname IN ('your_schema1', 'your_schema2') -- Add more schema names if needed
            AND tablename IN ('your_table1', 'your_table2') -- Add more table names if needed
            AND identity = 'YES'
    )
GROUP BY
    table_schema,
    table_name;
