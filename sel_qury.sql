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
            column_name
        FROM
            information_schema.columns
        WHERE
            table_schema IN ('your_schema1', 'your_schema2') -- Add more schema names if needed
            AND table_name IN ('your_table1', 'your_table2') -- Add more table names if needed
            AND is_identity = 'YES'
    )
GROUP BY
    table_schema,
    table_name;
