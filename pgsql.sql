-- Step 1: Create the procedure
CREATE OR REPLACE PROCEDURE migrate_data(
    source_schema TEXT,
    target_schema TEXT,
    table_list TEXT[]
)
LANGUAGE plpgsql
AS $$
DECLARE
    source_table TEXT;
    target_table TEXT;
    identity_column TEXT;
    query TEXT;
BEGIN
    FOR source_table IN ARRAY table_list LOOP
        target_table := target_schema || '.' || source_table;
        
        -- Get the identity column name for the target table
        EXECUTE format('SELECT column_name
                        FROM information_schema.columns
                        WHERE table_schema = %L
                        AND table_name = %L
                        AND column_default LIKE %L',
                        target_schema, source_table, 'nextval%')
        INTO identity_column;
        
        IF identity_column IS NOT NULL THEN
            -- If an identity column is found, perform the migration with identity insert
            query := format('INSERT INTO %I.%I (%s) SELECT %s FROM %I.%I',
                             target_schema, source_table, identity_column,
                             identity_column, source_schema, source_table);
        ELSE
            -- If no identity column is found, perform the migration without identity insert
            query := format('INSERT INTO %I.%I SELECT * FROM %I.%I',
                             target_schema, source_table, source_schema, source_table);
        END IF;
        
        EXECUTE query;
    END LOOP;
END;
$$;

-- Step 2: Call the procedure with the desired source schema, target schema, and table list
CALL migrate_data('source_schema', 'target_schema', ARRAY['table1', 'table2', 'table3']);
