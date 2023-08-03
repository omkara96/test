CREATE OR REPLACE PROCEDURE migrate_data_without_identity(
    source_schema TEXT,
    target_schema TEXT,
    table_name TEXT
)
LANGUAGE plpgsql
AS $$
DECLARE
    target_identity_col TEXT;
BEGIN
    -- Find the identity column in the target table
    SELECT column_name
    INTO target_identity_col
    FROM information_schema.columns
    WHERE table_schema = target_schema
        AND table_name = table_name
        AND is_identity = 'YES';

    IF target_identity_col IS NULL THEN
        RAISE EXCEPTION 'The target table does not have an identity column';
    END IF;

    -- Generate the dynamic SQL query for the migration
    EXECUTE format('INSERT INTO %I.%I (%I) SELECT %I FROM %I.%I', target_schema, table_name, target_identity_col, target_identity_col, source_schema, table_name);

    -- Display the message once the migration is complete
    RAISE INFO 'Data migration completed successfully.';
END;
$$;
