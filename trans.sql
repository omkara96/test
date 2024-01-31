-- Create a stored procedure to translate full-width katakana to half-width katakana
CREATE OR REPLACE PROCEDURE translate_katakana(
    IN input_string VARCHAR
)
LANGUAGE plpgsql
AS $$
DECLARE
    result_string VARCHAR := '';
    i INT := 1;
    character_code INT;
BEGIN
    -- Loop through each character in the input string
    WHILE i <= LENGTH(input_string) LOOP
        -- Get the Unicode code point of the current character
        character_code := ASCII(SUBSTRING(input_string FROM i FOR 1));

        -- Check if the character is within the range of full-width katakana
        IF character_code >= 0xFF66 AND character_code <= 0xFF9D THEN
            -- Translate full-width katakana to half-width katakana
            result_string := result_string || CHR(character_code - 0xFEE0);
        ELSE
            -- Keep non-katakana characters as they are
            result_string := result_string || SUBSTRING(input_string FROM i FOR 1);
        END IF;

        -- Move to the next character
        i := i + 1;
    END LOOP;

    -- Return the translated string
    RETURN result_string;
END;
$$;
