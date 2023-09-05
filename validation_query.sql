SELECT column_name,
       CASE WHEN is_nullable = 'YES' THEN 'NULLABLE' ELSE 'NOT NULLABLE' END AS column_type
FROM information_schema.columns
WHERE table_name = 'your_table_name';


SELECT column_name
FROM information_schema.columns
WHERE table_name = 'your_table_name'
GROUP BY column_name
HAVING COUNT(CASE WHEN column_name IS NULL THEN 1 ELSE NULL END) = COUNT(*);
