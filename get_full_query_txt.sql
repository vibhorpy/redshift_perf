--To Find the Full Query Text :

SELECT query,
            LISTAGG(text)  WITHIN
     GROUP (
            ORDER BY sequence) AS full_query_text
     FROM stl_querytext
     WHERE sequence < 100
     GROUP BY query;

--replace(full_query_text,'\\n',' ')