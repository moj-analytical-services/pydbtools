WITH a AS
(
    SELECT col1, col2 FROM db.onw limit 100
),

b AS (
    SELECT c1, c2 FROM (SELECT * FROM db.two)
), 
/* Comment that breaks parser */

c AS (
SELECT x, y, z FROM db.three
)

SELECT *
FROM a, b, c