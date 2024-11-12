
# clear the cache and run query to check performance of query
SET use_cached_result = false;

To create the table try to:
CREATE TABLE (without CLUSTER BY)
ALTER TABLE CLUSTER BY
OPTIMIZE
It may run faster then CREATE with CLUSTER BY + OPTIMIZE
 
 