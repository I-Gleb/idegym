-- Preserve the additive column and indexes when reverting the schema revision.
-- The Python migration refuses downgrade after any payload has expired.
SELECT 1;
