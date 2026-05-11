USE `__OVERTON_SCHEMA__`;

SELECT DATABASE() AS active_schema;

SELECT 'docs' AS table_name, COUNT(*) AS exact_rows FROM docs
UNION ALL
SELECT 'authors', COUNT(*) FROM authors
UNION ALL
SELECT 'topics', COUNT(*) FROM topics
UNION ALL
SELECT 'src_tags', COUNT(*) FROM src_tags
UNION ALL
SELECT 'sdg_cats', COUNT(*) FROM sdg_cats
UNION ALL
SELECT 'classifications', COUNT(*) FROM classifications
UNION ALL
SELECT 'entities', COUNT(*) FROM entities
UNION ALL
SELECT 'policy_src_region', COUNT(*) FROM policy_src_region
UNION ALL
SELECT 'policy_src_country', COUNT(*) FROM policy_src_country
UNION ALL
SELECT 'policy_src_type', COUNT(*) FROM policy_src_type
UNION ALL
SELECT 'policy_doc_ids_cited', COUNT(*) FROM policy_doc_ids_cited
UNION ALL
SELECT 'cited_dois', COUNT(*) FROM cited_dois
UNION ALL
SELECT 'self_ids', COUNT(*) FROM self_ids
UNION ALL
SELECT 'mentions_people', COUNT(*) FROM mentions_people
UNION ALL
SELECT 'policy_src_country_iso', COUNT(*) FROM policy_src_country_iso
UNION ALL
SELECT 'ref_ctx', COUNT(*) FROM ref_ctx
UNION ALL
SELECT 'cited_policy_dois', COUNT(*) FROM cited_policy_dois
UNION ALL
SELECT 'src_function', COUNT(*) FROM src_function
UNION ALL
SELECT 'src_sector', COUNT(*) FROM src_sector
UNION ALL
SELECT 'src_type', COUNT(*) FROM src_type
ORDER BY table_name;

SELECT 'docs' AS table_name, COUNT(*) - COUNT(DISTINCT policy_document_id) AS extra_rows_by_doc FROM docs
UNION ALL
SELECT 'policy_src_region', COUNT(*) - COUNT(DISTINCT policy_document_id) FROM policy_src_region
UNION ALL
SELECT 'policy_src_country', COUNT(*) - COUNT(DISTINCT policy_document_id) FROM policy_src_country
UNION ALL
SELECT 'policy_src_type', COUNT(*) - COUNT(DISTINCT policy_document_id) FROM policy_src_type
UNION ALL
SELECT 'policy_src_country_iso', COUNT(*) - COUNT(DISTINCT policy_document_id) FROM policy_src_country_iso
UNION ALL
SELECT 'src_function', COUNT(*) - COUNT(DISTINCT policy_document_id) FROM src_function
UNION ALL
SELECT 'src_sector', COUNT(*) - COUNT(DISTINCT policy_document_id) FROM src_sector
UNION ALL
SELECT 'src_type', COUNT(*) - COUNT(DISTINCT policy_document_id) FROM src_type
ORDER BY table_name;

SELECT 'policy_src_country_iso' AS table_name, COUNT(*) AS duplicate_groups, COALESCE(SUM(cnt - 1), 0) AS duplicate_rows
FROM (
  SELECT policy_document_id, policy_source_country_iso_codes, COUNT(*) AS cnt
  FROM policy_src_country_iso
  GROUP BY policy_document_id, policy_source_country_iso_codes
  HAVING COUNT(*) > 1
) q
UNION ALL
SELECT 'src_sector', COUNT(*), COALESCE(SUM(cnt - 1), 0)
FROM (
  SELECT policy_document_id, source_sector, COUNT(*) AS cnt
  FROM src_sector
  GROUP BY policy_document_id, source_sector
  HAVING COUNT(*) > 1
) q
UNION ALL
SELECT 'src_type', COUNT(*), COALESCE(SUM(cnt - 1), 0)
FROM (
  SELECT policy_document_id, source_type, COUNT(*) AS cnt
  FROM src_type
  GROUP BY policy_document_id, source_type
  HAVING COUNT(*) > 1
) q
ORDER BY table_name;

SELECT 'authors' AS table_name, COUNT(*) AS orphan_rows
FROM authors t LEFT JOIN docs d USING (policy_document_id)
WHERE d.policy_document_id IS NULL
UNION ALL
SELECT 'topics', COUNT(*) FROM topics t LEFT JOIN docs d USING (policy_document_id) WHERE d.policy_document_id IS NULL
UNION ALL
SELECT 'src_tags', COUNT(*) FROM src_tags t LEFT JOIN docs d USING (policy_document_id) WHERE d.policy_document_id IS NULL
UNION ALL
SELECT 'sdg_cats', COUNT(*) FROM sdg_cats t LEFT JOIN docs d USING (policy_document_id) WHERE d.policy_document_id IS NULL
UNION ALL
SELECT 'classifications', COUNT(*) FROM classifications t LEFT JOIN docs d USING (policy_document_id) WHERE d.policy_document_id IS NULL
UNION ALL
SELECT 'entities', COUNT(*) FROM entities t LEFT JOIN docs d USING (policy_document_id) WHERE d.policy_document_id IS NULL
UNION ALL
SELECT 'policy_src_region', COUNT(*) FROM policy_src_region t LEFT JOIN docs d USING (policy_document_id) WHERE d.policy_document_id IS NULL
UNION ALL
SELECT 'policy_src_country', COUNT(*) FROM policy_src_country t LEFT JOIN docs d USING (policy_document_id) WHERE d.policy_document_id IS NULL
UNION ALL
SELECT 'policy_src_type', COUNT(*) FROM policy_src_type t LEFT JOIN docs d USING (policy_document_id) WHERE d.policy_document_id IS NULL
UNION ALL
SELECT 'policy_doc_ids_cited', COUNT(*) FROM policy_doc_ids_cited t LEFT JOIN docs d USING (policy_document_id) WHERE d.policy_document_id IS NULL
UNION ALL
SELECT 'cited_dois', COUNT(*) FROM cited_dois t LEFT JOIN docs d USING (policy_document_id) WHERE d.policy_document_id IS NULL
UNION ALL
SELECT 'self_ids', COUNT(*) FROM self_ids t LEFT JOIN docs d USING (policy_document_id) WHERE d.policy_document_id IS NULL
UNION ALL
SELECT 'mentions_people', COUNT(*) FROM mentions_people t LEFT JOIN docs d USING (policy_document_id) WHERE d.policy_document_id IS NULL
UNION ALL
SELECT 'policy_src_country_iso', COUNT(*) FROM policy_src_country_iso t LEFT JOIN docs d USING (policy_document_id) WHERE d.policy_document_id IS NULL
UNION ALL
SELECT 'ref_ctx', COUNT(*) FROM ref_ctx t LEFT JOIN docs d USING (policy_document_id) WHERE d.policy_document_id IS NULL
UNION ALL
SELECT 'cited_policy_dois', COUNT(*) FROM cited_policy_dois t LEFT JOIN docs d USING (policy_document_id) WHERE d.policy_document_id IS NULL
UNION ALL
SELECT 'src_function', COUNT(*) FROM src_function t LEFT JOIN docs d USING (policy_document_id) WHERE d.policy_document_id IS NULL
UNION ALL
SELECT 'src_sector', COUNT(*) FROM src_sector t LEFT JOIN docs d USING (policy_document_id) WHERE d.policy_document_id IS NULL
UNION ALL
SELECT 'src_type', COUNT(*) FROM src_type t LEFT JOIN docs d USING (policy_document_id) WHERE d.policy_document_id IS NULL
ORDER BY table_name;

SELECT 'topics' AS table_name, policy_document_id, COUNT(*) AS rows_per_doc
FROM topics
GROUP BY policy_document_id
ORDER BY rows_per_doc DESC, policy_document_id
LIMIT 20;

SELECT 'ref_ctx' AS table_name, policy_document_id, COUNT(*) AS rows_per_doc
FROM ref_ctx
GROUP BY policy_document_id
ORDER BY rows_per_doc DESC, policy_document_id
LIMIT 20;
