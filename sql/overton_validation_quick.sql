USE `__OVERTON_SCHEMA__`;

SELECT DATABASE() AS active_schema;

SELECT TABLE_TYPE, COUNT(*) AS object_count
FROM information_schema.tables
WHERE table_schema = '__OVERTON_SCHEMA__'
GROUP BY TABLE_TYPE
ORDER BY TABLE_TYPE;

SELECT
  TABLE_NAME,
  TABLE_ROWS AS estimated_rows,
  ROUND(DATA_LENGTH / 1024 / 1024, 1) AS data_mb,
  ROUND(INDEX_LENGTH / 1024 / 1024, 1) AS index_mb,
  ROUND((DATA_LENGTH + INDEX_LENGTH) / 1024 / 1024, 1) AS total_mb
FROM information_schema.tables
WHERE table_schema = '__OVERTON_SCHEMA__'
  AND TABLE_TYPE = 'BASE TABLE'
ORDER BY TABLE_NAME;

SELECT
  TABLE_NAME,
  GROUP_CONCAT(DISTINCT INDEX_NAME ORDER BY INDEX_NAME SEPARATOR ', ') AS indexes
FROM information_schema.statistics
WHERE table_schema = '__OVERTON_SCHEMA__'
GROUP BY TABLE_NAME
ORDER BY TABLE_NAME;

SELECT TABLE_NAME, TABLE_TYPE
FROM information_schema.tables
WHERE table_schema = '__OVERTON_SCHEMA__'
  AND TABLE_NAME IN (
    'docs', 'authors', 'topics', 'src_tags', 'sdg_cats', 'classifications', 'entities',
    'policy_src_region', 'policy_src_country', 'policy_src_type', 'policy_doc_ids_cited',
    'cited_dois', 'self_ids', 'mentions_people', 'policy_src_country_iso', 'ref_ctx',
    'cited_policy_dois', 'src_function', 'src_sector', 'src_type',
    '01__overton_20260130_raw__main',
    '02__overton_20260130_raw__sub__authors',
    '03__overton_20260130_raw__sub__topics',
    '04__overton_20260130_raw__sub__source_tags',
    '05__overton_20260130_raw__sub__sdgcategories',
    '06__overton_20260130_raw__sub__classifications',
    '07__overton_20260130_raw__sub__entities',
    '08__overton_20260130_raw__sub__policy_source_region',
    '09__overton_20260130_raw__sub__policy_source_country',
    '10__overton_20260130_raw__sub__policy_source_type',
    '11__overton_20260130_raw__sub__policy_document_ids_cited',
    '12__overton_20260130_raw__sub__dois_cited',
    '13__overton_20260130_raw__sub__self_identifiers',
    '14__overton_20260130_raw__sub__mentions_people',
    '15__overton_20260130_raw__sub__policy_source_country_iso_codes',
    '16__overton_20260130_raw__sub__ref_contexts',
    '17__overton_20260130_raw__sub__cited_policy_document_dois',
    '18__overton_20260130_raw__sub__source_function',
    '19__overton_20260130_raw__sub__source_sector',
    '20__overton_20260130_raw__sub__source_type'
  )
ORDER BY TABLE_NAME;
