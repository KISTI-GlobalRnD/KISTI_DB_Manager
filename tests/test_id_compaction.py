import unittest


from KISTI_DB_Manager.id_compaction import IdCompactionError, IdCompactor, normalize_id_compaction_config


class TestIdCompaction(unittest.TestCase):
    def test_disabled_config_is_default(self):
        cfg = normalize_id_compaction_config({})
        self.assertFalse(cfg["enabled"])
        compactor = IdCompactor.from_config({}, sep="__", index_key="id")
        row = {"id": "https://openalex.org/W1"}
        self.assertIs(compactor.compact_row(row, table_name="works"), row)

    def test_openalex_semantic_column_strip(self):
        compactor = IdCompactor.from_config(
            {"id_compaction": {"enabled": True}},
            sep="__",
            index_key="id",
        )
        row = {
            "id": "https://openalex.org/W1",
            "author_id": "https://openalex.org/A1",
            "primary_location__source__id": "https://openalex.org/S1",
        }

        out = compactor.compact_row(row, table_name="works")

        self.assertEqual(out["id"], "W1")
        self.assertEqual(out["author_openalex_id"], "A1")
        self.assertEqual(out["primary_location__source_openalex_id"], "S1")
        self.assertNotIn("author_id", out)
        summary = compactor.summary()
        self.assertEqual(summary["counts"]["works.author_openalex_id"], 1)
        self.assertIn("description", summary["columns"][0])
        self.assertIn("rules_version", summary)
        self.assertIn("rules_hash", summary)

    def test_ror_doi_orcid_strip_when_column_semantic_is_clear(self):
        compactor = IdCompactor.from_config(
            {"id_compaction": {"enabled": True}},
            sep="__",
            index_key="id",
        )
        row = {
            "ror": "https://ror.org/03yrm5c26",
            "ids__doi": "https://doi.org/10.123/ABC",
            "ids__orcid": "https://orcid.org/0000-0002-1825-0097",
        }

        out = compactor.compact_row(row, table_name="authors")

        self.assertEqual(out["ror_id"], "03yrm5c26")
        self.assertEqual(out["ids__doi_id"], "10.123/ABC")
        self.assertEqual(out["ids__orcid_id"], "0000-0002-1825-0097")

    def test_ambiguous_url_like_column_is_preserved(self):
        compactor = IdCompactor.from_config(
            {"id_compaction": {"enabled": True}},
            sep="__",
            index_key="id",
        )
        row = {"landing_page_url": "https://openalex.org/W1"}

        out = compactor.compact_row(row, table_name="works")

        self.assertEqual(out["landing_page_url"], "https://openalex.org/W1")
        self.assertEqual(compactor.summary()["ambiguous_columns"]["works.landing_page_url"], 1)

    def test_semantic_column_rename_is_stable_for_url_null_and_bare_values(self):
        compactor = IdCompactor.from_config(
            {"id_compaction": {"enabled": True}},
            sep="__",
            index_key="id",
        )

        rows = compactor.compact_rows(
            [
                {"author_id": "https://openalex.org/A1"},
                {"author_id": None},
                {"author_id": "A2"},
            ],
            table_name="works_authorships",
        )

        self.assertEqual(rows[0], {"author_openalex_id": "A1"})
        self.assertEqual(rows[1], {"author_openalex_id": None})
        self.assertEqual(rows[2], {"author_openalex_id": "A2"})
        self.assertEqual(compactor.summary()["counts"]["works_authorships.author_openalex_id"], 3)

    def test_collision_raises_when_two_nonblank_values_map_to_same_column(self):
        compactor = IdCompactor.from_config(
            {"id_compaction": {"enabled": True}},
            sep="__",
            index_key="id",
        )

        with self.assertRaises(IdCompactionError):
            compactor.compact_row(
                {
                    "author_id": "https://openalex.org/A1",
                    "author_openalex_id": "A2",
                },
                table_name="works_authorships",
            )

    def test_collision_allows_blank_and_nonblank_to_converge(self):
        compactor = IdCompactor.from_config(
            {"id_compaction": {"enabled": True}},
            sep="__",
            index_key="id",
        )

        out = compactor.compact_row(
            {
                "author_openalex_id": "A2",
                "author_id": None,
            },
            table_name="works_authorships",
        )

        self.assertEqual(out, {"author_openalex_id": "A2"})

    def test_namespace_conflict_raises_by_default(self):
        compactor = IdCompactor.from_config(
            {"id_compaction": {"enabled": True}},
            sep="__",
            index_key="id",
        )

        with self.assertRaises(IdCompactionError):
            compactor.compact_row(
                {"author_id": "https://ror.org/03yrm5c26"},
                table_name="works_authorships",
            )

    def test_unknown_ids_column_is_not_inferred_from_openalex_value_only(self):
        compactor = IdCompactor.from_config(
            {"id_compaction": {"enabled": True}},
            sep="__",
            index_key="id",
        )

        out = compactor.compact_row({"external_ids": "https://openalex.org/A1"}, table_name="works")

        self.assertEqual(out["external_ids"], "https://openalex.org/A1")
        self.assertEqual(compactor.summary()["ambiguous_columns"]["works.external_ids"], 1)

    def test_topic_share_and_primary_topic_ids_are_topic_ids(self):
        compactor = IdCompactor.from_config(
            {"id_compaction": {"enabled": True}},
            sep="__",
            index_key="id",
        )

        out = compactor.compact_row({"topic_share__id": "https://openalex.org/T13303"}, table_name="sources_topic_share")
        out_primary = compactor.compact_row({"primary_topic__id": "https://openalex.org/T11113"}, table_name="works")

        self.assertEqual(out["topic_openalex_id"], "T13303")
        self.assertEqual(out_primary["topic_openalex_id"], "T11113")
        self.assertNotIn("topic_share__id", out)
        self.assertNotIn("primary_topic__id", out_primary)


if __name__ == "__main__":
    unittest.main()
