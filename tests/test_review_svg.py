import unittest


from KISTI_DB_Manager.review import TableInfo, render_simple_svg


class TestReviewSvg(unittest.TestCase):
    def test_render_simple_svg_uses_sql_primary_and_path_secondary(self):
        table_infos = [
            TableInfo(name_sql="openalex_works_20260225", name_original="openalex_works_20260225", columns=[{"name": "id"}]),
            TableInfo(
                name_sql="openalex_works_20260225__authorships",
                name_original="openalex_works_20260225__authorships",
                columns=[{"name": "id"}, {"name": "author_id"}],
            ),
            TableInfo(
                name_sql="openalex_works_20260225__excepted__abstract_inverted_index",
                name_original="openalex_works_20260225__excepted__abstract_inverted_index",
                columns=[{"name": "id"}, {"name": "value"}],
            ),
        ]

        svg = render_simple_svg(
            base_table="openalex_works_20260225",
            table_infos=table_infos,
            key_sep="__",
        )

        self.assertIn("openalex_works_20260225__authorships", svg)
        self.assertIn(">authorships</text>", svg)
        self.assertIn(">excepted/abstract_inverted_index</text>", svg)
        self.assertIn('fill="#FFF8C5"', svg)
        self.assertIn('class="edge-card"', svg)
        self.assertIn("1:N", svg)


if __name__ == "__main__":
    unittest.main()
