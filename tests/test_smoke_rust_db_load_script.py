import unittest

from KISTI_DB_Manager import rust_db_smoke


def _load_script_module():
    return rust_db_smoke


class TestSmokeRustDbLoadScript(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.module = _load_script_module()

    def test_drop_targets_are_limited_to_base_and_subtables(self):
        targets = self.module._smoke_drop_targets(
            [
                "kisti_smoke",
                "kisti_smoke__authorships",
                "kisti_smoke__topics__nested",
                "kisti_smoke_backup",
                "kisti_smokex",
                "other",
            ],
            base_table="kisti_smoke",
        )

        self.assertEqual(
            targets,
            [
                "kisti_smoke",
                "kisti_smoke__authorships",
                "kisti_smoke__topics__nested",
            ],
        )

    def test_safe_identifier_prefix_normalizes_user_input(self):
        self.assertEqual(self.module._safe_identifier_prefix("123 bad-prefix!!"), "t_123_bad_prefix")
        self.assertEqual(self.module._safe_identifier_prefix(""), "kisti_rust_db_smoke")


if __name__ == "__main__":
    unittest.main()
