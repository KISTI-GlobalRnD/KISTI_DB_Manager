import json
import tempfile
import unittest
from pathlib import Path


from KISTI_DB_Manager.review_preview import write_review_preview_report


class TestReviewPreview(unittest.TestCase):
    def test_openalex_abstract_spotlight_uses_auto_except(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            data_dir = root / "data"
            data_dir.mkdir(parents=True, exist_ok=True)
            data_path = data_dir / "oa.jsonl"
            out_dir = root / "preview_out"
            cfg_path = root / "config.json"

            records = [
                {
                    "id": "https://openalex.org/W1",
                    "display_name": "Paper One",
                    "abstract_inverted_index": {
                        "alpha": [0],
                        "beta": [1],
                        "gamma": [2],
                        "delta": [3],
                    },
                },
                {
                    "id": "https://openalex.org/W2",
                    "display_name": "Paper Two",
                    "abstract_inverted_index": {
                        "epsilon": [0],
                        "zeta": [1],
                        "eta": [2],
                        "theta": [3],
                    },
                },
            ]
            data_path.write_text("\n".join(json.dumps(r) for r in records), encoding="utf-8")

            cfg = {
                "data_config": {
                    "PATH": str(data_dir),
                    "file_name": data_path.name,
                    "file_type": "jsonl",
                    "table_name": "openalex_works_20260225",
                    "KEY_SEP": "__",
                    "index_key": "id",
                    "auto_except": True,
                    "auto_except_sample_records": 2,
                    "auto_except_sample_max_sources": 1,
                    "auto_except_unique_key_threshold": 2,
                    "auto_except_min_observations": 1,
                    "auto_except_novelty_threshold": 1.0,
                }
            }
            cfg_path.write_text(json.dumps(cfg), encoding="utf-8")

            res = write_review_preview_report(
                config_path=str(cfg_path),
                out_dir=str(out_dir),
                max_records=2,
            )

            preview_json = Path(res["preview_json"])
            preview_html = Path(res["preview_html"])
            self.assertTrue(preview_json.exists())
            self.assertTrue(preview_html.exists())

            payload = json.loads(preview_json.read_text(encoding="utf-8"))
            meta = payload["meta"]
            previews = payload["previews"]

            self.assertTrue(meta["auto_except"]["enabled"])
            self.assertIn("abstract_inverted_index", meta["except_keys"])

            first = previews[0]
            self.assertEqual(first["spotlight"]["kind"], "openalex_abstract")
            self.assertEqual(first["spotlight"]["flatten_mode"], "excepted")
            self.assertTrue(first["spotlight"]["excepted"])
            self.assertIn("abstract_inverted_index", first["flatten"]["excepted"])
            self.assertEqual(first["spotlight"]["reconstructed"]["text_preview"], "alpha beta gamma delta")

            html_text = preview_html.read_text(encoding="utf-8")
            self.assertIn("OpenAlex Abstract Spotlight", html_text)
            self.assertIn("Handled as excepted value", html_text)


if __name__ == "__main__":
    unittest.main()
