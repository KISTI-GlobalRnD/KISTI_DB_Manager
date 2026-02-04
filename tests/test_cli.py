import io
import unittest
from contextlib import redirect_stdout


from KISTI_DB_Manager.cli import main


class TestCLI(unittest.TestCase):
    def test_version(self):
        buf = io.StringIO()
        with redirect_stdout(buf):
            rc = main(["version"])
        self.assertEqual(rc, 0)
        self.assertTrue(buf.getvalue().strip())


if __name__ == "__main__":
    unittest.main()
