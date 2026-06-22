import ast
import importlib.util
import os
import unittest
from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
os.environ.setdefault("MPLCONFIGDIR", str(PROJECT_ROOT / "tmp" / "matplotlib"))


def load_script(name):
    script_path = PROJECT_ROOT / "scripts" / name
    spec = importlib.util.spec_from_file_location(script_path.stem, script_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def main_guard_count(script_name):
    tree = ast.parse((PROJECT_ROOT / "scripts" / script_name).read_text())
    count = 0
    for node in ast.walk(tree):
        if not isinstance(node, ast.If):
            continue
        if (
            isinstance(node.test, ast.Compare)
            and isinstance(node.test.left, ast.Name)
            and node.test.left.id == "__name__"
            and len(node.test.comparators) == 1
            and isinstance(node.test.comparators[0], ast.Constant)
            and node.test.comparators[0].value == "__main__"
        ):
            count += 1
    return count


class PipelineHelperTests(unittest.TestCase):
    def test_stream_rows_remove_csv_breaking_characters(self):
        stream_feed = load_script("05_stream_feed.py")
        row = pd.Series(
            {
                "time": "2026-06-15T00:00:00Z",
                "latitude": 1.23,
                "longitude": 4.56,
                "depth": 7.8,
                "mag": 5.4,
                "place": "Line one,\nLine two",
                "type": "earthquake",
                "region": "Pakistan",
            }
        )

        line = stream_feed.format_row_for_stream(row)

        self.assertEqual(len(line.split(",")), 8)
        self.assertNotIn("\n", line)
        self.assertIn("Line one  Line two", line)

    def test_amdahl_formula(self):
        amdahl = load_script("07_amdahl.py")

        self.assertEqual(amdahl.theoretical_speedup(1), 1.0)
        self.assertAlmostEqual(amdahl.theoretical_speedup(4), 2.75862069, places=6)

    def test_scripts_have_one_main_guard(self):
        for script_name in ["05_stream_feed.py", "06_stream_alert.py", "07_amdahl.py"]:
            with self.subTest(script=script_name):
                self.assertEqual(main_guard_count(script_name), 1)


if __name__ == "__main__":
    unittest.main()
