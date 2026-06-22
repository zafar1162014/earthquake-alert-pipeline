import importlib.util
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def load_script(name):
    script_path = PROJECT_ROOT / "scripts" / name
    spec = importlib.util.spec_from_file_location(script_path.stem, script_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class PipelineConfigTests(unittest.TestCase):
    def test_batch_defaults_target_hdfs(self):
        batch = load_script("03_batch_analysis.py")

        config = batch.parse_args([])

        self.assertEqual(config.input, "/earthquake/input/earthquakes.csv")
        self.assertEqual(config.output_base, "/earthquake/output/batch")
        self.assertIsNone(config.master)

    def test_batch_local_paths_are_absolute_paths(self):
        batch = load_script("03_batch_analysis.py")

        input_path = PROJECT_ROOT / "data" / "earthquakes.csv"
        config = batch.parse_args(
            [
                "--master",
                "local[*]",
                "--input",
                str(input_path),
                "--output-base",
                "output/batch",
            ]
        )

        self.assertEqual(config.master, "local[*]")
        self.assertEqual(config.input, str(input_path.resolve()))
        self.assertTrue(config.output_base.startswith(str(PROJECT_ROOT)))
        self.assertTrue(config.output_base.endswith("/output/batch"))

    def test_hotspot_defaults_target_hdfs(self):
        hotspot = load_script("04_hotspot.py")

        config = hotspot.parse_args([])

        self.assertEqual(config.input, "/earthquake/input/earthquakes.csv")
        self.assertEqual(config.output_base, "/earthquake/output/hotspots")
        self.assertIsNone(config.master)

    def test_hotspot_builds_output_paths_from_base(self):
        hotspot = load_script("04_hotspot.py")

        config = hotspot.parse_args(["--output-base", "output/hotspots"])

        self.assertTrue(config.global_output_path.endswith("/output/hotspots/global"))
        self.assertTrue(config.pakistan_output_path.endswith("/output/hotspots/pakistan"))


if __name__ == "__main__":
    unittest.main()
