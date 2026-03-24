import unittest

from common.cli import parse_args


class CliTests(unittest.TestCase):
    def test_sample_defaults_to_none(self):
        args = parse_args(["movie"])
        self.assertIsNone(args.sample)

    def test_sample_accepts_explicit_limit(self):
        args = parse_args(["sport", "--sample", "15"])
        self.assertEqual(args.sample, 15)


if __name__ == "__main__":
    unittest.main()
