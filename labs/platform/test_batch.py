import json
from pathlib import Path
import tempfile
import unittest
from batch import run


class BatchTests(unittest.TestCase):
    def test_repeat_and_export(self):
        with tempfile.TemporaryDirectory() as directory:
            source = Path(__file__).with_name("events.json")
            first = run(source, directory)
            second = run(source, directory)
            self.assertEqual(first, second)
            self.assertEqual(second, {"events": 2, "quantity": 16, "receipts": 6, "quarantined": 1})
            self.assertEqual(json.loads((Path(directory) / "events.json").read_text()), [
                {"event_id": "E1", "version": 2, "quantity": 12},
                {"event_id": "E2", "version": 1, "quantity": 4},
            ])
