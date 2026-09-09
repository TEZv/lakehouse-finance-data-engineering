import json
import unittest
from event_sink import connect, persist


class SinkTests(unittest.TestCase):
    def setUp(self):
        self.db = connect(":memory:")
        self.addCleanup(self.db.close)

    def send(self, offset, version=1, quantity=10):
        return persist(self.db, "events", 0, offset, json.dumps(
            {"event_id": "E1", "version": version, "quantity": quantity}
        ).encode())

    def test_correction_duplicate_and_stale_version(self):
        self.assertEqual(self.send(0), "applied")
        self.assertEqual(self.send(1), "ignored")
        self.assertEqual(self.send(2, 2, 12), "applied")
        self.assertEqual(self.send(3), "ignored")
        self.assertEqual(self.db.execute("SELECT * FROM events").fetchall(), [("E1", 2, 12)])

    def test_redelivery_after_sink_commit_does_not_duplicate_receipt(self):
        self.send(0)
        self.send(0)
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM receipts").fetchone()[0], 1)

    def test_conflicting_equal_version_is_quarantined(self):
        self.send(0)
        self.assertEqual(self.send(1, 1, 12), "quarantined")
        self.assertEqual(self.db.execute("SELECT quantity FROM events").fetchone()[0], 10)

    def test_invalid_contracts_are_recorded(self):
        for offset, raw in enumerate([b"broken", b"[]", b"null", b"\xff",
                b'{"event_id":"E1","version":true,"quantity":10}',
                b'{"event_id":"E1","version":1,"quantity":0}']):
            self.assertEqual(persist(self.db, "events", 0, offset, raw), "quarantined")
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM receipts").fetchone()[0], 6)

    def test_receipt_failure_rolls_back_state(self):
        self.db.execute("""CREATE TRIGGER fail_receipt BEFORE INSERT ON receipts
                           BEGIN SELECT RAISE(ABORT, 'simulated storage failure'); END""")
        with self.assertRaises(Exception):
            self.send(0)
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM events").fetchone()[0], 0)


if __name__ == "__main__":
    unittest.main()
