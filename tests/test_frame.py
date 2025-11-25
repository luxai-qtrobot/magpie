import unittest
import time
from luxai.magpie.frames import Frame


class TestFrame(unittest.TestCase):

    def test_default_initialization(self):
        f = Frame()

        # gid must be auto-generated and non-empty
        self.assertIsNotNone(f.gid)
        self.assertIsInstance(f.gid, str)
        self.assertNotEqual(f.gid, "")

        # id defaults to 0
        self.assertEqual(f.id, 0)

        # name must be class name
        self.assertEqual(f.name, "Frame")

        # timestamp must exist
        self.assertIsNotNone(f.timestamp)
        self.assertNotEqual(f.timestamp, "")

    def test_custom_initialization(self):
        f = Frame(gid="ABC123", id=42)

        self.assertEqual(f.gid, "ABC123")
        self.assertEqual(f.id, 42)
        self.assertEqual(f.name, "Frame")
        self.assertIsNotNone(f.timestamp)

    def test_str_representation(self):
        f = Frame(gid="XYZ", id=7)
        self.assertEqual(str(f), "Frame#XYZ:7")

    def test_to_dict_contains_all_fields(self):
        f = Frame(gid="G1", id=1)
        d = f.to_dict()

        # keys must include all dataclass fields
        expected_keys = {"gid", "id", "name", "timestamp"}
        self.assertEqual(set(d.keys()), expected_keys)

        # values must match
        self.assertEqual(d["gid"], "G1")
        self.assertEqual(d["id"], 1)
        self.assertEqual(d["name"], "Frame")

    def test_from_dict_restores_fields(self):
        data = {
            "gid": "GGG",
            "id": 123,
            # name + timestamp must be ignored here
            "name": "SomethingElse",
            "timestamp": "old_timestamp",
        }

        f = Frame.from_dict(data)

        # should override gid & id
        self.assertEqual(f.gid, "GGG")
        self.assertEqual(f.id, 123)

        # name must be overridden by class name (not taken from dict)
        self.assertEqual(f.name, "Frame")

        # timestamp must be newly generated (not old one)
        self.assertNotEqual(f.timestamp, "old_timestamp")
        self.assertNotEqual(f.timestamp, "")

    def test_round_trip(self):
        f1 = Frame(gid="AAA", id=11)
        d = f1.to_dict()
        f2 = Frame.from_dict(d)

        self.assertEqual(f2.gid, "AAA")
        self.assertEqual(f2.id, 11)
        self.assertEqual(f2.name, "Frame")
        self.assertIsNotNone(f2.timestamp)


if __name__ == "__main__":
    unittest.main()
