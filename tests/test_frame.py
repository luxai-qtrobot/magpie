import unittest
from dataclasses import dataclass, field, fields
from luxai.magpie.frames import Frame


class TestFrameBasics(unittest.TestCase):
    def test_default_init_sets_metadata(self):
        f = Frame()

        # gid should be auto-generated and truthy
        self.assertIsNotNone(f.gid)
        self.assertTrue(f.gid)

        # id should default to 0
        self.assertEqual(f.id, 0)

        # name should be class name
        self.assertEqual(f.name, "Frame")

        # timestamp should be a non-empty string
        self.assertIsInstance(f.timestamp, str)
        self.assertNotEqual(f.timestamp, "")

    def test_custom_gid_and_id_respected(self):
        f = Frame(gid="MYGID", id=5)
        self.assertEqual(f.gid, "MYGID")
        self.assertEqual(f.id, 5)

    def test_gid_falsy_replaced_id_falsy_zero(self):
        # gid is falsy -> replaced
        f1 = Frame(gid=None, id=None)
        self.assertIsNotNone(f1.gid)
        self.assertTrue(f1.gid)
        self.assertEqual(f1.id, 0)

        # gid is falsy (0) -> replaced
        f2 = Frame(gid=0, id=0)
        self.assertIsNotNone(f2.gid)
        self.assertTrue(f2.gid)
        self.assertEqual(f2.id, 0)

        # gid truthy, id truthy -> kept
        f3 = Frame(gid=123, id=-1)
        self.assertEqual(f3.gid, 123)
        self.assertEqual(f3.id, -1)

    def test_str_representation(self):
        f = Frame(gid="G1", id=7)
        s = str(f)
        self.assertIn("Frame#G1:7", s)


class TestFrameRegistry(unittest.TestCase):
    def test_base_class_not_registered(self):
        # Frame itself must not be present as a key
        self.assertNotIn("Frame", Frame._registry)

    def test_subclass_registration(self):
        class MyTestFrame(Frame):
            pass

        self.assertIn("MyTestFrame", Frame._registry)
        self.assertIs(Frame._registry["MyTestFrame"], MyTestFrame)

    def test_multiple_subclasses_all_registered(self):
        class MyFrameA(Frame):
            pass

        class MyFrameB(Frame):
            pass

        self.assertIn("MyFrameA", Frame._registry)
        self.assertIn("MyFrameB", Frame._registry)
        self.assertIs(Frame._registry["MyFrameA"], MyFrameA)
        self.assertIs(Frame._registry["MyFrameB"], MyFrameB)


class TestFrameToDict(unittest.TestCase):
    def test_to_dict_includes_all_fields(self):
        f = Frame(gid="G1", id=2)
        d = f.to_dict()

        self.assertIn("gid", d)
        self.assertIn("id", d)
        self.assertIn("name", d)
        self.assertIn("timestamp", d)

        self.assertEqual(d["gid"], "G1")
        self.assertEqual(d["id"], 2)
        self.assertEqual(d["name"], "Frame")
        self.assertEqual(d["timestamp"], f.timestamp)

    def test_to_dict_includes_subclass_fields(self):
        @dataclass
        class SubFrame(Frame):
            extra: int = 42

        sf = SubFrame()
        d = sf.to_dict()

        self.assertIn("gid", d)
        self.assertIn("id", d)
        self.assertIn("name", d)
        self.assertIn("timestamp", d)
        self.assertIn("extra", d)
        self.assertEqual(d["extra"], 42)
        self.assertEqual(d["name"], "SubFrame")


class TestFrameFromDictBase(unittest.TestCase):
    def test_from_dict_with_unknown_name_falls_back_to_frame(self):
        data = {
            "gid": "GGG",
            "id": 123,
            "name": "SomethingThatDoesNotExistXYZ",
            "timestamp": "old_timestamp",
            "extra": "ignored",
        }

        f = Frame.from_dict(data)

        # Should create a plain Frame, not a subclass
        self.assertIsInstance(f, Frame)
        self.assertEqual(f.gid, "GGG")
        self.assertEqual(f.id, 123)
        # name must be overridden by class name
        self.assertEqual(f.name, "Frame")
        # timestamp must be regenerated, not taken from dict
        self.assertNotEqual(f.timestamp, "old_timestamp")
        self.assertIsInstance(f.timestamp, str)
        self.assertNotEqual(f.timestamp, "")

    def test_from_dict_without_name_falls_back_to_frame(self):
        data = {
            "gid": "HHH",
            "id": 9,
            "timestamp": "old_timestamp",
        }

        f = Frame.from_dict(data)

        self.assertIsInstance(f, Frame)
        self.assertEqual(f.gid, "HHH")
        self.assertEqual(f.id, 9)
        self.assertEqual(f.name, "Frame")
        self.assertNotEqual(f.timestamp, "old_timestamp")

    def test_from_dict_with_non_string_name_ignored_for_dispatch(self):
        data = {
            "gid": "III",
            "id": 11,
            "name": 1234,   # non-string: dispatch should be skipped
            "timestamp": "old_timestamp",
        }

        f = Frame.from_dict(data)

        self.assertIsInstance(f, Frame)
        self.assertEqual(f.gid, "III")
        self.assertEqual(f.id, 11)
        self.assertEqual(f.name, "Frame")
        self.assertNotEqual(f.timestamp, "old_timestamp")


class TestFrameFromDictSubclass(unittest.TestCase):
    def test_subclass_from_dict_ignores_name_and_timestamp(self):
        @dataclass
        class MyFrame(Frame):
            value: int = 0

        data = {
            "gid": "AAA",
            "id": 1,
            "name": "WrongName",
            "timestamp": "old_timestamp",
            "value": 42,
            "extra": "ignored",
        }

        f = MyFrame.from_dict(data)

        self.assertIsInstance(f, MyFrame)
        # gid and id taken from dict
        self.assertEqual(f.gid, "AAA")
        self.assertEqual(f.id, 1)
        # subclass field taken from dict
        self.assertEqual(f.value, 42)
        # name overridden by class name
        self.assertEqual(f.name, "MyFrame")
        # timestamp regenerated
        self.assertNotEqual(f.timestamp, "old_timestamp")
        self.assertIsInstance(f.timestamp, str)
        self.assertNotEqual(f.timestamp, "")

    def test_frame_from_dict_dispatches_to_registered_subclass(self):
        @dataclass
        class MyDispatchedFrame(Frame):
            value: int = 0

        # Ensure it's registered
        self.assertIn("MyDispatchedFrame", Frame._registry)

        # Simulate serialized dict coming over the wire
        data = {
            "gid": "JJJ",
            "id": 7,
            "name": "MyDispatchedFrame",
            "timestamp": "old_timestamp",
            "value": 99,
        }

        f = Frame.from_dict(data)

        self.assertIsInstance(f, MyDispatchedFrame)
        self.assertEqual(f.gid, "JJJ")
        self.assertEqual(f.id, 7)
        self.assertEqual(f.value, 99)
        self.assertEqual(f.name, "MyDispatchedFrame")
        self.assertNotEqual(f.timestamp, "old_timestamp")

    def test_subclass_from_dict_does_not_re_dispatch(self):
        @dataclass
        class MyNoRedispatch(Frame):
            value: int = 0

        data = {
            "gid": "KKK",
            "id": 8,
            "name": "SomeOtherName",
            "timestamp": "old_timestamp",
            "value": 5,
        }

        # Calling on the subclass directly should NOT consider name for dispatch;
        # it should just behave as a normal initializer.
        f = MyNoRedispatch.from_dict(data)

        self.assertIsInstance(f, MyNoRedispatch)
        self.assertEqual(f.gid, "KKK")
        self.assertEqual(f.id, 8)
        self.assertEqual(f.value, 5)
        self.assertEqual(f.name, "MyNoRedispatch")
        self.assertNotEqual(f.timestamp, "old_timestamp")

    def test_from_dict_ignores_unknown_extra_fields(self):
        @dataclass
        class ExtraFrame(Frame):
            x: int = 0

        data = {
            "gid": "LLL",
            "id": 10,
            "x": 123,
            "y": 999,  # unknown -> ignored
        }

        f = ExtraFrame.from_dict(data)

        self.assertIsInstance(f, ExtraFrame)
        self.assertEqual(f.gid, "LLL")
        self.assertEqual(f.id, 10)
        self.assertEqual(f.x, 123)
        # y is not a field -> must not exist on the instance
        self.assertFalse(hasattr(f, "y"))

    def test_subclass_to_dict_and_back_round_trip(self):
        @dataclass
        class RoundTripFrame(Frame):
            a: int = 1
            b: str = "test"

        f = RoundTripFrame(a=10, b="hello")
        d = f.to_dict()

        # round-trip via Frame.from_dict (dispatch)
        f2 = Frame.from_dict(d)
        self.assertIsInstance(f2, RoundTripFrame)
        self.assertEqual(f2.a, 10)
        self.assertEqual(f2.b, "hello")
        self.assertEqual(f2.gid, f.gid)
        self.assertEqual(f2.id, f.id)
        self.assertEqual(f2.name, "RoundTripFrame")


if __name__ == "__main__":
    unittest.main()
