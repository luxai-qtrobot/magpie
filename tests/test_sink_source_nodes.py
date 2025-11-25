import unittest
import time

from luxai.magpie.nodes import SinkNode
from luxai.magpie.nodes import SourceNode
from tests.fake_stream_transports import FakeSharedBuffer, FakeStreamReader, FakeStreamWriter


class TestSink(SinkNode):
    def __init__(self, *args, **kwargs):
        self.process_calls = 0
        super().__init__(*args, **kwargs)

    def process(self):
        item = self.stream_reader.read()
        if item:
            self.process_calls += 1
        time.sleep(0.002)


class TestSource(SourceNode):
    def __init__(self, *args, **kwargs):
        self.counter = 0
        super().__init__(*args, **kwargs)

    def process(self):
        self.stream_writer.write(self.counter, "t")
        self.counter += 1
        time.sleep(0.002)


class TestSinkNode(unittest.TestCase):

    def test_requires_reader(self):
        with self.assertRaises(ValueError):
            TestSink(None)

    def test_sink_reads_items(self):
        transport = FakeSharedBuffer()
        reader = FakeStreamReader(transport)
        writer = FakeStreamWriter(transport)  # just for pushing

        # producer side
        writer.write(10, "x")
        writer.write(20, "y")

        sink = TestSink(reader, paused=False)
        time.sleep(0.05)
        sink.terminate()

        self.assertGreaterEqual(sink.process_calls, 2)

    def test_sink_terminate_closes_transport(self):
        transport = FakeSharedBuffer()
        reader = FakeStreamReader(transport)
        sink = TestSink(reader)
        sink.terminate()

        self.assertTrue(transport.closed)


class TestSourceNode(unittest.TestCase):

    def test_requires_writer(self):
        with self.assertRaises(ValueError):
            TestSource(None)

    def test_source_writes_items(self):
        transport = FakeSharedBuffer()
        writer = FakeStreamWriter(transport)
        source = TestSource(writer, paused=False)

        time.sleep(0.05)
        source.terminate()

        # Read everything off the shared transport
        out = []
        while True:
            item = transport.pop(timeout=0.01)
            if item is None:
                break
            out.append(item)

        self.assertGreater(len(out), 0)

        values = [d for (d, _) in out]
        for i, v in enumerate(values):
            self.assertEqual(v, i)

    def test_source_terminate_closes_transport(self):
        transport = FakeSharedBuffer()
        writer = FakeStreamWriter(transport)
        source = TestSource(writer)
        source.terminate()

        self.assertTrue(transport.closed)
