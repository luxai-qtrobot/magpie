import unittest
import time

from luxai.magpie.nodes import ProcessNode
from tests.fake_stream_transports import FakeStreamReader, FakeStreamWriter, FakeSharedBuffer


class EchoProcessNode(ProcessNode):
    def __init__(self, *args, **kwargs):
        self.process_calls = 0
        super().__init__(*args, **kwargs)

    def process(self):
        item = self.stream_reader.read()        
        if item is not None:            
            data, topic = item
            self.stream_writer.write(data, topic)
            self.process_calls += 1
        time.sleep(0.002)


class TestProcessNode(unittest.TestCase):

    def test_process_flow(self):
        # Shared transport
        transport = FakeSharedBuffer()
        reader = FakeStreamReader(transport)
        writer = FakeStreamWriter(transport)

        # preload messages (writer-side)
        transport.push(1, "a")
        transport.push(2, "b")
        transport.push(3, "c")

        node = EchoProcessNode(reader, writer, paused=False)

        time.sleep(0.05)
        node.terminate()

        # Now writer wrote back into the SAME transport.
        # Let's inspect everything written into the transport queue.
        # Note: transport buffer has already been partially consumed by reader.
        # So we inspect writer's queue by draining whatever remains.
        results = []
        while True:
            item = transport.pop(timeout=0.01)
            if item is None:
                break
            results.append(item)

        # Output verification
        self.assertIn((1, "a"), results)
        self.assertIn((2, "b"), results)
        self.assertIn((3, "c"), results)
        self.assertGreaterEqual(node.process_calls, 3)

    def test_terminate_closes_reader_and_writer(self):
        transport = FakeSharedBuffer()
        reader = FakeStreamReader(transport)
        writer = FakeStreamWriter(transport)

        node = EchoProcessNode(reader, writer)
        node.terminate()

        self.assertTrue(transport.closed)

    def test_thread_starts_automatically(self):
        transport = FakeSharedBuffer()
        reader = FakeStreamReader(transport)
        writer = FakeStreamWriter(transport)

        transport.push("hello", "topic")
        node = EchoProcessNode(reader, writer)

        time.sleep(0.02)
        node.terminate()

        # Should have written at least once
        results = []
        while True:
            item = transport.pop(timeout=0.01)
            if item is None:
                break
            results.append(item)

        self.assertGreater(len(results), 0)
