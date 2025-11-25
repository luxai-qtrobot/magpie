import unittest
import time
from magpie.nodes.base_node import BaseNode


class TestNode(BaseNode):
    """
    Minimal concrete node for testing.
    process() increments a counter whenever it runs.
    """

    def __init__(self, *args, **kwargs):
        self.process_calls = 0
        self.setup_calls = 0
        self.cleanup_calls = 0
        self.interrupt_calls = 0
        super().__init__(*args, **kwargs)

    def setup(self, **kwargs):
        self.setup_calls += 1

    def process(self):
        # Simulate small work
        self.process_calls += 1
        time.sleep(0.01)

    def cleanup(self):
        self.cleanup_calls += 1

    def interrupt(self):
        self.interrupt_calls += 1


class TestBaseNode(unittest.TestCase):

    def test_initial_setup_called(self):
        node = TestNode(paused=True)   # paused → thread runs but won't call process()
        time.sleep(0.05)
        node.terminate()
        self.assertEqual(node.setup_calls, 1)

    def test_thread_starts_automatically(self):
        node = TestNode(paused=True)
        time.sleep(0.05)
        # thread started but process() should NOT have been called yet
        self.assertEqual(node.process_calls, 0)
        node.terminate()

    def test_resume_triggers_processing(self):
        node = TestNode(paused=True)
        time.sleep(0.05)
        self.assertEqual(node.process_calls, 0)

        node.resume()
        time.sleep(0.05)

        # process() should now be called at least once
        self.assertGreater(node.process_calls, 0)

        node.terminate()

    def test_pause_stops_processing(self):
        node = TestNode(paused=False)
        time.sleep(0.05)
        called_before = node.process_calls

        node.pause()
        time.sleep(0.05)
        called_after = node.process_calls

        # During pause, process() must NOT be called
        self.assertEqual(called_before, called_after)

        node.terminate()

    def test_terminate_calls_interrupt_and_cleanup(self):
        node = TestNode(paused=False)
        time.sleep(0.05)

        node.terminate()

        self.assertEqual(node.interrupt_calls, 1)
        self.assertEqual(node.cleanup_calls, 1)

    def test_paused_and_terminating_flags(self):
        node = TestNode(paused=True)
        self.assertTrue(node.paused())
        self.assertFalse(node.terminating())

        node.resume()
        self.assertFalse(node.paused())

        node.terminate()
        self.assertTrue(node.terminating())


if __name__ == "__main__":
    unittest.main()
