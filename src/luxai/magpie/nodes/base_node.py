from abc import ABC, abstractmethod
from threading import Event, Thread
import weakref

from luxai.magpie.utils.logger import Logger


class BaseNode(ABC):
    """
    BaseNode class.
    """

    def __init__(self, name=None, paused=False, setup_kwargs={}):
        """
        Initializes the BaseNode, sets up events, and starts the thread.

        Args:
            name (str, optional): Name of the node. Defaults to the class name if not provided.
            paused (bool, optional): start the node in paused mode
        """
        self.name = name if name is not None else self.__class__.__name__

        # Termination / pause coordination
        self.terminate_event = Event()
        self.pause_event = Event()
        if not paused:
            self.pause_event.set()

        # Internal flag to make terminate() idempotent and safe to call multiple times.
        self._terminated = False

        # Optional setup hook for subclasses.
        # Note: subclasses should declare `def setup(self, **kwargs): ...`
        self.setup(**setup_kwargs)

        # Worker thread: daemon=True so forgotten terminate() won't block process exit.
        self.thread = Thread(target=self._run, name=self.name, daemon=True)
        self.thread.start()

        # Safety net: if user forgets to call terminate() and the object becomes unreachable,
        # this finalizer will perform a best-effort terminate() with a short timeout.
        self._finalizer = weakref.finalize(
            self,
            type(self)._finalize,
            weakref.proxy(self),
        )

    @staticmethod
    def _finalize(self_proxy):
        """
        Best-effort cleanup when the node is garbage-collected and terminate()
        was not called explicitly.

        This should never be relied upon for normal control flow, but it helps
        prevent background threads from lingering if the user forgets to shut
        the node down cleanly.
        """
        try:
            # Use a short timeout to avoid blocking GC / interpreter shutdown.
            self_proxy.terminate(timeout=1.0)
        except ReferenceError:
            # Object is already partially destroyed; nothing left to do.
            pass
        except Exception as exc:
            Logger.debug(
                f"BaseNode finalizer error for {getattr(self_proxy, 'name', '?')}: {exc}"
            )

    def setup(self, **kwargs):
        """Performs any necessary setup before the thread starts. Override in subclasses."""
        pass

    def cleanup(self):
        """Performs cleanup after the thread has been terminated. Override in subclasses."""
        pass

    def interrupt(self):
        """Performs actions after the thread has been interrupted by terminate. Override in subclasses."""
        pass

    @abstractmethod
    def process(self):
        """
        Defines the main processing task of the thread.
        Must be implemented by subclasses.
        """
        raise NotImplementedError

    def paused(self):
        return not self.pause_event.is_set()

    def terminating(self):
        return self.terminate_event.is_set()

    def pause(self):
        """Pauses the processing by setting the pause_event."""
        self.pause_event.clear()
        Logger.debug(f"{self.name} paused.")

    def resume(self):
        """Resuming the processing by clearing the pause_event."""
        self.pause_event.set()
        Logger.debug(f"{self.name} resumed.")

    def terminate(self, timeout=None):
        """
        Stops the processing by setting the terminate_event and unpausing if necessary.
        This allows the thread to exit cleanly.

        This method is idempotent and can be safely called multiple times.
        """
        if self._terminated:
            return

        self._terminated = True

        # Signal the run loop to exit and ensure it is not stuck in a paused state.
        self.terminate_event.set()
        self.pause_event.set()

        # Allow subclasses to interrupt any blocking operations (I/O, waits, etc.).
        self.interrupt()

        # Best-effort join; since the thread is a daemon, failure to join will not
        # block interpreter shutdown, but a successful join gives deterministic cleanup.
        try:
            self.thread.join(timeout=timeout)
        except RuntimeError:
            # Thread was never started or already finished; nothing to do.
            pass

        # We have performed explicit cleanup; prevent the finalizer from running again.
        if hasattr(self, "_finalizer"):
            self._finalizer.detach()

        Logger.debug(f"{self.name} terminated.")

    def _run(self):
        """
        The main loop for the thread. Processes data while not terminated and pauses if pause_event is set.
        Cleans up resources when the loop is exited.
        """
        Logger.debug(f"{self.name} started.")
        while not self.terminate_event.is_set():
            # If the pause_event is set, wait until it is cleared.
            self.pause_event.wait()
            if self.terminate_event.is_set():
                break

            self.process()

        # Give subclasses a chance to release resources (sockets, streams, etc.).
        self.cleanup()

    def __enter__(self):
        """
        Allow BaseNode subclasses to be used as context managers:

            with SomeNode(...) as node:
                ...

        The node will be terminated automatically when leaving the context.
        """
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        # Use a small timeout here to avoid blocking teardown for too long.
        self.terminate(timeout=1.0)

    def __str__(self):
        """Returns a user-friendly string representation of the object."""
        return f"Node '{self.name}' (type: {self.__class__.__name__})"
