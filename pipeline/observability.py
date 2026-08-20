"""Phase-level RSS observability for pipeline runs.

Prints ``[mem]`` lines to stderr so a container that dies from memory
pressure leaves a record of the last phase it reached.  The awslogs driver
streams stderr asynchronously, so these lines survive an OOM kill even
though the process itself is SIGKILLed.  Wired into ``pipeline.run`` at
phase boundaries; deliberately dependency-light and side-effect free
otherwise.

Decision: docs/adr/0115-phase-level-rss-memory-observability.md
"""

from __future__ import annotations

import sys
import threading
from types import TracebackType
from typing import Self

import psutil

_PROC = psutil.Process()
_peak_mb = 0.0
_peak_lock = threading.Lock()


def rss_mb() -> float:
    """Current resident set size of this process, in MiB."""
    return _PROC.memory_info().rss / (1024 * 1024)


def _note_peak(mb: float) -> float:
    global _peak_mb
    with _peak_lock:
        _peak_mb = max(_peak_mb, mb)
    return _peak_mb


def log_memory(phase: str) -> None:
    """Print an RSS snapshot for *phase* to stderr, with a running peak.

    The last ``[mem]`` line in a dead container's log is the phase it reached
    before the OOM kill.
    """
    mb = rss_mb()
    peak = _note_peak(mb)
    print(
        f"[mem] phase={phase} rss_mb={mb:.1f} peak_mb={peak:.1f}",
        file=sys.stderr,
        flush=True,
    )


class MemorySampler:
    """Sample RSS every 100 ms over a ``with`` block and log it.

    Catches spikes that happen *inside* a phase (e.g. mid dedup/write) that
    boundary logs would miss.  The sampler thread flushes its own lines as it
    samples — roughly every second, plus one final line on exit — because
    ``__exit__`` never runs on a SIGKILL: the last line in a dead container's
    log is at most ~1 s stale and carries the RSS it died at.

    ``enabled=False`` makes the block a no-op: the parallel docker 'full' path
    runs all connectors as threads of one process, so a single process-wide
    sampler replaces the per-connector ones instead of duplicating their
    syscalls on the same process.
    """

    def __init__(self, phase: str, enabled: bool = True) -> None:
        self.phase = phase
        self._enabled = enabled
        self._stop = threading.Event()
        self._block_peak_mb = 0.0
        self._thread: threading.Thread | None = None

    def _sample(self) -> None:
        """Sample RSS every 100 ms; emit a ~1 s heartbeat plus a final line.

        The thread (not ``__exit__``) prints every line so a SIGKILLed
        container's CloudWatch stream still ends with a fresh ``[mem]`` line.
        ``peak_mb`` is the process-wide running peak, monotonic across all
        line types; ``block_peak_mb`` is this block's own peak.
        """
        ticks = 0
        while True:
            stopping = self._stop.wait(0.1)
            try:
                mb = rss_mb()
            except psutil.Error:
                # Process is tearing down; stop sampling quietly.
                return
            self._block_peak_mb = max(self._block_peak_mb, mb)
            _note_peak(mb)
            ticks += 1
            if stopping or ticks % 10 == 0:
                print(
                    f"[mem] phase={self.phase} rss_mb={mb:.1f} "
                    f"peak_mb={_peak_mb:.1f} block_peak_mb={self._block_peak_mb:.1f}",
                    file=sys.stderr,
                    flush=True,
                )
            if stopping:
                return

    def __enter__(self) -> Self:
        if not self._enabled:
            return self
        self._thread = threading.Thread(
            target=self._sample,
            name=f"rss-sampler-{self.phase}",
            daemon=True,
        )
        self._thread.start()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        if not self._enabled:
            return
        self._stop.set()
        if self._thread is not None:
            # The thread prints its final line once it observes the stop;
            # join so it is flushed (and ordered) before the block returns.
            self._thread.join()
