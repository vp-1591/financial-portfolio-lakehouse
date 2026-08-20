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
    """Sample RSS every 100 ms over a ``with`` block; log the block's peak.

    Catches spikes that happen *inside* a phase (e.g. mid dedup/write) that
    boundary logs would miss.  The peak line prints on both normal exit and
    exception, so the last line of a dying container still names the phase.
    """

    def __init__(self, phase: str) -> None:
        self.phase = phase
        self._stop = threading.Event()
        self._block_peak_mb = 0.0
        self._thread: threading.Thread | None = None

    def _sample(self) -> None:
        while not self._stop.wait(0.1):
            try:
                mb = rss_mb()
            except psutil.Error:
                # Process is tearing down; stop sampling quietly.
                return
            self._block_peak_mb = max(self._block_peak_mb, mb)
            _note_peak(mb)

    def __enter__(self) -> Self:
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
        self._stop.set()
        if self._thread is not None:
            # Join so the final <=100 ms sample is counted before we read the
            # block peak; without it the last sample races the print.
            self._thread.join()
        _note_peak(self._block_peak_mb)
        print(
            f"[mem] phase={self.phase} peak_mb={self._block_peak_mb:.1f}",
            file=sys.stderr,
            flush=True,
        )
