"""Async read-write lock for state consistency.

Allows multiple concurrent readers OR a single exclusive writer.
Fair scheduling: once a writer is waiting, new readers queue behind it
to prevent writer starvation.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator


class ReadWriteLock:
    """Async read-write lock with fair scheduling.

    Multiple readers can hold the lock concurrently. A writer gets exclusive
    access -- it waits for all active readers to finish, and blocks new readers
    until the write is complete.

    Fair scheduling prevents writer starvation: once a writer is waiting,
    new readers queue behind the writer instead of jumping ahead.
    """

    def __init__(self) -> None:
        self._readers: int = 0
        self._writer_active: bool = False
        self._writer_waiting: bool = False
        self._lock = asyncio.Lock()
        self._no_readers = asyncio.Event()
        self._no_readers.set()  # initially no readers
        self._no_writer = asyncio.Event()
        self._no_writer.set()  # initially no writer

    @property
    def readers(self) -> int:
        """Number of active readers."""
        return self._readers

    @property
    def writer_active(self) -> bool:
        """Whether a writer currently holds the lock."""
        return self._writer_active

    @contextlib.asynccontextmanager
    async def read(self) -> AsyncIterator[None]:
        """Acquire read access. Multiple readers can hold this concurrently."""
        await self._acquire_read()
        try:
            yield
        finally:
            await self._release_read()

    @contextlib.asynccontextmanager
    async def write(self) -> AsyncIterator[None]:
        """Acquire exclusive write access."""
        await self._acquire_write()
        try:
            yield
        finally:
            await self._release_write()

    async def _acquire_read(self) -> None:
        """Acquire read lock, waiting for any active or pending writer."""
        async with self._lock:
            # Fair scheduling: if a writer is waiting or active, let it go first
            while self._writer_active or self._writer_waiting:
                # Release _lock so the writer can proceed
                self._lock.release()
                await self._no_writer.wait()
                await self._lock.acquire()

            self._readers += 1
            self._no_readers.clear()

    async def _release_read(self) -> None:
        """Release read lock."""
        async with self._lock:
            self._readers -= 1
            if self._readers == 0:
                self._no_readers.set()

    async def _acquire_write(self) -> None:
        """Acquire exclusive write lock."""
        async with self._lock:
            # Signal that a writer is waiting (blocks new readers)
            self._writer_waiting = True
            self._no_writer.clear()

            # Wait for all readers to finish
            while self._readers > 0:
                self._lock.release()
                await self._no_readers.wait()
                await self._lock.acquire()

            self._writer_waiting = False
            self._writer_active = True

    async def _release_write(self) -> None:
        """Release exclusive write lock."""
        async with self._lock:
            self._writer_active = False
            self._no_writer.set()
