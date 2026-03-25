"""
Virtual file object for MemFS.
Provides file-like interface for in-memory files.
"""

import io
import threading
from typing import Optional, BinaryIO


class VirtualFile(io.RawIOBase):
    """
    Virtual file object with file-like interface.

    Wraps bytes data and provides standard file operations.
    """

    def __init__(
        self,
        key: str,
        data: bytes = b"",
        mode: str = "rb",
        filesystem: Optional["MemFileSystem"] = None,
    ):
        """
        Initialize virtual file.

        Args:
            key: File key/path.
            data: Initial data (for write modes).
            mode: File mode ('r', 'w', 'a', 'rb', 'wb', 'ab', etc.).
            filesystem: Parent filesystem reference.
        """
        self.key = key
        self.mode = mode
        self._filesystem = filesystem

        self._buffer = io.BytesIO(data)
        self._closed = False
        self._lock = threading.Lock()

        if "a" in mode:
            self._buffer.seek(0, 2)

    def readable(self) -> bool:
        """Check if file is readable."""
        return "r" in self.mode or "+" in self.mode

    def writable(self) -> bool:
        """Check if file is writable."""
        return "w" in self.mode or "a" in self.mode or "+" in self.mode

    def seekable(self) -> bool:
        """Check if file supports seeking."""
        return True

    def read(self, size: int = -1) -> bytes:
        """
        Read data from file.

        Args:
            size: Number of bytes to read (-1 for all).

        Returns:
            Bytes data.
        """
        if self._closed:
            raise ValueError("I/O operation on closed file")

        if not self.readable():
            raise io.UnsupportedOperation("File not open for reading")

        with self._lock:
            if size < 0:
                return self._buffer.read()
            return self._buffer.read(size)

    def readinto(self, b: bytearray) -> int:
        """
        Read bytes into buffer.

        Args:
            b: Buffer to read into.

        Returns:
            Number of bytes read.
        """
        if self._closed:
            raise ValueError("I/O operation on closed file")

        data = self.read(len(b))
        b[: len(data)] = data
        return len(data)

    def write(self, data: bytes) -> int:
        """
        Write data to file.

        Args:
            data: Data to write.

        Returns:
            Number of bytes written.
        """
        if self._closed:
            raise ValueError("I/O operation on closed file")

        if not self.writable():
            raise io.UnsupportedOperation("File not open for writing")

        with self._lock:
            return self._buffer.write(data)

    def seek(self, pos: int, whence: int = 0) -> int:
        """
        Seek to position.

        Args:
            pos: Position to seek to.
            whence: Seek mode (0=start, 1=current, 2=end).

        Returns:
            New position.
        """
        if self._closed:
            raise ValueError("I/O operation on closed file")

        with self._lock:
            return self._buffer.seek(pos, whence)

    def tell(self) -> int:
        """
        Get current position.

        Returns:
            Current position.
        """
        if self._closed:
            raise ValueError("I/O operation on closed file")

        with self._lock:
            return self._buffer.tell()

    def truncate(self, size: Optional[int] = None) -> int:
        """
        Truncate file.

        Args:
            size: Size to truncate to (None = current position).

        Returns:
            New size.
        """
        if self._closed:
            raise ValueError("I/O operation on closed file")

        if not self.writable():
            raise io.UnsupportedOperation("File not open for writing")

        with self._lock:
            if size is None:
                size = self._buffer.tell()
            return self._buffer.truncate(size)

    def flush(self):
        """Flush file (no-op for virtual files)."""
        if self._closed:
            raise ValueError("I/O operation on closed file")

    def close(self):
        """Close file and save to filesystem."""
        if self._closed:
            return

        with self._lock:
            if self._filesystem and self.writable():
                data = self._buffer.getvalue()
                self._filesystem.write(self.key, data)

            self._buffer.close()
            self._closed = True

    @property
    def closed(self) -> bool:
        """Check if file is closed."""
        return self._closed

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()

    def __iter__(self):
        """Iterate over lines."""
        return self

    def __next__(self):
        """Get next line."""
        line = self.readline()
        if not line:
            raise StopIteration
        return line

    def readline(self, size: int = -1) -> bytes:
        """Read a line."""
        if self._closed:
            raise ValueError("I/O operation on closed file")

        with self._lock:
            return self._buffer.readline(size)

    def writelines(self, lines):
        """Write multiple lines."""
        if self._closed:
            raise ValueError("I/O operation on closed file")

        with self._lock:
            for line in lines:
                self._buffer.write(line)
