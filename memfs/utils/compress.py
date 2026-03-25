"""
Compressor utilities for MemFS.
Supports gzip, lz4, and zstd compression algorithms.
"""

import gzip
import io
from abc import ABC, abstractmethod
from typing import Optional


class Compressor(ABC):
    """Abstract base class for compression algorithms."""

    @abstractmethod
    def compress(self, data: bytes) -> bytes:
        """Compress data and return compressed bytes."""
        pass

    @abstractmethod
    def decompress(self, data: bytes) -> bytes:
        """Decompress data and return original bytes."""
        pass


class GzipCompressor(Compressor):
    """Gzip compression using standard library."""

    def __init__(self, level: int = 6):
        """
        Initialize gzip compressor.

        Args:
            level: Compression level (1-9), higher is better compression but slower.
        """
        self.level = level

    def compress(self, data: bytes) -> bytes:
        return gzip.compress(data, compresslevel=self.level)

    def decompress(self, data: bytes) -> bytes:
        return gzip.decompress(data)


class Lz4Compressor(Compressor):
    """LZ4 compression - extremely fast."""

    def __init__(self, level: int = 0):
        """
        Initialize lz4 compressor.

        Args:
            level: Compression level (0-12), higher is better compression.
                   0 = fast, 1-12 = high compression mode.
        """
        self.level = level
        self._check_dependency()

    def _check_dependency(self):
        try:
            import lz4.block
        except ImportError:
            raise ImportError(
                "lz4 library not installed. Install with: pip install lz4"
            )

    def compress(self, data: bytes) -> bytes:
        import lz4.block

        return lz4.block.compress(data, mode=self.level)

    def decompress(self, data: bytes) -> bytes:
        import lz4.block

        return lz4.block.decompress(data)


class ZstdCompressor(Compressor):
    """Zstandard compression - balanced speed and ratio."""

    def __init__(self, level: int = 3):
        """
        Initialize zstd compressor.

        Args:
            level: Compression level (1-22), higher is better compression but slower.
        """
        self.level = level
        self._check_dependency()

    def _check_dependency(self):
        try:
            import zstandard
        except ImportError:
            raise ImportError(
                "zstandard library not installed. Install with: pip install zstandard"
            )

    def compress(self, data: bytes) -> bytes:
        import zstandard

        cctx = zstandard.ZstdCompressor(level=self.level)
        return cctx.compress(data)

    def decompress(self, data: bytes) -> bytes:
        import zstandard

        dctx = zstandard.ZstdDecompressor()
        return dctx.decompress(data)


class CompressionFactory:
    """Factory for creating compressor instances."""

    _compressors = {
        "gzip": GzipCompressor,
        "lz4": Lz4Compressor,
        "zstd": ZstdCompressor,
    }

    @classmethod
    def create(cls, algorithm: str = "gzip", level: Optional[int] = None) -> Compressor:
        """
        Create a compressor instance.

        Args:
            algorithm: Compression algorithm ('gzip', 'lz4', 'zstd').
            level: Compression level (uses default if None).

        Returns:
            Compressor instance.

        Raises:
            ValueError: If algorithm is not supported.
        """
        if algorithm not in cls._compressors:
            raise ValueError(
                f"Unsupported compression algorithm: {algorithm}. "
                f"Supported: {list(cls._compressors.keys())}"
            )

        if level is None:
            return cls._compressors[algorithm]()
        return cls._compressors[algorithm](level)

    @classmethod
    def register(cls, name: str, compressor_class: type):
        """Register a custom compressor."""
        cls._compressors[name] = compressor_class

    @classmethod
    def get_available_algorithms(cls) -> list:
        """Get list of available compression algorithms."""
        available = []
        for name, compressor_class in cls._compressors.items():
            try:
                compressor_class()
                available.append(name)
            except (ImportError, Exception):
                continue
        return available
