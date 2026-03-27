"""
Example usage of MemFS.
"""

import sys

sys.path.insert(0, ".")

from memfs import MemFileSystem


def main():
    print("=== MemFS Demo ===")

    fs = MemFileSystem(
        memory_limit=0.5,
        persist_path="./tmp/memfs_data",
        storage_mode="persist",
    )

    # Write
    fs.write("/hello.txt", "Hello, World!", priority=5)
    print("Wrote file")

    # Read
    content = fs.read("/hello.txt")
    print(f"Read: {content.decode()}")

    # Priority
    fs.set_priority("/hello.txt", priority=8)
    print(f"Priority: {fs.get_priority('/hello.txt')}")

    # Stats
    stats = fs.get_stats()
    print(f"Memory: {stats['memory']['usage_percent']:.1f}%")

    # GC
    swapped = fs.gc(target_usage=0.3)
    print(f"Swapped: {swapped}")

    # Exists
    print(f"Exists: {fs.exists('/hello.txt')}")

    fs.shutdown()
    print("Done!")


if __name__ == "__main__":
    main()
