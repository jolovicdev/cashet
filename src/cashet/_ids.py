from __future__ import annotations


def normalize_hash_prefix(hash: str) -> str | None:
    if 0 < len(hash) <= 64 and all(c in "0123456789abcdefABCDEF" for c in hash):
        return hash.lower()
    return None
