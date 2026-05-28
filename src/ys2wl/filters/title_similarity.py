import re
from typing import Optional
from ys2wl.models.pipeline import FilterResult


def _normalize(title: str) -> str:
    return re.sub(r'[^a-zA-Z0-9\-_]+', ' ', title).lower()


def _fuzz_ratio(s1: str, s2: str) -> int:
    if not s1 or not s2:
        return 0
    shorter, longer = (s1, s2) if len(s1) <= len(s2) else (s2, s1)
    m = len(shorter)
    n = len(longer)
    if m == 0:
        return 0
    costs = list(range(m + 1))
    for i in range(1, n + 1):
        prev = costs[0]
        costs[0] = i
        for j in range(1, m + 1):
            temp = costs[j]
            costs[j] = min(
                costs[j] + 1,
                costs[j - 1] + 1,
                prev + (0 if shorter[j - 1] == longer[i - 1] else 1),
            )
            prev = temp
    max_len = max(n, m)
    if max_len == 0:
        return 100
    return int((1 - costs[m] / max_len) * 100)


def title_similarity(new_title: str, existing_titles: list[tuple[str, str]], threshold: int) -> FilterResult:
    normalized_new = _normalize(new_title)
    for video_id, existing_title in existing_titles:
        normalized_existing = _normalize(existing_title)
        ratio = _fuzz_ratio(normalized_new, normalized_existing)
        if ratio > threshold:
            return FilterResult(
                passed=False,
                reason=f"Title '{new_title}' is {ratio}% similar to existing video '{video_id}' (threshold: {threshold}%)",
                skipped_by="title_similarity",
            )
    return FilterResult(passed=True)
