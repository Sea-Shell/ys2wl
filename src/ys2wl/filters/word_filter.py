import re
from typing import Optional
from ys2wl.models.pipeline import FilterResult


def word_filter(title: str, ignore_words: list[str]) -> FilterResult:
    if not ignore_words:
        return FilterResult(passed=True)
    title_lower = title.lower()
    for word in ignore_words:
        if not word:
            continue
        pattern = re.compile(r'\b' + re.escape(word.lower()) + r'\b', re.IGNORECASE)
        if pattern.search(title_lower):
            return FilterResult(passed=False, reason=f"Title contains ignored word '{word}'", skipped_by="word_filter")
    return FilterResult(passed=True)
