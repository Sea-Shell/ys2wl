from sortarr.models.pipeline import FilterResult


def ignore_list_filter(video_id: str, ignore_list: list[str]) -> FilterResult:
    if video_id in ignore_list:
        return FilterResult(
            passed=False,
            reason=f"Video {video_id} is in ignore list",
            skipped_by="ignore_list",
            matched_video_id=video_id,
            match_type="exact",
        )
    return FilterResult(passed=True)
