## Summary

Enhances the pipeline skip decision logging with detailed matched video information and adds a new API endpoint for video lookup.

## Changes

### Enhanced Skip Reasoning
- **FilterResult model** (`src/ys2wl/models/pipeline.py`): Added `matched_video_id`, `matched_title`, `match_type` fields
- **All filter functions** now populate match details:
  - `video_exists_for_pipeline` returns full video details (title, timestamp, etc.)
  - `title_similarity` includes matched video ID, title, and similarity %
  - `ignore_list_filter` includes matched video ID
  - `word_filter` includes matched title and word
  - Duration checks (min/max) include video ID and title
  - Selector filter includes video ID and title

### Decision Logging
- Pipeline run decisions now include structured `reason_detail` with:
  - `matched_video_id=<id>`
  - `matched_title='<title>'`
  - `match_type=<exact|title_similarity|word|duration_min|duration_max|selector|ignore_list>`

### New Video Lookup API
- Added `GET /api/videos/{video_id}` endpoint
- Returns video details across all pipelines for cross-referencing skipped videos

### Database
- Added `get_video_by_id` repository function
- `video_exists_for_pipeline` now returns `(bool, Optional[dict])` tuple

## Testing
All 112 tests pass.