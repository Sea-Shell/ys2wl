import re
from typing import Optional


def time_to_seconds(time_str: str) -> int:
    if not time_str or time_str == "0s":
        return 0
    pattern = r'(\d+)([smhd])'
    units = {'s': 1, 'm': 60, 'h': 3600, 'd': 86400}
    matches = re.findall(pattern, time_str)
    total = 0
    for value, unit in matches:
        total += int(value) * units[unit]
    return total


def find_channel(channels: list[dict], channel_name: str) -> Optional[dict]:
    for ch in channels:
        if channel_name.lower() in ch["title"].lower():
            return ch
    return None


def find_playlist(playlists: list[dict], playlist_name: str) -> Optional[dict]:
    for pl in playlists:
        if playlist_name.lower() in pl["title"].lower():
            return pl
    return None
