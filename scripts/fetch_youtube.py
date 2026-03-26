import os
from json import JSONDecodeError

import requests
from dotenv import load_dotenv

load_dotenv()

SEARCH_URL = "https://www.googleapis.com/youtube/v3/search"
VIDEOS_URL = "https://www.googleapis.com/youtube/v3/videos"
REQUEST_TIMEOUT = 15
MAX_RESULTS_PER_PAGE = 50

YOUTUBE_API_KEY = (os.getenv("YOUTUBE_API_KEY") or "").strip()


def _validate_api_key():
    if not YOUTUBE_API_KEY:
        raise ValueError("Missing YOUTUBE_API_KEY in .env.")


def _parse_json_response(response, context):
    try:
        return response.json()
    except JSONDecodeError as exc:
        body_preview = (response.text or "").strip()
        if len(body_preview) > 200:
            body_preview = f"{body_preview[:200]}..."
        raise ValueError(
            f"{context} returned invalid JSON. Status {response.status_code}. "
            f"Response body: {body_preview or '<empty>'}"
        ) from exc


def _request_json(url, params, context):
    response = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
    try:
        response.raise_for_status()
    except requests.HTTPError as exc:
        body_preview = (response.text or "").strip()
        if len(body_preview) > 200:
            body_preview = f"{body_preview[:200]}..."
        raise ValueError(
            f"{context} failed with status {response.status_code}. "
            f"Response body: {body_preview or '<empty>'}"
        ) from exc

    payload = _parse_json_response(response, context)
    if "error" in payload:
        raise ValueError(f"{context} returned an API error: {payload['error']}")
    return payload


def _fetch_video_statistics(video_ids):
    if not video_ids:
        return {}

    payload = _request_json(
        VIDEOS_URL,
        {
            "key": YOUTUBE_API_KEY,
            "part": "statistics,snippet",
            "id": ",".join(video_ids),
            "maxResults": len(video_ids),
        },
        "YouTube videos request",
    )

    stats_by_id = {}
    for item in payload.get("items", []):
        stats_by_id[item.get("id")] = {
            "title": item.get("snippet", {}).get("title", ""),
            "channel_title": item.get("snippet", {}).get("channelTitle", ""),
            "published_at": item.get("snippet", {}).get("publishedAt"),
            "view_count": int(item.get("statistics", {}).get("viewCount", 0)),
            "like_count": int(item.get("statistics", {}).get("likeCount", 0)),
            "comment_count": int(item.get("statistics", {}).get("commentCount", 0)),
        }

    return stats_by_id


def fetch_youtube_data(game_names, max_games=25, pages_per_game=1):
    _validate_api_key()

    if not game_names:
        return {}

    youtube_data = {}

    for game_name in game_names[:max_games]:
        next_page_token = None
        aggregated = {
            "game_name": game_name,
            "videos": 0,
            "views": 0,
            "likes": 0,
            "comments": 0,
        }

        for _ in range(pages_per_game):
            payload = _request_json(
                SEARCH_URL,
                {
                    "key": YOUTUBE_API_KEY,
                    "part": "snippet",
                    "q": f"{game_name} game",
                    "type": "video",
                    "videoCategoryId": "20",
                    "order": "relevance",
                    "maxResults": MAX_RESULTS_PER_PAGE,
                    "pageToken": next_page_token,
                },
                f"YouTube search request for {game_name}",
            )

            video_ids = [
                item.get("id", {}).get("videoId")
                for item in payload.get("items", [])
                if item.get("id", {}).get("videoId")
            ]
            video_stats = _fetch_video_statistics(video_ids)

            for video_id in video_ids:
                stats = video_stats.get(video_id)
                if not stats:
                    continue

                aggregated["videos"] += 1
                aggregated["views"] += stats["view_count"]
                aggregated["likes"] += stats["like_count"]
                aggregated["comments"] += stats["comment_count"]

            next_page_token = payload.get("nextPageToken")
            if not next_page_token:
                break

        if aggregated["videos"] > 0:
            aggregated["avg_views"] = round(aggregated["views"] / aggregated["videos"], 2)
            aggregated["engagement"] = aggregated["likes"] + aggregated["comments"]
            youtube_data[game_name] = aggregated

    return youtube_data


if __name__ == "__main__":
    sample_games = ["Minecraft", "Fortnite", "Apex Legends", "League of Legends"]
    data = fetch_youtube_data(sample_games, max_games=4, pages_per_game=1)

    for game_name, stats in data.items():
        print(f"{game_name}: {stats}")
