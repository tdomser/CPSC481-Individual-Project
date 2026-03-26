import os
from datetime import datetime, timezone
from json import JSONDecodeError

import requests
from dotenv import load_dotenv

load_dotenv()

TOKEN_URL = "https://id.twitch.tv/oauth2/token"
STREAMS_URL = "https://api.twitch.tv/helix/streams"
GAMES_URL = "https://api.twitch.tv/helix/games"
USERS_URL = "https://api.twitch.tv/helix/users"
REQUEST_TIMEOUT = 15
PAGE_SIZE = 100

CLIENT_ID = (os.getenv("TWITCH_CLIENT_ID") or "").strip()
CLIENT_SECRET = (os.getenv("TWITCH_CLIENT_SECRET") or "").strip()


def _validate_credentials():
    if not CLIENT_ID or not CLIENT_SECRET:
        raise ValueError("Missing Twitch credentials. Set TWITCH_CLIENT_ID and TWITCH_CLIENT_SECRET in .env.")


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


def get_access_token():
    _validate_credentials()

    params = {
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "grant_type": "client_credentials",
    }

    response = requests.post(TOKEN_URL, params=params, timeout=REQUEST_TIMEOUT)
    try:
        response.raise_for_status()
    except requests.HTTPError as exc:
        body_preview = (response.text or "").strip()
        if len(body_preview) > 200:
            body_preview = f"{body_preview[:200]}..."
        raise ValueError(
            f"Twitch token request failed with status {response.status_code}. "
            f"Response body: {body_preview or '<empty>'}"
        ) from exc

    data = _parse_json_response(response, "Twitch token request")
    access_token = data.get("access_token")
    if not access_token:
        raise ValueError(f"Twitch token response did not include an access token. Response: {data}")

    return access_token


def _request_twitch_json(url, headers, params, context):
    response = requests.get(url, headers=headers, params=params, timeout=REQUEST_TIMEOUT)
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

    return _parse_json_response(response, context)


def _fetch_game_metadata(headers, game_ids):
    metadata = {}
    unique_ids = [game_id for game_id in sorted(set(game_ids)) if game_id]

    for start in range(0, len(unique_ids), 100):
        chunk = unique_ids[start : start + 100]
        params = [("id", game_id) for game_id in chunk]
        payload = _request_twitch_json(GAMES_URL, headers, params, "Twitch games request")

        for item in payload.get("data", []):
            box_art_url = item.get("box_art_url", "")
            metadata[item.get("id")] = {
                "name": item.get("name"),
                "box_art_url": box_art_url.replace("{width}", "285").replace("{height}", "380"),
            }

    return metadata


def _fetch_user_metadata(headers, user_ids):
    metadata = {}
    unique_ids = [user_id for user_id in sorted(set(user_ids)) if user_id]

    for start in range(0, len(unique_ids), 100):
        chunk = unique_ids[start : start + 100]
        params = [("id", user_id) for user_id in chunk]
        payload = _request_twitch_json(USERS_URL, headers, params, "Twitch users request")

        for item in payload.get("data", []):
            metadata[item.get("id")] = {
                "display_name": item.get("display_name") or item.get("login") or "Unknown streamer",
                "profile_image_url": item.get("profile_image_url"),
            }

    return metadata


def fetch_twitch_data(max_pages=None, min_viewers=0):
    token = get_access_token()
    snapshot_time = datetime.now(timezone.utc).isoformat()

    headers = {
        "Client-ID": CLIENT_ID,
        "Authorization": f"Bearer {token}",
    }

    game_data = {}
    cursor = None
    page_count = 0

    while True:
        if max_pages is not None and page_count >= max_pages:
            break

        params = {"first": PAGE_SIZE}
        if cursor:
            params["after"] = cursor

        response = requests.get(STREAMS_URL, headers=headers, params=params, timeout=REQUEST_TIMEOUT)
        try:
            response.raise_for_status()
        except requests.HTTPError as exc:
            body_preview = (response.text or "").strip()
            if len(body_preview) > 200:
                body_preview = f"{body_preview[:200]}..."
            raise ValueError(
                f"Twitch streams request failed with status {response.status_code}. "
                f"Response body: {body_preview or '<empty>'}"
            ) from exc

        payload = _parse_json_response(response, "Twitch streams request")
        streams = payload.get("data", [])
        if not streams:
            break

        page_count += 1
        hit_viewer_floor = False

        for stream in streams:
            game_id = stream.get("game_id")
            game_name = stream.get("game_name") or "Unknown"
            viewers = stream.get("viewer_count", 0)

            if viewers < min_viewers:
                hit_viewer_floor = True
                continue

            if game_name not in game_data:
                game_data[game_name] = {
                    "game_id": game_id,
                    "game_name": game_name,
                    "viewers": 0,
                    "streams": 0,
                    "snapshot_time": snapshot_time,
                }

            game_data[game_name]["viewers"] += viewers
            game_data[game_name]["streams"] += 1

        cursor = payload.get("pagination", {}).get("cursor")
        if hit_viewer_floor or not cursor:
            break

    game_metadata = _fetch_game_metadata(headers, [stats.get("game_id") for stats in game_data.values()])
    for game_name, stats in game_data.items():
        metadata = game_metadata.get(stats.get("game_id"), {})
        stats["game_name"] = metadata.get("name", stats.get("game_name", game_name))
        stats["box_art_url"] = metadata.get("box_art_url")

    return game_data


def fetch_top_streamers_for_game(game_id, limit=5):
    if not game_id:
        return []

    token = get_access_token()
    headers = {
        "Client-ID": CLIENT_ID,
        "Authorization": f"Bearer {token}",
    }

    params = {
        "first": max(1, min(limit, 20)),
        "game_id": game_id,
    }

    payload = _request_twitch_json(STREAMS_URL, headers, params, "Twitch top streamers request")
    user_metadata = _fetch_user_metadata(
        headers,
        [item.get("user_id") for item in payload.get("data", [])],
    )
    streamers = []
    for item in payload.get("data", []):
        user_info = user_metadata.get(item.get("user_id"), {})
        channel_login = item.get("user_login") or (item.get("user_name") or "").strip()
        streamers.append({
            "user_name": user_info.get("display_name") or item.get("user_name") or "Unknown streamer",
            "viewer_count": item.get("viewer_count", 0),
            "language": item.get("language") or "",
            "profile_image_url": user_info.get("profile_image_url"),
            "started_at": item.get("started_at"),
            "stream_url": f"https://www.twitch.tv/{channel_login}" if channel_login else None,
        })

    return streamers


def fetch_game_live_snapshot(game_id, top_streamer_limit=5):
    if not game_id:
        return {"viewers": 0, "streams": 0, "top_streamers": []}

    token = get_access_token()
    headers = {
        "Client-ID": CLIENT_ID,
        "Authorization": f"Bearer {token}",
    }

    cursor = None
    total_viewers = 0
    total_streams = 0
    top_streamers = []

    while True:
        params = {
            "first": PAGE_SIZE,
            "game_id": game_id,
        }
        if cursor:
            params["after"] = cursor

        payload = _request_twitch_json(STREAMS_URL, headers, params, "Twitch category live snapshot request")
        streams = payload.get("data", [])
        if not streams:
            break

        for item in streams:
            total_viewers += item.get("viewer_count", 0)
            total_streams += 1
            top_streamers.append(item)

        cursor = payload.get("pagination", {}).get("cursor")
        if not cursor:
            break

    top_streamers.sort(key=lambda item: item.get("viewer_count", 0), reverse=True)
    top_streamers = top_streamers[: max(1, min(top_streamer_limit, 20))]

    user_metadata = _fetch_user_metadata(
        headers,
        [item.get("user_id") for item in top_streamers],
    )

    formatted_streamers = []
    for item in top_streamers:
        user_info = user_metadata.get(item.get("user_id"), {})
        channel_login = item.get("user_login") or (item.get("user_name") or "").strip()
        formatted_streamers.append({
            "user_name": user_info.get("display_name") or item.get("user_name") or "Unknown streamer",
            "viewer_count": item.get("viewer_count", 0),
            "language": item.get("language") or "",
            "profile_image_url": user_info.get("profile_image_url"),
            "started_at": item.get("started_at"),
            "stream_url": f"https://www.twitch.tv/{channel_login}" if channel_login else None,
        })

    return {
        "viewers": total_viewers,
        "streams": total_streams,
        "top_streamers": formatted_streamers,
    }


if __name__ == "__main__":
    data = fetch_twitch_data()

    print("\nTop Games:\n")
    for game, stats in list(data.items())[:10]:
        print(f"{game}: {stats}")
