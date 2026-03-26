import json
import os

CACHE_FILE = "cache/last_snapshot.json"
DASHBOARD_CACHE_FILE = "cache/dashboard_snapshot.json"
HISTORY_FILE = "cache/metric_history.json"
HISTORY_ARCHIVE_DIR = "cache/history_archive"
MAX_HISTORY_POINTS = 20000
_json_cache = {}
_archive_cache = {}


def _ensure_parent_dir(path):
    directory = os.path.dirname(path)
    if directory:
        os.makedirs(directory, exist_ok=True)


def _load_json_file(path):
    if not os.path.exists(path):
        _json_cache.pop(path, None)
        return {}
    if os.path.getsize(path) == 0:
        _json_cache.pop(path, None)
        return {}

    stat = os.stat(path)
    cache_key = (stat.st_mtime_ns, stat.st_size)
    cached_entry = _json_cache.get(path)
    if cached_entry and cached_entry["cache_key"] == cache_key:
        return cached_entry["data"]

    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
            _json_cache[path] = {
                "cache_key": cache_key,
                "data": data,
            }
            return data
    except (json.JSONDecodeError, OSError):
        _json_cache.pop(path, None)
        return {}


def _save_json_file(path, data):
    _ensure_parent_dir(path)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f)
    try:
        stat = os.stat(path)
    except OSError:
        _json_cache.pop(path, None)
        return

    _json_cache[path] = {
        "cache_key": (stat.st_mtime_ns, stat.st_size),
        "data": data,
    }


def load_cache():
    return _load_json_file(CACHE_FILE)


def save_cache(data):
    _save_json_file(CACHE_FILE, data)


def load_dashboard_cache():
    return _load_json_file(DASHBOARD_CACHE_FILE)


def save_dashboard_cache(data):
    _save_json_file(DASHBOARD_CACHE_FILE, data)


def load_metric_history():
    return _load_json_file(HISTORY_FILE)


def save_metric_history(data):
    _save_json_file(HISTORY_FILE, data)


def append_metric_history(games):
    history = load_metric_history()

    for game in games:
        game_name = game.get("game_name") or game.get("game")
        if not game_name:
            continue

        entries = history.setdefault(game_name, [])
        entry = {
            "timestamp": game.get("snapshot_time"),
            "rank": game.get("rank"),
            "score": game.get("score", 0),
            "viewers": game.get("viewers", 0),
            "streams": game.get("streams", 0),
            "ratio": game.get("ratio", 0),
            "growth": game.get("growth", 0),
            "viewer_change": game.get("viewer_change", 0),
        }

        if entries and entries[-1].get("timestamp") == entry["timestamp"]:
            entries[-1] = entry
        else:
            entries.append(entry)

        if len(entries) > MAX_HISTORY_POINTS:
            del entries[:-MAX_HISTORY_POINTS]

    save_metric_history(history)


def _archive_path_for_timestamp(timestamp):
    month_key = "unknown"
    if timestamp:
        month_key = timestamp[:7]
    return os.path.join(HISTORY_ARCHIVE_DIR, f"{month_key}.jsonl")


def append_snapshot_archive(games, snapshot_time=None):
    if not games:
        return

    archive_timestamp = snapshot_time or games[0].get("snapshot_time")
    if not archive_timestamp:
        return

    archive_path = _archive_path_for_timestamp(archive_timestamp)
    _ensure_parent_dir(archive_path)

    archive_entry = {
        "timestamp": archive_timestamp,
        "games": {},
    }

    for game in games:
        game_name = game.get("game_name") or game.get("game")
        if not game_name:
            continue

        archive_entry["games"][game_name] = {
            "rank": game.get("rank"),
            "score": game.get("score", 0),
            "viewers": game.get("viewers", 0),
            "streams": game.get("streams", 0),
            "ratio": game.get("ratio", 0),
            "growth": game.get("growth", 0),
            "viewer_change": game.get("viewer_change", 0),
        }

    if not archive_entry["games"]:
        return

    with open(archive_path, "a", encoding="utf-8") as archive_file:
        archive_file.write(json.dumps(archive_entry))
        archive_file.write("\n")

    _archive_cache.pop(archive_path, None)


def _load_archive_file_index(archive_path):
    if not os.path.exists(archive_path):
        _archive_cache.pop(archive_path, None)
        return {}

    try:
        stat = os.stat(archive_path)
    except OSError:
        _archive_cache.pop(archive_path, None)
        return {}

    cache_key = (stat.st_mtime_ns, stat.st_size)
    cached_entry = _archive_cache.get(archive_path)
    if cached_entry and cached_entry["cache_key"] == cache_key:
        return cached_entry["data"]

    indexed_entries = {}

    try:
        with open(archive_path, "r", encoding="utf-8") as archive_file:
            for line in archive_file:
                line = line.strip()
                if not line:
                    continue

                try:
                    snapshot = json.loads(line)
                except json.JSONDecodeError:
                    continue

                timestamp = snapshot.get("timestamp")
                if not timestamp:
                    continue

                for game_name, game_snapshot in snapshot.get("games", {}).items():
                    if not game_name:
                        continue

                    game_entries = indexed_entries.setdefault(game_name, {})
                    game_entries[timestamp] = {
                        "timestamp": timestamp,
                        "rank": game_snapshot.get("rank"),
                        "score": game_snapshot.get("score", 0),
                        "viewers": game_snapshot.get("viewers", 0),
                        "streams": game_snapshot.get("streams", 0),
                        "ratio": game_snapshot.get("ratio", 0),
                        "growth": game_snapshot.get("growth", 0),
                        "viewer_change": game_snapshot.get("viewer_change", 0),
                    }
    except OSError:
        _archive_cache.pop(archive_path, None)
        return {}

    _archive_cache[archive_path] = {
        "cache_key": cache_key,
        "data": indexed_entries,
    }
    return indexed_entries


def _load_archived_game_history(game_name):
    if not os.path.isdir(HISTORY_ARCHIVE_DIR):
        return []

    archived_entries = {}
    for filename in sorted(os.listdir(HISTORY_ARCHIVE_DIR)):
        if not filename.endswith(".jsonl"):
            continue

        archive_path = os.path.join(HISTORY_ARCHIVE_DIR, filename)
        archive_index = _load_archive_file_index(archive_path)
        for timestamp, entry in archive_index.get(game_name, {}).items():
            archived_entries[timestamp] = entry

    return [archived_entries[key] for key in sorted(archived_entries)]


def get_game_history(game_name):
    merged_entries = {
        entry.get("timestamp"): entry
        for entry in _load_archived_game_history(game_name)
        if entry.get("timestamp")
    }

    recent_history = load_metric_history().get(game_name, [])
    for entry in recent_history:
        timestamp = entry.get("timestamp")
        if timestamp:
            merged_entries[timestamp] = entry

    return [merged_entries[key] for key in sorted(merged_entries)]


def get_game_history_summary(game_name):
    history = get_game_history(game_name)
    if not history:
        return {
            "count": 0,
            "start": None,
            "end": None,
        }

    return {
        "count": len(history),
        "start": history[0].get("timestamp"),
        "end": history[-1].get("timestamp"),
    }
