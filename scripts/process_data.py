from scripts.utils import load_cache, save_cache

MIN_VIEWER_CHANGE_THRESHOLD = 5


def compute_metrics(game_data):
    previous = load_cache()
    results = []
    new_cache = {}

    for game_name, stats in game_data.items():
        viewers = stats.get("viewers", 0)
        streams = stats.get("streams", 0)
        ratio = viewers / streams if streams > 0 else 0

        previous_stats = previous.get(game_name, {})
        prev_viewers = previous_stats.get("viewers", viewers)
        prev_growth = previous_stats.get("growth", 0)
        prev_viewer_change = previous_stats.get("viewer_change", 0)

        raw_viewer_change = viewers - prev_viewers
        if abs(raw_viewer_change) < MIN_VIEWER_CHANGE_THRESHOLD:
            viewer_change = prev_viewer_change
            growth_percent = prev_growth
        else:
            viewer_change = raw_viewer_change
            growth_rate = (viewer_change / prev_viewers) if prev_viewers > 0 else 0
            growth_percent = round(growth_rate * 100, 2)

        results.append(
            {
                "game": game_name,
                "game_id": stats.get("game_id"),
                "game_name": stats.get("game_name", game_name),
                "viewers": viewers,
                "streams": streams,
                "ratio": round(ratio, 2),
                "growth": growth_percent,
                "previous_viewers": prev_viewers,
                "viewer_change": viewer_change,
                "snapshot_time": stats.get("snapshot_time"),
                "box_art_url": stats.get("box_art_url"),
            }
        )

        new_cache[game_name] = {
            "game_id": stats.get("game_id"),
            "game_name": stats.get("game_name", game_name),
            "viewers": viewers,
            "streams": streams,
            "growth": growth_percent,
            "viewer_change": viewer_change,
            "snapshot_time": stats.get("snapshot_time"),
            "box_art_url": stats.get("box_art_url"),
        }

    save_cache(new_cache)

    return results


if __name__ == "__main__":
    from fetch_twitch import fetch_twitch_data

    data = fetch_twitch_data()
    metrics = compute_metrics(data)

    for game in metrics[:10]:
        print(game)
