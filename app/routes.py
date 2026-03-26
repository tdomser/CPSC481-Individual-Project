from datetime import datetime, timedelta, timezone
from statistics import pstdev
from threading import Lock, Thread
from time import sleep

from flask import Blueprint, render_template, request

from app.config import CACHE_VERSION
from app.config import LIVE_REFRESH_MAX_PAGES
from app.config import REFRESH_INTERVAL
from app.services.category_logic import build_also_watch_categories
from app.services.category_logic import build_similar_categories
from app.services.category_logic import build_streaming_outlook
from scripts.compute_scores import calculate_score_components
from scripts.compute_scores import compute_scores
from scripts.fetch_twitch import fetch_twitch_data
from scripts.fetch_twitch import fetch_top_streamers_for_game
from scripts.process_data import compute_metrics
from scripts.utils import (
    append_metric_history,
    append_snapshot_archive,
    get_game_history,
    get_game_history_summary,
    load_dashboard_cache,
    save_dashboard_cache,
)

main = Blueprint("main", __name__)

_refresh_lock = Lock()
_refresh_in_progress = False
_periodic_refresh_started = False


def _parse_cached_timestamp(timestamp):
    if not timestamp:
        return None

    try:
        return datetime.fromisoformat(timestamp)
    except ValueError:
        return None


def _cache_is_fresh(cached_snapshot):
    if cached_snapshot.get("cache_version") != CACHE_VERSION:
        return False

    generated_at = _parse_cached_timestamp(cached_snapshot.get("generated_at"))
    if generated_at is None:
        return False

    if generated_at.tzinfo is None:
        generated_at = generated_at.replace(tzinfo=timezone.utc)

    return datetime.now(timezone.utc) - generated_at < REFRESH_INTERVAL


def _cache_is_usable(cached_snapshot):
    return cached_snapshot.get("cache_version") == CACHE_VERSION and bool(cached_snapshot.get("games"))


def _build_dashboard_snapshot(max_pages, source_label, status_message=None):
    raw = fetch_twitch_data(max_pages=max_pages)
    metrics = compute_metrics(raw)
    scored = compute_scores(metrics)
    for index, game in enumerate(scored, start=1):
        game["rank"] = index
    append_metric_history(scored)
    append_snapshot_archive(scored)

    snapshot = {
        "cache_version": CACHE_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "games": scored,
        "total_games": len(scored),
        "source_label": source_label,
        "status_message": status_message,
    }
    save_dashboard_cache(snapshot)
    return snapshot


def _run_background_refresh():
    global _refresh_in_progress

    try:
        _build_dashboard_snapshot(
            max_pages=LIVE_REFRESH_MAX_PAGES,
            source_label="Live Twitch refresh",
            status_message="Live Twitch data is refreshing automatically in the background.",
        )
    finally:
        with _refresh_lock:
            _refresh_in_progress = False


def _start_background_refresh():
    global _refresh_in_progress

    with _refresh_lock:
        if _refresh_in_progress:
            return False
        _refresh_in_progress = True

    thread = Thread(target=_run_background_refresh, daemon=True)
    thread.start()
    return True


def _ensure_periodic_refresh():
    global _periodic_refresh_started

    with _refresh_lock:
        if _periodic_refresh_started:
            return
        _periodic_refresh_started = True

    def _refresh_loop():
        while True:
            try:
                cached_snapshot = load_dashboard_cache()
                if not _cache_is_fresh(cached_snapshot):
                    _start_background_refresh()
            except Exception:
                pass
            sleep(5)

    Thread(target=_refresh_loop, daemon=True).start()


def _build_view_model(games):
    games = [
        game
        for game in games
        if game.get("streams", 0) > 0 and game.get("score", 0) > 0
    ]

    if not games:
        return {
            "games": [],
            "selected_game": None,
            "top_three": [],
            "selected_history_summary": {"count": 0, "start": None, "end": None},
            "selected_analytics": None,
            "selected_streaming_outlook": None,
            "similar_categories": [],
            "also_watch_categories": [],
            "selected_top_streamers": [],
            "metrics_summary": {
                "avg_score": 0,
                "avg_growth": 0,
                "total_viewers": 0,
            },
        }

    selected_key = request.args.get("category", "").strip().lower()
    selected_game = None
    if selected_key:
        selected_game = next(
            (
                game
                for game in games
                if (game.get("game_name") or game.get("game", "")).lower() == selected_key
            ),
            None,
        )
        if selected_game:
            selected_game = _enrich_selected_game_live(selected_game)

    metrics_summary = {
        "avg_score": round(sum(game.get("score", 0) for game in games) / len(games), 2),
        "avg_growth": round(sum(game.get("growth", 0) for game in games) / len(games), 2),
        "total_viewers": sum(game.get("viewers", 0) for game in games),
    }

    similar_categories = _build_similar_categories(selected_game, games)
    excluded_also_watch = {
        (category.get("game_name") or "").lower()
        for category in similar_categories
        if category.get("game_name")
    }

    return {
        "games": games,
        "selected_game": selected_game,
        "selected_description": _build_selected_description(selected_game),
        "selected_history": _build_selected_history(selected_game),
        "selected_history_summary": _build_selected_history_summary(selected_game),
        "selected_analytics": _build_selected_analytics(selected_game),
        "selected_streaming_outlook": _build_streaming_outlook(selected_game, games),
        "similar_categories": similar_categories,
        "also_watch_categories": _build_also_watch_categories(
            selected_game,
            games,
            excluded_names=excluded_also_watch,
        ),
        "selected_top_streamers": _build_selected_top_streamers(selected_game),
        "top_three": games[:3],
        "metrics_summary": metrics_summary,
    }


def _build_selected_description(selected_game):
    if not selected_game:
        return ""

    game_name = selected_game.get("game_name") or selected_game.get("game") or "This category"
    viewers = selected_game.get("viewers", 0)
    streams = selected_game.get("streams", 0)
    growth = selected_game.get("growth", 0)
    ratio = selected_game.get("ratio", 0)

    direction = "holding steady"
    if growth > 0:
        direction = "gaining momentum"
    elif growth < 0:
        direction = "cooling off"

    return (
        f"{game_name} is currently {direction} on Twitch with "
        f"{viewers:,} live viewers across {streams:,} streams and about {ratio:,.2f} "
        f"viewers per stream."
    )


def _build_selected_history(selected_game):
    if not selected_game:
        return []

    game_name = selected_game.get("game_name") or selected_game.get("game")
    if not game_name:
        return []

    return get_game_history(game_name)


def _build_selected_history_summary(selected_game):
    if not selected_game:
        return {"count": 0, "start": None, "end": None}

    game_name = selected_game.get("game_name") or selected_game.get("game")
    if not game_name:
        return {"count": 0, "start": None, "end": None}

    return get_game_history_summary(game_name)


def _enrich_selected_game_live(selected_game):
    if not selected_game:
        return selected_game

    return selected_game


def _build_selected_top_streamers(selected_game):
    if not selected_game:
        return []

    if selected_game.get("top_streamers"):
        return selected_game.get("top_streamers", [])

    game_id = selected_game.get("game_id")
    stream_limit = max(0, min(5, int(selected_game.get("streams", 0) or 0)))
    if not game_id:
        return []
    if stream_limit == 0:
        return []

    try:
        streamers = fetch_top_streamers_for_game(game_id, limit=stream_limit)
    except Exception:
        streamers = []
    return streamers[:stream_limit]


def _format_range_timestamp(timestamp):
    if not timestamp:
        return "No data yet"

    try:
        parsed = datetime.fromisoformat(timestamp)
    except ValueError:
        return timestamp

    return parsed.astimezone().strftime("%b %d, %I:%M %p")


def _calculate_range_delta(history, field, minutes):
    if len(history) < 2:
        return None

    end_entry = history[-1]
    end_timestamp = datetime.fromisoformat(end_entry["timestamp"])
    target_time = end_timestamp - timedelta(minutes=minutes)

    baseline = None
    for entry in reversed(history[:-1]):
        entry_timestamp = datetime.fromisoformat(entry["timestamp"])
        if entry_timestamp <= target_time:
            baseline = entry
            break

    if baseline is None:
        baseline = history[0]

    return round(end_entry.get(field, 0) - baseline.get(field, 0), 2)


def _build_selected_analytics(selected_game):
    if not selected_game:
        return None

    history = _build_selected_history(selected_game)
    if not history:
        score_components = selected_game.get("score_components") or calculate_score_components(selected_game)
        return {
            "rank": selected_game.get("rank"),
            "rank_change": None,
            "score_high": selected_game.get("score", 0),
            "score_low": selected_game.get("score", 0),
            "viewer_high": selected_game.get("viewers", 0),
            "viewer_low": selected_game.get("viewers", 0),
            "avg_score": selected_game.get("score", 0),
            "avg_viewers": selected_game.get("viewers", 0),
            "avg_ratio": selected_game.get("ratio", 0),
            "momentum_label": "Building history",
            "momentum_tone": "muted",
            "volatility_label": "Insufficient history",
            "volatility_tone": "muted",
            "peak_timestamp": _format_range_timestamp(selected_game.get("snapshot_time")),
            "supply_demand_summary": _build_supply_demand_summary(selected_game),
            "score_components": score_components,
            "score_range_tone": "muted",
            "viewer_range_tone": "muted",
            "score_delta_5m": None,
            "score_delta_30m": None,
            "score_delta_60m": None,
            "viewer_delta_5m": None,
            "viewer_delta_30m": None,
            "viewer_delta_60m": None,
        }

    score_values = [entry.get("score", 0) for entry in history]
    viewer_values = [entry.get("viewers", 0) for entry in history]
    ratio_values = [entry.get("ratio", 0) for entry in history]
    rank_values = [entry.get("rank") for entry in history if entry.get("rank") is not None]
    peak_entry = max(history, key=lambda entry: entry.get("score", 0))

    score_components = selected_game.get("score_components") or calculate_score_components(selected_game)

    return {
        "rank": selected_game.get("rank"),
        "rank_change": _calculate_rank_change(rank_values),
        "score_high": round(max(score_values), 2),
        "score_low": round(min(score_values), 2),
        "viewer_high": max(viewer_values),
        "viewer_low": min(viewer_values),
        "avg_score": round(sum(score_values) / len(score_values), 2),
        "avg_viewers": round(sum(viewer_values) / len(viewer_values)),
        "avg_ratio": round(sum(ratio_values) / len(ratio_values), 2),
        "momentum_label": _build_momentum_label(history),
        "momentum_tone": _build_momentum_tone(history),
        "volatility_label": _build_volatility_label(score_values),
        "volatility_tone": _build_volatility_tone(score_values),
        "peak_timestamp": _format_range_timestamp(peak_entry.get("timestamp")),
        "supply_demand_summary": _build_supply_demand_summary(selected_game),
        "score_components": score_components,
        "score_range_tone": _build_range_tone(score_values),
        "viewer_range_tone": _build_range_tone(viewer_values),
        "score_delta_5m": _calculate_range_delta(history, "score", 5),
        "score_delta_30m": _calculate_range_delta(history, "score", 30),
        "score_delta_60m": _calculate_range_delta(history, "score", 60),
        "viewer_delta_5m": _calculate_range_delta(history, "viewers", 5),
        "viewer_delta_30m": _calculate_range_delta(history, "viewers", 30),
        "viewer_delta_60m": _calculate_range_delta(history, "viewers", 60),
    }


def _build_streaming_outlook(selected_game, games):
    return build_streaming_outlook(selected_game, games)


def _build_similar_categories(selected_game, games):
    return build_similar_categories(selected_game, games, request.path)


def _build_also_watch_categories(selected_game, games, excluded_names=None):
    return build_also_watch_categories(selected_game, games, request.path, excluded_names)


def _calculate_rank_change(rank_values):
    if len(rank_values) < 2:
        return None

    return rank_values[-2] - rank_values[-1]


def _build_momentum_label(history):
    if len(history) < 3:
        return "Building history"

    recent = history[-1]
    prior = history[max(0, len(history) - 4)]
    score_delta = recent.get("score", 0) - prior.get("score", 0)
    viewer_delta = recent.get("viewers", 0) - prior.get("viewers", 0)

    if score_delta > 3 or viewer_delta > 500:
        return "Accelerating"
    if score_delta < -3 or viewer_delta < -500:
        return "Cooling off"
    return "Steady"


def _build_momentum_tone(history):
    label = _build_momentum_label(history)
    if label == "Accelerating":
        return "positive"
    if label == "Cooling off":
        return "negative"
    if label == "Steady":
        return "neutral"
    return "muted"


def _build_volatility_label(score_values):
    if len(score_values) < 3:
        return "Building history"

    deviation = pstdev(score_values)
    if deviation >= 6:
        return "High volatility"
    if deviation >= 2.5:
        return "Moderate volatility"
    return "Stable"


def _build_volatility_tone(score_values):
    label = _build_volatility_label(score_values)
    if label == "Stable":
        return "positive"
    if label == "Moderate volatility":
        return "neutral"
    if label == "High volatility":
        return "negative"
    return "muted"


def _build_range_tone(values):
    if len(values) < 2:
        return "muted"

    max_value = max(values)
    min_value = min(values)
    baseline = max(abs(sum(values) / len(values)), 1)
    spread_ratio = (max_value - min_value) / baseline

    if spread_ratio <= 0.12:
        return "positive"
    if spread_ratio <= 0.28:
        return "neutral"
    return "negative"


def _build_supply_demand_summary(selected_game):
    viewers = selected_game.get("viewers", 0)
    streams = selected_game.get("streams", 0)
    ratio = selected_game.get("ratio", 0)

    if ratio >= 2000 and streams <= 15:
        return f"Strong demand with {viewers:,} viewers spread across only {streams:,} streams."
    if ratio >= 1200:
        return f"Demand is healthy with about {ratio:,.0f} viewers per stream."
    if ratio >= 600:
        return f"Supply and demand are fairly balanced right now at about {ratio:,.0f} viewers per stream."
    if ratio >= 250:
        return f"Supply is getting denser right now, with {streams:,} streams sharing {viewers:,} viewers."
    return f"Supply is relatively dense right now, with {streams:,} streams serving {viewers:,} viewers."


@main.route("/", methods=["GET", "POST"])
def home():
    _ensure_periodic_refresh()
    cached_snapshot = load_dashboard_cache()

    if _cache_is_fresh(cached_snapshot):
        view_model = _build_view_model(cached_snapshot.get("games", []))
        return render_template(
            "index.html",
            games=view_model["games"],
            selected_game=view_model["selected_game"],
            selected_description=view_model["selected_description"],
            selected_top_streamers=view_model["selected_top_streamers"],
            selected_history=view_model["selected_history"],
            selected_history_summary=view_model["selected_history_summary"],
            selected_analytics=view_model["selected_analytics"],
            selected_streaming_outlook=view_model["selected_streaming_outlook"],
            similar_categories=view_model["similar_categories"],
            also_watch_categories=view_model["also_watch_categories"],
            top_three=view_model["top_three"],
            metrics_summary=view_model["metrics_summary"],
            error=cached_snapshot.get("status_message"),
            generated_at=cached_snapshot.get("generated_at"),
            total_games=len(view_model["games"]),
            source_label=cached_snapshot.get("source_label", "Cached Twitch data"),
        )

    if _cache_is_usable(cached_snapshot):
        view_model = _build_view_model(cached_snapshot.get("games", []))

        return render_template(
            "index.html",
            games=view_model["games"],
            selected_game=view_model["selected_game"],
            selected_description=view_model["selected_description"],
            selected_top_streamers=view_model["selected_top_streamers"],
            selected_history=view_model["selected_history"],
            selected_history_summary=view_model["selected_history_summary"],
            selected_analytics=view_model["selected_analytics"],
            selected_streaming_outlook=view_model["selected_streaming_outlook"],
            similar_categories=view_model["similar_categories"],
            also_watch_categories=view_model["also_watch_categories"],
            top_three=view_model["top_three"],
            metrics_summary=view_model["metrics_summary"],
            error=cached_snapshot.get("status_message"),
            generated_at=cached_snapshot.get("generated_at"),
            total_games=len(view_model["games"]),
            source_label=cached_snapshot.get("source_label", "Cached Twitch data"),
        )

    try:
        snapshot = _build_dashboard_snapshot(
            max_pages=LIVE_REFRESH_MAX_PAGES,
            source_label="Live Twitch refresh",
            status_message="Live Twitch data is refreshing automatically in the background.",
        )
        view_model = _build_view_model(snapshot["games"])

        return render_template(
            "index.html",
            games=view_model["games"],
            selected_game=view_model["selected_game"],
            selected_description=view_model["selected_description"],
            selected_top_streamers=view_model["selected_top_streamers"],
            selected_history=view_model["selected_history"],
            selected_history_summary=view_model["selected_history_summary"],
            selected_analytics=view_model["selected_analytics"],
            selected_streaming_outlook=view_model["selected_streaming_outlook"],
            similar_categories=view_model["similar_categories"],
            also_watch_categories=view_model["also_watch_categories"],
            top_three=view_model["top_three"],
            metrics_summary=view_model["metrics_summary"],
            error=snapshot["status_message"],
            generated_at=snapshot["generated_at"],
            total_games=len(view_model["games"]),
            source_label=snapshot["source_label"],
        )
    except Exception as exc:
        return render_template(
            "index.html",
            games=[],
            selected_game=None,
            selected_description="",
            selected_top_streamers=[],
            selected_history=[],
            selected_history_summary={"count": 0, "start": None, "end": None},
            selected_analytics=None,
            selected_streaming_outlook=None,
            similar_categories=[],
            also_watch_categories=[],
            top_three=[],
            metrics_summary={"avg_score": 0, "avg_growth": 0, "total_viewers": 0},
            error=str(exc),
            generated_at=None,
            total_games=0,
            source_label="No data loaded",
        )
