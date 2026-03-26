from datetime import datetime, timedelta, timezone
from statistics import pstdev
from threading import Lock, Thread
from time import sleep
from urllib.parse import quote

from flask import Blueprint, render_template, request

from scripts.compute_scores import calculate_score_components
from scripts.compute_scores import compute_scores
from scripts.fetch_twitch import fetch_twitch_data
from scripts.fetch_twitch import fetch_game_live_snapshot
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

REFRESH_INTERVAL = timedelta(seconds=20)
LIVE_REFRESH_MAX_PAGES = 12
CACHE_VERSION = 8

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

    game_id = selected_game.get("game_id")
    if not game_id:
        return selected_game

    try:
        live_snapshot = fetch_game_live_snapshot(game_id, top_streamer_limit=5)
    except Exception:
        return selected_game

    streams = live_snapshot.get("streams", selected_game.get("streams", 0))
    viewers = live_snapshot.get("viewers", selected_game.get("viewers", 0))
    ratio = round((viewers / streams), 2) if streams > 0 else 0
    rescored_games = compute_scores(
        [
            {
                **selected_game,
                "streams": streams,
                "viewers": viewers,
                "ratio": ratio,
            }
        ]
    )
    rescored_game = rescored_games[0] if rescored_games else {}

    return {
        **selected_game,
        "streams": streams,
        "viewers": viewers,
        "ratio": ratio,
        "score": rescored_game.get("score", selected_game.get("score", 0)),
        "score_components": rescored_game.get(
            "score_components",
            selected_game.get("score_components", {}),
        ),
        "opportunity": rescored_game.get(
            "opportunity",
            selected_game.get("opportunity"),
        ),
        "top_streamers": live_snapshot.get("top_streamers", []),
    }


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


def _percentile_rank(values, target):
    comparable = sorted(value for value in values if value is not None)
    if not comparable:
        return None

    less_or_equal = sum(1 for value in comparable if value <= target)
    return round((less_or_equal / len(comparable)) * 100)


def _build_streaming_outlook(selected_game, games):
    if not selected_game:
        return None

    ratio = selected_game.get("ratio", 0)
    viewers = selected_game.get("viewers", 0)
    streams = selected_game.get("streams", 0)
    score = selected_game.get("score", 0)

    ratio_percentile = _percentile_rank([game.get("ratio", 0) for game in games], ratio)
    score_percentile = _percentile_rank([game.get("score", 0) for game in games], score)
    viewer_percentile = _percentile_rank([game.get("viewers", 0) for game in games], viewers)
    stream_percentile = _percentile_rank([game.get("streams", 0) for game in games], streams)
    verdict = "Mixed opportunity"
    recommendation = "Worth considering, but not a clear standout."
    tone = "neutral"
    summary = (
        f"This category has {viewers:,} viewers across {streams:,} streams, which works out to about "
        f"{ratio:,.0f} viewers per stream."
    )
    why_points = [
        f"Viewer-per-stream demand is ahead of about {ratio_percentile or 0}% of tracked categories.",
        f"Raw viewership is ahead of about {viewer_percentile or 0}% of tracked categories.",
        f"Overall opportunity score is ahead of about {score_percentile or 0}% of tracked categories.",
    ]
    caution_points = [
        "Results here can depend a lot on timing and how strong the current live lineup is.",
        "A nearby category may still offer a cleaner discovery path at the same moment.",
    ]
    streamer_fit = "Best for creators who want a mix of audience demand and manageable competition."

    ratio_percentile = ratio_percentile or 0
    score_percentile = score_percentile or 0
    viewer_percentile = viewer_percentile or 0
    stream_percentile = stream_percentile or 0

    crowded_signal = (
        (ratio <= 70 and streams >= 8)
        or (ratio <= 100 and streams >= 14)
        or (ratio <= 140 and streams >= 24)
    )

    promising_signal = (
        (ratio >= 850 and viewers >= 800)
        or (ratio >= 600 and viewers >= 1500)
        or (ratio >= 400 and viewers >= 4000)
        or (score >= 42 and ratio >= 300)
        or (ratio_percentile >= 68 and score_percentile >= 50)
    )

    if promising_signal and not crowded_signal:
        verdict = "Promising opportunity"
        recommendation = "A good category to test if it fits your content."
        tone = "positive"
        summary = (
            f"This category looks promising right now. With about {ratio:,.0f} viewers per stream and "
            f"{viewers:,} live viewers, it is showing enough live demand to give creators a better shot at discovery."
        )
        why_points = [
            f"Viewer-per-stream demand is ahead of about {ratio_percentile}% of tracked categories.",
            f"The category still has a live audience of {viewers:,} viewers available right now.",
            f"Its current score and live balance are stronger than a large part of the tracked field.",
        ]
        caution_points = [
            "This is still not a guaranteed breakout lane, so timing and positioning matter.",
            "If stream count rises sharply, the current edge can narrow quickly.",
        ]
        streamer_fit = "Good for creators looking for a category with real upside without jumping into the hardest lanes."
    elif crowded_signal:
        verdict = "Crowded category"
        recommendation = "Probably a tougher category for growth right now."
        tone = "negative"
        summary = (
            f"This category looks crowded right now. Supply is outpacing demand, with {streams:,} live channels sharing "
            f"about {ratio:,.0f} viewers per stream."
        )
        why_points = [
            f"Viewer-per-stream demand is only ahead of about {ratio_percentile}% of tracked categories.",
            f"There are {streams:,} active streams competing for the same audience pool.",
            "Discoverability is likely harder here than in better-balanced alternatives.",
        ]
        caution_points = [
            "You may need stronger branding, a niche angle, or outside traffic to grow here.",
            "A smaller but less crowded category could offer a better entry point right now.",
        ]
        streamer_fit = "Better for established creators or creators with a specific niche in this game."
    else:
        verdict = "Mixed opportunity"
        recommendation = "Potentially workable, but not a standout lane."
        tone = "neutral"
        summary = (
            f"This category sits in the middle of the field. With about {ratio:,.0f} viewers per stream, it looks usable, "
            f"but not clearly better than stronger alternatives."
        )
        why_points = [
            f"Viewer-per-stream demand is ahead of about {ratio_percentile}% of tracked categories.",
            f"It has enough audience to be viable, but not enough to clearly outpace stronger alternatives.",
            f"The overall score and audience level are more middle-of-the-pack than standout.",
        ]
        caution_points = [
            "A stronger niche fit may matter more here than pure category selection.",
            "This lane can be usable, but better discovery options may exist nearby.",
        ]
        streamer_fit = "Best for creators whose content naturally fits the game, rather than creators optimizing purely for discoverability."

    comparison = (
        f"Current standing: score ahead of {score_percentile}% of tracked categories, "
        f"viewers ahead of {viewer_percentile}%, viewer-per-stream ahead of {ratio_percentile}%, "
        f"and stream count ahead of {stream_percentile}%."
    )

    return {
        "verdict": verdict,
        "recommendation": recommendation,
        "tone": tone,
        "summary": summary,
        "comparison": comparison,
        "why_points": why_points,
        "caution_points": caution_points,
        "streamer_fit": streamer_fit,
    }


def _build_similar_categories(selected_game, games):
    if not selected_game:
        return []

    selected_name = (selected_game.get("game_name") or selected_game.get("game") or "").lower()
    selected_score = selected_game.get("score", 0)
    selected_ratio = selected_game.get("ratio", 0)
    selected_viewers = selected_game.get("viewers", 0)
    selected_streams = selected_game.get("streams", 0)
    selected_growth = selected_game.get("growth", 0)

    closest_candidates = []
    stronger_candidates = []
    promising_candidates = []
    all_candidates = []

    for game in games:
        game_name = (game.get("game_name") or game.get("game") or "").lower()
        if not game_name or game_name == selected_name:
            continue

        game_score = game.get("score", 0)
        game_ratio = game.get("ratio", 0)
        game_viewers = game.get("viewers", 0)
        game_streams = game.get("streams", 0)

        score_gap = abs(game.get("score", 0) - selected_score)
        ratio_gap = abs(game.get("ratio", 0) - selected_ratio) / max(selected_ratio, 1)
        viewer_gap = abs(game.get("viewers", 0) - selected_viewers) / max(selected_viewers, 1)
        stream_gap = abs(game.get("streams", 0) - selected_streams) / max(selected_streams, 1)
        distance = score_gap + (ratio_gap * 12) + (viewer_gap * 8) + (stream_gap * 5)

        candidate = {
            "game_name": game.get("game_name") or game.get("game"),
            "score": game_score,
            "viewers": game_viewers,
            "ratio": game_ratio,
            "box_art_url": game.get("box_art_url"),
            "opportunity": game.get("opportunity"),
            "href": f"{request.path}?category={quote(game.get('game_name') or game.get('game') or '')}",
            "distance": distance,
        }
        all_candidates.append(candidate.copy())

        if (
            game_score >= max(selected_score * 1.08, selected_score + 3)
            and game_ratio >= max(selected_ratio * 1.08, selected_ratio + 80)
            and game_viewers >= selected_viewers * 0.7
            and game_streams <= max(selected_streams * 1.4, selected_streams + 6)
        ):
            candidate["slot"] = "Stronger alternative"
            candidate["recommendation"] = "A bit stronger than the current category"
            candidate["reason"] = (
                f"It is outperforming this category on score and demand per stream, which suggests a slightly better streaming lane right now."
            )
            stronger_candidates.append(candidate)
        elif (
            game_score <= selected_score * 0.98
            and game_ratio >= selected_ratio * 0.75
            and (
                game.get("growth", 0) >= selected_growth + 2
                or game.get("viewer_change", 0) > max(selected_game.get("viewer_change", 0), 0)
            )
        ):
            candidate["slot"] = "Early growth option"
            candidate["recommendation"] = "Early growth option"
            candidate["reason"] = (
                f"It is a bit weaker right now, but its recent growth signals suggest it could be improving into a better opportunity."
            )
            promising_candidates.append(candidate)
        else:
            candidate["slot"] = "Closest comparison"
            candidate["recommendation"] = "Closest comparison"
            candidate["reason"] = (
                f"It has the closest overall mix of score, live audience, and viewers per stream to the category you are viewing."
            )
            closest_candidates.append(candidate)

    closest_candidates.sort(key=lambda item: item["distance"])
    stronger_candidates.sort(key=lambda item: item["distance"])
    promising_candidates.sort(key=lambda item: item["distance"])
    all_candidates.sort(key=lambda item: item["distance"])

    used_names = set()

    def _pick_candidate(bucket, fallback_builder):
        for candidate in bucket:
            key = candidate["game_name"].lower()
            if key not in used_names:
                used_names.add(key)
                return candidate

        for candidate in all_candidates:
            key = candidate["game_name"].lower()
            if key not in used_names:
                fallback = fallback_builder(candidate.copy())
                used_names.add(key)
                return fallback

        return None

    selected = []

    closest_choice = _pick_candidate(
        closest_candidates,
        lambda candidate: {
            **candidate,
            "slot": "Closest comparison",
            "recommendation": "Closest comparison",
            "reason": "This is the nearest overall match on score, live audience, and viewers per stream from the remaining tracked categories.",
        },
    )
    if closest_choice:
        selected.append(closest_choice)

    stronger_choice = _pick_candidate(
        stronger_candidates,
        lambda candidate: {
            **candidate,
            "slot": "Stronger alternative",
            "recommendation": "Best stronger alternative available",
            "reason": "This is the strongest nearby alternative from the remaining tracked categories, even if it is not a perfect direct upgrade on every metric.",
        },
    )
    if stronger_choice:
        selected.append(stronger_choice)

    promising_choice = _pick_candidate(
        promising_candidates,
        lambda candidate: {
            **candidate,
            "slot": "Early growth option",
            "recommendation": "Best early growth option available",
            "reason": "This is the closest remaining category that still looks like an earlier-stage bet with some room to improve.",
        },
    )
    if promising_choice:
        selected.append(promising_choice)

    return selected


def _build_also_watch_categories(selected_game, games, excluded_names=None):
    if not selected_game:
        return []

    excluded_names = {name.lower() for name in (excluded_names or set()) if name}
    selected_name = (selected_game.get("game_name") or selected_game.get("game") or "").lower()
    selected_ratio = selected_game.get("ratio", 0)
    selected_viewers = selected_game.get("viewers", 0)
    selected_growth = selected_game.get("growth", 0)

    candidates = []
    for game in games:
        game_name = (game.get("game_name") or game.get("game") or "").lower()
        if not game_name or game_name == selected_name or game_name in excluded_names:
            continue

        game_ratio = game.get("ratio", 0)
        game_viewers = game.get("viewers", 0)
        game_growth = game.get("growth", 0)
        game_streams = game.get("streams", 0)

        audience_gap = abs(game_viewers - selected_viewers) / max(selected_viewers, 1)
        ratio_gap = abs(game_ratio - selected_ratio) / max(max(selected_ratio, game_ratio), 1)
        growth_gap = abs(game_growth - selected_growth) / max(max(abs(selected_growth), abs(game_growth)), 5)

        affinity = 100
        affinity -= audience_gap * 48
        affinity -= ratio_gap * 18
        affinity -= growth_gap * 8
        affinity -= min(game_streams / 120, 6)

        viewer_band = game_viewers / max(selected_viewers, 1)
        ratio_band = game_ratio / max(selected_ratio, 1)

        if 0.65 <= viewer_band <= 1.6:
            affinity += 12
        if 0.75 <= ratio_band <= 1.45:
            affinity += 6
        if game_growth > 0:
            affinity += min(game_growth / 5, 5)

        if affinity <= 0:
            continue

        audience_bucket = round(game_viewers / 5000) if game_viewers > 0 else 0

        candidates.append({
            "game_name": game.get("game_name") or game.get("game"),
            "viewers": game_viewers,
            "ratio": round(game_ratio, 2),
            "opportunity": game.get("opportunity"),
            "box_art_url": game.get("box_art_url"),
            "href": f"{request.path}?category={quote(game.get('game_name') or game.get('game') or '')}",
            "affinity": round(affinity, 1),
            "audience_bucket": audience_bucket,
        })

    candidates.sort(key=lambda item: (-item["affinity"], -item["viewers"], -item["ratio"]))
    selected = []
    used_names = set()
    used_buckets = set()

    for candidate in candidates:
        key = candidate["game_name"].lower()
        bucket = candidate["audience_bucket"]
        if key in used_names:
            continue
        if bucket in used_buckets and len(selected) < 3:
            continue
        selected.append(candidate)
        used_names.add(key)
        used_buckets.add(bucket)
        if len(selected) == 5:
            break

    if len(selected) < 5:
        for candidate in candidates:
            key = candidate["game_name"].lower()
            if key in used_names:
                continue
            selected.append(candidate)
            used_names.add(key)
            if len(selected) == 5:
                break

    return selected


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
