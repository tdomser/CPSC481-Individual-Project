from urllib.parse import quote


def percentile_rank(values, target):
    comparable = sorted(value for value in values if value is not None)
    if not comparable:
        return None

    less_or_equal = sum(1 for value in comparable if value <= target)
    return round((less_or_equal / len(comparable)) * 100)


def lane_score(ratio_percentile, score_percentile, viewer_percentile, stream_percentile):
    return round(
        (ratio_percentile * 0.42)
        + (score_percentile * 0.28)
        + (viewer_percentile * 0.18)
        - (stream_percentile * 0.12),
        2,
    )


def build_streaming_outlook(selected_game, games):
    if not selected_game:
        return None

    ratio = selected_game.get("ratio", 0)
    viewers = selected_game.get("viewers", 0)
    streams = selected_game.get("streams", 0)
    score = selected_game.get("score", 0)

    ratio_values = [game.get("ratio", 0) for game in games]
    score_values = [game.get("score", 0) for game in games]
    viewer_values = [game.get("viewers", 0) for game in games]
    stream_values = [game.get("streams", 0) for game in games]

    ratio_percentile = percentile_rank(ratio_values, ratio) or 0
    score_percentile = percentile_rank(score_values, score) or 0
    viewer_percentile = percentile_rank(viewer_values, viewers) or 0
    stream_percentile = percentile_rank(stream_values, streams) or 0

    verdict = "Mixed opportunity"
    recommendation = "Worth considering, but not a clear standout."
    tone = "neutral"
    summary = (
        f"This category has {viewers:,} viewers across {streams:,} streams, which works out to about "
        f"{ratio:,.0f} viewers per stream."
    )
    why_points = [
        f"Viewer-per-stream demand is ahead of about {ratio_percentile}% of tracked categories.",
        f"Raw viewership is ahead of about {viewer_percentile}% of tracked categories.",
        f"Overall opportunity score is ahead of about {score_percentile}% of tracked categories.",
    ]
    caution_points = [
        "Results here can depend a lot on timing and how strong the current live lineup is.",
        "A nearby category may still offer a cleaner discovery path at the same moment.",
    ]
    streamer_fit = "Best for creators who want a mix of audience demand and manageable competition."

    current_lane_score = lane_score(
        ratio_percentile,
        score_percentile,
        viewer_percentile,
        stream_percentile,
    )
    lane_scores = []
    for game in games:
        lane_scores.append(
            lane_score(
                percentile_rank(ratio_values, game.get("ratio", 0)) or 0,
                percentile_rank(score_values, game.get("score", 0)) or 0,
                percentile_rank(viewer_values, game.get("viewers", 0)) or 0,
                percentile_rank(stream_values, game.get("streams", 0)) or 0,
            )
        )

    lane_percentile = percentile_rank(lane_scores, current_lane_score) or 0
    crowd_pressure = stream_percentile - ratio_percentile

    crowded_signal = (
        lane_percentile <= 33
    ) or (
        stream_percentile >= 88
        and ratio_percentile <= 72
        and crowd_pressure >= 14
    ) or (
        ratio <= 275
        and streams >= 6
    )

    promising_signal = (
        lane_percentile >= 67
    ) or (
        ratio_percentile >= 88
        and stream_percentile <= 84
    ) or (
        ratio >= 1200
        and viewers >= 1500
        and streams <= 18
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


def build_similar_categories(selected_game, games, base_path):
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
    developing_candidates = []
    fallback_candidates = []

    for game in games:
        game_name = (game.get("game_name") or game.get("game") or "").lower()
        if not game_name or game_name == selected_name:
            continue

        game_score = game.get("score", 0)
        game_ratio = game.get("ratio", 0)
        game_viewers = game.get("viewers", 0)
        game_streams = game.get("streams", 0)

        score_gap = abs(game_score - selected_score) / max(selected_score, 8)
        ratio_gap = abs(game_ratio - selected_ratio) / max(selected_ratio, 75)
        viewer_gap = abs(game_viewers - selected_viewers) / max(selected_viewers, 250)
        stream_gap = abs(game_streams - selected_streams) / max(selected_streams, 2)
        growth_gap = abs(game.get("growth", 0) - selected_growth) / max(abs(selected_growth), 5)
        distance = (
            (score_gap * 30)
            + (ratio_gap * 35)
            + (viewer_gap * 20)
            + (stream_gap * 10)
            + (growth_gap * 5)
        )
        viewer_band = game_viewers / max(selected_viewers, 1)
        ratio_band = game_ratio / max(selected_ratio, 1)
        stream_band = game_streams / max(selected_streams, 1)

        candidate = {
            "game_name": game.get("game_name") or game.get("game"),
            "score": game_score,
            "viewers": game_viewers,
            "ratio": game_ratio,
            "box_art_url": game.get("box_art_url"),
            "opportunity": game.get("opportunity"),
            "href": f"{base_path}?category={quote(game.get('game_name') or game.get('game') or '')}",
            "distance": distance,
        }
        fallback_candidates.append(candidate)

        if (
            0.55 <= viewer_band <= 1.8
            and 0.65 <= ratio_band <= 1.6
            and 0.4 <= stream_band <= 2.2
        ):
            closest_candidates.append(
                {
                    **candidate,
                    "slot": "Closest comparison",
                    "recommendation": "Most comparable tracked lane",
                    "reason": "Its tracked score, audience size, and viewers-per-stream profile are the closest overall match to this category.",
                }
            )

        if (
            game_score >= max(selected_score * 1.08, selected_score + 2)
            and game_ratio >= max(selected_ratio * 1.1, selected_ratio + 40)
            and game_viewers >= selected_viewers * 0.6
            and game_streams <= max(selected_streams * 1.6, selected_streams + 8)
        ):
            stronger_candidates.append(
                {
                    **candidate,
                    "slot": "Stronger alternative",
                    "recommendation": "Stronger tracked lane",
                    "reason": "It is currently posting a better score and stronger viewers-per-stream balance without requiring a much bigger audience jump.",
                }
            )

        if (
            game_score <= selected_score * 1.05
            and 0.45 <= viewer_band <= 1.35
            and game_ratio >= selected_ratio * 0.9
            and (
                game.get("growth", 0) >= selected_growth + 2
                or game.get("viewer_change", 0) > max(selected_game.get("viewer_change", 0), 0)
            )
        ):
            developing_candidates.append(
                {
                    **candidate,
                    "slot": "Developing option",
                    "recommendation": "Nearby category with improving momentum",
                    "reason": "Its current lane is still in range of this category, but the recent movement looks a bit healthier.",
                }
            )

    closest_candidates.sort(key=lambda item: item["distance"])
    stronger_candidates.sort(key=lambda item: item["distance"])
    developing_candidates.sort(key=lambda item: item["distance"])
    fallback_candidates.sort(key=lambda item: item["distance"])

    used_names = set()

    def pick_candidate(bucket):
        for candidate in bucket:
            key = candidate["game_name"].lower()
            if key not in used_names:
                used_names.add(key)
                return candidate
        return None

    def pick_fallback(slot, recommendation, reason):
        for candidate in fallback_candidates:
            key = candidate["game_name"].lower()
            if key not in used_names:
                used_names.add(key)
                return {
                    **candidate,
                    "slot": slot,
                    "recommendation": recommendation,
                    "reason": reason,
                }
        return None

    selected = []

    closest_choice = pick_candidate(closest_candidates) or pick_fallback(
        "Closest comparison",
        "Closest comparison",
        "This is the nearest overall match on score, live audience, and viewers per stream in the current tracked snapshot.",
    )
    if closest_choice:
        selected.append(closest_choice)

    stronger_choice = pick_candidate(stronger_candidates) or pick_fallback(
        "Stronger alternative",
        "Best stronger alternative available",
        "This is the strongest nearby tracked option still reasonably close to the current category.",
    )
    if stronger_choice:
        selected.append(stronger_choice)

    developing_choice = pick_candidate(developing_candidates) or pick_fallback(
        "Developing option",
        "Best developing option available",
        "This is the closest remaining tracked category showing some room to improve based on recent movement.",
    )
    if developing_choice:
        selected.append(developing_choice)

    return selected


def build_also_watch_categories(selected_game, games, base_path, excluded_names=None):
    if not selected_game:
        return []

    excluded_names = {name.lower() for name in (excluded_names or set()) if name}
    selected_name = (selected_game.get("game_name") or selected_game.get("game") or "").lower()
    selected_ratio = selected_game.get("ratio", 0)
    selected_viewers = selected_game.get("viewers", 0)
    selected_growth = selected_game.get("growth", 0)
    selected_streams = selected_game.get("streams", 0)

    candidates = []
    fallback_candidates = []

    for game in games:
        game_name = (game.get("game_name") or game.get("game") or "").lower()
        if not game_name or game_name == selected_name or game_name in excluded_names:
            continue

        game_ratio = game.get("ratio", 0)
        game_viewers = game.get("viewers", 0)
        game_growth = game.get("growth", 0)
        game_streams = game.get("streams", 0)

        viewer_band = game_viewers / max(selected_viewers, 1)
        ratio_band = game_ratio / max(selected_ratio, 1)
        stream_band = game_streams / max(selected_streams, 1)
        audience_gap = abs(1 - viewer_band)
        ratio_gap = abs(1 - ratio_band)
        stream_gap = abs(1 - stream_band)
        growth_gap = abs(game_growth - selected_growth) / max(max(abs(selected_growth), abs(game_growth)), 5)

        affinity = 100
        affinity -= audience_gap * 38
        affinity -= ratio_gap * 28
        affinity -= stream_gap * 16
        affinity -= growth_gap * 10

        if 0.65 <= viewer_band <= 1.6:
            affinity += 12
        if 0.75 <= ratio_band <= 1.45:
            affinity += 6
        if 0.7 <= stream_band <= 1.5:
            affinity += 5
        if game_growth > selected_growth:
            affinity += min((game_growth - selected_growth) / 4, 6)

        audience_bucket = round(game_viewers / max(selected_viewers, 1), 1)
        candidate = {
            "game_name": game.get("game_name") or game.get("game"),
            "viewers": game_viewers,
            "ratio": round(game_ratio, 2),
            "opportunity": game.get("opportunity"),
            "box_art_url": game.get("box_art_url"),
            "href": f"{base_path}?category={quote(game.get('game_name') or game.get('game') or '')}",
            "affinity": round(affinity, 1),
            "audience_bucket": audience_bucket,
        }
        fallback_candidates.append(candidate)

        if not (0.35 <= viewer_band <= 2.4 and 0.45 <= ratio_band <= 1.9 and 0.25 <= stream_band <= 2.8):
            continue
        if affinity <= 25:
            continue

        candidates.append(candidate)

    candidates.sort(key=lambda item: (-item["affinity"], -item["viewers"], -item["ratio"]))
    fallback_candidates.sort(key=lambda item: (-item["affinity"], -item["viewers"], -item["ratio"]))

    selected = []
    used_names = set()
    used_buckets = set()

    for candidate in candidates:
        key = candidate["game_name"].lower()
        bucket = candidate["audience_bucket"]
        if key in used_names:
            continue
        if bucket in used_buckets and len(selected) < 4:
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

    if len(selected) < 5:
        for candidate in fallback_candidates:
            key = candidate["game_name"].lower()
            if key in used_names:
                continue
            selected.append(candidate)
            used_names.add(key)
            if len(selected) == 5:
                break

    return selected
