def _get_opportunity_label(score):
    if score >= 95:
        return "EXTREME"
    if score >= 80:
        return "VERY HIGH"
    if score >= 65:
        return "HIGH"
    if score >= 30:
        return "MEDIUM"
    if score >= 16:
        return "LOW"
    return "VERY LOW"


def _get_dynamic_opportunity_label(score, percentile):
    if score < 10:
        return "VERY LOW"
    if score < 20:
        return "LOW"

    if percentile >= 0.985 and score >= 55:
        return "EXTREME"
    if percentile >= 0.94 and score >= 48:
        return "VERY HIGH"
    if percentile >= 0.84 and score >= 40:
        return "HIGH"
    if percentile >= 0.42 and score >= 28:
        return "MEDIUM"
    if percentile >= 0.18:
        return "LOW"
    return "VERY LOW"


def calculate_score_components(game):
    viewers = game.get("viewers", 0)
    streams = game.get("streams", 0)
    ratio = game.get("ratio", 0)
    growth = game.get("growth", 0)
    viewer_change = game.get("viewer_change", 0)

    viewer_score = min(viewers / 1000, 40)
    ratio_score = min(ratio / 250, 35)
    growth_score = min(max(growth, 0), 25)
    momentum_bonus = min(max(viewer_change, 0) / 500, 10)
    stream_penalty = 0

    if streams == 1:
        stream_penalty = 12
    elif streams == 2:
        stream_penalty = 5

    total_score = max(
        viewer_score
        + ratio_score
        + growth_score
        + momentum_bonus
        - stream_penalty,
        0,
    )

    return {
        "viewer_score": round(viewer_score, 2),
        "ratio_score": round(ratio_score, 2),
        "growth_score": round(growth_score, 2),
        "momentum_bonus": round(momentum_bonus, 2),
        "stream_penalty": round(stream_penalty, 2),
        "total_score": round(total_score, 2),
    }


def compute_scores(data):
    results = []

    for game in data:
        streams = game.get("streams", 0)

        if streams <= 0:
            continue

        score_components = calculate_score_components(game)
        score = score_components["total_score"]

        rounded_score = round(score, 2)
        if rounded_score <= 0:
            continue

        scored_game = {
            **game,
            "score_components": score_components,
            "score": rounded_score,
            "opportunity": _get_opportunity_label(score),
        }
        results.append(scored_game)

    results.sort(key=lambda x: x["score"], reverse=True)

    total_results = len(results)
    if total_results == 0:
        return results

    if total_results < 12:
        for game in results:
            game["opportunity"] = _get_opportunity_label(game["score"])
        return results

    for index, game in enumerate(results):
        percentile = 1 - (index / max(total_results - 1, 1))
        game["opportunity"] = _get_dynamic_opportunity_label(
            game["score"],
            percentile,
        )

    return results


if __name__ == "__main__":
    from fetch_twitch import fetch_twitch_data
    from process_data import compute_metrics

    raw = fetch_twitch_data()
    metrics = compute_metrics(raw)
    scored = compute_scores(metrics)

    for game in scored[:10]:
        print(game)
