# Live Trend Predictor

A Flask app for exploring tracked Twitch categories, scoring their current opportunity, and comparing nearby streaming lanes from a live snapshot.

## What the app does

- Pulls live Twitch stream data with the Helix API
- Aggregates that data into per-category viewer, stream, and viewer-per-stream metrics
- Scores categories using current demand, supply balance, and short-term movement
- Shows a category detail page with:
  - streaming outlook
  - top live streamers
  - similar categories
  - nearby audience-interest categories
  - score and viewer history

## Project structure

- [app/routes.py](/C:/Users/tdoms/OneDrive/Desktop/School/CPSC481/CPSC481-Individual-Project/app/routes.py): Flask routes and page view-model assembly
- [app/services/category_logic.py](/C:/Users/tdoms/OneDrive/Desktop/School/CPSC481/CPSC481-Individual-Project/app/services/category_logic.py): streaming outlook and recommendation helpers
- [app/config.py](/C:/Users/tdoms/OneDrive/Desktop/School/CPSC481/CPSC481-Individual-Project/app/config.py): app settings and refresh limits
- [scripts/fetch_twitch.py](/C:/Users/tdoms/OneDrive/Desktop/School/CPSC481/CPSC481-Individual-Project/scripts/fetch_twitch.py): Twitch API collection
- [scripts/process_data.py](/C:/Users/tdoms/OneDrive/Desktop/School/CPSC481/CPSC481-Individual-Project/scripts/process_data.py): metric preparation
- [scripts/compute_scores.py](/C:/Users/tdoms/OneDrive/Desktop/School/CPSC481/CPSC481-Individual-Project/scripts/compute_scores.py): category scoring
- [scripts/utils.py](/C:/Users/tdoms/OneDrive/Desktop/School/CPSC481/CPSC481-Individual-Project/scripts/utils.py): cache and history helpers

## Setup

1. Create and activate a virtual environment.
2. Install dependencies:

```powershell
pip install -r requirements.txt
```

3. Copy `.env.example` to `.env` and add your Twitch API credentials.

## Run

```powershell
python run.py
```

Then open `http://127.0.0.1:5000/`.

## Environment variables

- `TWITCH_CLIENT_ID`: Twitch API client id
- `TWITCH_CLIENT_SECRET`: Twitch API client secret
- `FLASK_DEBUG`: `1` for debug mode, `0` to disable it
- `LIVE_REFRESH_MAX_PAGES`: how many Twitch stream pages to sample each refresh
- `REFRESH_INTERVAL_SECONDS`: background refresh interval

## Notes on the data

- The dashboard is based on a tracked live snapshot, not a perfect full-Twitch behavioral dataset.
- Recommendation sections like similar categories and nearby audience interests are heuristic comparisons built from the tracked snapshot.
- Cache files are written under `cache/` and are ignored by git.

## Suggested next improvements

- Add tests for scoring, outlook classification, and recommendation selection
- Move analytics helpers out of the route file into dedicated service modules
- Add UI filters for outlook labels and minimum audience size
- Add sparklines for category history on the home gallery
