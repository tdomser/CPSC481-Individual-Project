import os
from datetime import timedelta


class Config:
    CACHE_VERSION = 8
    LIVE_REFRESH_MAX_PAGES = int(os.getenv("LIVE_REFRESH_MAX_PAGES", "12"))
    REFRESH_INTERVAL_SECONDS = int(os.getenv("REFRESH_INTERVAL_SECONDS", "20"))
    DEBUG = os.getenv("FLASK_DEBUG", "1") == "1"


REFRESH_INTERVAL = timedelta(seconds=Config.REFRESH_INTERVAL_SECONDS)
CACHE_VERSION = Config.CACHE_VERSION
LIVE_REFRESH_MAX_PAGES = Config.LIVE_REFRESH_MAX_PAGES
