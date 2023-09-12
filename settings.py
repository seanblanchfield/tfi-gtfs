
import os

GTFS_STATIC_URL = os.environ.get('GTFS_STATIC_URL', "https://www.transportforireland.ie/transitData/Data/GTFS_Realtime.zip")
GTFS_LIVE_URL = os.environ.get('GTFS_LIVE_URL', "https://api.nationaltransport.ie/gtfsr/v2/TripUpdates")
API_KEY = os.environ.get('API_KEY')
# Redis URL, probably something like redis://localhost:6379
REDIS_URL = os.environ.get('REDIS_URL', None)
POLLING_PERIOD = os.environ.get('POLLING_PERIOD', 60)
MAX_MINUTES = os.environ.get('MAX_MINUTES', 60)
HOST = os.environ.get('HOST', 'localhost')
PORT = os.environ.get('PORT', 7341)
WORKERS = os.environ.get('WORKERS', 1)

# Download the static GTFS dataset every Sunday at 7am
DOWNLOAD_SCHEDULE = os.environ.get('DOWNLOAD_SCHEDULE', '0 7 * * SUN')

# set default logging level to INFO
LOG_LEVEL = os.environ.get('LOG_LEVEL', 'INFO').upper()
if LOG_LEVEL not in ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']:
    print(f"Invalid log level: {LOG_LEVEL}. Defaulting to 'INFO'.")
    LOG_LEVEL = 'INFO'

# Optimize memory usage by only storing data related to a given list of stops
FILTER_STOPS = os.environ.get('FILTER_STOPS', None)
if FILTER_STOPS:
    FILTER_STOPS = [stop.strip() for stop in FILTER_STOPS.split(',')]

# Optionally create a `local_settings.py` file to override these settings
# during development. This file will be ignored by git. 
try:
    from local_settings import *
except ImportError:
    pass
