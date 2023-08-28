
import os

GTFS_STATIC_URL = os.environ.get('GTFS_STATIC_URL', "https://www.transportforireland.ie/transitData/Data/GTFS_Realtime.zip")
GTFS_LIVE_URL = os.environ.get('GTFS_LIVE_URL', "https://api.nationaltransport.ie/gtfsr/v2/TripUpdates")
API_KEY = os.environ.get('API_KEY')

# set default logging level to INFO
LOG_LEVEL = os.environ.get('LOG_LEVEL', 'INFO')

# Optionally create a `local_settings.py` file to override these settings
# during development. This file will be ignored by git. 
try:
    from local_settings import *
except ImportError:
    pass
