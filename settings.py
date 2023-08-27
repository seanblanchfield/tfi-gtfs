
GTFS_STATIC_URL = "https://www.transportforireland.ie/transitData/Data/GTFS_Realtime.zip"
GTFS_REALTIME_URL = "https://api.nationaltransport.ie/gtfsr/v2/TripUpdates"
API_KEY = ""

try:
    from local_settings import *
except ImportError:
    pass
