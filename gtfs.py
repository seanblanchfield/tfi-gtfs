import os
import csv
import datetime
import collections
import time
import sys
import struct
import pickle
from google.transit import gtfs_realtime_pb2

import size

# From: https://developers.google.com/transit/gtfs/reference

class CALENDAR_EXCEPTIONS:
    CALENDAR_EXCEPTION_SERVICE_ADDED = 1
    CALENDAR_EXCEPTION_SERVICE_REMOVED = 2

def _s2b(s):
    return s.encode('utf-8')

def _b2s(b):
    return b.decode('utf-8').split(chr(0))[0]

def parseLiveData(buf):
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(buf)
    return feed

def load_agencies() -> dict:
    # open agency.txt and parse it as a CSV file, then return a dict
    # keyed on agency_id
    agencies = {}
    with(open("data/agency.txt", "r")) as f:
        reader = csv.reader(f)
        # skip the first row of fieldnames
        next(reader)
        for row in reader:
            agency_id, agency_name = row[0:2]
            agencies[agency_id] = agency_name
    return agencies

def load_routes() -> dict:
    # open routes.txt and parse it as a CSV file, then return a dict
    # keyed on route_id
    routes = {}
    with(open("data/routes.txt", "r")) as f:
        reader = csv.reader(f)
        # skip the first row of fieldnames
        next(reader)
        for row in reader:
            route_id, agency, short_name = row[0:3]
            routes[route_id] = {
                'name': short_name,
                'agency': agency
            }
    return routes

def load_calendar() -> dict:
    # open calendar.txt and parse it as a CSV file, then return a dict
    # keyed on service_id
    # each service_id maps to a dict keyed on day of week
    calendar = {}
    earliest_date = datetime.date.today()
    latest_date = datetime.date(1970, 1, 1)
    with(open("data/calendar.txt", "r")) as f:
        reader = csv.reader(f)
        # skip the first row of fieldnames
        next(reader)
        for row in reader:
            service_id = row[0]
            start_date = datetime.datetime.strptime(row[8], '%Y%m%d').date()
            end_date = datetime.datetime.strptime(row[9], '%Y%m%d').date()
            # days represented by '0' or '1'. Convert to bools.
            days = [bool(int(day)) for day in row[1:8]]
            calendar[service_id] = {
                'start_date': start_date,
                'end_date': end_date,
                'days': days
            }
            if start_date < earliest_date:
                earliest_date = start_date
            if end_date > latest_date:
                latest_date = end_date
    print(f"Loaded calendar with start dates ranging from {earliest_date} to {latest_date}")
    return calendar

def load_exceptions() -> dict:
    # open calendar_dates.txt and parse it as a CSV file, then return a dict
    # keyed on service_id, which maps to a dict keyed on date, which maps to 
    # exception type.
    # {
    #      <service_id>: {
    #          <date>: <exception_type>
    #      }
    # }
    exceptions = collections.defaultdict(dict)
    with(open("data/calendar_dates.txt", "r")) as f:
        reader = csv.reader(f)
        # skip the first row of fieldnames
        next(reader)
        for row in reader:
            service_id = row[0]
            date = datetime.datetime.strptime(row[1], '%Y%m%d').date()
            exception_type = int(row[2])
            exceptions[service_id][date] = exception_type
    return exceptions

def load_stops() -> dict:
    # open stops.txt and parse it as a CSV file, then return a dict
    # of stop_code -> stop_id (stop_code, as written on bus stops)
    stops = {}
    with(open("data/stops.txt", "r")) as f:
        reader = csv.reader(f)
        # skip the first row of fieldnames
        next(reader)
        for row in reader:
            stop_id = row[0]
            stop_code = row[1]
            stops[stop_code] = stop_id
    return stops


def pack_trip(route_id, service_id):
    # bit pack the data to save space (it's easy to consume gigabytes of memory)
    return struct.pack('12s4s', _s2b(route_id), _s2b(service_id))

def unpack_trip(trip_buffer):
    # unpack the data from the bit packed format
    route_id, service_id = struct.unpack('12s4s', trip_buffer)
    return _b2s(route_id), _b2s(service_id)

def load_trips() -> dict:
    # open trips.txt and parse it as a CSV file, then return a dict
    # that maps trip_id -> route_id and service_id
    trips = {}
    with(open("data/trips.txt", "r")) as f:
        reader = csv.reader(f)
        # skip the first row of fieldnames
        next(reader)
        for row in reader:
            route_id = row[0]
            service_id = row[1]
            trip_id = row[2]
            trips[trip_id] = pack_trip(route_id, service_id)
    return trips


def pack_stop_time(trip_id, arrival_time, stop_sequence):
    # split arrival_time into hours and minutes
    arrival_time_hrs, arrival_time_mins, arrival_time_secs = [int(x) for x in arrival_time.split(':')]
    # bit pack the data to save space (it's easy to consume gigabytes of memory)
    return struct.pack('12s4b', _s2b(trip_id), arrival_time_hrs, arrival_time_mins, arrival_time_secs, int(stop_sequence))

def unpack_stop_time(trip_buffer):
    # unpack the data from the bit packed format
    trip_id, arrival_time_hrs, arrival_time_mins, arrival_time_secs, stop_sequence = struct.unpack('12s4b', trip_buffer)
    # express arrival time as a timedelta since midnight
    arrival_time = datetime.timedelta(hours=arrival_time_hrs, minutes=arrival_time_mins, seconds=arrival_time_secs)
    return _b2s(trip_id), arrival_time, stop_sequence

def load_stop_times() -> dict:
    # open stop_times.txt and parse it as a CSV file, then return a dict
    # stop_id -> list of (trip_id, arrival_time, departure_time, stop_sequence)
    start_time = time.time()
    print("Loading stop times...", end='')
    stop_times = collections.defaultdict(list)
    with(open("data/stop_times.txt", "r")) as f:
        reader = csv.reader(f)
        # skip the first row of fieldnames
        next(reader)
        for idx, row in enumerate(reader):
            if idx % 10000 == 0:
                sys.stdout.write('.')
                sys.stdout.flush()
            trip_id, arrival_time, _, stop_id, stop_sequence = row[0:5]
            stop_times[stop_id].append(
                pack_stop_time(trip_id, arrival_time, stop_sequence)
            )

    print(f"Loaded {idx + 1} stop times in {time.time() - start_time} seconds")
    return stop_times

# main
if __name__ == "__main__":
    if os.path.exists("data/cache.pickle"):
        with open("data/cache.pickle", "rb") as f:
            print("Loading GTFS data from cache.")
            routes, agencies, calendar, exceptions, stops, trips, stop_times = pickle.load(f)
    else:
        print("Loading GTFS data from scratch.")
        routes = load_routes()
        agencies = load_agencies()
        calendar = load_calendar()
        exceptions = load_exceptions()
        stops = load_stops()
        trips = load_trips()
        stop_times = load_stop_times()
        with open("data/cache.pickle", "wb") as f:
            pickle.dump((routes, agencies, calendar, exceptions, stops, trips, stop_times), f)

    # print("exceptions size: {}".format(size.total_size(exceptions)))
    # print("calendar size: {}".format(size.total_size(calendar)))
    # print("stops size: {}".format(size.total_size(stops)))
    # print("trips size: {}".format(size.total_size(trips)))
    # print("stop_times size: {}".format(size.total_size(stop_times)))

    def get_scheduled_arrivals(stop_code: str, max_wait: datetime.timedelta):
        # get all the scheduled arrivals at a given stop_id
        # returns a list of (trip_id, arrival_time, stop_sequence)
        scheduled_arrivals = []
        stop_id = stops[stop_code]
        for trip_buffer in stop_times[stop_id]:
            trip_id, arrival_time, stop_sequence = unpack_stop_time(trip_buffer)
            now = datetime.datetime.now()
            time_since_midnight = datetime.timedelta(hours=now.hour, minutes=now.minute, seconds=now.second)
            # if the arrival time is in the past, add one day
            if arrival_time < time_since_midnight:
                arrival_time += datetime.timedelta(days=1)
            # if time to arrival is soon
            if arrival_time - time_since_midnight < max_wait:
                # Check if service is calendared to run
                arrival_datetime = datetime.datetime(now.year, now.month, now.day) + arrival_time
                route_id, service_id = unpack_trip(trips[trip_id])
                service_is_scheduled = \
                    calendar[service_id]['start_date'] <= arrival_datetime.date() <= calendar[service_id]['end_date'] and \
                    calendar[service_id]['days'][arrival_datetime.date().weekday()]
                calendar_exception = exceptions.get(service_id, {}).get(arrival_datetime.date())
                # check if there is a calendar exception
                if calendar_exception == CALENDAR_EXCEPTIONS.CALENDAR_EXCEPTION_SERVICE_ADDED or \
                    service_is_scheduled and calendar_exception != CALENDAR_EXCEPTIONS.CALENDAR_EXCEPTION_SERVICE_REMOVED:
                    # service is expected to run. add it to the list.

                    scheduled_arrivals.append({
                        'route': routes[route_id]['name'],
                        'agency': agencies[routes[route_id]['agency']],
                        'arrival_time': arrival_datetime,
                        'stop_sequence': stop_sequence
                    })
    
        return scheduled_arrivals
    
    arrivals = get_scheduled_arrivals("2189", datetime.timedelta(minutes=30))
    with open("test_data/example_live_response", "rb") as f:
        feed = parseLiveData(f.read())
        for entity in feed.entity:
            if entity.HasField('trip_update'):
                # print(entity.trip_update)
                pass

def downloadStaticGTFS():
    import settings
    url = settings.GTFS_FEED_URL
    # download the GTFS zip file and extract it into the data directory
    import urllib.request
    import zipfile
    import io
    print(f"Downloading GTFS data from {url}")
    with urllib.request.urlopen(url) as response:
        with zipfile.ZipFile(io.BytesIO(response.read())) as zip_ref:
            zip_ref.extractall("data")
    print("Done.")
