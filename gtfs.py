import os
import csv
import datetime
import collections
import time
import sys
import struct
import pickle
import urllib.request
import logging
from google.transit import gtfs_realtime_pb2

import size

def _s2b(s):
    return s.encode('utf-8')

def _b2s(b):
    return b.decode('utf-8').split(chr(0))[0]

class GTFS:
    def __init__(self, real_time_url:str, api_key: str, nocache:bool = False, pollingPeriod:int=60):
        if nocache or not os.path.exists("data/cache.pickle"):
            print("Loading GTFS static data from scratch.")
            self.routes = self._read_routes()
            self.agencies = self._read_agencies()
            self.calendar = self._read_calendar()
            self.exceptions = self._read_exceptions()
            self.stops = self._read_stops()
            self.trips = self._read_trips()
            self.stop_times = self._read_stop_times()
            with open("data/cache.pickle", "wb") as f:
                pickle.dump((self.routes, self.agencies, self.calendar, self.exceptions, self.stops, self.trips, self.stop_times, self._stop_trip_arrival_times), f)
        else:
            with open("data/cache.pickle", "rb") as f:
                print("Loading GTFS static data from cache.")
                self.routes, self.agencies, self.calendar, self.exceptions, self.stops, self.trips, self.stop_times, self._stop_trip_arrival_times = pickle.load(f)

        self.real_time_url = real_time_url
        self.api_key = api_key
        self.pollingPeriod = pollingPeriod
        self.lastPoll = 0
    
    def _read_agencies(self) -> dict:
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

    def _read_routes(self) -> dict:
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

    def _read_calendar(self) -> dict:
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

    def _read_exceptions(self) -> dict:
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

    def get_exception(self, service_id, date):
        return self.exceptions.get(service_id, {}).get(date)

    def _read_stops(self) -> dict:
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


    def _pack_trip(self, route_id, service_id):
        # bit pack the data to save space (it's easy to consume gigabytes of memory)
        return struct.pack('12s4s', _s2b(route_id), _s2b(service_id))

    def _unpack_trip(self, trip_buffer):
        # unpack the data from the bit packed format
        route_id, service_id = struct.unpack('12s4s', trip_buffer)
        return _b2s(route_id), _b2s(service_id)

    def _read_trips(self) -> dict:
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
                trip_key = struct.pack('12s', _s2b(trip_id))
                trips[trip_key] = self._pack_trip(route_id, service_id)
        return trips

    def get_trip_info(self, trip_id):
        trip_key = struct.pack('12s', _s2b(trip_id))
        try:
            route_id, service_id = self._unpack_trip(self.trips[trip_key])
            return {
                'route': self.routes[route_id]['name'],
                'agency': self.agencies[self.routes[route_id]['agency']],
                'service_id': service_id,
                'start_date': self.calendar[service_id]['start_date'],
                'end_date': self.calendar[service_id]['end_date'],
                'days': self.calendar[service_id]['days']
            }
        except KeyError:
            return None


    def _pack_stop_time(self, trip_id, arrival_time, stop_sequence):
        # split arrival_time into hours and minutes
        arrival_time_hrs, arrival_time_mins, arrival_time_secs = [int(x) for x in arrival_time.split(':')]
        # bit pack the data to save space (it's easy to consume gigabytes of memory)
        return struct.pack('12s4b8s', _s2b(trip_id), arrival_time_hrs, arrival_time_mins, arrival_time_secs, int(stop_sequence), _s2b(arrival_time))

    def _unpack_stop_time(self, trip_buffer):
        # unpack the data from the bit packed format
        trip_id, arrival_time_hrs, arrival_time_mins, arrival_time_secs, stop_sequence, arrival_time_orig = struct.unpack('12s4b8s', trip_buffer)
        # express arrival time as a timedelta since midnight
        arrival_time = datetime.timedelta(hours=arrival_time_hrs, minutes=arrival_time_mins, seconds=arrival_time_secs)
        return _b2s(trip_id), arrival_time, stop_sequence, _b2s(arrival_time_orig)

    def _read_stop_times(self) -> dict:
        # open stop_times.txt and parse it as a CSV file, then return a dict
        # stop_id -> list of (trip_id, arrival_time, departure_time, stop_sequence)
        start_time = time.time()
        print("Loading stop times...", end='')
        stop_times = collections.defaultdict(list)
        self._stop_trip_arrival_times = collections.defaultdict(dict)
        with(open("data/stop_times.txt", "r")) as f:
            reader = csv.reader(f)
            # skip the first row of fieldnames
            next(reader)
            for idx, row in enumerate(reader):
                if idx % 10000 == 0:
                    sys.stdout.write('.')
                    sys.stdout.flush()
                trip_id, arrival_time, _, stop_id, stop_sequence = row[0:5]
                packed_stop_time = self._pack_stop_time(trip_id, arrival_time, stop_sequence)
                stop_times[stop_id].append(packed_stop_time)
                self._stop_trip_arrival_times[stop_id][trip_id] = packed_stop_time

        print(f"Loaded {idx + 1} stop times in {time.time() - start_time} seconds")
        return stop_times

    def get_stop_times(self, stop_code: str):
        stop_id = self.stops[stop_code]
        for stop_time_buf in self.stop_times[stop_id]:
            yield self._unpack_stop_time(stop_time_buf)

    def _parseTripUpdates(self, buf: bytes):
        # https://developers.google.com/transit/gtfs-realtime/reference#enum-schedulerelationship-2
        TRIP_SCHEDULED = 0
        TRIP_ADDED = 1
        TRIP_UNSCHEDULED = 2
        TRIP_CANCELLED = 3

        # https://developers.google.com/transit/gtfs-realtime/reference#enum-schedulerelationship
        STOP_SCHEDULED = 0
        STOP_SKIPPED = 1
        STOP_NO_DATA = 2

        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(buf)
        trip_updates = collections.defaultdict(list)
        added_trips = collections.defaultdict(list)
        cancelled_trips = set()
        for entity in feed.entity:
            if entity.HasField('trip_update'):
                num_updates, num_unrecognised_trips = 0, 0
                for stop_time_update in entity.trip_update.stop_time_update:
                    start = datetime.datetime.strptime(f"{entity.trip_update.trip.start_date} {entity.trip_update.trip.start_time}", '%Y%m%d %H:%M:%S')
                    if entity.trip_update.trip.schedule_relationship == TRIP_ADDED:
                        # We can only work with an unscheduled "added" trip if we are given the expected arrival time.
                        if stop_time_update.arrival.time:
                            added_trips[stop_time_update.stop_id].append({
                                'route_id': entity.trip_update.trip.route_id,
                                'arrival': datetime.datetime.fromtimestamp(stop_time_update.arrival.time)                            })
                    elif entity.trip_update.trip.schedule_relationship == TRIP_CANCELLED:
                        cancelled_trips.add(entity.trip_update.trip.trip_id)
                    elif entity.trip_update.trip.schedule_relationship == TRIP_SCHEDULED and stop_time_update.schedule_relationship == STOP_SCHEDULED:
                        trip_info = self.get_trip_info(entity.trip_update.trip.trip_id)
                        if trip_info is None:
                            # logging.error("Trip ID %s is not recognised.", entity.trip_update.trip.trip_id)
                            num_unrecognised_trips += 1
                            continue

                        # stop_arrival_times = self._stop_trip_arrival_times[stop_time_update.stop_id]
                        # if entity.trip_update.trip.trip_id not in stop_arrival_times:
                        #     # logging.error("Trip ID %s is not recorded in _stop_trip_arrival_times.", entity.trip_update.trip.trip_id)
                        #     num_unrecognised_trips += 1
                        #     continue
                        stop_id, arrival_time, stop_sequence, orig_arrival_time = self._unpack_stop_time(self._stop_trip_arrival_times[stop_time_update.stop_id][entity.trip_update.trip.trip_id])
                        # print(f"Got live info for route {trip_info['route']} scheduled to stop at {stop_time_update.stop_id} (seq. {stop_sequence}) at {arrival_time} / {orig_arrival_time}")
                        num_updates += 1
                        # `stop_time_update.arrival` might have `time` instead of delay, in which case it is a POSIX timestamp that can be decoded with
                        # datetime.datetime.fromtimestamp(stop_time_update.arrival.time)
                        # the next update might have an arrival time with an erroneous delay, basically
                        # a negative timestamp roughly equal to -1 * timestamp from previous update.

                        trip_updates[entity.trip_update.trip.trip_id].append({
                            'stop_id': stop_time_update.stop_id,
                            'sequence': stop_time_update.stop_sequence,
                            'delay': stop_time_update.arrival.delay                        
                        })
        print(f"Got {num_updates} trip updates, {num_unrecognised_trips} unrecognised trips, {len(added_trips)} added trips, {len(cancelled_trips)} cancelled trips")
        return trip_updates, added_trips, cancelled_trips
    
    @property
    def _real_time_updates(self):
        if time.time() - self.lastPoll > self.pollingPeriod:
            try:
                # Time to get new data
                req = urllib.request.Request(self.real_time_url, None, {
                    'x-api-key': self.api_key,
                    'Cache-Control': 'no-cache'
                })
                f = urllib.request.urlopen(req)
                self._trip_updates, self._added_trips, self._cancelled_trips = gtfs._parseTripUpdates(f.read())
                f.close()
                self.lastPoll = time.time()
            except urllib.error.HTTPError as e:
                print(f"Error fetching real time updates: {e}")
        return self._trip_updates, self._added_trips, self._cancelled_trips
    
    def is_cancelled(self, trip_id):
        cancelled_trips = self._real_time_updates[2]
        return trip_id in cancelled_trips
    
    def get_real_time_delay(self, trip_id: str, stop_sequence: int):
        # find the real time update for this stop or the one with the highest sequence number
        # lower than this stop
        updates = self._real_time_updates[0][trip_id]
        delay = updates[0]['delay'] if updates else None
        for update in updates:
            if update['sequence'] > stop_sequence:
                break
            delay = update['delay']
            
        return delay
    
    def get_added_trips(self, stop_code: str):
        stop_id = self.stops[stop_code]
        return self._added_trips.get(stop_id, [])
    
    def get_scheduled_arrivals(self, stop_code: str, now: datetime, max_wait: datetime.timedelta):
        # get all the scheduled arrivals at a given stop_id
        # returns a list of (trip_id, arrival_time, stop_sequence)
        scheduled_arrivals = []
        for trip_id, arrival_time, stop_sequence, orig_arrival_time in self.get_stop_times(stop_code):
            time_since_midnight = datetime.timedelta(hours=now.hour, minutes=now.minute, seconds=now.second)
            # if the arrival time is in the past, add one day
            if arrival_time < time_since_midnight:
                arrival_time += datetime.timedelta(days=1)
            # if time to arrival is soon
            if arrival_time - time_since_midnight < max_wait:
                # Check if service is calendared to run
                arrival_datetime = datetime.datetime(now.year, now.month, now.day) + arrival_time
                trip_info = self.get_trip_info(trip_id)
                service_is_scheduled = \
                    trip_info['start_date'] <= arrival_datetime.date() <= trip_info['end_date'] and \
                    trip_info['days'][arrival_datetime.date().weekday()]
                calendar_exception = self.get_exception(trip_info['service_id'], arrival_datetime.date())
                # check if there is a calendar exception
                added = calendar_exception == 1
                removed = calendar_exception == 2
                if added or service_is_scheduled and not removed:
                    delay = self.get_real_time_delay(trip_id, stop_sequence)
                    
                    if self.is_cancelled(trip_id):
                        continue
                    # service is expected to run. add it to the list.
                    # print(f"Adding trip {trip_info['route']} arriving at {arrival_datetime} to list of scheduled arrivals")
                    scheduled_arrivals.append({
                        'route': trip_info['route'],
                        'agency': trip_info['agency'],
                        'scheduled_arrival': arrival_datetime,
                        'real_time_arrival': arrival_datetime + datetime.timedelta(seconds=delay) if delay else None,
                    })
        for added_trip in self.get_added_trips(stop_code):
            route = self.routes[added_trip['route_id']]
            scheduled_arrivals.append({
                'route': route['name'],
                'agency': self.agencies[route['agency']],
                'scheduled_arrival': added_trip['arrival'],
                'real_time_arrival': added_trip['arrival'],
            })
        scheduled_arrivals.sort(key=lambda x: x['real_time_arrival'] or x['scheduled_arrival'])
        return scheduled_arrivals

import settings
if __name__ == "__main__":
    gtfs = GTFS(
        real_time_url=settings.GTFS_REALTIME_URL, 
        api_key=settings.API_KEY, 
        nocache=False
    )
    
    # print("exceptions size: {}".format(size.total_size(exceptions)))
    # print("calendar size: {}".format(size.total_size(calendar)))
    # print("stops size: {}".format(size.total_size(stops)))
    # print("trips size: {}".format(size.total_size(trips)))
    # print("stop_times size: {}".format(size.total_size(stop_times)))
    # print("routes size: {}".format(size.total_size(routes)))
    
    now = datetime.datetime.now()
    # now = now.replace(hour=12, minute=00)
    arrivals = gtfs.get_scheduled_arrivals("2189", now, datetime.timedelta(minutes=120))
    # arrivals = gtfs.get_scheduled_arrivals("455591", now, datetime.timedelta(minutes=30))
    for arrival in arrivals:
        # print(arrival)
        rt_arrival = "N/A"
        if arrival['real_time_arrival']:
            if arrival['real_time_arrival'].year == 1970:
                rt_arrival = "epoch"
            else:
                rt_arrival = arrival['real_time_arrival'].strftime('%H:%M')
        print(f"{arrival['route']}, {arrival['scheduled_arrival'].strftime('%H:%M')}, {rt_arrival}")

def downloadStaticGTFS():
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
