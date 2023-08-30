import os
import csv
import datetime
import collections
import time
import sys
import struct
import urllib.request
import logging
from google.transit import gtfs_realtime_pb2

import memstore

def _s2b(s):
    return s.encode('utf-8')

def _b2s(b):
    return b.decode('utf-8').split(chr(0))[0]

class GTFS:
    def __init__(self, live_url:str, api_key: str, redis_url:str=None, no_cache:bool = False, polling_period:int=60, profile_memory:bool=False):
        self.store = memstore.MemStore(redis_url=redis_url, no_cache=no_cache, keys_config={
            'route': {
                'memoize': True,
                'expiry': 60 * 60 # 1 hour
            },
            'service': {
                'memoize': True,
                'expiry': 60 * 60 # 1 hour
            },
            'stop': {
                'memoize': True,
                'expiry': 60 * 60 # 1 hour
            },
            'stop_numbers': {
                'memoize': True,
                'expiry': 60 * 60 # 1 hour
            },
            # service, stops, stop_numbers, trips
        })
        if self.store.get("initialized") is None:
            logging.info("Loading GTFS static data from scratch.")
            self._read_routes()
            self._read_agencies()
            self._read_calendar()
            self._read_exceptions()
            self._read_stops()
            self._read_trips()
            self._read_stop_times()
            self.store.set("initialized", True)
            self.store.store_cache()
        else:
            logging.info("Loading GTFS static data from cache.")

        self.live_url = live_url
        self.api_key = api_key
        self.polling_period = polling_period
        self.last_poll = 0
        self.poll_backoff = 0
        self._refresh_live_data()
        
        if profile_memory:
            logging.info("Profiling memory usage...")
            logging.info(f"Total memory size is {self.store.profile_memory() / 1024 / 1024:.0f} MB")
    
    def _read_agencies(self):
        with(open("data/agency.txt", "r")) as f:
            reader = csv.reader(f)
            # skip the first row of fieldnames
            next(reader)
            for row in reader:
                agency_id, agency_name = row[0:2]
                self.store.set(f"agency:{agency_id}", agency_name)

    def _read_routes(self):
        with(open("data/routes.txt", "r")) as f:
            reader = csv.reader(f)
            # skip the first row of fieldnames
            next(reader)
            for row in reader:
                route_id, agency, short_name = row[0:3]
                self.store.set(f"route:{route_id}", {
                    'name': short_name,
                    'agency': agency
                })

    def _read_calendar(self):
        # each service_id maps to a dict keyed on day of week
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
                self.store.set(f"service:{service_id}", {
                    'start_date': start_date,
                    'end_date': end_date,
                    'days': days
                })
                if start_date < earliest_date:
                    earliest_date = start_date
                if end_date > latest_date:
                    latest_date = end_date
        print(f"Loaded calendar with start dates ranging from {earliest_date} to {latest_date}")

    def _read_exceptions(self):
        with(open("data/calendar_dates.txt", "r")) as f:
            reader = csv.reader(f)
            # skip the first row of fieldnames
            next(reader)
            for row in reader:
                service_id = row[0]
                date = datetime.datetime.strptime(row[1], '%Y%m%d').date()
                exception_type = int(row[2])
                self.store.set(f"exception:{service_id}:{date}", exception_type)

    def _read_stops(self):
        # open stops.txt and parse it as a CSV file, then return a dict
        # of stop_number -> stop_id (stop_number, as written on bus stops)
        with(open("data/stops.txt", "r")) as f:
            reader = csv.reader(f)
            # skip the first row of fieldnames
            next(reader)
            for row in reader:
                stop_id = row[0]
                stop_number = row[1]
                # some stops (in Northern Ireland) don't have a stop code. Use the stop_id instead.
                self.store.set(f"stop:{stop_id}", stop_number or stop_id)
                self.store.add(f"stop_numbers", stop_number or stop_id)
    
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
        with(open("data/trips.txt", "r")) as f:
            reader = csv.reader(f)
            # skip the first row of fieldnames
            next(reader)
            for row in reader:
                route_id = row[0]
                service_id = row[1]
                trip_id = row[2]
                self.store.set(f"trip:{trip_id}", self._pack_trip(route_id, service_id))

    def get_trip_info(self, trip_id):
        try:
            packed_trip = self.store.get(f"trip:{trip_id}")
            if packed_trip:
                route_id, service_id = self._unpack_trip(packed_trip)
                route_info = self.store.get(f"route:{route_id}")
                agency_info = self.store.get(f"agency:{route_info['agency']}")
                calendar_info = self.store.get(f"service:{service_id}")
                return {
                    'route': route_info['name'],
                    'agency': agency_info,
                    'service_id': service_id,
                    'start_date': calendar_info['start_date'],
                    'end_date': calendar_info['end_date'],
                    'days': calendar_info['days']
                }
        except KeyError:
            return None


    def _pack_stop_data(self, trip_id, arrival_hour, arrival_min, arrival_sec, stop_sequence):
        # bit pack the data to save space (it's easy to consume gigabytes of memory)
        return struct.pack('12s4b', _s2b(trip_id), arrival_hour, arrival_min, arrival_sec, int(stop_sequence))

    def _unpack_stop_data(self, trip_buffer):
        # unpack the data from the bit packed format
        trip_id, arrival_hour, arrival_min, arrival_sec, stop_sequence = struct.unpack('12s4b', trip_buffer)
        # express arrival time as a timedelta since midnight
        return _b2s(trip_id), arrival_hour, arrival_min, arrival_sec, stop_sequence

    def _read_stop_times(self) -> dict:
        # open stop_times.txt and parse it as a CSV file, then return a dict
        # stop_id -> list of (trip_id, arrival_time, departure_time, stop_sequence)
        start_time = time.time()
        print("Loading stop times...", end='')
        stop_times = collections.defaultdict(dict)
        with(open("data/stop_times.txt", "r")) as f:
            reader = csv.reader(f)
            # skip the first row of fieldnames
            next(reader)
            for idx, row in enumerate(reader):
                if idx % 10000 == 0:
                    sys.stdout.write('.')
                    sys.stdout.flush()
                trip_id, arrival_time, _, stop_id, stop_sequence = row[0:5]
                stop_number = self.store.get(f"stop:{stop_id}")
                arrival_hour, arrival_min, arrival_sec = [int(x) for x in arrival_time.split(':')]
                # arrival time is in the format HH:MM:SS. Pull out the hour so that trips can be 
                # looked up by stop and hour.
                hour = arrival_hour % 24
                if hour not in stop_times[stop_number]:
                     stop_times[stop_number][hour] = []
                stop_times[stop_number][hour].append(self._pack_stop_data(trip_id, arrival_hour, arrival_min, arrival_sec, stop_sequence))

        logging.info(f"\nLoaded {idx + 1} stop times in {time.time() - start_time:.0f} seconds")
        for stop_number in stop_times:
            for hour in stop_times[stop_number]:
                self.store.set(f"stop_times:{stop_number}:{hour}", stop_times[stop_number][hour])

    def _parse_live_data(self, buf: bytes):
        # https://developers.google.com/transit/gtfs-realtime/reference#enum-schedulerelationship-2
        TRIP_SCHEDULED = 0
        TRIP_ADDED = 1
        TRIP_UNSCHEDULED = 2
        TRIP_CANCELLED = 3

        # https://developers.google.com/transit/gtfs-realtime/reference#enum-schedulerelationship
        STOP_SCHEDULED = 0
        STOP_SKIPPED = 1
        STOP_NO_DATA = 2

        # data structure into which updates from the live feed will be loaded.
        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(buf)
        timestamp = feed.header.timestamp
        num_updates, num_unrecognised_trips, num_added, num_cancelled = 0, 0, 0, 0
        for entity in feed.entity:
            if entity.HasField('trip_update'):
                trip_id = entity.trip_update.trip.trip_id
                trip_delays = []
                for stop_time_update in entity.trip_update.stop_time_update:
                    if stop_time_update.schedule_relationship != STOP_SCHEDULED:
                        continue
                    
                    stop_number = self.store.get(f"stop:{stop_time_update.stop_id}")
                    if stop_number is None:
                        logging.warning(f"Unrecognised stop_id {stop_time_update.stop_id} in live data feed.")
                        continue
                    if entity.trip_update.trip.schedule_relationship == TRIP_ADDED:
                        # We can only work with an unscheduled "added" trip if we are given the expected arrival time.
                        if stop_time_update.arrival.time:
                            num_added += 1
                            self.store.set(f"live_additions:{stop_number}", {
                                'route_id': entity.trip_update.trip.route_id,
                                'arrival': datetime.datetime.fromtimestamp(stop_time_update.arrival.time),
                                'timestamp': timestamp
                            })
                    elif entity.trip_update.trip.schedule_relationship == TRIP_CANCELLED:
                        num_cancelled += 1
                        self.store.set(f"live_cancelations:{trip_id}", True)
                        self._live_data['cancelled'][trip_id] = {
                            'timestamp': timestamp
                        }
                    elif entity.trip_update.trip.schedule_relationship == TRIP_SCHEDULED:
                        trip_info = self.get_trip_info(trip_id)
                        if trip_info is None:
                            num_unrecognised_trips += 1
                            continue

                        delay = arrival_time = None
                        if stop_time_update.arrival.time:
                            arrival_time = datetime.datetime.fromtimestamp(stop_time_update.arrival.time)
                        else:
                            delay = stop_time_update.arrival.delay
                            # Ignore delays greater than a week
                            # Some updates contain delays that are approximately equal to the timestamp but negative.
                            # These are presumed to be due to a bug in the NTA code. ignore them.
                            if delay < -60 * 60 * 24 * 7: 
                                continue
                        num_updates += 1
                        trip_delays.append({
                            'stop_sequence': stop_time_update.stop_sequence,
                            'stop_number': stop_number,
                            'delay': delay,
                            'arrival_time': arrival_time,
                            'timestamp': timestamp
                        })
                self.store.set(f"live_delays:{trip_id}", trip_delays)
        logging.info(f"Got {num_updates} trip updates, {num_unrecognised_trips} unrecognised trips, {num_added} added trips, {num_cancelled} cancelled trips")
    
    def _refresh_live_data(self):
        now = time.time()
        if now - self.last_poll > self.polling_period + self.poll_backoff:
            try:
                # Time to get new data
                req = urllib.request.Request(self.live_url, None, {
                    'x-api-key': self.api_key,
                    'Cache-Control': 'no-cache'
                })
                f = urllib.request.urlopen(req)
                self._parse_live_data(f.read())
                f.close()
                self.last_poll = now
                self.poll_backoff = 0
            except urllib.error.HTTPError as e:
                logging.error(f"Error fetching real time updates: {e}")
                # so long as we get rate-limited, back off exponentially
                if e.code == 429:
                    self.last_poll = now
                    self.poll_backoff += self.polling_period
    

    def _get_live_delay(self, trip_id: str, stop_sequence: int):
        # find the real time update for this stop or the one with the highest sequence number
        # lower than this stop
        updates = self.store.get(f"live_delays:{trip_id}")
        if updates:
            # updates is a sorted list of dicts, each of which contain a stop_sequence and delay
            # binary search through the list to find the item with the closest stop_sequence that is 
            # less than or equal to the stop_sequence we are looking for
            left, right = 0, len(updates) - 1
            while left <= right:
                mid = (left + right) // 2
                if updates[mid]['stop_sequence'] < stop_sequence:
                    left = mid + 1
                elif updates[mid]['stop_sequence'] > stop_sequence:
                    right = mid - 1
                else:
                    return updates[mid]['delay']
            # if we get here, we didn't find an exact match. left is the index of the first item
            # with a stop_sequence greater than the one we are looking for. If left is 0, there
            # is no update for this trip at this stop. Otherwise, return the delay of the previous
            # stop.
            if left == 0:
                return None
            else:
                return updates[left - 1]['delay']

    def is_valid_stop_number(self, stop_number: str):
        return self.store.has("stop_numbers", stop_number)
    
    def get_scheduled_arrivals(self, stop_number: str, now: datetime, max_wait: datetime.timedelta):
        self._refresh_live_data()
        # get all the scheduled arrivals at a given stop_id
        # returns a list of (trip_id, arrival_time, stop_sequence)
        scheduled_arrivals = []
        # try the previous hour and the next few (per max_wait)
        try_hours: list
        if now.hour == 0:
            try_hours = [23]
        else:
            try_hours = [now.hour - 1]
        try_hours.extend([h % 24 for h in range(now.hour, now.hour + int(max_wait.total_seconds() // 3600) + 1)])
        for hour in try_hours:
            for packed_stop_data in self.store.get(f"stop_times:{stop_number}:{hour}"):
                trip_id, arrival_hour, arrival_min, arrival_sec, stop_sequence = self._unpack_stop_data(packed_stop_data)
                time_since_midnight = datetime.timedelta(hours=now.hour, minutes=now.minute, seconds=now.second)
                
                # arrival time is stored as HH:MM:SS. Convert to datetime.
                arrival_time = datetime.timedelta(hours=arrival_hour, minutes=arrival_min, seconds=arrival_sec)
                stop_sequence = int(stop_sequence)

                # if the arrival time over 12 hours in the past, assume it refers to tomorrow and add one day.
                if time_since_midnight - datetime.timedelta(hours=12) > arrival_time:
                    arrival_time += datetime.timedelta(days=1)
                
                # Check if service is calendared to run
                arrival_datetime = datetime.datetime(now.year, now.month, now.day) + arrival_time
                trip_info = self.get_trip_info(trip_id)
                
                service_is_scheduled = \
                    trip_info['start_date'] <= arrival_datetime.date() <= trip_info['end_date'] and \
                    trip_info['days'][arrival_datetime.date().weekday()]
                calendar_exception = self.store.get(f"exception:{trip_info['service_id']}:{arrival_datetime.date()}")
                # check if there is a calendar exception
                added = calendar_exception == 1
                removed = calendar_exception == 2
                if added or service_is_scheduled and not removed:
                    delay = self._get_live_delay(trip_id, stop_sequence)
                    if self.store.has('live_cancelations', trip_id):
                        continue
                    # We expect this arrival.
                    arrival = {
                        'route': trip_info['route'],
                        'agency': trip_info['agency'],
                        'scheduled_arrival': arrival_datetime,
                        'real_time_arrival': arrival_datetime + datetime.timedelta(seconds=delay) if delay is not None else None,
                    }
                    # if it has not already arrived, add it to the list.
                    if arrival['scheduled_arrival'] > now or \
                        (arrival['real_time_arrival'] and arrival['real_time_arrival'] > now):
                        scheduled_arrivals.append(arrival)
        
        # add any added trips
        for added_trip in self.store.get(f"live_additions:{stop_number}", []):
            route_info = self.store.get(f"route:{added_trip['route_id']}")
            agency_info = self.store.get(f"agency:{route_info['agency']}")
            scheduled_arrivals.append({
                'route': route_info['name'],
                'agency': agency_info['name'],
                'scheduled_arrival': added_trip['arrival'],
                'real_time_arrival': added_trip['arrival'],
            })
        scheduled_arrivals.sort(key=lambda x: x['real_time_arrival'] or x['scheduled_arrival'])
        return scheduled_arrivals


def downloadStaticGTFS():
    # download the GTFS zip file and extract it into the data directory
    import urllib.request
    import zipfile
    import io
    logging.info(f"Downloading GTFS data from {settings.GTFS_STATIC_URL}")
    with urllib.request.urlopen(settings.GTFS_STATIC_URL) as response:
        with zipfile.ZipFile(io.BytesIO(response.read())) as zip_ref:
            # copy old .txt files in "data" directory to "data/bak"
            import shutil
            if os.path.exists("data/bak"):
                shutil.rmtree("data/bak")
            os.mkdir("data/bak")
            # only copy .txt files
            for file in os.listdir("data"):
                if file.endswith(".txt"):
                    shutil.copy(os.path.join("data", file), "data/bak/")
            # extract the new .txt files into "data"
            zip_ref.extractall("data")
            # remove the cache file
            if os.path.exists("data/cache.pickle"):
                os.remove("data/cache.pickle")
    logging.info("Done.")

import settings
if __name__ == "__main__":

    # Read program options for live_url, api_key, no_cache, polling_period and the max_wait time
    import argparse
    parser = argparse.ArgumentParser(description='Perform a live query against the API for upcoming scheduled arrivals.')
    parser.add_argument('-l', '--live_url', type=str, default=settings.GTFS_LIVE_URL,
                        help='URL of the live GTFS feed')
    parser.add_argument('-k', '--api_key', type=str, default=settings.API_KEY,
                        help='API key for the live GTFS feed')
    parser.add_argument('-r', '--redis', type=str, default=settings.REDIS_URL,
                        help='URL of a redis instance to use as a data store backend')
    parser.add_argument('--no_cache', action='store_true',default=False,
                        help='Ignore cached GTFS data and load static data from scratch')
    parser.add_argument('--profile', action='store_true',default=False,
                        help='Profile memory usage')
    parser.add_argument('-p', '--polling_period', type=int, default=60,
                        help='Polling period for live GTFS feed')
    parser.add_argument('--logging', type=str, choices=['DEBUG', 'INFO', 'WARN', 'ERROR'], default=settings.LOG_LEVEL, dest='log_level',
                        help='Print verbose output')
    parser.add_argument('-w', '--max_wait', type=int, default=60,
                        help='Maximum minutes in the future to return results for')
    parser.add_argument('--download', action='store_true', default=False,
                        help='Download and extract the static GTFS archive and exit')
    parser.add_argument('stop_numbers', metavar='stop numbers', type=str, nargs='*',
                        help='Stop numbers to query (as shown on the bus stop)')
    
    args = parser.parse_args()
    # if the --download option was specified, download the static GTFS archive and exit
    if args.download:
        downloadStaticGTFS()
        sys.exit(0)
    # additional unnamed arguments specify a list of stop codes to query
    if not args.stop_numbers:
        # print an error message and exit if no stop codes were specified
        print("No stop numbers specified.")
        sys.exit(1)

    
    logging.basicConfig(level=getattr(logging, args.log_level))

    gtfs = GTFS(
        live_url=args.live_url, 
        api_key=args.api_key, 
        redis_url=args.redis,
        no_cache=args.no_cache,
        polling_period=args.polling_period,
        profile_memory=args.profile
    )
        
    now = datetime.datetime.now()
    for stop_number in args.stop_numbers:
        if not gtfs.is_valid_stop_number(stop_number):
            print(f"Stop number {stop_number} is not recognised.")
            continue
        arrivals = gtfs.get_scheduled_arrivals(stop_number, now, datetime.timedelta(minutes=args.max_wait))
        for arrival in arrivals:
            rt_arrival = "N/A"
            if arrival['real_time_arrival']:
                if arrival['real_time_arrival'].year == 1970:
                    rt_arrival = "epoch"
                else:
                    rt_arrival = arrival['real_time_arrival'].strftime('%H:%M')
            print(f"{stop_number}, {arrival['route']}, {arrival['scheduled_arrival'].strftime('%H:%M')}, {rt_arrival}")

