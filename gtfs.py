import os
import csv
import json
import datetime
import collections
import time
import sys
import struct
import urllib.request
import logging
import argparse

from google.transit import gtfs_realtime_pb2

import store

def _s2b(s):
    return s.encode('utf-8')

def _b2s(b):
    return b.decode('utf-8').split(chr(0))[0]

class GTFS:
    def __init__(self, live_url:str, api_key: str, redis_url:str=None, rebuild_cache:bool = False, filter_stops:list=None, data_path:str = None, profile_memory:bool=False):
        # Exit with error if static data doesn't exist
        if not static_data_ok():
            logging.error("No static GTFS data found. Download it with `python gtfs.py --download` and try again.")
            sys.exit(1)

        logging.info(f"""Initializing GTFS with:
            live_url={live_url}
            api_key={api_key}
            redis_url={redis_url}
            rebuild_cache={rebuild_cache}
            filter_stops={filter_stops}
            profile_memory={profile_memory}
        """.replace('\t', ' '))
        self.live_url = live_url
        self.api_key = api_key
        self.filter_stops = set(filter_stops) if filter_stops is not None else None
        self.filter_trips = None
        # The set of trip_ids serving each stop. Used in conjunction with filter_stops. 
        self.stop_trips = collections.defaultdict(set) 
        self.rate_limit_count = 0
        namespace_config = {}
        if redis_url:
            namespace_config['route'] = namespace_config['service'] = namespace_config['stop'] = namespace_config['stop_numbers'] = {
                'cache': True,
                'expiry': 3600
            }
        self.store = store.Store(redis_url=redis_url, namespace_config=namespace_config, data_path=data_path)
        if rebuild_cache:
            self.store.clear_cache()

        if self.store.get('status', "initialized") is None:
            self.load_static()

        if profile_memory:
            logging.info("Profiling memory usage...")
            stats = self.store.profile_memory()
            for key in stats:
                logging.info(f"{ key }: { stats[key] / 1024 / 1024 :.02f} MB")
    
    def load_static(self):
        logging.info("Loading GTFS static data from scratch.")
        logging.info("Loading routes.")
        self._read_routes()
        logging.info("Loading agencies.")
        self._read_agencies()
        logging.info("Loading calendar.")
        self._read_calendar()
        logging.info("Loading calendar exceptions.")
        self._read_exceptions()
        logging.info("Loading stops.")
        self._read_stops()
        logging.info("Loading stop times.")
        self._read_stop_times()
        logging.info("Loading trips.")
        self._read_trips()
        self.store.set('status', "initialized", True)
        logging.info("Persisting data.")
        self.store.write_cache()
        # write a json file containing the self.filter_stops to cache_info.txt
        write_cache_info(self.filter_stops)
    
    def _read_agencies(self):
        with(open("data/agency.txt", "r")) as f:
            reader = csv.reader(f)
            # skip the first row of fieldnames
            next(reader)
            for row in reader:
                agency_id, agency_name = row[0:2]
                self.store.set('agency', agency_id, agency_name)

    def _read_routes(self):
        with(open("data/routes.txt", "r")) as f:
            reader = csv.reader(f)
            # skip the first row of fieldnames
            next(reader)
            for row in reader:
                route_id, agency, short_name = row[0:3]
                self.store.set('route', route_id, {
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
                self.store.set('service', service_id, {
                    'start_date': start_date,
                    'end_date': end_date,
                    'days': days
                })
                if start_date < earliest_date:
                    earliest_date = start_date
                if end_date > latest_date:
                    latest_date = end_date
        logging.info(f"Loaded calendar with start dates ranging from {earliest_date} to {latest_date}")

    def _read_exceptions(self):
        with(open("data/calendar_dates.txt", "r")) as f:
            reader = csv.reader(f)
            # skip the first row of fieldnames
            next(reader)
            for row in reader:
                service_id = row[0]
                date = datetime.datetime.strptime(row[1], '%Y%m%d').date()
                exception_type = int(row[2])
                self.store.set('exception', f"{service_id}:{date}", exception_type)

    def _read_stops(self):
        # open stops.txt and parse it as a CSV file, then return a dict
        # of stop_number -> stop_id (stop_number, as written on bus stops)
        with(open("data/stops.txt", "r")) as f:
            reader = csv.reader(f)
            # skip the first row of fieldnames
            next(reader)
            for row in reader:
                stop_id = row[0]
                # some stops (in Northern Ireland) don't have a stop code. Use the stop_id instead.
                stop_number = row[1] or row[0]
                self.store.set('stop', stop_id, stop_number)
                self.store.add('stop_numbers', stop_number)
    
    def _pack_stop_data(self, trip_id, arrival_hour, arrival_min, arrival_sec, stop_sequence):
        # byte pack the data to save space
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
                stop_number = self.store.get('stop', stop_id)
                # keep track of which trips serve which stops
                self.stop_trips[stop_number].add(trip_id)
                # skip this stop if we have a filter list and it's not in it
                if self.filter_stops and stop_number not in self.filter_stops:
                    continue
                arrival_hour, arrival_min, arrival_sec = [int(x) for x in arrival_time.split(':')]
                # arrival time is in the format HH:MM:SS. Pull out the hour so that trips can be 
                # looked up by stop and hour.
                hour = arrival_hour % 24
                if hour not in stop_times[stop_number]:
                     stop_times[stop_number][hour] = []
                stop_times[stop_number][hour].append(self._pack_stop_data(trip_id, arrival_hour, arrival_min, arrival_sec, stop_sequence))

        logging.info(f"\n\nLoaded {idx + 1} stop times in {time.time() - start_time:.0f} seconds")
        for stop_number in stop_times:
            for hour in stop_times[stop_number]:
                self.store.set('stop_times', f"{stop_number}:{hour}", stop_times[stop_number][hour])

        # make a set containing the trip_ids that serve any of the stops in self.filter_stops
        # so that we can filter out trips that don't serve any of the stops we are interested in.
        if self.filter_stops is not None:
            # flatten the list of lists into a single list
            self.filter_trips = set([
                trip_id for trip_ids in 
                [self.stop_trips[stop_number] for stop_number in self.filter_stops] 
                for trip_id in trip_ids
            ])

    def _pack_trip(self, route_id, service_id):
        # byte pack the data to save space
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
                if self.filter_trips and trip_id not in self.filter_trips:
                    continue
                self.store.set('trip', trip_id, self._pack_trip(route_id, service_id))

    def get_trip_info(self, trip_id):
        try:
            packed_trip = self.store.get('trip', trip_id)
            if packed_trip:
                route_id, service_id = self._unpack_trip(packed_trip)
                route_info = self.store.get('route', route_id)
                if route_info is None:
                    logging.warning(f"Unrecognised route_id {route_id} in trip {trip_id}")
                    return
                agency_info = self.store.get('agency', route_info['agency'])
                calendar_info = self.store.get('service', service_id)
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
                if self.filter_trips and trip_id not in self.filter_trips:
                    continue
                trip_delays = []
                for stop_time_update in entity.trip_update.stop_time_update:
                    if stop_time_update.schedule_relationship != STOP_SCHEDULED:
                        continue
                    
                    stop_number = self.store.get('stop', stop_time_update.stop_id)
                    if self.filter_stops is None and stop_number is None:
                        # Not filtering stops, so we should recognise all of them.
                        logging.warning(f"Unrecognised stop_id {stop_time_update.stop_id} in live data feed.")
                        continue
                    if entity.trip_update.trip.schedule_relationship == TRIP_ADDED and stop_number is not None:
                        # We can only work with an unscheduled "added" trip if we are given the expected arrival time.
                        if stop_time_update.arrival.time:
                            num_added += 1
                            live_additions = self.store.get('live_additions', stop_number, [])
                            live_additions.append({
                                'route_id': entity.trip_update.trip.route_id,
                                'arrival': datetime.datetime.fromtimestamp(stop_time_update.arrival.time),
                                'timestamp': timestamp
                            })
                            self.store.set('live_additions', stop_number, live_additions)
                    elif entity.trip_update.trip.schedule_relationship == TRIP_CANCELLED:
                        num_cancelled += 1
                        self.store.set('live_cancelations', trip_id, True)
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

                if len(trip_delays):
                    self.store.set('live_delays', trip_id, trip_delays)
        logging.info(f"Got {num_updates} trip updates, {num_unrecognised_trips} unrecognised trips, {num_added} added trips, {num_cancelled} cancelled trips")
    
    def refresh_live_data(self):
        now = time.time()
        try:
            # Time to get new data
            req = urllib.request.Request(self.live_url, None, {
                'x-api-key': self.api_key,
                'Cache-Control': 'no-cache'
            })
            f = urllib.request.urlopen(req)
            self._parse_live_data(f.read())
            f.close()
            self.rate_limit_count = 0
        except urllib.error.HTTPError as e:
            logging.error(f"Error fetching real time updates: {e}")
            # so long as we get rate-limited, back off exponentially
            if e.code == 429:
                self.rate_limit_count += 1
        return self.rate_limit_count
    

    def _get_live_delay(self, trip_id: str, stop_sequence: int):
        # find the real time update for this stop or the one with the highest sequence number
        # lower than this stop
        updates = self.store.get('live_delays', trip_id)
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
        return self.store.has('stop_numbers', stop_number)
    
    def get_scheduled_arrivals(self, stop_number: str, now: datetime, max_wait: datetime.timedelta):
        # get all the scheduled arrivals at a given stop_id
        # returns a list of (trip_id, arrival_time, stop_sequence)
        scheduled_arrivals = []
        # try the previous hour and the next few (per minutes)
        try_hours: list
        if now.hour == 0:
            try_hours = [23]
        else:
            try_hours = [now.hour - 1]
        try_hours.extend([h % 24 for h in range(now.hour, now.hour + int(max_wait.total_seconds() // 3600) + 1)])
        for hour in try_hours:
            stop_times = self.store.get('stop_times', f"{stop_number}:{hour}")
            if stop_times is None:
                continue
            for packed_stop_data in self.store.get('stop_times', f"{stop_number}:{hour}"):
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
                if trip_info is None:
                    continue
                service_is_scheduled = \
                    trip_info['start_date'] <= arrival_datetime.date() <= trip_info['end_date'] and \
                    trip_info['days'][arrival_datetime.date().weekday()]
                calendar_exception = self.store.get('exception', f"{trip_info['service_id']}:{arrival_datetime.date()}")
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
        for added_trip in self.store.get('live_additions', stop_number, []):
            route_info = self.store.get('route', added_trip['route_id'])
            agency_name = self.store.get('agency', route_info['agency'])
            scheduled_arrivals.append({
                'route': route_info['name'],
                'agency': agency_name,
                'scheduled_arrival': added_trip['arrival'],
                'real_time_arrival': added_trip['arrival'],
            })
        scheduled_arrivals.sort(key=lambda x: x['real_time_arrival'] or x['scheduled_arrival'])
        return scheduled_arrivals


def static_data_ok(max_seconds_old=None):
    if os.path.exists("data/timestamp.txt"):
        if max_seconds_old is None:
            return True
        else:
            with open("data/timestamp.txt", "r") as f:
                timestamp = datetime.datetime.fromisoformat(f.read())
                if datetime.datetime.utcnow() - timestamp < datetime.timedelta(seconds=max_seconds_old):
                    return True
    return False

CACHE_INFO_FILE = "data/cache_info.txt"
def write_cache_info(filter_stops):
    with open(CACHE_INFO_FILE, "w") as f:
        f.write(json.dumps({
            'filter_stops': sorted(list(filter_stops)) if filter_stops else None
        }, indent=4))

def check_cache_file():
    return os.path.exists(store.DATA_PATH)

def check_cache_info(filter_stops):
    if not os.path.exists(CACHE_INFO_FILE):
        return False
    with open(CACHE_INFO_FILE, "r") as f:
        cache_info = json.load(f)
        if cache_info.get('filter_stops') != sorted(list(filter_stops)) if filter_stops else None:
            return False
    return True
            
def download_static_data():
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
            os.makedirs("data/bak", exist_ok=True)
            # only copy .txt files
            for file in os.listdir("data"):
                if file.endswith(".txt"):
                    shutil.copy(os.path.join("data", file), "data/bak/")
            # extract the new .txt files into "data"
            zip_ref.extractall("data")
            # Ideally the feed would include a `feed_info.txt` file that has a 
            # `feed_end_date` field, but it doesn't, so we'll make our own.
            # (see https://gtfs.org/schedule/reference/#feed_infotxt for details)
            # write a file called "timestamp.txt" that contains the ISO timestamp of the last time the data was updated
            with open("data/timestamp.txt", "w") as f:
                f.write(datetime.datetime.utcnow().isoformat())
            # remove the cache file
            if os.path.exists(store.DATA_PATH):
                os.remove(store.DATA_PATH)
    logging.info("Done.")


def make_base_arg_parser(description):
    
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument('-l', '--live_url', type=str, default=settings.GTFS_LIVE_URL,
                        help=f"URL of the live GTFS feed (default: {settings.GTFS_LIVE_URL})")
    parser.add_argument('-k', '--api_key', type=str, default=settings.API_KEY,
                        help=f"Your API key for the live GTFS feed")
    parser.add_argument('-r', '--redis', type=str, default=settings.REDIS_URL,
                        help=f"URL of a redis instance to use as a data store backend (default: {settings.REDIS_URL})")
    parser.add_argument('-m', '--minutes', type=int, default=settings.MAX_MINUTES,
                        help=f"Maximum minutes in the future to return results for (default: {settings.MAX_MINUTES})")
    parser.add_argument('-f', '--filter', type=str, default=None,
                        help=f"Comma separated list of stop numbers that stored data must relate to, for memory optimization. If not specified, all stops are included. (default: {settings.FILTER_STOPS})")
    parser.add_argument('--logging', type=str, choices=['DEBUG', 'INFO', 'WARN', 'ERROR'], default=settings.LOG_LEVEL, dest='log_level',
                        help=f"Print verbose output (default: {settings.LOG_LEVEL}))")
    parser.add_argument('--profile', action='store_true',default=False,
                        help='Profile memory usage')
    return parser


import settings
if __name__ == "__main__":
    # Parse command line arguments
    parser = make_base_arg_parser("Perform a live query against the API for upcoming scheduled arrivals.")
    
    parser.add_argument('--download', action='store_true', default=False,
                        help='Download and extract the static GTFS archive and exit')
    parser.add_argument('--rebuild_cache', action='store_true',default=False,
                        help="Ignore cached GTFS data and load static data from scratch")
    parser.add_argument('stop_numbers', metavar='stop numbers', type=str, nargs='*',
                        help='Stop numbers to query (as shown on the bus stop)')
    args = parser.parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level))

    # if the --download option was specified, download the static GTFS archive and exit
    if args.download:
        download_static_data()
    
    filter_stops = settings.FILTER_STOPS
    if args.filter is not None:
        filter_stops = args.filter.split(',')
    gtfs = GTFS(
        live_url=args.live_url, 
        api_key=args.api_key, 
        redis_url=args.redis,
        rebuild_cache=args.rebuild_cache,
        filter_stops=filter_stops,
        profile_memory=args.profile
    )

    logging.info("Updating from live feed.")
    gtfs.refresh_live_data()
    logging.info("Live feed loaded.")

    # Get results for each stop_number, if any
    now = datetime.datetime.now()
    for stop_number in args.stop_numbers:
        if not gtfs.is_valid_stop_number(stop_number):
            print(f"Stop number {stop_number} is not recognised.")
            continue
        arrivals = gtfs.get_scheduled_arrivals(stop_number, now, datetime.timedelta(minutes=args.minutes))
        for arrival in arrivals:
            rt_arrival = "N/A"
            if arrival['real_time_arrival']:
                if arrival['real_time_arrival'].year == 1970:
                    rt_arrival = "epoch"
                else:
                    rt_arrival = arrival['real_time_arrival'].strftime('%H:%M')
            print(f"{stop_number}, {arrival['route']}, {arrival['scheduled_arrival'].strftime('%H:%M')}, {rt_arrival}")

