# Unit tests for the `gtfs` and `store` modules
#
# Run with `python -m unittest test`
#

import unittest
import os
import datetime
from pathlib import Path
from unittest.mock import patch

import gtfs
import store
import settings

class TestStore(unittest.TestCase):
    
    def testHash(self):
        s = store.Store()
        s.set('testnamespace', 'val1', 1)
        s.set('testnamespace', 'val2', 2)
        s.set('testnamespace', 'val3', 3)
        self.assertEqual(s.get('testnamespace', 'val2'), 2)
        s.delete('testnamespace', 'val2')
        self.assertIsNone(s.get('testnamespace', 'val2'))

    def testSet(self):
        s = store.Store()
        s.add('testnamespace', 1)
        s.add('testnamespace', 1)
        s.add('testnamespace', 2)
        s.add('testnamespace', 2)
        s.add('testnamespace', 3)
        s.add('testnamespace', 3)
        self.assertTrue(s.has('testnamespace', 2))
        self.assertEqual(s.cardinality('testnamespace'), 3)
        s.remove('testnamespace', 2)
        self.assertFalse(s.has('testnamespace', 2))
        self.assertEqual(s.cardinality('testnamespace'), 2)
        s.remove('testnamespace', 1)
        s.remove('testnamespace', 2)
        s.remove('testnamespace', 3)
        self.assertEqual(s.cardinality('testnamespace'), 0)
    
    def testRedisNamespacesArePrefixed(self):
        class FakeRedis:
            def __init__(self):
                self.hashes = {}
                self.sets = {}

            def hset(self, namespace, key, value):
                self.hashes.setdefault(namespace, {})[key] = value

            def hget(self, namespace, key):
                return self.hashes.get(namespace, {}).get(key)

            def sadd(self, namespace, value):
                self.sets.setdefault(namespace, set()).add(value)

            def sismember(self, namespace, value):
                return int(value in self.sets.get(namespace, set()))

        s = store.Store(namespace_prefix="tfi-gtfs:v1:")
        s.redis = FakeRedis()
        s.set('trip', 'trip-1', b'trip data')
        s.add('stop_numbers', '1358')

        self.assertIn('tfi-gtfs:v1:trip', s.redis.hashes)
        self.assertIn('tfi-gtfs:v1:stop_numbers', s.redis.sets)
        self.assertEqual(s.get('trip', 'trip-1'), b'trip data')
        self.assertTrue(s.has('stop_numbers', '1358'))

    def testRedisDoesNotLoadLocalPickleCache(self):
        with patch.object(store.redis, 'from_url', return_value=object()), \
             patch.object(store.Store, 'reload_cache') as reload_cache:
            store.Store(redis_url='redis://localhost', namespace_prefix='tfi-gtfs:v1:')

        reload_cache.assert_not_called()

    def testClearCacheOnlyDeletesPrefixedRedisKeys(self):
        class FakeRedis:
            def __init__(self):
                self.keys = {
                    b'tfi-gtfs:v1:trip',
                    b'tfi-gtfs:v1:status',
                    b'another-app:data',
                }

            def scan_iter(self, match):
                prefix = match[:-1].encode()
                return (key for key in self.keys if key.startswith(prefix))

            def delete(self, *keys):
                self.keys.difference_update(keys)

        s = store.Store(namespace_prefix="tfi-gtfs:v1:")
        s.redis = FakeRedis()
        s.clear_cache()

        self.assertEqual(s.redis.keys, {b'another-app:data'})

    def testClearRedisCacheRequiresPrefix(self):
        s = store.Store()
        s.redis = object()

        with self.assertRaises(ValueError):
            s.clear_cache()

    def testCache(self):
        old_cache_file = store.CACHE_FILE
        store.CACHE_FILE = Path("test_data/store_test.pickle")

        s1 = store.Store()
        s1.set('testhash', 'val1', 1)
        s1.set('testhash', 'val2', 2)
        s1.set('testhash', 'val3', 3)
        s1.add('testset', 1)
        s1.add('testset', 2)
        s1.add('testset', 3)
        s1.write_cache()

        # Initialize a second store from the same cache.
        s2 = store.Store()
        self.assertEqual(s2.get('testhash', 'val2'), 2)
        self.assertTrue(s2.has('testset', 2))

        # Clear the cache and reload it.
        s1.clear_cache()
        self.assertFalse(s1.has('testset', 2))
        s1.write_cache()
        # should still be in s2
        self.assertTrue(s2.has('testset', 2))
        # reload s2
        s2.reload_cache()
        # should now be gone from s2
        self.assertFalse(s2.has('testset', 2))

        os.remove(store.CACHE_FILE)
        store.CACHE_FILE = old_cache_file


class TestCacheSchema(unittest.TestCase):

    def testSchemaMismatchClearsCacheBeforeReloadingStaticData(self):
        class FakeStore:
            instance = None

            def __init__(self, **kwargs):
                self.data = {
                    'status': {
                        'initialized': True,
                        'schema_version': gtfs.CACHE_SCHEMA_VERSION - 1,
                    }
                }
                self.clear_count = 0
                FakeStore.instance = self

            def get(self, namespace, key, default=None):
                return self.data.get(namespace, {}).get(key, default)

            def set(self, namespace, key, value):
                self.data.setdefault(namespace, {})[key] = value

            def clear_cache(self):
                self.clear_count += 1
                self.data = {}

        def fake_load_static(instance):
            instance.store.set('status', 'schema_version', gtfs.CACHE_SCHEMA_VERSION)
            instance.store.set('status', 'initialized', True)

        with patch.object(gtfs, 'check_for_new_static_data', return_value=False), \
             patch.object(gtfs.store, 'Store', FakeStore), \
             patch.object(gtfs.GTFS, 'load_static', fake_load_static):
            gtfs.GTFS(settings.GTFS_LIVE_URL, settings.API_KEY)

        self.assertEqual(FakeStore.instance.clear_count, 1)
        self.assertEqual(
            FakeStore.instance.get('status', 'schema_version'),
            gtfs.CACHE_SCHEMA_VERSION
        )


class TestCacheInfo(unittest.TestCase):

    def testFilteredCacheIsNotAcceptedForUnfilteredRun(self):
        old_cache_info_file = gtfs.CACHE_INFO_FILE
        gtfs.CACHE_INFO_FILE = Path("test_data/cache_info_test.json")
        try:
            with open(gtfs.CACHE_INFO_FILE, "w") as cache_info:
                cache_info.write('{"filter_stops": ["1358"]}')

            self.assertFalse(gtfs.check_cache_info(None))
        finally:
            if os.path.exists(gtfs.CACHE_INFO_FILE):
                os.remove(gtfs.CACHE_INFO_FILE)
            gtfs.CACHE_INFO_FILE = old_cache_info_file


class TestGTFS(unittest.TestCase):

    def setUp(self):
        # Load a cached dataset, which was created by filtering for stop 1358 (Dame St.)
        # on 15/9/2023 at 09:10am IST.
        self.old_cache_file = store.CACHE_FILE
        store.CACHE_FILE = Path("test_data/cache.pickle")
        self.gtfs = gtfs.GTFS(settings.GTFS_LIVE_URL, settings.API_KEY)
        with open("test_data/test_live_response.gtfsr", 'rb') as f:
            live_data = f.read()
            self.gtfs._parse_live_data(live_data)

    def test_string2bytes(self):
        test_string = "test_string123!£$"
        self.assertEqual(gtfs._b2s(gtfs._s2b(test_string)), test_string)

    def test_pack_stop_data(self):
        # stop data: trip_id, arrival_hour, arrival_min, arrival_sec, stop_sequence
        stop_data = ("12345", 12, 15, 10, 5)
        packed = self.gtfs._pack_stop_data(*stop_data)
        unpacked = self.gtfs._unpack_stop_data(packed)
        self.assertEqual(stop_data, unpacked)

    def test_pack_trip(self):
        # trip data: route_id, service_id
        trip_data = ("12345", "67AB", "Far Far Away")
        packed = self.gtfs._pack_trip(*trip_data)
        unpacked = self.gtfs._unpack_trip(packed)
        self.assertEqual(trip_data, unpacked)

    def test_pack_trip_preserves_long_route_id(self):
        trip_data = ("DUB-PORTLAOIS-O", "67AB", "Portlaoise")
        packed = self.gtfs._pack_trip(*trip_data)

        self.assertEqual(len(packed), 49)
        self.assertEqual(self.gtfs._unpack_trip(packed), trip_data)
    
    def test_valid_stop_number(self):
        # Data in test_data/cache.pickle is filtered for stop 1358 (Dame St.)
        self.assertTrue(self.gtfs.is_valid_stop_number("1358"))
    
    def test_invalid_stop_number(self):
        self.assertFalse(self.gtfs.is_valid_stop_number("9999"))

    def test_trip_info(self):
        # trip ID 3599_6532 is a 27 bus trip that is valid for stop 1358
        trip_info = self.gtfs.get_trip_info("3582_11643")
        self.assertIsNotNone(trip_info)
        self.assertEqual(trip_info['route'], "49")
        self.assertEqual(trip_info['service_id'], "180")
        self.assertEqual(trip_info['start_date'].isoformat(), "2023-09-15")
        self.assertEqual(trip_info['end_date'].isoformat(), "2023-09-15")
        self.assertEqual(trip_info['days'], [False, False, False, False, True, False, False])
    
    def test_live_delay(self):
        trip_id = "3582_6405"
        stop_sequence = 78
        live_delay = self.gtfs._get_live_delay(trip_id, stop_sequence)
        self.assertEqual(live_delay, 88)

    def test_scheduled_arrivals(self):

        scheduled_arrivals = self.gtfs.get_scheduled_arrivals(
            "1358", 
            now=datetime.datetime.fromisoformat("2023-09-15T09:10:00"),
            max_wait=datetime.timedelta(minutes=60)
        )
        self.assertTrue(len(scheduled_arrivals))
        routes = sorted(set([x['route'] for x in scheduled_arrivals]))
        self.assertListEqual(routes, 
                             ['150', '27', '49', '54A', '56A', '65', '65B', '68', '68A', '69', '77A']
        )
        # Check that the first arrival is a 77A, which is due at 11:32:11
        self.assertEqual(scheduled_arrivals[0]['route'], "68")
        self.assertEqual(scheduled_arrivals[0]['scheduled_arrival'].isoformat(), "2023-09-15T09:15:50")
        self.assertEqual(scheduled_arrivals[0]['real_time_arrival'].isoformat(), "2023-09-15T09:13:38")
        
        # Check the 5th arrival is a 27, which is due at 11:33:00 and has no real-time info
        self.assertEqual(scheduled_arrivals[5]['route'], "49")
        self.assertEqual(scheduled_arrivals[5]['scheduled_arrival'].isoformat(), "2023-09-15T09:24:16")
        self.assertIsNone(scheduled_arrivals[5]['real_time_arrival'])

    def tearDown(self):
        store.CACHE_FILE = self.old_cache_file

if __name__ == '__main__':
    unittest.main()
