# Unit tests for the `gtfs` and `store` modules
#
# Run with `python -m unittest test`
#

import unittest
import os
import datetime
from pathlib import Path

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
