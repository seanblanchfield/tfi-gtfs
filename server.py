# a HTTP server that exposes a REST API to to access the GTFS data via the `gtfs` module
#
# The server is implemented using the Flask framework.
#
# The server exposes the following endpoints:
#
# /api/v1/arrivals
# one or more stop numbers must be passed as a query parameter, e.g. /api/v1/arrivals?stop=7602
# /api/v1/arrivals?stop=7602&stop=7603

import datetime

from flask import Flask, jsonify, request, Response
from flask_cors import CORS
from flask.json.provider import DefaultJSONProvider
import yaml
import waitress
from functools import wraps

import settings
from gtfs import GTFS

app = Flask(__name__)
CORS(app)

# create a subclass of flask.json.provider.DefaultJSONProvider returns JSON responses
# in which datetime objects are serialized to ISO 8601 strings
class JsonProvider(DefaultJSONProvider):
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.isoformat()
        return super(JsonProvider, self).default(obj)    

app.json = JsonProvider(app)

def format_response(func):
    @wraps(func)
    def decorated_function(*args, **kwargs):
        response_data = func(*args, **kwargs)
        accept_header = request.headers.get('Accept')
        # default to JSON
        if accept_header in ('*/*', '', 'application/*'):
            accept_header = 'application/json'
        
        if 'application/json' in accept_header:
            return jsonify(response_data)
        elif 'application/yaml' in accept_header:
            return Response(yaml.dump(response_data, default_flow_style=False), mimetype='application/yaml')
        elif 'text/csv' in accept_header:
            # convert the nested response_data dict into a list of dicts
            # with the keys as the first row
            def to_iso_date(d):
                if isinstance(d, datetime.datetime):
                    return d.isoformat()
                return ""
            headers = ",".join(["stop", "route", "agency", "scheduled_arrival", "estimated_arrival"])
            data = [
                ", ".join([stop_number, stop['route'], stop['agency'], to_iso_date(stop['scheduled_arrival']), to_iso_date(stop['real_time_arrival'])])
                for stop_number, stops in response_data.items() 
                for stop in stops
            ]
            # convert the list of dicts into a CSV string
            csv = "\n".join([headers] + data)
            return Response(csv, mimetype='text/csv')
    
    return decorated_function

if __name__ == "__main__":

    # Read program options for live_url, api_key, no_cache, polling_period and the max_wait time
    import argparse
    parser = argparse.ArgumentParser(description='Run a REST server that allows the API to be queried for upcoming scheduled arrivals.')
    parser.add_argument('-l', '--live_url', type=str, default=settings.GTFS_LIVE_URL,
                        help='URL of the live GTFS feed')
    parser.add_argument('-k', '--api_key', type=str, default=settings.API_KEY,
                        help='API key for the live GTFS feed')
    parser.add_argument('-r', '--redis', type=str, default=settings.REDIS_URL,
                        help='URL of a redis instance to use as a data store backend')
    parser.add_argument('--no_cache', action='store_true',default=False,
                        help='Ignore cached GTFS data and load static data from scratch')
    parser.add_argument('-p', '--polling_period', type=int, default=60,
                        help='Polling period for live GTFS feed')
    parser.add_argument('-w', '--max_wait', type=int, default=60,
                        help='Maximum minutes in the future to return results for')
    parser.add_argument('-H', '--host', type=str, default='localhost',
                        help='Host to listen on')
    parser.add_argument('-P', '--port', type=int, default=5000,
                        help='Port to listen on')


    args = parser.parse_args()

    # set up the GTFS object
    gtfs = GTFS(
        live_url=args.live_url, 
        api_key=args.api_key, 
        redis_url=args.redis,
        no_cache=args.no_cache,
        polling_period=args.polling_period
    )

    # set up the API endpoint
    @app.route('/api/v1/arrivals')
    @format_response
    def arrivals():
        now = datetime.datetime.now()
        stop_numbers = request.args.getlist('stop')
        arrivals = {}
        for stop_number in stop_numbers:
            if gtfs.is_valid_stop_number(stop_number):
                arrivals[stop_number] = gtfs.get_scheduled_arrivals(stop_number, now, datetime.timedelta(minutes=args.max_wait))
        return arrivals
    
    # start server
    print("Waiting for requests...")
    waitress.serve(app, host=args.host, port=args.port, threads=1)
