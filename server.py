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
import logging
import threading
import subprocess
import time

from flask import Flask, jsonify, request, Response
from flask_cors import CORS
from flask.json.provider import DefaultJSONProvider
import yaml
import waitress
from functools import wraps

from gtfs import GTFS, make_base_arg_parser, check_for_new_static_data, check_cache_info, check_cache_file
import settings


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

# Some templates to render HTML responses with when queried with accept: text/html

HTML_TEMPLATE = """
<html>
    <head>
        <title>GTFS API</title>
        <style>
            {css}
        </style>
    </head>
    <body>
        <select name="accept" id="accept" style="float: right">
            <option value="text/html">HTML</option>
            <option value="application/json">JSON</option>
            <option value="application/yaml">YAML</option>
            <option value="text/csv">CSV</option>
            <option value="text/plain">Plain</option>
        </select>
        <form action="/api/v1/arrivals" method="get" id="form">
            <input type="text" name="stop" id="stop1" placeholder="Stop number" />
            <input type="text" name="stop" id="stop2" placeholder="Stop number" />
            <button type="submit">Submit</button>
        </form>
        <table>
        <thead>
            <tr>
                {headers}
            </tr>
        </thead>
        <tbody>
            {rows}
        </tbody>
        </table>
        <script>
            {script}
        </script>
    </body>
</html>
"""
CSS = """
table {
    color: #333;
    background: white;
    border: 1px solid grey;
    font-size: 12pt;
    border-collapse: collapse;
}
table thead th {
    color: #777;
    background: lightgrey;
}
table td {
    padding: 5px 10px;
}
table tr:nth-child(even) {
    background: #eee;
}
"""
SCRIPT = """
// Initialise the form with the stop numbers from the query string
function initStopNumbers() {
    let stops = window.location.search.substring(1).split('&').filter(q => q.startsWith('stop=')).map(q => q.replace('stop=', ''));
    let stopNumber = 1;
    for (let stop of stops) {
        let input = document.getElementById(`stop${stopNumber}`);
        input.value = stop;
        stopNumber++;
    }
}

// Explicitly handle form submission so we can alter the accept header
function submitHandler(e) {
    const form = document.getElementById('form');
    e.preventDefault();
    const xhr = new XMLHttpRequest();
    const formData = new FormData(form);
    const accept = document.getElementById("accept").value;
    let url = form.getAttribute("action") + "?" + new URLSearchParams(formData).toString();
    // remove any empty stop query params
    if(url.indexOf('stop=&') != -1)
        url = url.replace('stop=&', '');
    if(url.endsWith("stop="))
        url = url.substring(0, url.length - 5);
    if(url.endsWith("&"))
        url = url.substring(0, url.length - 1);
    xhr.open(form.method, url);
    // set the accept header to the value of the accept query parameter
    xhr.setRequestHeader('Accept', accept);
    xhr.onload = () => {
        if (xhr.readyState === xhr.DONE && xhr.status === 200) {
            if(xhr.getResponseHeader("content-type").indexOf('text/html') != -1) {
                document.body.innerHTML = xhr.response;
                // update the window location so that the query string is updated
                window.history.pushState({}, '', url);
                initStopNumbers();
                document.getElementById('form').addEventListener('submit', submitHandler);
            }
            else {
                document.body.innerHTML = `<pre>${xhr.response}</pre>`;
            }
        }
    };
    xhr.send();
    return false;
}
initStopNumbers();
document.getElementById('form').addEventListener('submit', submitHandler);

"""

def format_response(func):
    @wraps(func)
    def decorated_function(*args, **kwargs):
        response_data = func(*args, **kwargs)
        accept_header = request.headers.get('Accept')
        # default to JSON
        mime_type = None
        if accept_header in ('*/*', '', 'application/*'):
            mime_type = 'application/json'
        else:
            for mime_type in ('application/json', 'application/yaml', 'text/csv', 'text/plain', 'text/html', None):
                if mime_type in accept_header:
                    break
        
        if mime_type == 'application/json':
            return jsonify(response_data)
        elif mime_type == 'application/yaml':
            return Response(yaml.dump(response_data, default_flow_style=False), mimetype=mime_type)
        elif mime_type in ('text/csv', 'text/plain', 'text/html'):
            # convert the nested response_data dict into a list of dicts
            # with the keys as the first row
            def to_iso_date(d):
                if isinstance(d, datetime.datetime):
                    return d.isoformat()
                return ""
            headers = ",".join(["stop", "stop_name", "route", "headsign", "agency", "scheduled_arrival", "estimated_arrival"])
            data = [
                ", ".join([stop_number, stop_data['stop_name'], stop['route'], stop['headsign'], stop['agency'],  to_iso_date(stop['scheduled_arrival']), to_iso_date(stop['real_time_arrival'])])
                for stop_number, stop_data in response_data.items() 
                for stop in stop_data['arrivals']
            ]
            # convert the list of dicts into a CSV string
            if mime_type in ('text/csv', 'text/plain'):
                csv = "\n".join([headers] + data)
                return Response(csv, mimetype=mime_type)
            elif mime_type=='text/html':
                headers = f"<th>{headers.replace(',', '</th><th>')}</th>"
                rows = "\n".join([f"<tr><td>{row.replace(',', '</td><td>')}</td></tr>" for row in data])
                html = HTML_TEMPLATE.format(headers=headers, rows=rows, css=CSS, script=SCRIPT)
                return Response(html, mimetype=mime_type)
    
    return decorated_function


def start_scheduled_jobs(gtfs, polling_period):
    # start a thread that refreshes live data every polling_period seconds
    def refresh():
        next_static_download_check = datetime.datetime.utcnow() + datetime.timedelta(hours=1)
        while True:
            logging.info("Updating from live feed.")
            rate_limit_count = gtfs.refresh_live_data()
            logging.info("Live feed updated.")
            # sleep for a period of time that is exponentially proportional to the rate limit count
            time.sleep(int(polling_period + polling_period * 1.5**rate_limit_count))

            # Check if the static GTFS data should be downloaded
            if datetime.datetime.utcnow() > next_static_download_check:
                if check_for_new_static_data():
                    logging.info("Downloading new static data.")
                    proc = subprocess.Popen(["python", "gtfs.py", "--download", "--rebuild-cache"])
                    proc.wait()
                    # Not reload the cache
                    gtfs.store.reload_cache()
                next_static_download_check = datetime.datetime.utcnow() + datetime.timedelta(hours=1)
    t = threading.Thread(target=refresh)
    t.daemon = True
    t.start()


if __name__ == "__main__":
    # Parse command line arguments
    parser = make_base_arg_parser("Run a REST server that allows the API to be queried for upcoming scheduled arrivals.")
    parser.add_argument('-H', '--host', type=str, default=settings.HOST,
                        help=f"Host to listen on (default: {settings.HOST})")
    parser.add_argument('-P', '--port', type=int, default=settings.PORT,
                        help=f"Port to listen on (default: {settings.PORT})")
    parser.add_argument('-w', '--workers', type=int, default=settings.WORKERS,
                        help=f"Number of worker threads to use (default: {settings.WORKERS})")
    parser.add_argument('-p', '--polling-period', type=int, default=settings.POLLING_PERIOD,
                        help=f"Polling period for live GTFS feed (default: {settings.POLLING_PERIOD})")
    args = parser.parse_args()
    
    logging.basicConfig(level=getattr(logging, args.log_level))

    filter_stops = settings.FILTER_STOPS
    if args.filter is not None:
        filter_stops = args.filter.split(',')
    
    # prepare to fork a sub-process to download or reparse static data
    sub_process_args = None
    if check_for_new_static_data():
        sub_process_args = ["python", "gtfs.py", "--download", "--rebuild-cache"]
    elif not args.redis and not check_cache_file() or not check_cache_info(filter_stops):
        logging.info(f"Rebuilding.")
        sub_process_args = ["python", "gtfs.py", "--rebuild-cache"]
    
    if sub_process_args:
        if filter_stops is not None:
            sub_process_args += ["--filter", ",".join(filter_stops)]
        if args.redis is not None:
            sub_process_args += ["--redis", args.redis]
        # fork the process and wait for it.
        # forking allows us to avoid incurring the memory overhead of parsing the data in this
        proc = subprocess.Popen(sub_process_args)
        proc.wait()
    
    # set up the GTFS object
    gtfs = GTFS(
        live_url=args.live_url, 
        api_key=args.api_key, 
        redis_url=args.redis,
        filter_stops=filter_stops,
        profile_memory=args.profile
    )
    start_scheduled_jobs(gtfs, args.polling_period)

    # set up the API endpoint
    @app.route('/api/v1/arrivals')
    @format_response
    def arrivals():
        now = datetime.datetime.now()
        stop_numbers = request.args.getlist('stop')
        arrivals = {}
        for stop_number in stop_numbers:
            if gtfs.is_valid_stop_number(stop_number):
                arrivals[stop_number] = {
                    'stop_name': gtfs.get_stop_name(stop_number),
                    'arrivals': gtfs.get_scheduled_arrivals(stop_number, now, datetime.timedelta(minutes=args.minutes))
                }
        return arrivals
    
    # start server
    print("Waiting for requests...")
    waitress.serve(app, host=args.host, port=args.port, threads=args.workers)
