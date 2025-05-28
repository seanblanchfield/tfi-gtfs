# Transport for Ireland GTFS REST API
This project implements a simple REST server and command line utility for retrieving real-time information about public transport in Ireland (at least, for services operated by Dublin Bus, Bus Éireann, and Go-Ahead Ireland).

This project is inspired by [Sean Rees's GTFS Upcoming](https://github.com/seanrees/gtfs-upcoming). I started from scratch because I wanted to significantly optimise memory consumption so I could host the API on a single board computer. I found the full dataset consumed up to 10 gigabytes of RAM when using *GTFS Upcoming*, while I've managed to get it down to less than 200 megabytes after a significant rewrite. Both projects allow you to reduce RAM consumption by discarding data that does not pertain to a list of specific transport stops.


#### Consider joining the Dublin Smart Home mailing list

There's a [Dublin Smart Home](https://groups.google.com/g/smart-home-dublin) google group. Consider joining it!

## Background

The [National Transport Authority (NTA)](https://www.nationaltransport.ie/) of Ireland operates a public-transport brand called [Transport for Ireland](https://www.transportforireland.ie/) or *TFI*, which pulls together all information related to public transport. From the point of view of the average commuter, *TFI* is in charge of buses and trains. The NTA previously provided a real-time passenger information (RTPI) REST API that allowed the status of routes serving particular stops to be easily queried, but this API was discontinued in September 2020 ([perhaps due to scalability issues and breaches of fair use](https://data.gov.ie/blog/update-on-availability-of-the-real-time-travel-information-api)), and was replaced with a [GTFS-R API](https://www.transportforireland.ie/news/new-transport-data-feed-for-app-developers-now-online/).  *General Transit Feed Specification* (GTFS) is a protocol designed by Google to allow transit operators to communicate static and real-time schedule information to Google Maps. The static data consists of a zip file at a well-known URL that containing metadata files describing operators, routes, stops, and the schedule (static data is available for most public transport services, as described [here](https://www.transportforireland.ie/transitData/PT_Data.html)). The real-time part is an API endpoint that returns the status of the entire fleet of vehicles that are currently on the road/tracks (real-time information is currently only provided for a subset of bus operators: Dublin Bus, Bus Éireann, and Go-Ahead Ireland). This real-time feed needs to be interpreted in conjunction with the metadata from the static zip file.  This architecture seems convenient for Google, who are interested in mass-syncing all available information. However, it is inconvenient if you are an average user who has a specific query about upcoming arrivals at a particular stop or station.  This project bridges that gap.

## How it works

This project is a GTFS-R client, which reads all static and realtime transport fleet information into RAM. It then provides a simple REST API to allow querying of upcoming scheduled and real-time arrivals at any particular stop.

On startup, it downloads the static data and parses it into memory (by default, it will re-download this whenever the data is updated). It also periodically queries the real-time API, and stores received information about arrival delays, cancelations and additions into memory (by default, it will do this every minute). The in-memory information can then be efficiently queried to return a list of all scheduled and real-time arrivals at any particular stop.

## How to Run

You can run this project either as a python program, as a Docker container or as a Home Assistant Addon. When running as a python program or a docker container you can choose whether to run the REST API HTTP server, or whether to directly invoke the gtfs.py module as a command-line utility.

## Configuration

Before you start, you will need your own NTA API key, which you can get for free by signing up to the  [NTA developer portal](
https://developer.nationaltransport.ie/).

The API key, and all other settings, can be alternatively specified as (in order of precedence):
- environment variables
- settings file variables
- command-line arguments

> If using the settings file, you might find it more convenient not to directly modify `settings.py`, but instead to create a `local_settings.py` file where you can override just the settings you care about. `local_settings.py` is ignored by git.

You can view the available settings and default values by running `server.py` or `gtfs.py` with the `--help` argument.

For reference, the available settings are:
- `GTFS_STATIC_URL`. URL of the static NTA data. Defaults to "https://www.transportforireland.ie/transitData/Data/GTFS_Realtime.zip"
- `GTFS_LIVE_URL`. URL of the realtime NTA data. Defaults to "https://api.nationaltransport.ie/gtfsr/v2/TripUpdates"
- `API_KEY`. Your NTA API key. Either your "primary" or "secondary" key should work.
- `REDIS_URL`. The URL of a redis instance to use as a memory store for the purposes of memory optimisation or horizontal scalability. Typically something like `redis://localhost:6379`. Defaults to `None`, i.e., uses in-process memory instead.
- `POLLING_PERIOD`. How over to query the real-time API in seconds. Defaults to *60*.
- `MAX_MINUTES`. The maximum number of minutes into the future that arrivals returned in results are expected to arrive before. Defaults to 60 minutes.
- `HOST`. The host to run the API server at. Defaults to "localhost".
- `PORT`. The port to run the API server on. Defaults to "7341".
- `LOG_LEVEL`. The verbosity of output. Possible values are `DEBUG`, `INFO`, `WARN`, `ERR`. Defaults to `INFO`.
- `FILTER_STOPS`. A list of stop numbers that should be filtered for. Information received not pertaining to these stop numbers will be discarded, yielding a significant RAM saving. Defaults to `None`, meaning that information about all stops will be kept in memory.

The exact name of the corresponding command-line arguments might vary, so please run with `--help` to check the correct form. Please also run with `--help` to confirm the default values.

### Example
You can specify your API key in either of the following ways:

Export an environment variable called `API_KEY`:
``` bash
export API_KEY=abcdefghijklmnopqrstuvwxyz1234567890
python3 server.py
```
or
``` bash
API_KEY=abcdefghijklmnopqrstuvwxyz1234567890 python3 server.py
```

Specify it in the settings file (preferably in  `local_settings.py`):
``` python
API_KEY = "abcdefghijklmnopqrstuvwxyz1234567890"
```

Or specify it as a command-line argument:
``` bash
python3 server.py --api-key=abcdefghijklmnopqrstuvwxyz1234567890
```

## Running directly as a python program

Create a python virtual environment and install requirements:

``` bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

Run a web server:
``` bash
python3 server.py --help

python3 server.py --api_key=abcdefghijklmnopqrstuvwxyz1234567890
```

Run as a command-line interface:

``` bash
python3 gtfs.py --help
```

Download the static database and exit:

``` bash
python3 gtfs.py --download
```

Query specific stops:
``` bash
python3 gtfs.py 1358 7581
```

> Note that running `gtfs.py` in this way is slow because a lot of data needs to be loaded from disk into memory every time that it is invoked. It is convenient for testing or very occasional use, but in production, you should run `server.py`, which only has to load data once at startup.


## Running in Docker

A dockerfile is provided to allow you to easily build and run the project in a Docker container. This container is configured to start and use an internal *redis* instance, producing optimal memory efficiency.

To build the docker container, change into the project directory where the `Dockerfile` is and run:
``` bash
docker build --tag tfi-gtfs .
```

You can run the docker container as follows:

``` bash
docker run -p 7341:7341 tfi-gtfs
```

You can specify arguments as follows:

``` bash
docker run -p 7341:7341 tfi-gtfs --help
docker run -p 7341:7341 tfi-gtfs --api-key=abcdefghijklmnopqrstuvwxyz1234567890
```

You can pass a `local_settings.py` file into the container as follows:
``` bash
docker run -p 7341:7341 -v ./settings.py:/app/settings.py:ro tfi-gtfs
```

In the above examples, `7341` is the default port number used by the API server. You could map port `8000` on the host to `7341` in the container by instead specifying `-p 8000:7341`.

### SSL support

Basic HTTPS support is provided for instance when homeassistant with https enabled (e.g. to have mobile app access), https is required.
I replaved the waitress server, which is more suited to handle serveral requests and can scale with the flask embedded server.
This works well for the use case of home assistant, where no scalability is needed and only a few clients will do requests.
But *this should not be used* in a context where many customers and requests are expected.

To enable SSL, certificate and key must be provided either in the settings, throuhg env var, or arguments.
If none of those is not specified, the server will revert to http.

I've published a [docker image](https://hub.docker.com/r/vche/tfi-gtfs) as well that can be used. Example of use with docker compose:
```
    # Dublin bus real time api
    tfi:
        image: vche/tfi-gtfs:latest
        environment:
            API_KEY: f93g81b821324593bd3563f1d72f0f47
            REDIS_URL: 192.168.0.165
            SSL_CERT: "/certs/server-cert.pem"
            SSL_KEY: "/certs/server-key.pem"
        volumes:
            - $DOCKERDIR/certs:/certs
        ports:
            - 7341:7341
```

## Running in Home Assistant
This project is also available as a *Home Assistant* addon. Visit my [Home Assistant Addons repository](https://github.com/seanblanchfield/seans-homeassistant-addons) for more information on how to add that repository to your Home Assistant installation and install the addon.


## Querying the server
The API is hosted at the path `/api/v1/arrivals`. You can query it by supplying one or more "stop" query parameters.

For example, if the server is running on localhost, visit [http://localhost:7341/api/v1/arrivals?stop=1358](http://localhost:7341/api/v1/arrivals?stop=1358) in your web browser to receive a table of upcoming arrivals at stop `1358`.


Alternatively, using `cURL`, run the following command:
``` bash
curl "http://localhost:7341/api/v1/arrivals?stop=1358"
```

Multiple stops can be queried by providing multiple `stop` parameters. For example:
``` bash
curl "http://localhost:7341/api/v1/arrivals?stop=1358&stop=7581"
```

### Finding your stop number
Stop numbers are printed on bus stops. You can also find relevant stops on the official [TFI journey planner](https://www.transportforireland.ie/plan-a-journey/). Click on a stop to see its stop number.

## Response format

The server returns responses in JSON format by default. It also supports YAML, CSV and HTML. Here are some commands that test this using cURL:
``` bash
# JSON (default)
curl "http://localhost:7341/api/v1/arrivals?stop=1358" -H "Accept: application/json"
# YAML
curl "http://localhost:7341/api/v1/arrivals?stop=1358" -H "Accept: application/yaml"
# CSV
curl "http://localhost:7341/api/v1/arrivals?stop=1358" -H "Accept: text/csv"
# Plain text also returns CSV
curl "http://localhost:7341/api/v1/arrivals?stop=1358" -H "Accept: text/plain"
# HTML
curl "http://localhost:7341/api/v1/arrivals?stop=1358" -H "Accept: text/html"
```

If you visit the URL from your web browser, you web browser will automatically send an `Accept: text/html` header, so you should receive the response as a HTML table.

## Running with Redis

If you are running this project directly as python and memory consumption is an issue, you can use the `REDIS_URL` option to specify an external [redis](https://redis.io/) instance to use as a more efficient data store. Redis is a highly-performant distributed data store written in C, and has very efficient storage. If you don't have a *redis* instance, you can start one using Docker as follows:

``` bash
docker run --name redis-gtfs -p 127.0.0.1:6379:6379/tcp  -d redis
```

You may see warnings from *redis* saying that "*Memory overcommit must be enabled!*". This issue is discussed in [this docker issue](https://github.com/docker-library/redis/issues/19) and to get rid of the warning you must currently modify a setting on the host:

``` bash
sudo sysctl vm.overcommit_memory=1
```

Then, pass that redis instance to the python program passing the `--redis` argument or setting the `REDIS_URL` setting, for example:

```bash
python3 server.py --redis redis://localhost:6379
```

## Memory Requirements

When run directly, the default behaviour is to parse schedule data into local in-process data structures. This is convenient, but  python structures are space-inefficient, resulting in a lot of system memory being consumed.

Another way to reduce memory consumption is to specify a list of stops that you are solely interested in, so that data pertaining to all other stops can be discarded. You can do this by using the `FILTER_STOPS` option or `--filter` argument.

### Memory usage comparison


> *Note*: *"Total"* columns below refer to the RSS (Resident Set Size) reported by the `ps` command. This includes shared system libraries, so it's not really an accurate reflection of the marginal cost of running the process, but it is a decent comparative guideline. The *"data"* columns were measured using [this `total_size.py` gist](https://gist.github.com/nkonin/072e891b0e27ef7fa8e072aa7c7a7cb1). This is bundled in this project. You can generate a report on the size of python data structures (and any data in *redis*) if you pass the `--profile` argument.

#### Loading data for all stops

In a comparitive test on a 64 bit laptop, with all stops loaded I observed the following memory consumption patterns while running `server.py` (comparative values
for [GTFS-Upcoming](https://github.com/seanrees/gtfs-upcoming) are also given).
| Test             | Python data  | Python Total  | Redis data   | Redis Total  |
|---               |---           | ---           | ---          |   ---        |
|  Without redis   | 317MB        | 439MB         | N/A          |   N/A        |
|  With redis      | 1MB          | 39MB          | 131MB        |   141MB      |
|  GTFS-Upcoming   | 7,909MB      | 8,687MB       | N/A          |   N/A        |


#### Loading data for a single stop

If `FILTER_STOPS` is supplied, data that does not pertain to the given stops will be discarded, allowing memory use to be reduced. Repeating the above test while running the server with a single stop:

| Test             | Python data    | Python Total  | Redis data   | Redis Total  |
|---               |---             | ---           | ---          |   ---        |
|  Without redis   | 3.4MB          | 42.7MB        | N/A          |   N/A        |
|  With redis      | 1MB            | 39.9MB        | 3.4MB        |   13MB      |
|  GTFS-Upcoming   | 71MB           | 123MB         | N/A          |    N/A       |


**Conclusions**:

- We can subtract the "data" size from the "total" to get a rough estimate of the base memory consumed by each process. In this way, we see that the base memory required by **Redis seems to be less than 10 MB, while `server.py` requires about 38 MB**.

- Broadly speaking, we see that storing all the data in **python is about 2.4 times less space-efficient than storing it in Redis**.

- Storing data for one or two stops (using the `FILTER_STOPS` option) results in negligible memory use beyond the base requirements of the program, regardless of whether redis is used or not.

- The on-disk cache (`cache.pickle`) file expands by a factor of approximately 3 in RAM. For example, a 119MB cache file will translate into an additional 384MB RAM usage.

#### Beware of occasional high RAM use

When first run, or whenever the cache file doesn't exist or is old or invalid (e.g., was generated for a different set of filter stops), it will be rebuilt at startup by a sub-process. This sub-process is short-lived but memory intensive, and in my testing grows to up to 1.5 gigabytes before finishing.

## Execution Model

The `gtfs.py` module can be invoked directly as a command line utility, and runs as a single-threaded process. However, `server.py` starts multiple threads and subprocesses.

Internally, `server.py` uses [Waitress](https://docs.pylonsproject.org/projects/waitress/en/latest/index.html) to serve HTTP API requests. *Waitress* starts a pool of worker threads to handle requests. The default number of threads is specified by the `WORKERS` setting or `--workers` argument, and defaults to `1`.

`server.py` also starts a long-lived thread to handle scheduled tasks like polling the live API, or redownloading the static schedule data.

Actual downloading and parsing of static schedule data is handled in sub-processes, as it is a memory-intensive operation, and we want to allow the system to reclaim that memory after the new schedule has been processed. These sub-processes are simply instances of `gtfs.py`. `server.py` will launch `gtfs.py` in this way on startup (if the current downloaded schedule is out of date, or if the current cache is out of data or invalid). It will also check every hour if there is new static GTFS data (by performing a `HTTP HEAD` request) available and if necessary will launch `gtfs.py` to download it.

`server.py` runs `gtfs.py ` with the `--rebuild-cache` argument, which causes it to re-parse the static GTFS data (which may consume in the region of 1.5 gigabytes of RAM) and write a new `cache.pickle` file (which may take a minute or more depending on your hardware).  After writing the pickle file, the `gtfs.py` process ends, its memory is released, and `server.py` continues execution, by loading or reloading that pickle file, which is a fast operation.

## Advice for high-volume deployments

- Workers will generally only be blocked on network I/O with redis, which is minimal. To compensate for this, consider increasing the number of requests that can be simultaneously served by `server.py` by increasing `WORKERS` to 2 or 3.
- To allow multiple CPU cores to be used, you will need to launch multiple instances of `server.py`. This is due to the python [Global Interpreter Lock](https://superfastpython.com/gil-removed-from-python/) (GIL).
- If launching multiple instances, use *Redis* to avoid duplicating all the schedule data in each process. Also, avoid duplicating work of parsing schedule data and polling the live API from each process by configuring just one of the processes with the desired `POLLING_INTERVAL`, and set it to a very high value in all the other processes (3153600000 == 100 years in seconds).

## Developing

### Architecture

The project consists of the following modules:

- `settings.py` is a simple settings file.
- `size.py` is the memory-counting function from [this gist](https://gist.github.com/nkonin/072e891b0e27ef7fa8e072aa7c7a7cb1)
- `store.py` is a data store, which is backed by either *redis* or an internal `dict` depending on configuration.  It supports key-value style `get`/`set` operations, and `Set`-like `add`/`remove`/`has` operations. Everything is added to a "namespace", and a config `dict` can be passed in at initialization with optional rules for how items in each namespace should be expired.
- `gtfs.py` contains all code related to interacting with the GTFS static schedule data and GTFS-R live feed. It provides  functions to check, download and extract the static GTFS data, and provides a `GTFS` class that loads that data, can query the live GTFS feed, and allows the data to be queried for upcoming arrivals at any given stop. It uses `store.py` to record all GTFS data, making it agnostic to whether data is being stored in-process or in redis. It also exposes an entrypoint so it can be run as a standalone command line utility.

- `server.py`:
    - runs `gtfs.py` in a sub-process as-required to download static data and rebuild the cache.
    - creates an instance of the `gtfs.GTFS` class that it uses to fulfil API requests
    - starts a thread to manage scheduled tasks
    - runs the HTTP server

Static GTFS data is downloaded to the `/data` directory.

### Running and Debugging

The following tips are based on *VSCode*, but you should be able to adapt them to other IDEs.

Make sure you have set up the virtual environment as follows:

``` bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

If *VSCode* does not automatically detect this virtual environment, pop open the command palette (CTRL-SHIFT-P) and run "*Python: Select Interpreter*".

Consider creating a `local_settings.py` file, so you don't have to always passing common arguments every time. This file is ignored by git. For example:

``` python
API_KEY = "abcdefghijklmnopqrstuvwxyz1234567890"
LOG_LEVEL = "DEBUG"
```

A `launch.json` file is provided for *VSCode*, which contains a configuration to launch either `server.py` or `gtfs.py`. You should be able to run either of them at this point.

If you want to debug with redis, you will need a redis instance. See the "Running with Redis" section for info on starting a docker redis container. For debugging purposes I suggest you use your `local_settings.py` file to pass a `REDIS_URL` (to avoid accidentally committing changes to `launch.json`).

### Pull Requests
Further development and PRs are very welcome.

### Testing

Unit tests are provided in `tests.py`, with test fixtures in `test_data/`. Run tests as follows:
``` bash
python3 -m unittest test
```
