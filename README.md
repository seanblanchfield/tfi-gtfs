# Transport for Ireland GTFS
Transport for Ireland API server making the GTFS-R feed more convenient to use.

To run directly, create a python virtual environment and install requirements:
``` bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```


Run as a command-line tools:

```
python3 gtfs.py --help
```

Download the static database and exit:
```
python3 gtfs.py --download
```

Run a web server:
``` bash
python3 server.py --help
```

Query the server for arrivals at a specific set of stops:
``` bash
curl -i "http://localhost:5000/api/v1/arrivals?stop=2189&stop=1509"

# In yaml
curl -i "http://localhost:5000/api/v1/arrivals?stop=2189&stop=1509" -H "Accept: application/yaml"

# As CSV
curl -i "http://localhost:5000/api/v1/arrivals?stop=2189&stop=1509" -H "Accept: text/csv"

```

By default, the `gtfs` module will store data in local process memory, which is space inefficient (because they are python data structures), and which makes it risky to use multiple threads to access the data due to potential concurrency issues. The full dataset is large, and will
consume hundreds of megabytes of RAM.

For production, it is preferable to configure `gtfs` to store data in redis for the following benefits:
- space efficient
- permits multiple workers
- natively persists data across restarts

A local redis instance can be easily started with docker as follows:
``` bash
docker run --name redis-gtfs -p 127.0.0.1:6379:6379/tcp  -d redis
```

Then, tell gtfs to use that redis instance by passing the `--redis` argument to `server.py` or `gtfs.py`:
```bash
python3 server.py --redis redis://localhost:6379
```

# Memory usage comparison
Running with all stops loaded:
- GTFS-Upcoming used approximately 10Gb of RAM
- TFI-GTFS without redis: ~450Mb
- GTFS with redis: 170Mb
