#!/bin/sh

# Start the first process
redis-server --dir $DATA_DIR &
REDIS_PID=$!

cd /app
cmd="python3 server.py --host 0.0.0.0 --redis redis://localhost:6379 $@"
echo $cmd
$cmd &
PID=$!

clean_up() {
    EXIT_STATUS=$!
    # Remove our trapped signals.
    trap - TERM
    echo "Forwarding signal TERM to redis server $REDIS_PID."
    kill -TERM $REDIS_PID
    echo "Forwarding signal TERM to python server $PID."
    kill -TERM $PID
    echo "Waiting on $PID to exit."
    wait $PID
    echo "Backend server PID $PID stopped with exit code $EXIT_STATUS"
    echo "Shutting down redis"
    redis-cli shutdown
    exit 0
}
trap clean_up TERM
wait $PID
