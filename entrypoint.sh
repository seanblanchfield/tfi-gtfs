#!/bin/sh

# Start the first process
redis-server --daemonize yes

cd /app
cmd="python3 server.py --host 0.0.0.0 --redis redis://localhost:6379 $@"
echo $cmd
$cmd
