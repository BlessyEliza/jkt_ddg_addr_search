#!/bin/bash

# Function to start the python process
start_process() {
    # Start python noproxy.py in the background and redirect stdout and stderr
    python3 noproxy.py > noproxy.log 2>&1 &
    PID=$!
    echo "Started process with PID: $PID"
}

# Start the initial process
start_process

# Wait for an initial period of 30 seconds before starting to check
echo "Waiting for an initial 30 seconds before starting checks..."
sleep 60

# Run perpetually
while true; do
    # Get current time and last modification time of output.csv
    current_time=$(date +%s)
    last_mod_time=$(stat -c %Y output.csv)

    # Calculate the difference in seconds
    diff=$((current_time - last_mod_time))

    # Check if the log file has been modified in the last 30 seconds
    if [ $diff -gt 60 ]; then
        echo "Log file hasn't changed in the last 30 seconds. Restarting the process."
        kill $PID
        wait $PID 2>/dev/null
        start_process
        # Wait for an initial period of 30 seconds before starting to check
        echo "Waiting for an initial 30 seconds before starting checks..."
        sleep 60
    else
        echo "Log file is active. Continuing."
    fi

    sleep 10
done
