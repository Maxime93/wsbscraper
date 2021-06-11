#!/bin/bash

if [[ ! $(pwd) == */wsbscraper ]]; then
    echo "Run this script from the base hunterx directory with ./script/run.sh"
    exit
fi

# Build the docker container before running
docker build -t wsbscraper .

# Run the docker container and pass through all the arguments
docker run -v /data:/hunterx/data wsbscraper