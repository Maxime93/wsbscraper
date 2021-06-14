#!/bin/bash

if [[ ! $(pwd) == */wsbscraper ]]; then
    echo "Run this script from the base hunterx directory with ./script/run.sh"
    exit
fi

DEPLOY=$1

if [ -n "$DEPLOY" ]; then
    echo "Running on $DEPLOY"
    if [ $DEPLOY == "raspberry" ]; then
        # running on raspberry
        conda activate wsbscraper
        python runner.py
    # else
        # Build the docker container before running
        # docker build -t wsbscraper .
        # Run the docker container and pass through all the arguments
        # docker run -v /home/pi/wsbscraper/data:/wsbscraper/data wsbscraper
    fi
else
    echo "Empty Argument."
fi
