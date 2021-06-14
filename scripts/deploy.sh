#!/bin/bash

# Move old version to a backup folder in case we need to rollback
mv ~/wsbscraper ~/wsbscraper-backup

# Download latest version from git
git clone git@github.com:Maxime93/wsbscraper.git
