#!/usr/bin/env bash

subreddit-archiver update --file combatfootage.sqlite --credentials credentials.config

python3 cf_update.py >> log.txt