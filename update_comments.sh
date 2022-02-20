#!/usr/bin/env bash

timeout 5m subreddit-archiver archive --subreddit combatfootage --file cf_update.sqlite --credentials credentials.config

python3 update_comments.py >> cf_comment_update.txt

rm cf_update.sqlite