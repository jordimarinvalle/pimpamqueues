#!/usr/bin/env python
# -*- coding: utf-8 -*-

import redis

# REDISLABS FREE ACCOUNT
# https://redislabs.com/
REDIS_HOST = 'pub-redis-10356.us-east-1-3.6.ec2.redislabs.com'
REDIS_PORT = '10356'
REDIS_PASSWORD = 'eggbaconspam42'
REDIS_DATABASE = '0'

redis_conn = redis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    password=REDIS_PASSWORD,
    db=REDIS_DATABASE,
)
