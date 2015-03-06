#!/usr/bin/env python
# -*- coding: utf-8 -*-

import redis

# REDISLABS FREE ACCOUNT
# https://redislabs.com/
REDIS_HOST = 'pub-redis-12294.us-east-1-1.1.ec2.garantiadata.com'
REDIS_PORT = '12294'
REDIS_PASSWORD = 'eggbaconspam42'
REDIS_DATABASE = '0'

redis_conn = redis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    password=REDIS_PASSWORD,
    db=REDIS_DATABASE,
)
