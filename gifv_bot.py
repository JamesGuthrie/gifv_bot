#!/usr/bin/env python3
import configparser
import http.client
import logging
import pprint
import praw
import queue
import re
import redis
import string
import threading
import time
import urllib.parse
import yaml

name = "gifv_bot"

def transform(url):
    return url+"v"

def consumer():
    clogger = logging.getLogger(name+'.consumer')
    clogger.setLevel(logging.DEBUG)
    clogger.info("Starting up consumer thread")
    rdb = redis.StrictRedis(host='localhost', port=6379, db=0)
    while True:
        submission = q.get()
        if rdb.get(submission.id):
            clogger.debug("Skipping submission %s: already processed", submission.id)
            continue
        clogger.debug(transform(submission.url))
        try:
            comment = submission.add_comment(transform(submission.url))
        except praw.errors.RateLimitExceeded as err:
            clogger.debug("Rate limited, comment on %s failed: %s", submission.id, err)
            q.task_done()
        else:
            if comment:
                rdb.set(submission.id, comment.id);
                clogger.debug("New comment %s posted on thread %s", comment.id, submission.id)
                q.task_done()
            else:
                clogger.error("Comment on post failed")

def popular(submission):
    if (submission.num_comments > 10):
        return True
    if (submission.score > 50):
        return True
    return False

def large(url, plogger):
    #TODO: reuse connections
    pieces = urllib.parse.urlsplit(url)
    conn = http.client.HTTPConnection(pieces.netloc)
    conn.request("HEAD", pieces.path)
    response = conn.getresponse()
    conn.close()
    if response.status != 200:
        if response.status == 302:
            plogger.debug("Following redirect")
            url = response.getheader("Location")
            return large(url, plogger)
    length = response.getheader("Content-Length")
    if int(length) >= 5000000:
        plogger.debug("Resource is large")
        return True
    return False

def producer(reddit, subreddit):
    plogger = logging.getLogger(name+'.producer.'+subreddit)
    plogger.setLevel(logging.DEBUG)
    plogger.info("Starting up producer thread with \"%s\"", subreddit)
    while True:
        # get submissions
        submissions = reddit.get_subreddit(subreddit).get_new(limit=100)
        for x in submissions:
            # filter submissions by gif y/n
            if not re.search('imgur\.com.*\.gif$', x.url):
                continue
            # TODO: with various match options
            # filter submissions by popularity
            if not popular(x):
                if not large(x.url, plogger):
                    continue
            plogger.debug("Adding %s to queue", x.id)
            q.put(x)
        time.sleep(600);

def readconfig():
    with open('config.yaml') as f:
        return yaml.load(f)

# prepare logging
FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
logging.basicConfig(format=FORMAT)
logger = logging.getLogger(name)
logger.setLevel(logging.DEBUG)

q = queue.Queue(100)

#TODO: catch errors here
cfg = readconfig()

reddit = praw.Reddit(user_agent=cfg['user_agent'])
reddit.login(cfg['user'],cfg['password'])

# prepare and start producer
for subreddit in cfg['subreddits']:
    prod = threading.Thread(target=producer, args=[reddit, subreddit])
    prod.daemon = True
    prod.start()

# prepare and start consumer
cons = threading.Thread(target=consumer)
cons.daemon = True
cons.start()

cons.join()

