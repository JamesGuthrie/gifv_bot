#!/usr/bin/env python3
import http.client
import logging
import praw
import queue
import re
import redis
import threading
import time
import urllib.parse
import yaml

name = "gifv_bot"

def transform(url):
    return url+"v"

def consumer(debug):
    clogger = logging.getLogger(name+'.consumer')
    clogger.setLevel(logging.DEBUG)
    clogger.info("Starting up consumer thread")
    #TODO: handle redis ConnectionError
    rdb = redis.StrictRedis(host='localhost', port=6379, db=0)
    while True:
        submission = q.get()
        if rdb.get(submission.id):
            clogger.debug("Skipping submission %s: already processed", submission.id)
            continue
        clogger.debug(transform(submission.url))
        if debug:
            clogger.debug("Adding comment here")
            continue
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

class Producer(object):

    def __init__(self, reddit, subreddit):
        self.reddit = reddit
        self.subreddit = subreddit
        self.logger = logging.getLogger(name + '.producer.' + self.subreddit)
        self.logger.setLevel(logging.DEBUG)

    def start(self):
        self.thread = threading.Thread(target=self.run)
        self.thread.daemon = True
        self.thread.start()

    def run(self):
        self.logger.info("Starting up producer thread with \"%s\"", self.subreddit)
        while True:
            # get submissions
            submissions = self.reddit.get_subreddit(self.subreddit).get_new(limit=100)
            for x in submissions:
                # filter submissions by gif y/n
                if not re.search('imgur\.com.*\.gif$', x.url):
                    continue
                # TODO: with various match options
                # filter submissions by popularity
                if not self.popular(x):
                    if not self.large(x.url):
                        continue
                self.logger.debug("Adding %s to queue", x.id)
                q.put(x)
            time.sleep(600);

    def large(self, url):
        #TODO: reuse http connections
        pieces = urllib.parse.urlsplit(url)
        conn = http.client.HTTPConnection(pieces.netloc)
        conn.request("HEAD", pieces.path)
        response = conn.getresponse()
        conn.close()
        if response.status != 200:
            if response.status == 302:
                self.logger.debug("Following redirect")
                url = response.getheader("Location")
                return self.large(url)
        length = response.getheader("Content-Length")
        if int(length) >= 5000000:
            self.logger.debug("Resource is large")
            return True
        return False

    def popular(self, submission):
        if submission.num_comments > 10:
            return True
        if submission.score > 50:
            return True
        return False

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
reddit.login(cfg['user'], cfg['password'])

# prepare and start producer
for subreddit in cfg['subreddits']:
    prod = Producer(reddit, subreddit)
    prod.start()

# prepare and start consumer
cons = threading.Thread(target=consumer, args=[cfg['debug']])
cons.daemon = True
cons.start()

cons.join()

