#!/usr/bin/env python3
import datetime
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

class Consumer(object):

    def __init__(self, debug=False, delay=30):
        self.debug = debug
        self.delay = delay
        self.logger = logging.getLogger(name + '.consumer')
        self.logger.setLevel(logging.DEBUG)

    def start(self):
        self.thread = threading.Thread(target=self.run)
        self.thread.daemon = True
        self.thread.start()

    def join(self):
        self.thread.join()

    def connect_redis(self):
        try:
            self.rdb = redis.StrictRedis(host='localhost', port=6379, db=0)
            self.rdb.ping()
        except redis.exceptions.ConnectionError as err:
            self.logger.error("Unable to connect to Redis: %s", err)
            return False
        return True

    def post_comment(self, submission):
        if self.debug:
            self.logger.debug("Adding comment here")
            return
        try:
            comment = submission.add_comment(transform(submission.url))
        except praw.errors.RateLimitExceeded as err:
            self.logger.debug("Rate limited, comment on %s failed: %s", submission.id, err)
        except praw.errors.APIException as err:
            if err.error_type == 'TOO_OLD':
                self.rdb.set(submission.id, "None");
                self.logger.debug("Cannot post on thread %s, too old", submission.id)
            else:
                raise praw.errors.APIException(err)
        else:
            if comment:
                self.rdb.set(submission.id, comment.id);
                self.logger.info("New comment %s posted on thread %s", comment.id, submission.id)
                time.sleep(self.delay)
            else:
                self.logger.error("Comment on post failed")

    def run(self):
        self.logger = logging.getLogger(name+'.consumer')
        self.logger.setLevel(logging.DEBUG)
        self.logger.info("Starting up consumer thread")
        if not self.connect_redis():
            return
        while True:
            submission = q.get()
            if self.rdb.get(submission.id):
                self.logger.debug("Skipping submission %s: already processed", submission.id)
                continue
            self.post_comment(submission)
            q.task_done()

class Producer(object):

    def __init__(self, reddit, subreddit, length=5000000):
        self.reddit = reddit
        self.subreddit = subreddit
        self.length = length
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
                # TODO: with various match options
                if not re.search('imgur\.com.*\.gif$', x.url):
                    continue
                # if not current, discard
                if not self.current(x):
                    continue
                # if popular or large, process
                if self.popular(x) or self.large(x.url):
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
            #TODO: maybe throw exception here?
            return False
        length = response.getheader("Content-Length")
        if int(length) >= self.length:
            self.logger.debug("Resource is large")
            return True
        return False

    def popular(self, submission):
        if submission.num_comments > 10:
            return True
        if submission.score > 50:
            return True
        return False

    def current(self, submission):
        subtime = datetime.datetime.fromtimestamp(submission.created_utc, datetime.timezone.utc)
        now = datetime.datetime.utcnow().replace(tzinfo = datetime.timezone.utc)
        if (now - subtime) > datetime.timedelta(days=1):
            self.logger.debug("Submission is not current")
            return False
        else:
            self.logger.debug("Submission is current")
            return True
        return True

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
    prod = Producer(reddit, subreddit, cfg['large_length'])
    prod.start()

# prepare and start consumer
cons = Consumer(debug=cfg['debug'], delay=cfg['comment_delay'])
cons.start()

cons.join()

#TODO: GC redis

