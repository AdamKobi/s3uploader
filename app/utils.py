#!/usr/bin/python2
# Author: Adam Kobi <AdamKobi@Gmail.Com>

import os
import sys
import time
import argparse
import yaml
import logging
from contextlib import contextmanager
from boto.s3.key import Key
from boto.s3.connection import S3Connection, OrdinaryCallingFormat
from boto.exception import S3ResponseError
import pymqi
from functools import wraps


def retry(ExceptionToCheck, tries=5, delay=5, backoff=1, logger=None):
    """Retry calling the decorated function using an exponential backoff.

    http://www.saltycrane.com/blog/2009/11/trying-out-retry-decorator-python/
    original from: http://wiki.python.org/moin/PythonDecoratorLibrary#Retry

    :param ExceptionToCheck: the exception to check. may be a tuple of
        exceptions to check
    :type ExceptionToCheck: Exception or tuple
    :param tries: number of times to try (not retry) before giving up
    :type tries: int
    :param delay: initial delay between retries in seconds
    :type delay: int
    :param backoff: backoff multiplier e.g. value of 2 will double the delay
        each retry
    :type backoff: int
    :param logger: logger to use. If None, print
    :type logger: logging.Logger instance
    """
    def deco_retry(f):

        @wraps(f)
        def f_retry(*args, **kwargs):
            mtries, mdelay = tries, delay
            while mtries > 1:
                try:
                    return f(*args, **kwargs)
                except ExceptionToCheck as e:
                    msg = "%s, Retrying in %d seconds..." % (str(e), mdelay)
                    if logger:
                        logger.warning(msg)
                    else:
                        print (msg)
                    time.sleep(mdelay)
                    mtries -= 1
                    mdelay *= backoff
            return f(*args, **kwargs)

        return f_retry  # true decorator

    return deco_retry

logger = logging.getLogger('uploader')

def init_logging(mod_name):
    """
    To use this, do logger = init_logging(__name__)
    """
    logger = logging.getLogger(mod_name)
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s [%(threadName)-10s] %(levelname)-8s %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    return logger

def init_config():
    ''' Command line argument processing
        Initialize ENV variables if provided,
        Initialize YAML configuration file '''

    parser = argparse.ArgumentParser(
        description='Upload messages from LPR to Scality',
        usage='%(prog)s --debug'
    )
    parser.add_argument('--debug', action='store_true', default=False,
                        help='Debug mode (default: False)')
    parser.add_argument('--max-workers', action='store', type=int, dest='max_workers',
                        help='Specify number of worker threads (default: number of cores)')
    parser.add_argument('--access-key', action='store', type=str, dest='s3_access_key',
                        help='S3 Access Key')
    parser.add_argument('--secret-key', action='store', type=str, dest='s3_secret_key',
                        help='S3 Secret Key')
    args = parser.parse_args()

    ''' Environment variable argument processing '''
    config_vars = ['max_workers', 's3_host', 's3_port', 's3_is_secure', 's3_access_key', 's3_secret_key', 
                   'mq_host', 'mq_port', 'mq_queue_manager', 'mq_channel', 'mq_request_queue', 'mq_replyto_queue', 'debug']

    with open('uploader.yml', 'r') as ymlfile:
        ymlcfg = yaml.load(ymlfile)

    cfg = {}
    for key in config_vars:
        if key.upper() in os.environ:
            cfg[key] = os.environ[key.upper()]
        elif key in vars(args) and vars(args)[key] is not None:
            cfg[key] = vars(args)[key]
        elif key in ymlcfg:
            cfg[key] = ymlcfg[key]
        else:
            logger.error("Missing required paramter [{}]".format(key))
            sys.exit(1)
    return cfg

def init_scality(cfg):
    ''' Initialize connection to Scality'''
    s3_connection = S3Connection(
        host=cfg['s3_host'],
        aws_access_key_id=cfg['s3_access_key'],
        aws_secret_access_key=cfg['s3_secret_key'],
        is_secure=bool(cfg['s3_is_secure']),
        port=int(cfg['s3_port']),
        calling_format=OrdinaryCallingFormat()
    )
    return s3_connection

@retry(pymqi.MQMIError, logger=logger)
def init_message_queue(cfg):
    ''' Initialize IBM message queue'''
    mq_conn_info = "%s(%s)" % (cfg['mq_host'], cfg['mq_port'])

    cd = pymqi.CD()
    cd.ChannelName = cfg['mq_channel']
    cd.ConnectionName = mq_conn_info
    cd.ChannelType = pymqi.CMQC.MQCHT_CLNTCONN
    cd.TransportType = pymqi.CMQC.MQXPT_TCP

    qmgr = pymqi.QueueManager(None)     
    logger.debug("Initiating connection")
    qmgr.connect_with_options(
        cfg['mq_queue_manager'], opts=pymqi.CMQC.MQCNO_HANDLE_SHARE_BLOCK, cd=cd)
    logger.info("Connected to MQ at [{}]".format(cfg['mq_host']))

    return qmgr

def open_queues(cfg,qmgr):
    request_queue = pymqi.Queue(qmgr)
    replyto_queue = pymqi.Queue(qmgr)
    request_queue.open(cfg['mq_request_queue'], pymqi.CMQC.MQOO_INPUT_SHARED)
    replyto_queue.open(cfg['mq_replyto_queue'], pymqi.CMQC.MQOO_OUTPUT)
    logger.info("Connected to request queue: {} and replyto queue {}".format(
        cfg['mq_request_queue'], cfg['mq_replyto_queue']))
        
    return request_queue, replyto_queue

