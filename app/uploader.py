#!/usr/bin/python2
# Author: Adam Kobi <AdamKobi@Gmail.Com>

import os
import signal
import logging
import time
import threading
from string import Template
import xml.etree.ElementTree as ET
import base64
import pymqi
from .utils import init_scality, init_message_queue, open_queues, retry
from boto.s3.key import Key
from boto.exception import S3ResponseError

logger = logging.getLogger('uploader')

class Uploader(threading.Thread):
    def __init__(self, cfg):
        super(Uploader, self).__init__()
        self.daemon = True
        self.stop_request = threading.Event()
        self.cfg = cfg
        self.should_break = False

        # Init Scality S3
        self.s3_connection = init_scality(self.cfg)

        # Init IBM MQ
        self.qmgr = init_message_queue(self.cfg)
        self.request_queue, self.replyto_queue = open_queues(self.cfg,self.qmgr)

    def kill_self(self):
        self.stop_request.set()
        self.join()

    @retry(pymqi.MQMIError, logger=logger)
    def get_message(self):
        # Message Descriptor
        request_md = pymqi.MD()

        # Get Message Options
        gmo = pymqi.GMO()
        gmo.Options = pymqi.CMQC.MQGMO_FAIL_IF_QUIESCING
        start = time.time()
        message = {}

        try:
            request_message = ET.fromstring(self.request_queue.get(None, request_md, gmo))
            message['req_guid'] = request_message.find('Request_GUID').text
            message['filename'] = request_message.find('FileName').text
            message['bucket'] = request_message.find('BucketName').text
            message['data'] = request_message.find('FileContent').text
            end = time.time()
            logger.debug("Retrieving message from queue took: {}".format(end - start))

        except pymqi.MQMIError as e:
            message = None
            if e.comp == pymqi.CMQC.MQCC_FAILED and e.reason == pymqi.CMQC.MQRC_NO_MSG_AVAILABLE:
                # No messages, that's OK, we can ignore it.
                logger.debug('No messages in queue, retrying in 0.5 second...')
                time.sleep(0.5)
            else:
                raise

        return message, request_md

    
    def upload(self, message):

        response_message = {}
        response_message['req_guid'] = message['req_guid']
        response_message['filename'] = message['filename']
            
        s3_bucket = self.s3_connection.lookup(message['bucket'])
        if message['data'] is None or message['req_guid'] is None:
            response_message['status_code'] = "ERROR"
            response_message['error_desc'] = "Received an empty message! request GUID: {}".format(message['req_guid'])
            logger.error(response_message['error_desc'])
        elif not s3_bucket:
            self.should_break = True
            response_message['status_code'] = "ERROR"
            response_message['error_desc'] = "Unable to find bucket: [{}] or Host: [{}] not available.".format(
                message['bucket'], self.cfg['s3_host'])
            logger.error(response_message['error_desc'])
        else:
            key = s3_bucket.get_key(message['req_guid'])
            if not key or not key.exists():
                response_message['status_code'] = "OK"
                response_message['error_desc'] = "Uploaded"
                logger.debug(response_message['error_desc'])
            else:
                response_message['status_code'] = "OK"
                response_message['error_desc'] = "Exists already, overwriting"
                logger.error(response_message['error_desc'])

            key = Key(s3_bucket)
            key.key = message['req_guid']
            key.set_metadata('filename', message['filename'])
            start = time.time()
            key.set_contents_from_string(base64.b64decode(message['data']))
            key.set_acl('public-read')
            end = time.time()
            logger.debug('Uploaded {}. Upload took: {}'.format(message['req_guid'], end - start))

        return response_message
    
    @retry(pymqi.MQMIError, logger=logger)
    def put_message(self, response_message, request_md):

        response_md = pymqi.MD()
        response_md.CorrelId = request_md.MsgId

        response_message_template = ("<Response>\n"
                                     "<Request_GUID>{req_guid}</Request_GUID>\n"
                                     "<Status>{status}</Status>\n"
                                     "<Filename>{filename}</Filename>\n"
                                     "<ErrorDescription>{error_desc}</ErrorDescription>\n"
                                     "</Response>").format(req_guid=response_message['req_guid'], status=response_message['status_code'], 
                                                           filename=response_message['filename'], error_desc=response_message['error_desc'])

        self.replyto_queue.put(response_message_template, response_md)

    def run(self):

        while not self.stop_request.is_set():
            message, request_md = self.get_message()
            if message is not None:
                response_message = self.upload(message)
                self.put_message(response_message, request_md)
                if self.should_break:
                   break
        self.request_queue.close()
        self.replyto_queue.close()
        logger.info("Stopped")


class GracefulKiller:
  kill_now = False
  def __init__(self):
    signal.signal(signal.SIGINT, self.exit_gracefully)
    signal.signal(signal.SIGTERM, self.exit_gracefully)

  def exit_gracefully(self,signum, frame):
    logger.debug("Caught signal {}".format(signum))
    self.kill_now = True
