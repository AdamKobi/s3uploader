#!/usr/bin/python2
# Author: Adam Kobi <adamkobi12@gmail.com>

from .utils import *

logger = init_logging('uploader')
cfg = init_config()

if cfg['debug']:
    logger.setLevel(logging.DEBUG)
    logger.debug("Configuration - '{}'".format(cfg))

logger.info("Initiated")
