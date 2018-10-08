#!/usr/bin/python2
# Author: Adam Kobi <AdamKobi@Gmail.Com>

import time
import threading
import logging
import sys
from app import cfg
from app.utils import init_scality, init_message_queue
from app.uploader import Uploader, GracefulKiller

logger = logging.getLogger('uploader')


def main():
    killer = GracefulKiller()
    exit_status = 0
    threads = []
    heartbeat_interval = 15
    max_workers = int(cfg['max_workers'])

    def init_thread():
        thread = Uploader(cfg)
        thread.start()
        threads.append(thread)
        time.sleep(0.25)
    
    def close_threads():
        for thread in threads:
            # Terminate the running threads.
            thread.kill_self()
        logger.info("Closed all threads exiting now.")

    for _ in range(max_workers):
        init_thread()

    while not killer.kill_now:
        for thread in threads:
            if not thread.is_alive():
                logger.error("{} heartbeat failed!".format(thread.name))
                killer.kill_now = True
                exit_status = 1
                break
            logger.info("{} heartbeat success.".format(thread.name))
        time.sleep(heartbeat_interval)
    close_threads()
    sys.exit(exit_status)

if __name__ == "__main__":
    main()