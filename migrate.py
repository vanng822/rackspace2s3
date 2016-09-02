#!/usr/bin/env python27

import argparse
import os
import io
import threading
import logging
import time
import multiprocessing

import pyrax
import redis
import boto3


logging.basicConfig(
    filename='rackspace2s3.log',
    filemode='a+',
    level=logging.INFO,
    format="%(asctime)s %(levelname)-5.5s [%(name)s] %(message)s")

logger = logging.getLogger('rackspace2s3')


s3_bucket = None  # your bucket name
rackspace_container = None  # your container
rackspace_prefix = None
aws_region = 'eu-west-1'
redis_key = 'ids'
redis_url = '127.0.0.1:6379'
redis_db = 1
id_filename = 'rackspace2s3.txt'  # file


S3_ACCESS_KEY = os.environ.get('S3_ACCESS_KEY')
S3_SECRET_KEY = os.environ.get('S3_SECRET_KEY')
RACKSPACE_IDENTITY_TYPE = os.environ.get('RACKSPACE_IDENTITY_TYPE')
RACKSPACE_USERNAME = os.environ.get('RACKSPACE_USERNAME')
RACKSPACE_API_KEY = os.environ.get('RACKSPACE_API_KEY')


def get_image_data(container, filename):
    image = container.get_object(filename)
    return image.fetch()


def fetch_filenames(container, store_filename, prefix=rackspace_prefix):
    result = container.get_objects(prefix=prefix, full_listing=True)

    fd = open(store_filename, 'w+')
    for r in result:
        fd.write(r.id + '\n')
    fd.close()


def setup_filenames(redis_connection, store_filename):
    fd = open(store_filename, 'r')
    while True:
        image = fd.readline()
        if image == '':
            break
        redis_connection.lpush(redis_key, image.strip())

    fd.close()


def worker_s3(container, redis_connection, stop_signal):
    if not S3_SECRET_KEY or not S3_ACCESS_KEY:
        logger.error('Need to provide s3 credentials')
        return
    session = boto3.session.Session()
    s3 = session.client('s3', aws_region,
                        aws_access_key_id=S3_ACCESS_KEY,
                        aws_secret_access_key=S3_SECRET_KEY)
    while True:
        if stop_signal.isSet():
            logger.info('Worker stop due to stop signal')
            return

        image_name = redis_connection.lpop(redis_key)
        if not image_name:
            logger.info('DONE!')
            return

        try:
            img_data = get_image_data(container, image_name)
            img = io.BytesIO(img_data)
            s3.upload_fileobj(img, s3_bucket, image_name)
            logger.info('uploading: ' + image_name)
        except Exception:
            logger.exception('Failed: ' + image_name)
            redis_connection.rpush(redis_key, image_name)


def threads_alive(threads):
    for t in threads:
        if t.isAlive():
            return True
    return False


def run_s3(container, redis_connection, num_workers):
    threads = []
    logger.info('Start workers: ' + str(num_workers))
    stop_signal = threading.Event()
    for _ in range(num_workers):
        t = threading.Thread(target=worker_s3,
                             args=(container, redis_connection, stop_signal))
        t.start()
        threads.append(t)

    try:
        while threads_alive(threads):
            time.sleep(15)
    except KeyboardInterrupt:
        print 'Sending stop signal'
        logger.info('Sending stop signal')
        stop_signal.set()


def main():
    parser = argparse.ArgumentParser(
        description="Migrate images from rackspace to s3")
    parser.add_argument("-a", "--action",
                        required=True,
                        help="""
                            Action to perform
                                - fetch: fetching all the filenames
                                on rackspace and store it to a file
                                - setup: put filenames from fetch
                                into redis for running
                                - migrate: download file from rackspace
                                and upload to s3
                            """)

    parser.add_argument('-n', '--num_workers',
                        required=False, type=int,
                        default=multiprocessing.cpu_count(),
                        help='Number of workers. Default is number of cpus')

    args = parser.parse_args()

    if not S3_ACCESS_KEY or not S3_SECRET_KEY:
        print("""Please set up environment variable
                 S3_ACCESS_KEY and S3_SECRET_KEY""")
        return

    if (not RACKSPACE_IDENTITY_TYPE or not RACKSPACE_USERNAME or not RACKSPACE_API_KEY):
        print(""" Please set up environment variable
            RACKSPACE_IDENTITY_TYPE
            RACKSPACE_USERNAME
            RACKSPACE_API_KEY
            """)
        return

    if not s3_bucket or not rackspace_container:
        print("Please setup s3_bucket and rackspace_container")
        return

    redis_host, redis_port = redis_url.split(':')
    redis_connection = redis.StrictRedis(host=redis_host,
                                         port=int(redis_port),
                                         db=redis_db)

    if args.action == 'migrate':
        pyrax.set_setting('identity_type', RACKSPACE_IDENTITY_TYPE)
        pyrax.set_credentials(RACKSPACE_USERNAME, RACKSPACE_API_KEY)
        container = pyrax.cloudfiles.get_container(rackspace_container)
        run_s3(container, redis_connection, args.num_workers)
    elif args.action == 'fetch':
        pyrax.set_setting('identity_type', RACKSPACE_IDENTITY_TYPE)
        pyrax.set_credentials(RACKSPACE_USERNAME, RACKSPACE_API_KEY)
        container = pyrax.cloudfiles.get_container(rackspace_container)
        fetch_filenames(container, id_filename)
    elif args.action == 'setup':
        total = redis_connection.llen(redis_key)
        if total > 0:
            raise Exception('Not empty')
        setup_filenames(redis_connection, id_filename)

if __name__ == '__main__':
    main()
