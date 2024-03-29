#! /usr/bin/env python3
# -*- coding: utf-8 -*-

from datetime import datetime, timedelta
import gc
import logging
from multiprocessing import get_context
import os
from pathlib import Path
import queue
import sys
import traceback
from urllib.parse import urlparse
import uuid

import pandas as pd
import pyarrow as pa
import pytz
import s3fs

from s3access.normalize import deserialize_file
from s3access.parquet import write_dataset
from s3access.schema import create_schema
from s3access.wg import WaitGroup


def parse_time(object_name):
    return datetime.strptime(object_name[0:19], "%Y-%m-%d-%H-%M-%S")


def create_files_index(src, hour, timezone, fs):
    """
    :param str src: The filesystem, s3 or local
    :param str hour: The hour being targetted in the format YYYY-MM-DD-HH
    :param str timezone: The timezone from pytz
    :param str fs:  The filesystem
    :return: A Data Frame with the set of files including path and datetime
    """

    files = []
    if src.startswith("s3://"):
        u = urlparse(src)
        files = [
            {"path": f, "dt": timezone.localize(parse_time(os.path.basename(f)))}
            for f in fs.glob(("{}{}/{}*").format(u.netloc, u.path, hour))
        ]
    else:
        files = [
            {
                "path": f.as_posix(),
                "dt": timezone.localize(parse_time(os.path.basename(f))),
            }
            for f in Path(src).rglob("*")
        ]

    return pd.DataFrame(files)


def create_file_system(root, endpoint_url, endpoint_region, s3_acl, logger):
    logger.info("Creating filesystem for {}".format(root))
    if root.startswith("s3://"):
        return s3fs.S3FileSystem(
            anon=False,
            client_kwargs={
                "endpoint_url": endpoint_url,
                "region_name": endpoint_region,
                "use_ssl": True,
            },
            s3_additional_kwargs={
                "ACL": s3_acl,
            },
        )
    else:
        os.makedirs(root, exist_ok=True)

    return None


def aggregate_range(
    ctx,
    src,
    dst,
    files,
    timezone,
    logger,
    schema,
    input_file_system,
    output_file_system,
    tracking_file_system,
    tracking_dst,
    hour,
    cpu_count,
    timeout,
    logging_queue,
):

    items = []

    logger.info("Deserializing data in files from {}".format(src))

    with ctx.Pool(processes=int(cpu_count)) as pool:

        wg = WaitGroup()

        def deserialize_file_callback(outputs):
            items.extend(outputs)
            wg.done()

        def deserialize_file_error_callback(err):
            traceback.print_exc()
            raise err

        for f in files.itertuples():
            wg.add(1)
            pool.apply_async(
                deserialize_file,
                args=(f.path, input_file_system, logging_queue),
                callback=deserialize_file_callback,
                error_callback=deserialize_file_error_callback,
            )

        logger.info("Waiting for deserialization to complete")

        wg.wait(timeout=timeout)

    logger.info("Deserialization data in files complete")

    if len(items) == 0:
        logger.info("No items found in filesystem")
        return

    gc.collect()

    df = pd.DataFrame(items)

    logger.info("Serializing {} items to {}".format(len(items), dst))

    # Drop the memory footprint and garbage collect
    items = []
    gc.collect()

    write_dataset(
        pa.Table.from_pandas(df, schema=schema, preserve_index=False),
        dst,
        compression="SNAPPY",
        partition_cols=["bucket_name", "operation", "year", "month", "day", "hour"],
        partition_filename_cb=lambda x: "-".join([str(y) for y in x]) + ".parquet",
        row_group_cols=["requester", "remoteip_int", "is_assumed_role", "is_user"],
        fs=output_file_system,
        cpu_count=cpu_count,
        makedirs=(not dst.startswith("s3://")),
        timeout=timeout,
        logging_queue=logging_queue,
    )

    logger.info("Serializing items to {} is complete".format(dst))

    if tracking_file_system is not None:
        logger.info("Tracking completion of task")
        tracking_file = "{}{}".format(tracking_dst, hour)
        tracking_file_system.touch(tracking_file)
        with s3fs.S3File(tracking_file_system, tracking_file, mode="wb") as f:
            f.write(
                bytearray(
                    "Completed hour {}. Now: {}\n".format(hour, datetime.now()), "utf-8"
                )
            )
        logger.info("Successful creation file: {}!".format(tracking_file))


def configure_logging():
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    logger.addHandler(ch)

    return logger


def logging_process(logging_queue):
    logger = configure_logging()
    while True:
        try:
            # Don't add a timeout here, it just adds log noise
            record = logging_queue.get(True)
            # We send 'None' as a sentinel to tell the listener to quit looping.
            # At the same time tell the logging_queue that no more data is coming.
            if record is None:
                break
            logger.info(record)
        except queue.Empty:
            print("Queue is empty, killing logging process")
            break
        except (ValueError, EOFError):
            print("Queue is closed, killing logging process")
            break
        except Exception:
            print("Queue is broken")
            traceback.print_exc()
            break


def main():

    #
    # CPU Count
    #

    cpu_count = os.cpu_count()

    #
    # Now
    #

    utc = pytz.timezone("UTC")
    now = datetime.now(utc)

    #
    # Setup Logging
    #

    logger = configure_logging()

    ctx = get_context("spawn")
    logging_queue = ctx.Manager().Queue(-1)
    listener = ctx.Process(target=logging_process, args=(logging_queue,))
    listener.start()

    #
    # Settings
    #

    src = os.getenv("SRC")
    dst = os.getenv("DST")
    tracking_dst = os.getenv("TRACKING_DST")

    # Default is to look at the previous hour with the assumption that all the logs exist from that period
    # Importantly this makes it easier to trigger on a cron job and know that the appropriate files are being found
    default_hour = (now - timedelta(hours=1)).strftime("%Y-%m-%d-%H")
    hour = os.getenv("HOUR", default_hour)

    s3_default_region = os.getenv("AWS_REGION")

    input_s3_acl = os.getenv("INPUT_S3_ACL", "bucket-owner-full-control")
    input_s3_region = os.getenv("INPUT_S3_REGION", s3_default_region)
    input_s3_endpoint = os.getenv(
        "OUTPUT_S3_ENDPOINT",
        "https://s3-fips.{}.amazonaws.com".format(input_s3_region),
    )

    output_s3_acl = os.getenv("OUTPUT_S3_ACL", "bucket-owner-full-control")
    output_s3_region = os.getenv("OUTPUT_S3_REGION", s3_default_region)
    output_s3_endpoint = os.getenv(
        "OUTPUT_S3_ENDPOINT",
        "https://s3-fips.{}.amazonaws.com".format(output_s3_region),
    )

    timeout = int(os.getenv("TIMEOUT", "300"))

    logger.info("now:          {}".format(now))
    logger.info("cpu_count:    {}".format(cpu_count))
    logger.info("src:          {}".format(src))
    logger.info("dst:          {}".format(dst))
    logger.info("tracking_dst: {}".format(tracking_dst))
    logger.info("hour:         {}".format(hour))
    logger.info("timeout:      {}".format(timeout))
    logger.info("aws-region:   {}".format(s3_default_region))
    logger.info("input_s3_acl:       {}".format(input_s3_acl))
    logger.info("input_s3_region:    {}".format(input_s3_region))
    logger.info("input_s3_endpoint:  {}".format(input_s3_endpoint))
    logger.info("output_s3_acl:      {}".format(output_s3_acl))
    logger.info("output_s3_region:   {}".format(output_s3_region))
    logger.info("output_s3_endpoint: {}".format(output_s3_endpoint))

    if src is None or len(src) == 0:
        logger.error("{} is missing".format("src"))
        graceful_shutdown(listener, logging_queue, 1)

    if dst is None or len(dst) == 0:
        logger.error("{} is missing".format("dst"))
        graceful_shutdown(listener, logging_queue, 1)

    if src[len(src) - 1] != "/":
        src = src + "/"

    if dst[len(dst) - 1] != "/":
        dst = dst + "/"

    if tracking_dst is not None:
        if len(tracking_dst) > 0 and tracking_dst[len(tracking_dst) - 1] != "/":
            tracking_dst = tracking_dst + "/"

    #
    # Initialize File Systems
    #

    input_file_system = create_file_system(
        src, input_s3_endpoint, input_s3_region, input_s3_acl, logger
    )
    output_file_system = create_file_system(
        dst, output_s3_endpoint, output_s3_region, output_s3_acl, logger
    )
    tracking_file_system = None
    if tracking_dst is not None:
        if len(tracking_dst) > 0:
            tracking_file_system = create_file_system(
                tracking_dst,
                output_s3_endpoint,
                output_s3_region,
                output_s3_acl,
                logger,
            )

    #
    # Check if this task has been completed already
    #

    if tracking_file_system is not None:
        logger.info("Checking completion of task for hour: {}".format(hour))
        tracking_file = "{}{}".format(tracking_dst, hour)
        if tracking_file_system.exists(tracking_file):
            logger.info("Task completed for hour: {}!".format(tracking_file))
            graceful_shutdown(listener, logging_queue, 0)

    #
    # Load Schema
    #

    schema = create_schema()

    all_files = create_files_index(
        src,
        hour,
        utc,
        input_file_system,
    )

    if len(all_files) == 0:
        logger.info("no source files found within folder {}".format(src))
        graceful_shutdown(listener, logging_queue, 0)

    logger.info("List all files:")
    logger.info(all_files)

    # Test getting a file from the index and reading it
    if input_file_system is not None:
        logger.info("Test input filesystem")
        read_test = all_files.iloc[0]["path"]
        if input_file_system.exists(read_test):
            logger.info(read_test)
            with input_file_system.open(read_test) as f:
                line_count = 0
                for line in f:
                    line_count += 1
                logger.info("Lines in first file: {}".format(line_count))
                logger.info("Read test success!")
        else:
            logger.error("Unable to prove file {} exists".format(read_test))
            graceful_shutdown(listener, logging_queue, 1)

    if output_file_system is not None:
        logger.info("Test output filesystem")
        write_test = "{}{}".format(dst, uuid.uuid4())
        output_file_system.touch(write_test)
        logger.info("Successful create file: {}!".format(write_test))
        with s3fs.S3File(output_file_system, write_test, mode="wb") as f:
            f.write(
                bytearray(
                    "test for {}. Now: {}\n".format(hour, datetime.now()), "utf-8"
                )
            )
            logger.info("Successful write for file: {}!".format(write_test))
        output_file_system.rm(write_test)
        logger.info("Successfully deleted file: {}".format(write_test))
        logger.info("Write test success for file {}!".format(write_test))

    # The bulk of the work happens here
    aggregate_range(
        ctx,
        src,
        dst,
        all_files,
        utc,
        logger,
        schema,
        input_file_system,
        output_file_system,
        tracking_file_system,
        tracking_dst,
        hour,
        cpu_count,
        timeout,
        logging_queue,
    )

    graceful_shutdown(listener, logging_queue, 0)


def graceful_shutdown(listener, logging_queue, exit_code):

    # Put one last record on the logging_queue to kill it and then wait
    logging_queue.put_nowait(None)

    # Now disable the listener
    listener.join()
    listener.close()

    # Call an exit
    sys.exit(exit_code)


if __name__ == "__main__":
    try:
        main()
    except Exception:
        traceback.print_exc()
        sys.exit(1)
