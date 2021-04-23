# -*- coding: utf-8 -*-

from datetime import datetime
import ipaddress

from s3access.serializer import deserialize


def field_to_int(field):
    """
    Return an integer representation. If a "-" was provided return zero.
    """
    if field == "-":
        return 0
    return int(field)


def transform_item(item):

    #
    # Original record data
    #
    output = {
        "bucketowner": item[0],
        "bucket_name": item[1],
        "requestdatetime": item[2],
        "remoteip": item[3],
        "requester": item[4],
        "requestid": item[5],
        "operation": item[6],
        "key": item[7],
        "request_uri": item[8],
        "httpstatus": item[9],
        "errorcode": item[10],
        "bytessent": field_to_int(item[11]),
        "objectsize": field_to_int(item[12]),
        "totaltime": field_to_int(item[13]),
        "turnaroundtime": field_to_int(item[14]),
        "referrer": item[15],
        "useragent": item[16],
        "versionid": item[17],
        "hostid": item[18],
        "sigv": item[19],
        "ciphersuite": item[20],
        "authtype": item[21],
        "endpoint": item[22],
        "tlsversion": item[23],
    }

    #
    # Timestamp
    #
    ts = datetime.strptime(output["requestdatetime"], "%d/%b/%Y:%H:%M:%S %z")
    # convert timestamp from decimal to int
    output["ts"] = ts.timestamp()
    # parse timestamp
    # add the timestamp keys
    output["year"] = ts.year
    output["month"] = ts.month
    output["day"] = ts.day
    output["hour"] = ts.hour
    output["minute"] = ts.minute
    output["second"] = ts.second
    output["datetime"] = ts.isoformat()

    #
    # IP Address
    #

    output["remoteip_int"] = int(ipaddress.IPv4Address(output["remoteip"]))

    #
    # Assumed Role vs User
    #

    output["is_assumed_role"] = "assumed-role" in output["requester"]
    output["is_user"] = "user" in output["requester"]

    return output


def transform_items(items):
    return [transform_item(item) for item in items]


def deserialize_file(f, fs, logging_queue):
    items = transform_items(deserialize(src=f, format="csv", fs=fs))
    logging_queue.put("Completed deserializing {}".format(f))
    return items
