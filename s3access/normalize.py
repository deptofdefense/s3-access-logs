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
        "requestdatetime": " ".join([item[2], item[3]]),
        "remoteip": item[4],
        "requester": item[5],
        "requestid": item[6],
        "operation": item[7],
        "key": item[8],
        "request_uri": item[9],
        "httpstatus": item[10],
        "errorcode": item[11],
        "bytessent": field_to_int(item[12]),
        "objectsize": field_to_int(item[13]),
        "totaltime": field_to_int(item[14]),
        "turnaroundtime": field_to_int(item[15]),
        "referrer": item[16],
        "useragent": item[17],
        "versionid": item[18],
        "hostid": item[19],
        "sigv": item[20],
        "ciphersuite": item[21],
        "authtype": item[22],
        "endpoint": item[23],
        "tlsversion": item[24],
    }

    #
    # Timestamp
    #

    ts = datetime.strptime(output["requestdatetime"][1:-1], "%d/%b/%Y:%H:%M:%S %z")
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


def deserialize_file(f, fs):
    return transform_items(deserialize(src=f, format="csv", fs=fs))
