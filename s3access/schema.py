# -*- coding: utf-8 -*-
import pyarrow as pa


# https://docs.aws.amazon.com/AmazonS3/latest/userguide/LogFormat.html
def create_schema():
    fields = [
        pa.field("bucketowner", pa.string()),
        pa.field("bucket_name", pa.string()),
        pa.field("requestdatetime", pa.string()),
        pa.field("remoteip", pa.string()),
        pa.field("requester", pa.string()),
        pa.field("requestid", pa.string()),
        pa.field("operation", pa.string()),
        pa.field("key", pa.string()),
        pa.field("request_uri", pa.string()),
        pa.field("httpstatus", pa.string()),
        pa.field("errorcode", pa.string()),
        pa.field("bytessent", pa.int64()),
        pa.field("objectsize", pa.int64()),
        pa.field("totaltime", pa.int64()),
        pa.field("turnaroundtime", pa.int64()),
        pa.field("referrer", pa.string()),
        pa.field("useragent", pa.string()),
        pa.field("versionid", pa.string()),
        pa.field("hostid", pa.string()),
        pa.field("sigv", pa.string()),
        pa.field("ciphersuite", pa.string()),
        pa.field("authtype", pa.string()),
        pa.field("endpoint", pa.string()),
        pa.field("tlsversion", pa.string()),
        # New fields derived from data
        pa.field("ts", pa.int64()),
        pa.field("year", pa.int64()),
        pa.field("month", pa.int32()),
        pa.field("day", pa.int32()),
        pa.field("hour", pa.int32()),
        pa.field("minute", pa.int32()),
        pa.field("second", pa.int32()),
        pa.field("datetime", pa.string()),
        pa.field("remoteip_int", pa.uint32()),
        pa.field("is_assumed_role", pa.bool_()),
        pa.field("is_user", pa.bool_()),
    ]
    return pa.schema(fields)
