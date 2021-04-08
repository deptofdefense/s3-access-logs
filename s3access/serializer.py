# -*- coding: utf-8 -*-
import re

# https://stackoverflow.com/questions/7961316/regex-to-split-columns-of-an-amazon-s3-bucket-log
log_regex = re.compile(r'(?:"([^"]+)")|(?:\[([^\]]+)\])|([^ ]+)')


def match_log(line):
    result = log_regex.findall(line.strip())
    return [a or b or c for a, b, c in result]


def deserialize(
    src=None,
    format=None,
    fs=None,
):
    if format == "csv":
        if fs is not None:
            with fs.open(src, "r") as f:
                return [match_log(line) for line in f]
        else:
            with open(src, "r") as f:
                return [match_log(line) for line in f]
    else:
        raise Exception("invalid format " + format)
    return None
