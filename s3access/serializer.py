# -*- coding: utf-8 -*-
import csv


def deserialize(
    src=None,
    format=None,
    fs=None,
):
    if format == "csv":
        if fs is not None:
            with fs.open(src, "r") as f:
                return [row for row in csv.reader(f, delimiter=" ")]
        else:
            with open(src, "r") as f:
                return [row for row in csv.reader(f, delimiter=" ")]
    else:
        raise Exception("invalid format " + format)
    return None
