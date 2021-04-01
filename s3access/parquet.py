# -*- coding: utf-8 -*-
import os
from multiprocessing import Manager, Pool
import traceback

import pyarrow.parquet as pq
import pyarrow as pa
from pyarrow.util import guid


def write_partition(df, full_path, cols, schema, compression, fs):
    print("write_partition:", full_path, fs)
    try:
        writer = pq.ParquetWriter(
            full_path, schema, compression=compression, filesystem=fs
        )

        for keys, rg in df.groupby([df[col] for col in cols]):
            writer.write_table(pa.Table.from_pandas(rg, schema=schema, safe=False))

        writer.close()
    except Exception as err:
        print(err)
        traceback.print_exc()


# write_to_dataset supports writing row groups
# Copied heavily from https://github.com/apache/arrow/blob/master/python/pyarrow/parquet.py#L1829
def write_dataset(
    table,
    root_path,
    partition_cols=None,
    partition_filename_cb=None,
    row_group_cols=None,
    metadata_collector=None,
    compression=None,
    fs=None,
    cpu_count=None,
    makedirs=False,
    timeout=None,
):

    df = table.to_pandas()

    partition_keys = [df[col] for col in partition_cols]

    data_df = df.drop(partition_cols, axis="columns")

    data_cols = df.columns.drop(partition_cols)

    if len(data_cols) == 0:
        raise ValueError("No data left to save outside partition columns")

    subschema = table.schema

    for col in table.schema.names:
        if col in partition_cols:
            subschema = subschema.remove(subschema.get_field_index(col))

    with Manager() as manager:  # noqa: F841

        with Pool(cpu_count) as pool:

            results = []

            for keys, dfp in data_df.groupby(partition_keys):

                if not isinstance(keys, tuple):
                    keys = (keys,)

                subdir = "/".join(
                    [
                        "{colname}={value}".format(colname=name, value=val)
                        for name, val in zip(partition_cols, keys)
                    ]
                )

                if makedirs:
                    os.makedirs(os.path.join(root_path, subdir), exist_ok=True)

                if partition_filename_cb:
                    outfile = partition_filename_cb(keys)
                else:
                    outfile = guid() + ".parquet"

                full_path = os.path.join(root_path, subdir, outfile)

                result = pool.apply_async(
                    write_partition,
                    args=(dfp, full_path, row_group_cols, subschema, compression, fs),
                )

                results += [result]

            # Wait for all partitions to be written
            for i in range(len(results)):
                try:
                    results[i].get(timeout=timeout)
                except Exception as err:
                    print("Error serializing partition", i, err)
