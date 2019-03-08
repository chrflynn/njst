"""Utility functions for data ingest."""
import datetime
import io
import json
import logging
import os
import zipfile

import boto3


def load_json_records_from_file(path):
    """Load records from a file into a list of dictionaries."""
    records = []
    if isinstance(path, io.BytesIO):
        lines = path.getvalue().decode("utf-8").split("\n")
    elif isinstance(path, io.StringIO):
        lines = path.getvalue().split("\n")
    else:
        with open(path, "r") as f:
            lines = f.readlines()

    for line in lines:
        # ignore empty lines
        if line == "\n" or line == "":
            continue
        record = json.loads(line)
        records.append(record)

    return records


def epoch_ms_to_timestamp(epoch):
    """Convert an epoch milliseconds to a timestamp string."""
    dt = datetime.datetime.fromtimestamp(epoch / 1000)
    return dt.strftime("%m-%d-%YT%H:%M:%S")


def load_file_from_s3(path, bucket=None):
    """Fetch a file from s3. If no bucket is provided load from fs."""
    if bucket is None:
        return load_file_from_disk(path)

    s3 = boto3.resource("s3")
    obj = s3.Object(bucket, path)
    val = obj.get()["Body"].read().decode("utf-8")

    buffer = io.BytesIO(val)
    buffer.seek(0)

    return buffer


def save_file_to_s3(buffer, path, bucket=None):
    if bucket is None:
        return save_file_to_disk(buffer, path)

    if isinstance(buffer, io.StringIO):
        body = buffer.getvalue()
    else:
        body = buffer.getvalue().encode("utf-8")

    s3 = boto3.client("s3")
    s3.Bucket(bucket).put_object(Key=path, Body=body)


def load_file_from_disk(path):
    """Return the contents of a file from disk in a buffer."""
    buffer = io.BytesIO()
    with open(path, "rb") as f:
        buffer.write(f.read())

    buffer.seek(0)

    return buffer


def save_file_to_disk(buffer, path):
    """Save the contents of a file to disk"""
    logging.info(path)
    opts = "wb"
    if isinstance(buffer, io.StringIO):
        opts = "w"
    if not os.path.exists(os.path.dirname(path)):
        os.makedirs(os.path.dirname(path))
    with open(path, opts) as f:
        f.write(buffer.getvalue())


def unzip_files(buffer):
    """Return the contents of a zip file."""
    results = {}
    with zipfile.ZipFile(buffer) as z:
        for filename in z.namelist():
            unzipped_buffer = io.BytesIO()
            unzipped_buffer.write(z.read(filename))
            unzipped_buffer.seek(0)
            results.update({filename: unzipped_buffer})
    return results


def print_sample_records(prefix, dt="20160401"):
    """Show the records from a file."""
    records = load_json_records_from_file(f"export/{prefix}.{dt}.json")
    for record in records:
        print(json.dumps(record, indent=4))


if __name__ == "__main__":
    print_sample_records("profile")
