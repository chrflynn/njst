"""Ingestion tasks for Sailthru."""
import copy
import io
import json
import logging
import os

import util
import redshift


# S3 config

PROCESSED_PATH = "processed/sailthru/{table_name}/{y}/{m}/{d}/{filename}"
BUCKET = None

# Redshift config

# RS = redshift.Redshift(
#     {
#         "dbname": os.environ["REDSHIFT_DBNAME"],
#         "user": os.environ["REDSHIFT_USER"],
#         "password": os.environ["REDSHIFT_PASSWORD"],
#         "host": os.environ["REDSHIFT_HOST"],
#         "port": os.environ["REDSHIFT_PORT"],
#     }
# )
AWS_ACCOUNT_ID = os.environ.get("AWS_ACCOUNT_ID", 1234)
AWS_REDSHIFT_ROLE = os.environ.get("AWS_REDSHIFT_ROLE", "role")

SCHEMA = "sailthru"

COPY_QUERY = """
COPY {table_name}
FROM 's3://{bucket}/{s3_path}'
iam_role 'arn:aws:iam::{aws_account_id}:role/{aws_redshift_role}'
format as json 'auto';
"""


def ingest_sailthru_data_to_redshift(dt="2016-04-01"):
    """Import sailthru blast data into redshift in a single transaction."""
    logging.info(dt)

    export = util.load_file_from_s3(
        path="export/sailthru_data-exporter_samples.zip", bucket=BUCKET
    )

    files = util.unzip_files(export)

    logging.info("Files found: " + ", ".join(list(files.keys())))

    for filename, contents in files.items():
        prefix = filename.split(".")[0]
        if prefix == "blast":
            process_sailthru_blast_data(filename, contents, dt)
        elif prefix == "message_blast":
            process_sailthru_message_blast_data(filename, contents, dt)
        elif prefix == "message_transactional":
            process_sailthru_message_transactional_data(filename, contents, dt)
        elif prefix == "profile":
            process_sailthru_profile_data(filename, contents, dt)
        else:
            # Warn about mac zip artifacts and unexpected files
            logging.warning("Unknown file: " + filename)

    # # Treat this entire process as a single transaction and commit on success.
    # RS.commit()


def process_sailthru_blast_data(filename, contents, dt):
    """Process the sailthru blast payload."""
    logging.info("Processing blast data")
    y, m, d = dt.split("-")

    table_name = SCHEMA + ".blast"
    s3_path = PROCESSED_PATH.format(
        table_name="blast", y=y, m=m, d=d, filename=filename
    )

    records = util.load_json_records_from_file(contents)

    buffer = io.StringIO()
    # No transformation, just rewrite the records cleanly
    for record in records:
        buffer.write(json.dumps(record))
        buffer.write("\n")

    util.save_file_to_s3(buffer, s3_path, bucket=BUCKET)

    query = COPY_QUERY.format(
        table_name=table_name,
        bucket=BUCKET,
        s3_path=s3_path,
        aws_account_id=AWS_ACCOUNT_ID,
        aws_redshift_role=AWS_REDSHIFT_ROLE,
    )
    logging.info(f"Importing data into Redshift: {table_name}")
    # RS.execute(query)


def process_sailthru_message_blast_data(filename, contents, dt):
    """Process the sailthru message_blast payload."""
    logging.info("Processing message blast data")

    records = util.load_json_records_from_file(contents)

    buffer = io.StringIO()

    for record in records:
        # Extract the clicks and opens fields
        clicks = record.pop("clicks", [])
        opens = record.pop("opens", [])

        # Create a record for each open
        for entry in opens:
            sub_record = copy.deepcopy(record)
            sub_record["action_name"] = "open"
            sub_record["action_ts"] = util.epoch_ms_to_timestamp(entry["ts"]["$date"])
            buffer.write(json.dumps(sub_record))
            buffer.write("\n")

        # Create a record for each click
        for entry in clicks:
            sub_record = copy.deepcopy(record)
            sub_record["action_name"] = "click"
            sub_record["action_url"] = entry["url"]
            sub_record["action_ts"] = util.epoch_ms_to_timestamp(entry["ts"]["$date"])
            buffer.write(json.dumps(sub_record))
            buffer.write("\n")

        # Create an empty record for the case of no opens and clicks
        if len(clicks) == 0 and len(opens) == 0:
            buffer.write(json.dumps(record))
            buffer.write("\n")

    logging.info(buffer.getvalue()[:1000] + "...")

    y, m, d = dt.split("-")
    s3_path = PROCESSED_PATH.format(
        table_name="message_blast", y=y, m=m, d=d, filename=filename
    )
    util.save_file_to_s3(buffer, s3_path, bucket=BUCKET)

    table_name = SCHEMA + ".message_blast"
    query = COPY_QUERY.format(
        table_name=table_name,
        bucket=BUCKET,
        s3_path=s3_path,
        aws_account_id=AWS_ACCOUNT_ID,
        aws_redshift_role=AWS_REDSHIFT_ROLE,
    )
    logging.info(f"Importing data into Redshift: {table_name}")
    # RS.execute(query)


def process_sailthru_message_transactional_data(filename, contents, dt):
    logging.info("Processing message transactional data")

    records = util.load_json_records_from_file(contents)

    buffer = io.StringIO()

    for record in records:
        # Extract the clicks and opens fields
        clicks = record.pop("clicks", [])
        opens = record.pop("opens", [])

        # Create a record for each open
        for entry in opens:
            sub_record = copy.deepcopy(record)
            sub_record["action_name"] = "open"
            sub_record["action_ts"] = util.epoch_ms_to_timestamp(entry["ts"]["$date"])
            buffer.write(json.dumps(sub_record))
            buffer.write("\n")

        # Create a record for each click
        for entry in clicks:
            sub_record = copy.deepcopy(record)
            sub_record["action_name"] = "click"
            sub_record["action_url"] = entry["url"]
            sub_record["action_ts"] = util.epoch_ms_to_timestamp(entry["ts"]["$date"])
            buffer.write(json.dumps(sub_record))
            buffer.write("\n")

        # Create an empty record for the case of no opens and clicks
        if len(clicks) == 0 and len(opens) == 0:
            buffer.write(json.dumps(record))
            buffer.write("\n")

    logging.info(buffer.getvalue()[:1000] + "...")

    y, m, d = dt.split("-")
    s3_path = PROCESSED_PATH.format(
        table_name="message_transactional", y=y, m=m, d=d, filename=filename
    )
    util.save_file_to_s3(buffer, s3_path, bucket=BUCKET)

    table_name = SCHEMA + ".message_transactional"
    query = COPY_QUERY.format(
        table_name=table_name,
        bucket=BUCKET,
        s3_path=s3_path,
        aws_account_id=AWS_ACCOUNT_ID,
        aws_redshift_role=AWS_REDSHIFT_ROLE,
    )
    logging.info(f"Importing data into Redshift: {table_name}")
    # RS.execute(query)


def process_sailthru_profile_data(filename, contents, dt):
    logging.info("Processing profile data")

    records = util.load_json_records_from_file(contents)

    # profile table buffers
    profile_buffer = io.StringIO()
    profile_geo_buffer = io.StringIO()
    profile_browser_buffer = io.StringIO()
    profile_lists_signup_buffer = io.StringIO()
    profile_vars_buffer = io.StringIO()

    for record in records:

        # pop off the data that go into separate tables
        geo = record.pop("geo", {})
        browser = record.pop("browser", {})
        lists_signup = record.pop("lists_signup", {})
        vars_ = record.pop("vars", {})

        # id is required for joins so extract it from parent record
        profile_id = record["id"]

        # profile table
        record["geo_count"] = geo.pop("count", 0)
        profile_buffer.write(json.dumps(record))
        profile_buffer.write("\n")

        # separate profile tables
        for scope, entries in geo.items():
            geo_record = {"scope": scope, "id": profile_id}
            for label, value in entries.items():
                geo_record["label"] = label
                geo_record["value"] = value
                profile_geo_buffer.write(json.dumps(geo_record))
                profile_geo_buffer.write("\n")

        for browse, quantity in browser.items():
            browser_record = {"id": profile_id, "browser": browse, "quantity": quantity}
            profile_browser_buffer.write(json.dumps(browser_record))
            profile_browser_buffer.write("\n")

        for lst, ts in lists_signup.items():
            lst_record = {"id": profile_id, "list": lst, "ts": ts}
            profile_lists_signup_buffer.write(json.dumps(lst_record))
            profile_lists_signup_buffer.write("\n")

        for var, val in vars_.items():
            var_record = {
                "id": profile_id,
                "var": var,
                "val": str(val)
                if val is not None
                else val,  # different types for these in raw
            }
            profile_vars_buffer.write(json.dumps(var_record))
            profile_vars_buffer.write("\n")

    # map table names to buffers
    tables = {
        "profile": profile_buffer,
        "profile_lists_signup": profile_lists_signup_buffer,
        "profile_geo": profile_geo_buffer,
        "profile_browser": profile_browser_buffer,
        "profile_vars": profile_vars_buffer,
    }

    y, m, d = dt.split("-")

    # write and redshift copy each table
    for table, buffer in tables.items():
        s3_path = PROCESSED_PATH.format(
            table_name=table, y=y, m=m, d=d, filename=filename
        )
        logging.info(buffer.getvalue()[:1000] + "...")

        util.save_file_to_s3(buffer, s3_path, bucket=BUCKET)

        table_name = SCHEMA + "." + table
        query = COPY_QUERY.format(
            table_name=table_name,
            bucket=BUCKET,
            s3_path=s3_path,
            aws_account_id=AWS_ACCOUNT_ID,
            aws_redshift_role=AWS_REDSHIFT_ROLE,
        )
        logging.info(f"Importing data into Redshift: {table_name}")
        # RS.execute(query)


if __name__ == "__main__":
    # Expose the logs at runtime
    logging.basicConfig(level=logging.INFO)
    # Run
    ingest_sailthru_data_to_redshift()
