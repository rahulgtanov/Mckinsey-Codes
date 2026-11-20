import json
import os
import urllib.parse

import boto3
from botocore.config import Config

# Name of the Glue job to start. Injected by Terraform (see lambda.tf) so the
# same code works across dev / test / prod without editing the source.
GLUE_JOB_NAME = os.environ["GLUE_JOB_NAME"]

BOTO_CONFIG = Config(connect_timeout=5, read_timeout=10)
glue = boto3.client("glue", config=BOTO_CONFIG)


def lambda_handler(event, context):
    """Start the STAFFING_DEPLOYMENT weekly-load Glue job on an S3 upload.

    The Glue job discovers the latest CSV under its own ``source_prefix``
    (``incoming/``) via ``find_latest_csv``, so we do NOT need to pass the
    uploaded object key -- simply kicking off the job is enough.
    """
    for record in event.get("Records", []):
        bucket = record.get("s3", {}).get("bucket", {}).get("name")
        raw_key = record.get("s3", {}).get("object", {}).get("key", "")
        key = urllib.parse.unquote_plus(raw_key)
        print(f"Triggered by upload: s3://{bucket}/{key}")

    try:
        response = glue.start_job_run(JobName=GLUE_JOB_NAME)
        run_id = response["JobRunId"]
        print(f"Started Glue job '{GLUE_JOB_NAME}' (JobRunId={run_id})")
    except Exception as exc:
        print(f"Failed to start Glue job '{GLUE_JOB_NAME}': {exc}")
        raise

    return {
        "statusCode": 200,
        "body": json.dumps({"job": GLUE_JOB_NAME, "jobRunId": run_id}),
    }