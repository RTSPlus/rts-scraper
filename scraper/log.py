import sys
from time import time
from datetime import datetime

from boto3 import client as boto_client


ENABLE_CLOUDWATCH_LOGS = True

cloudwatch_log_group = "scraper-monitoring"


cloudwatch_logs = None
if ENABLE_CLOUDWATCH_LOGS:
    cloudwatch_logs = boto_client("logs")


def log(tag: str, msg: str, timestamp: int = None, log_stream: str | None = None):
    """
    Logs to both stdout and CloudWatch Logs.
    Requires a log_stream to be passed in if CloudWatch Logs is to be used.
    """
    timestamp = timestamp if timestamp else int(round(time() * 1000))
    message = f"[{tag}][{datetime.fromtimestamp(timestamp/1000)}] {msg}"

    sys.stdout.write(message + "\n")
    sys.stdout.flush()

    if log_stream and cloudwatch_logs:
        cloudwatch_logs.put_log_events(
            logGroupName=cloudwatch_log_group,
            logStreamName=log_stream,
            logEvents=[
                {
                    "timestamp": timestamp,
                    "message": message,
                }
            ],
        )
