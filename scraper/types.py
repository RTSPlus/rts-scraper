from types import CoroutineType
from typing import NamedTuple


class RequestDataType(NamedTuple):
    """
    `interval_val` comes from https://apscheduler.readthedocs.io/en/3.x/modules/triggers/interval.html?highlight=hours
    """

    db_table_name: str
    job: CoroutineType
    interval_val: dict[str, int]
    cloudwatch_log_stream: str
