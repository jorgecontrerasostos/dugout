from datetime import datetime, timedelta

from mlb_data.jobs.bronze_partitioned import _target_partition_key


def test_target_partition_key():
    date = datetime(2026, 6, 20)
    assert _target_partition_key(date) == (date - timedelta(days=1)).strftime(
        "%Y-%m-%d"
    )


def test_target_partition_key_handles_month_boundary():
    date = datetime(2026, 7, 1)
    assert _target_partition_key(date) == (date - timedelta(days=1)).strftime(
        "%Y-%m-%d"
    )


def test_target_partition_key_handles_year_boundary():
    date = datetime(2027, 1, 1)
    assert _target_partition_key(date) == (date - timedelta(days=1)).strftime(
        "%Y-%m-%d"
    )
