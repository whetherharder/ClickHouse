import pytest

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster
from helpers.config_cluster import minio_access_key, minio_secret_key

pytestmark = pytest.mark.timeout(300)

cluster = ClickHouseCluster(__file__)

NODES = [
    cluster.add_instance(
        name,
        main_configs=["configs/config.d/stateless_worker.xml"],
        with_minio=True,
        stay_alive=True,
    )
    for name in ("node1", "node2", "node3")
]
INITIATOR = NODES[0]

DISTRIBUTED = {
    "make_distributed_plan": 1,
    "enable_parallel_replicas": 0,
    "distributed_plan_fallback_to_local_execution": 0,
    "distributed_plan_max_rows_to_broadcast": 0,
    "distributed_plan_default_reader_bucket_count": 3,
    "distributed_plan_default_shuffle_join_bucket_count": 3,
    "max_rows_to_group_by": 0,
}
LOCAL = {"make_distributed_plan": 0}


def table_url(name):
    return f"http://minio1:9001/{cluster.minio_bucket}/distributed_plan_iceberg/{name}/"


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        credentials = f"'{minio_access_key}', '{minio_secret_key}'"
        INITIATOR.query(
            f"CREATE TABLE events (id UInt64, user_id UInt32, amount Int64, day Date)"
            f" ENGINE = IcebergS3('{table_url('events')}', {credentials}) PARTITION BY day"
        )
        INITIATOR.query(
            f"CREATE TABLE users (user_id UInt32, country String)"
            f" ENGINE = IcebergS3('{table_url('users')}', {credentials})"
        )
        # Many data files in several partitions, then position deletes.
        INITIATOR.query(
            "INSERT INTO events SELECT number, number % 1000, number % 97 - 40, toDate('2026-01-01') + number % 5"
            " FROM numbers(200000)",
            settings={"allow_insert_into_iceberg": 1, "iceberg_insert_max_rows_in_data_file": 5000},
        )
        INITIATOR.query(
            "INSERT INTO users SELECT number, ['de', 'fr', 'us'][number % 3 + 1] FROM numbers(1000)",
            settings={"allow_insert_into_iceberg": 1},
        )
        INITIATOR.query("ALTER TABLE events DELETE WHERE id % 11 = 0", settings={"allow_insert_into_iceberg": 1})

        # A worker resolves the table by name, so every node declares it.
        for node in NODES[1:]:
            node.query(f"CREATE TABLE events ENGINE = IcebergS3('{table_url('events')}', {credentials})")
            node.query(f"CREATE TABLE users ENGINE = IcebergS3('{table_url('users')}', {credentials})")
        yield cluster
    finally:
        cluster.shutdown()


QUERIES = [
    "SELECT count(), sum(amount) FROM events",
    "SELECT day, count(), sum(amount) FROM events GROUP BY day ORDER BY day",
    "SELECT user_id, count(), sum(amount) FROM events GROUP BY user_id ORDER BY user_id",
    "SELECT u.country, count(), sum(e.amount) FROM events AS e INNER JOIN users AS u ON e.user_id = u.user_id"
    " GROUP BY u.country ORDER BY u.country",
    "SELECT count() FROM events AS a INNER JOIN events AS b ON a.id = b.id",
    "SELECT count(), sum(amount) FROM events WHERE day = '2026-01-03' AND amount > 0",
]


@pytest.mark.parametrize("query", QUERIES)
def test_matches_local_execution(started_cluster, query):
    query_id = f"distributed_{abs(hash(query))}"
    distributed = INITIATOR.query(query, settings=DISTRIBUTED, query_id=query_id)
    assert distributed == INITIATOR.query(query, settings=LOCAL)

    INITIATOR.query("SYSTEM FLUSH LOGS")
    remote_tasks = int(
        INITIATOR.query(
            "SELECT ProfileEvents['DistributedPlanRemoteTasks'] FROM system.query_log"
            f" WHERE query_id = '{query_id}' AND type = 'QueryFinish'"
        )
    )
    assert remote_tasks >= 3


def test_every_worker_reads_part_of_the_table(started_cluster):
    query_id = "distributed_read_spread"
    INITIATOR.query("SELECT sum(amount) FROM events", settings=DISTRIBUTED, query_id=query_id)

    read_rows = []
    for node in NODES:
        node.query("SYSTEM FLUSH LOGS")
        read_rows.append(
            int(
                node.query(
                    "SELECT sum(read_rows) FROM system.query_log"
                    f" WHERE initial_query_id = '{query_id}' AND type = 'QueryFinish' AND NOT is_initial_query"
                )
            )
        )
    assert all(rows > 0 for rows in read_rows), read_rows


def test_table_function_is_not_distributed(started_cluster):
    with pytest.raises(QueryRuntimeException, match="SUPPORT_IS_DISABLED"):
        INITIATOR.query(
            f"SELECT count() FROM icebergS3('{table_url('events')}', '{minio_access_key}', '{minio_secret_key}')",
            settings=DISTRIBUTED,
        )
