#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"

DISTRIBUTED="SETTINGS make_distributed_plan = 1, distributed_plan_execute_locally = 1, enable_parallel_replicas = 0,
    distributed_plan_fallback_to_local_execution = 0, distributed_plan_max_rows_to_broadcast = 0,
    distributed_plan_default_reader_bucket_count = 3, distributed_plan_default_shuffle_join_bucket_count = 3,
    max_rows_to_group_by = 0"
LOCAL="SETTINGS make_distributed_plan = 0"

${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TABLE} (id UInt64, part UInt8, v String)
    ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet')
    PARTITION BY part
"

# Many small data files, and position deletes in some of them.
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --iceberg_insert_max_rows_in_data_file=100 --query "
    INSERT INTO ${TABLE} SELECT number, number % 3, toString(number) FROM numbers(3000)
"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "ALTER TABLE ${TABLE} DELETE WHERE id % 7 = 0"

for SETTINGS in "$DISTRIBUTED" "$LOCAL"; do
    ${CLICKHOUSE_CLIENT} --query "SELECT count(), sum(id) FROM ${TABLE} ${SETTINGS}"
    ${CLICKHOUSE_CLIENT} --query "SELECT part, count(), sum(id) FROM ${TABLE} GROUP BY part ORDER BY part ${SETTINGS}"
    ${CLICKHOUSE_CLIENT} --query "SELECT count(), sum(a.id) FROM ${TABLE} AS a INNER JOIN ${TABLE} AS b ON a.id = b.id ${SETTINGS}"
    ${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${TABLE} WHERE part = 1 AND v LIKE '%5%' ${SETTINGS}"
done

${CLICKHOUSE_CLIENT} --query "
    SELECT trim(explain) FROM (EXPLAIN SELECT sum(id) FROM ${TABLE} ${DISTRIBUTED})
    WHERE explain LIKE '%Exchange%' OR explain LIKE '%ReadFromObjectStorage%'
"

# A table function has no name a worker could resolve.
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM icebergLocal('${TABLE_PATH}') ${DISTRIBUTED}" 2>&1 | grep -o -m1 'SUPPORT_IS_DISABLED'

${CLICKHOUSE_CLIENT} --query "DROP TABLE ${TABLE}"
rm -rf "${TABLE_PATH}"
