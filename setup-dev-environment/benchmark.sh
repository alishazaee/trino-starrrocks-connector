#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
project_dir=$(cd -- "${script_dir}/.." && pwd)
compose=(docker compose --project-directory "${script_dir}" -f "${script_dir}/compose.yaml")
starrocks=("${compose[@]}" exec -T starrocks mysql -h127.0.0.1 -P9030 -uroot --batch --skip-column-names)
starrocks_connector=("${compose[@]}" exec -T trino trino --user benchmark --catalog starrocks --schema analytics --output-format TSV)
mysql_connector=("${compose[@]}" exec -T trino trino --user benchmark --catalog starrocks_mysql --schema analytics --output-format TSV)

fail() {
    echo "FAIL: $*" >&2
    exit 1
}

query() {
    local connector=$1
    local sql=$2
    if [[ "${connector}" == "starrocks" ]]; then
        "${starrocks_connector[@]}" --execute "${sql}"
    else
        "${mysql_connector[@]}" --execute "${sql}"
    fi
}

average_ms() {
    local connector=$1
    local sql=$2
    local total=0
    local started
    local finished
    for _ in 1 2 3; do
        started=$(date +%s%N)
        query "${connector}" "${sql}" >/dev/null
        finished=$(date +%s%N)
        total=$((total + (finished - started) / 1000000))
    done
    echo $((total / 3))
}

if [[ "${SKIP_IMAGE_BUILD:-false}" != "true" ]]; then
    "${project_dir}/gradlew" -p "${project_dir}" jibDockerBuild --no-daemon
fi

"${compose[@]}" up -d --wait

echo "Creating the deterministic 1,000,000-row fixture..."
"${starrocks[@]}" <<'SQL'
CREATE DATABASE IF NOT EXISTS analytics;
DROP TABLE IF EXISTS analytics.pushdown_benchmark;
CREATE TABLE analytics.pushdown_benchmark (
    id BIGINT NOT NULL,
    shard INT NOT NULL,
    amount BIGINT NOT NULL,
    event_time DATETIME NOT NULL
)
ENGINE=OLAP
DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 8
PROPERTIES ("replication_num" = "1");
INSERT INTO analytics.pushdown_benchmark
SELECT
    generate_series,
    generate_series % 100,
    generate_series * 10,
    FROM_UNIXTIME(1577836800 + generate_series * 60)
FROM TABLE(generate_series(1, 1000000));
SQL

aggregation_sql="SELECT count(*) FROM pushdown_benchmark WHERE month(event_time) = 12 AND day(event_time) = 31"
topn_sql="SELECT id, amount FROM pushdown_benchmark WHERE month(event_time) = 12 ORDER BY amount DESC LIMIT 10"

for sql in "${aggregation_sql}" "${topn_sql}"; do
    starrocks_result=$(query starrocks "${sql}")
    mysql_result=$(query mysql "${sql}")
    [[ "${starrocks_result}" == "${mysql_result}" ]] || fail "connectors returned different results for: ${sql}"
done

aggregation_plan=$(query starrocks "EXPLAIN ${aggregation_sql}")
mysql_aggregation_plan=$(query mysql "EXPLAIN ${aggregation_sql}")
topn_plan=$(query starrocks "EXPLAIN ${topn_sql}")
mysql_topn_plan=$(query mysql "EXPLAIN ${topn_sql}")

grep -Fq 'month(' <<<"${aggregation_plan}" || fail "StarRocks connector did not push the month predicate"
grep -Fq 'dayofmonth(' <<<"${aggregation_plan}" || fail "StarRocks connector did not push the day predicate"
grep -Fq 'SELECT count(*)' <<<"${aggregation_plan}" || fail "StarRocks connector did not push aggregation"
grep -Fq 'Aggregate[' <<<"${mysql_aggregation_plan}" || fail "MySQL control unexpectedly pushed the aggregation"
grep -Fq 'limit=10' <<<"${topn_plan}" || fail "StarRocks connector did not push the Top-N limit"
grep -Fq 'sortOrder=[' <<<"${topn_plan}" || fail "StarRocks connector did not push the Top-N ordering"
grep -Fq 'TopN[' <<<"${mysql_topn_plan}" || fail "MySQL control unexpectedly pushed Top-N"

# Warm both paths once before recording three client-observed executions.
query starrocks "${aggregation_sql}" >/dev/null
query mysql "${aggregation_sql}" >/dev/null
query starrocks "${topn_sql}" >/dev/null
query mysql "${topn_sql}" >/dev/null

printf '\n%-42s %12s %12s\n' "Scenario (three-run average)" "StarRocks" "MySQL"
printf '%-42s %9sms %9sms\n' "Temporal predicate + aggregation" \
    "$(average_ms starrocks "${aggregation_sql}")" "$(average_ms mysql "${aggregation_sql}")"
printf '%-42s %9sms %9sms\n' "Temporal predicate + Top-N" \
    "$(average_ms starrocks "${topn_sql}")" "$(average_ms mysql "${topn_sql}")"

printf '\nVerified remote StarRocks SQL for aggregation:\n%b\n' "${aggregation_plan}"
printf '\nVerified remote StarRocks SQL for Top-N:\n%b\n' "${topn_plan}"
printf '\nBenchmark passed: results match and the custom connector pushes both scenarios further than stock MySQL.\n'
