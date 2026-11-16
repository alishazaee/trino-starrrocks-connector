# Direct StarRocks Flight reads and Trino locality

This connector has an **opt-in experimental** Arrow Flight SQL read path. JDBC remains the default and handles metadata, DDL and unsupported reads. Writes still use Stream Load.

The Flight path submits the generated SQL to an FE, obtains a result ticket, and schedules a Trino split beside the backend serving that ticket. The worker fetches Arrow batches directly from that backend and converts them to Trino pages. The submitting FE session remains alive until Trino query cleanup, which closes the remote session (including on cancellation). Tickets are not printed in split descriptions or connector debug logs.

## What this does and does not provide

This is **result-producer locality**, not tablet locality. The inspected StarRocks `branch-3.5` implementation returns one endpoint for a query. The integration test uses StarRocks 3.5.21. Consequently, this extension removes FE forwarding and can place that result reader on the producing backend's host; it does **not** parallelize one StarRocks result across every Trino worker. StarRocks may already have performed exchanges before producing the result, and Trino may shuffle rows after reading it.

The connector supports multiple unordered endpoints if a server returns them. Alternative locations for one endpoint are read only once, not treated as additional partitions. Ordered multi-endpoint results are rejected rather than silently reordered.

Neither Arrow nor scheduling alone proves zero NIC traffic. The backend and mapped worker must actually share a host, and the container/network path must remain local. An incorrect mapping can make the scheduling look local while the bytes still cross machines.

## Deployment

1. Enable Arrow Flight on every FE/BE that participates:

   ```properties
   # fe.conf
   arrow_flight_port = 9408
   # be.conf
   arrow_flight_port = 9419
   ```

2. On StarRocks releases supporting FE Flight proxying, disable it before opening new connector sessions:

   ```sql
   SET GLOBAL arrow_flight_proxy_enabled = false;
   ```

   Some older releases do not expose this setting. Inspect the returned endpoints. Backend addresses advertised by StarRocks must be reachable directly from Trino workers. Do not put FE proxy addresses in the backend mapping.

3. Add these flags to the Trino JVM on the coordinator and every worker, then restart:

   ```text
   --add-opens=java.base/java.nio=ALL-UNNAMED
   --sun-misc-unsafe-memory-access=allow
   ```

   The Java 25/Arrow/Netty combination used by this connector requires these flags. The development JVM configuration and test JVM include them.

4. Keep your existing JDBC and Stream Load catalog configuration and add:

   ```properties
   starrocks.flight.enabled=true
   starrocks.flight.endpoint=grpc://fe-a:9408
   starrocks.flight.backend-workers=grpc://be-a:9419=worker-a:8080,grpc://be-b:9419=worker-b:8080
   starrocks.flight.locality=PREFERRED
   starrocks.flight.max-memory-bytes=268435456
   starrocks.flight.timeout-seconds=300
   ```

   The left side of each mapping is the actual Flight URI returned by StarRocks. The right side is the colocated Trino worker's **advertised host and Trino port**, not the BE port. A host-only worker address matches workers on that host. `grpc://` and `grpc+tcp://` are normalized to the same transport; `grpc+tls://` uses TLS. There is one configured FE endpoint in this version, with no automatic FE failover. JDBC credentials for the calling identity authenticate the FE Flight session. Direct StarRocks BE reads use the server-issued ticket, which should be treated as sensitive. Use trusted networking/TLS as appropriate for your deployment.

   The backend map is also an endpoint allowlist. Unmapped endpoints, FE-only responses, and proxy responses not present in the map fail explicitly after Flight execution; the connector does not re-execute those queries through JDBC. All nodes must have the same mapping.

5. Use the standard Trino uniform scheduler:

   ```properties
   node-scheduler.policy=uniform
   node-scheduler.include-coordinator=false
   ```

## Locality policies

| Policy | Scheduling | Unsupported read |
| --- | --- | --- |
| `PREFERRED` (default) | Supplies preferred worker addresses; scheduler may choose another worker | Uses JDBC |
| `REQUIRED` | Only the mapped worker address is eligible; absent worker causes failure | Fails rather than violating the locality requirement |
| `OFF` | No worker preference, still uses direct Flight reads | Uses JDBC |

Preferred locality is a scheduling preference, not a promise about which worker wins under load. Required locality trades availability for placement. Neither mode moves result production inside StarRocks. This implementation does not advertise resumable or fault-tolerant result tickets: do not rely on replay after a partial read or transparent backend failover.

## Supported reads and limits

Flight currently handles generated SQL with no bound parameters and these output types: boolean, integer types, real, double, decimal, varchar, char, varbinary, date and timestamp without time zone up to microsecond precision. This includes eligible pushed-down aggregates, joins, limits and projections.

StarRocks 3.5 Flight does not implement parameter binding. Queries with JDBC parameters (including common pushed predicates and dynamic filters), unsupported types such as arrays/JSON/time, merge scans and procedures use JDBC in PREFERRED/OFF mode and fail in REQUIRED mode. Parameters are never interpolated into SQL. Empty-projection scans also use the fallback path. Dynamic filters available before split planning are preserved through Trino's JDBC filtering wrapper; later filters do not restart an already submitted Flight query.

Arrow allocation is bounded by `max-memory-bytes` per catalog per node across active readers. Each reader converts up to 1024 rows per Trino page and keeps only the current Arrow batch at the application level; the transport also maintains bounded in-flight buffers. The timeout bounds query submission and data-stream RPCs; it is not a whole-query SLA. The decoder copies values into Trino blocks, so this is not an end-to-end zero-copy implementation.

## Proving placement and network locality

Run:

```bash
./gradlew test --tests '*TestStarRocksFlight*'
```

The integration test starts a disposable StarRocks 3.5.21 container and a Trino coordinator with two workers. It needs Docker and free host port 9419 because the test image advertises its BE on loopback. It uses REQUIRED mode mapped to one worker's unique Trino port and inspects completed query task statistics: the task with physical input must run on that exact worker. It compares Flight results with JDBC, then verifies a nonexistent mapped worker causes failure and PREFERRED mode permits a remote reader. Unit tests cover split serialization, alternative endpoint deduplication, endpoint allowlisting, multi-page reads and memory cleanup.

To inspect a deployed cluster, enable:

```properties
# etc/log.properties
io.trino.plugin.starrocks.StarRocksFlightPageSourceProvider=DEBUG
```

The log records query ID, actual reader worker, direct Flight endpoint and preferred worker addresses, without the ticket. Correlate those with Trino task placement. A successful query alone is not evidence of locality.

For physical network proof, use at least two separate hosts with one BE and one Trino worker per host. Compare the same large scan with correct and deliberately remote worker mappings. Record per-interface bytes or trace connections to the BE Flight port; isolate that transfer from Trino exchanges, StarRocks exchanges, replication and other workloads. Correct colocated routing should keep the BE-to-reader transfer off the inter-host interface. The single-host automated test proves scheduling and direct reading, not physical network savings or production throughput.

References: [StarRocks Flight](https://docs.starrocks.io/docs/unloading/arrow_flight/), [StarRocks 3.5 implementation](https://github.com/StarRocks/starrocks/blob/branch-3.5/fe/fe-core/src/main/java/com/starrocks/service/arrow/flight/sql/ArrowFlightSqlServiceImpl.java), [Arrow Flight protocol](https://arrow.apache.org/docs/format/Flight.html), [Trino scheduling](https://trino.io/docs/current/admin/properties-node-scheduler.html).
