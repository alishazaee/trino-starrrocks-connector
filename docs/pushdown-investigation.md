# StarRocks pushdown investigation

Input: `result-obfuscate.txt`, generated November 12, 2026. Its plans explicitly
identify Trino **475**. This project and its regression tests use **484**, with
StarRocks **3.5.21**. Install the matching connector and server version together;
the 484 plugin is not a drop-in replacement for a running 475 server.

The report contains EXPLAIN output, not executed queries. Its reconstructed remote
SQL and prose are interpretations, not an execution trace. In particular,
`/* EXPLAIN_REWRITE */` is an ordinary comment in this project: there is no SQL
transpiler here. A `Query[...]` table handle represents JDBC pushdown of supported
operators, not unrestricted translation of the original SQL.

## Fixes and remaining boundaries

| Report cases | Finding and treatment |
| --- | --- |
| Q1.2, Q1.7 | `ImplementCountDistinct(..., false)` incorrectly treated remote strings as case-insensitive. A rejected DISTINCT aggregate blocks the other aggregates too. Use StarRocks case-sensitive semantics; regression tests execute mixed DISTINCT/count/sum/avg, HAVING, and Top-N with case variants, duplicates, NULL, and empty input. |
| String min/max | The same inherited restriction disabled min/max over strings. Enable and compare results against Trino execution. |
| Q1.3 | AVG over INT requires Trino's implicit BIGINT cast. Add projection rewriting for lossless TINYINT/SMALLINT/INTEGER to BIGINT casts. This allows direct integer aggregation; in the multi-join shape the cast and joins push down, but aggregation may still remain local because of optimizer phase ordering. |
| Q2.2 | DATE was absent from supported comparison types, blocking the date equi-join and subsequent aggregation. Enable DATE comparisons. Regression coverage checks a three-table join including dates, aggregation, and HAVING; final Top-N can remain local after late join pushdown. |
| Q2.3 | An inherited MySQL guard rejected FULL OUTER JOIN, although StarRocks supports it. Remove that guard. COALESCE may remain as a cheap Trino projection. |
| Q1.1, Q2.1, Q2.4–Q2.6, Q6.1–Q6.2 | Some operators already push down in the supplied plans. Test derived DISTINCT-date aggregation joins and ordinary Top-N as controls. Join cost policy and optimizer ordering still affect complete plan shape. |
| Q1.4, Q4.1, Q4.4 | Window/ranking operators are not offered through the JDBC connector's pushdown API. Underlying grouping and joins can push down; use native queries when the whole operation must execute remotely. |
| Q1.5–Q1.6 | IN/NOT EXISTS depend on Trino's decorrelation and semi/anti-join rewrites. They are not unrestricted subquery pushdown. A successful inner-join rewrite does not imply every anti-join shape can push down. |
| Q4.3 | UNION ALL combines independent scans in Trino; no general JDBC union pushdown hook exists. Native queries can execute a same-StarRocks union remotely. |
| Q4.5 | General CASE expressions are not translated by Trino 484's ConnectorExpressionTranslator (its CASE visitor only recognizes the NULLIF pattern). The connector cannot receive and rewrite this conditional aggregate. Native query regression coverage executes this shape remotely. |
| Q3.*, Q5.5–Q5.6, Q6.5–Q6.6, cross-catalog geospatial/date queries | StarRocks cannot access another Trino catalog's tables. Trino must combine those inputs. Each side can still push down supported filters/projections/aggregates. |
| Q5.1–Q5.4, Q6.3–Q6.4 | Normal CTAS/INSERT SELECT uses Trino's writer/stream-load path. Pushing down its SELECT does not turn it into a remote CTAS/INSERT statement. The report's claim of automatic `system.execute` rewriting is not implemented here. |

The connector now defaults to enabled **EAGER** join pushdown: supported joins
are sent to StarRocks even when statistics are missing. Tests use these defaults
without overriding them. Explicit catalog/session settings still take precedence;
check **cat_main** for old disabled/AUTOMATIC settings. These settings make the
intended behavior explicit:

```properties
join-pushdown.enabled=true
join-pushdown.strategy=EAGER
complex-expression-pushdown.enabled=true
aggregation-pushdown.enabled=true
topn-pushdown.enabled=true
```

EAGER prioritizes remote execution, not cost estimation. Set the strategy to
AUTOMATIC explicitly if you want Trino's statistics-based decision instead.
Some distributed plans retain a final Top-N merge even when the remote table
handle already includes sort order and a limit; that is not a failure to push
down the remote Top-N. Regression tests verify both the handle and rows read.

The tests compare query results with `allow_pushdown_into_connectors=false`, inspect
plan nodes, and, for fully pushed shapes, verify physical input row counts. Small
fixtures establish correctness and capability, not production performance.

## Explicit remote execution for same-StarRocks queries

The existing `system.query` table function accepts **StarRocks SQL**. It is tested
for conditional aggregates, windows, and multi-join aggregate/Top-N queries. Use
remote schema names inside the string, not the Trino catalog prefix:

```sql
SELECT *
FROM TABLE(cat_main.system.query(query => '
    SELECT sub_id,
           count(*) AS total_calls,
           sum(dur) AS total_dur,
           avg(call_dur) AS avg_call,
           sum(CASE WHEN dir = ''INBOUND'' THEN 1 ELSE 0 END) AS inbound,
           sum(CASE WHEN dir = ''OUTBOUND'' THEN 1 ELSE 0 END) AS outbound
    FROM sch_main.tbl_event
    WHERE day BETWEEN ''2026-01-01'' AND ''2026-03-31''
      AND has_error = false
    GROUP BY sub_id
    HAVING count(*) > 10
    ORDER BY total_dur DESC NULLS LAST
    LIMIT 200
'));
```

Specify NULL ordering explicitly when translating sorts: defaults can differ
between engines. Native functions, casts, and approximate aggregates also need
semantic review. Native queries cannot reference `cat_ext` through Trino.

## Errors that occur before connector pushdown

* Q4.2 references `c.day` from a grouped correlated subquery without grouping it.
  Include it in the grouping, or rewrite the correlation at the intended granularity.
* Q4.6 uses `percentile_cont ... WITHIN GROUP`, which Trino rejects. Trino's
  `approx_percentile` is an approximate alternative, not an equivalent exact percentile.
* Q5.2/Q6.4 target tables do not exist; EXPLAIN of a CTAS does not create its table.
* Q5.3's CTAS target already exists.
* Q6.3 needs an explicit alias for `sum(dur)` in its CTAS output.
* The spatial examples use names such as `ST_GeogPoint`, `ST_GeogFromText`,
  `ST_MakeEnvelope`, and `ST_DWithin` that are not automatically made valid by
  installing Trino's geospatial plugin. Rewrite against Trino's actual geometry/
  spherical-geography functions, or use StarRocks-supported native functions via
  `system.query`. Check geometry versus geography and distance units explicitly.

References: [StarRocks joins](https://docs.starrocks.io/docs/sql-reference/sql-statements/table_bucket_part_index/SELECT/SELECT_JOIN/),
[Trino pushdown](https://trino.io/docs/current/optimizer/pushdown.html),
[Trino geospatial functions](https://trino.io/docs/current/functions/geospatial.html).
