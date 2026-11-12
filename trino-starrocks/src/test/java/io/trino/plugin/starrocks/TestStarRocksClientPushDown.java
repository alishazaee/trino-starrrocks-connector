package io.trino.plugin.starrocks;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.trino.Session;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.LimitNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.planner.plan.TopNNode;
import io.trino.sql.planner.plan.WindowNode;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedRow;
import io.trino.testing.QueryRunner.MaterializedResultWithPlan;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** Verifies that StarRocks pushdowns are present in the Trino plan and reduce connector input. */
public class TestStarRocksClientPushDown
        extends SetupEnvironment
{
    @AfterEach
    void tearDown()
            throws SQLException
    {
        try (Connection connection = DriverManager.getConnection(jdbcUrl, ROOT_USER, ROOT_PASSWORD);
                Statement statement = connection.createStatement()) {
            statement.execute("DROP DATABASE IF EXISTS `" + SCHEMA + "`");
        }
    }

    @Test
    void testLimitPushDown()
            throws SQLException
    {
        createFixtures();
        MaterializedResultWithPlan executionPlan = trino.executeWithPlan(trino.getDefaultSession(), """
                SELECT id
                FROM %s.%s.orders
                LIMIT 2
                """.formatted(CATALOG, SCHEMA));

        assertPushedDown(executionPlan, LimitNode.class);
        assertRowsReadFromStarRocks(executionPlan, 2);
    }

    @Test
    void testPredicatePushDown()
            throws SQLException
    {
        createFixtures();
        MaterializedResultWithPlan executionPlan = trino.executeWithPlan(trino.getDefaultSession(), """
                SELECT id
                FROM %s.%s.orders
                WHERE id = 2
                """.formatted(CATALOG, SCHEMA));

        assertPushedDown(executionPlan, FilterNode.class);
        assertRowsReadFromStarRocks(executionPlan, 1);
    }

    @Test
    void testCountAllPushDown()
            throws SQLException
    {
        createFixtures();
        MaterializedResultWithPlan executionPlan = trino.executeWithPlan(trino.getDefaultSession(), """
                SELECT count(*)
                FROM %s.%s.orders
                """.formatted(CATALOG, SCHEMA));

        assertPushedDown(executionPlan, AggregationNode.class);
        assertRowsReadFromStarRocks(executionPlan, 1);
        assertThat(executionPlan.result().getOnlyValue()).isEqualTo(4L);
    }

    @Test
    void testProjectionPushDown()
            throws SQLException
    {
        createFixtures();
        MaterializedResultWithPlan executionPlan = trino.executeWithPlan(trino.getDefaultSession(), """
                SELECT amount, id
                FROM %s.%s.orders
                """.formatted(CATALOG, SCHEMA));
        assertRowsReadFromStarRocks(executionPlan, 4);

        List<TableScanNode> scanNodes = getPlanNodes(executionPlan.queryPlan().orElseThrow().getRoot())
                .filter(TableScanNode.class::isInstance)
                .map(TableScanNode.class::cast)
                .toList();
        assertThat(scanNodes).hasSize(1);

        Set<String> columns = scanNodes.getFirst()
                .getAssignments()
                .values()
                .stream()
                .map(handle -> ((JdbcColumnHandle) handle).getColumnName())
                .collect(Collectors.toSet());
        assertThat(columns).containsExactlyInAnyOrder("id", "amount");
    }

    @Test
    void testJoinPushDown()
            throws SQLException
    {
        createFixtures();
        MaterializedResultWithPlan executionPlan = trino.executeWithPlan(trino.getDefaultSession(), """
                SELECT o.id, c.customer_name
                FROM %s.%s.orders o
                INNER JOIN %s.%s.customers c ON o.id = c.id
                """.formatted(CATALOG, SCHEMA, CATALOG, SCHEMA));

        assertThat(executionPlan.result().getMaterializedRows())
                .extracting(row -> row.getField(0))
                .containsExactlyInAnyOrder(2L, 3L);
        assertPushedDown(executionPlan, JoinNode.class);
        assertRowsReadFromStarRocks(executionPlan, 2);
    }

    @Test
    void testTableStatistics()
            throws SQLException
    {
        createFixtures();
        try (Connection connection = DriverManager.getConnection(jdbcUrl, ROOT_USER, ROOT_PASSWORD);
                Statement statement = connection.createStatement()) {
            statement.execute("ANALYZE FULL TABLE `%s`.`orders` WITH SYNC MODE".formatted(SCHEMA));
        }

        long remoteRowCount;
        try (Connection connection = DriverManager.getConnection(jdbcUrl, ROOT_USER, ROOT_PASSWORD);
                Statement statement = connection.createStatement();
                ResultSet data = statement.executeQuery("SHOW DATA FROM `%s`.`orders`".formatted(SCHEMA))) {
            assertThat(data.next()).isTrue();
            remoteRowCount = data.getLong("RowCount");
        }

        MaterializedResult result = trino.execute("SHOW STATS FOR %s.%s.orders".formatted(CATALOG, SCHEMA));
        MaterializedRow table = result.getMaterializedRows().stream()
                .filter(row -> row.getField(0) == null)
                .findFirst()
                .orElseThrow();
        assertThat(table.getField(4)).isEqualTo((double) remoteRowCount);
    }

    @Test
    void testMetadataAndBasicTypes()
            throws SQLException
    {
        createFixtures();

        assertThat(trino.execute("SHOW TABLES FROM %s.%s".formatted(CATALOG, SCHEMA)).getOnlyColumnAsSet())
                .containsExactlyInAnyOrder("customers", "orders");
        assertThat(trino.execute("DESCRIBE %s.%s.orders".formatted(CATALOG, SCHEMA)).getMaterializedRows())
                .extracting(row -> row.getField(0))
                .containsExactly("id", "amount", "name");
        assertThat(trino.execute("SELECT name FROM %s.%s.orders WHERE id = 1".formatted(CATALOG, SCHEMA)).getOnlyValue())
                .isEqualTo("Ali");
    }

    @Test
    void testExtendedTypeMappings()
            throws SQLException
    {
        createSchema();
        try (Connection connection = DriverManager.getConnection(jdbcUrl, ROOT_USER, ROOT_PASSWORD);
                Statement statement = connection.createStatement()) {
            statement.execute("""
                    CREATE TABLE `%s`.`type_mapping` (
                        id BIGINT NOT NULL,
                        flag BOOLEAN,
                        tiny_value TINYINT,
                        small_value SMALLINT,
                        int_value INT,
                        real_value FLOAT,
                        double_value DOUBLE,
                        decimal_value DECIMAL(10, 2),
                        date_value DATE,
                        timestamp_value DATETIME,
                        text_value VARCHAR(40),
                        binary_value VARBINARY(8),
                        json_data JSON
                    )
                    ENGINE=OLAP
                    DUPLICATE KEY(`id`)
                    DISTRIBUTED BY HASH(`id`) BUCKETS 1
                    PROPERTIES ("replication_num" = "1")
                    """.formatted(SCHEMA));
            statement.execute("""
                    INSERT INTO `%s`.`type_mapping`
                    VALUES (1, true, -1, 2, 3, 1.25, 2.5, 123.45,
                            '2026-11-12', '2026-11-12 10:11:12', 'hello', X'0102', PARSE_JSON('{"a": 1}'))
                    """.formatted(SCHEMA));
        }

        MaterializedRow row = trino.execute("""
                SELECT flag, tiny_value, small_value, int_value, real_value, double_value,
                       decimal_value, date_value, timestamp_value, text_value, binary_value,
                       json_data IS NOT NULL
                FROM %s.%s.type_mapping
                """.formatted(CATALOG, SCHEMA)).getMaterializedRows().getFirst();
        assertThat(row.getField(0)).isEqualTo(true);
        assertThat(row.getField(1)).isEqualTo((byte) -1);
        assertThat(row.getField(2)).isEqualTo((short) 2);
        assertThat(row.getField(3)).isEqualTo(3);
        assertThat(row.getField(4).toString()).isEqualTo("1.25");
        assertThat(row.getField(5)).isEqualTo(2.5);
        assertThat(row.getField(6).toString()).isEqualTo("123.45");
        assertThat(row.getField(7).toString()).isEqualTo("2026-11-12");
        assertThat(row.getField(8).toString()).startsWith("2026-11-12T10:11:12");
        assertThat(row.getField(9)).isEqualTo("hello");
        assertThat(row.getField(10)).isEqualTo(new byte[] {1, 2});
        assertThat(row.getField(11)).isEqualTo(true);
    }

    @Test
    void testCreateTableAndStreamLoadInsert()
            throws SQLException
    {
        createSchema();
        trino.execute("""
                CREATE TABLE %s.%s.trino_writes (
                    id BIGINT,
                    name VARCHAR,
                    amount DECIMAL(10, 2),
                    active BOOLEAN,
                    event_date DATE
                )
                WITH (
                    engine = 'OLAP',
                    table_type = 'DUPLICATE',
                    table_keys = 'id',
                    distributed_by = 'id',
                    buckets = '1',
                    replication_num = '1'
                )
                """.formatted(CATALOG, SCHEMA));

        MaterializedResult insert = trino.execute("""
                INSERT INTO %s.%s.trino_writes
                VALUES
                    (1, 'first', DECIMAL '10.25', true, DATE '2026-11-12'),
                    (2, 'second', DECIMAL '20.50', false, DATE '2026-11-13')
                """.formatted(CATALOG, SCHEMA));
        assertThat(insert.getOnlyValue()).isEqualTo(2L);

        assertThat(trino.execute("SELECT id, name FROM %s.%s.trino_writes ORDER BY id".formatted(CATALOG, SCHEMA)).getMaterializedRows())
                .extracting(row -> row.getField(1))
                .containsExactly("first", "second");
    }

    private void createSchema()
            throws SQLException
    {
        try (Connection connection = DriverManager.getConnection(jdbcUrl, ROOT_USER, ROOT_PASSWORD);
                Statement statement = connection.createStatement()) {
            statement.execute("CREATE DATABASE IF NOT EXISTS `" + SCHEMA + "`");
        }
    }

    // Regression cases from result-obfuscate.txt. Compare executed results with
    // pushdown disabled as well as inspecting the optimized plan.
    @Test
    void testMixedStringDistinctAggregationWithHavingAndTopN() throws SQLException
    {
        createFixtures();
        assertFullyPushedDown("SELECT count(*), count(DISTINCT name), sum(amount), avg(amount) FROM orders HAVING count(*) > 1 ORDER BY sum(amount) DESC LIMIT 2");
        assertFullyPushedDown("SELECT name, count(DISTINCT name) AS subs, sum(amount), max(amount) FROM orders GROUP BY name ORDER BY subs DESC, name LIMIT 2");
    }

    @Test
    void testStringMinMax() throws SQLException
    {
        createFixtures();
        assertFullyPushedDown("SELECT min(name), max(name), count(DISTINCT name) FROM orders");
    }

    @Test
    void testIntegerAverageAfterOuterJoin() throws SQLException
    {
        createBenchmarkFixtures();
        String sql = "SELECT o.name, count(*) AS n, sum(o.amount) AS total, avg(c.minutes) AS average FROM orders o JOIN customers p ON o.id = p.id LEFT JOIN benchmark c ON o.id = c.id GROUP BY o.name ORDER BY n DESC, o.name LIMIT 2";
        MaterializedResultWithPlan actual = assertEquivalent(sql);
        assertPushedDown(actual, JoinNode.class);
        assertPushedDown(actual, ProjectNode.class);
        // Join/projection pushdown occurs too late for the engine to revisit
        // aggregation in this shape. The native query path executes it all remotely.
        assertRemoteEquivalent(sql);
    }

    @Test
    void testIntegerAverageAndSum() throws SQLException
    {
        createBenchmarkFixtures();
        assertFullyPushedDown("SELECT day, avg(minutes), sum(minutes) FROM benchmark GROUP BY day ORDER BY day LIMIT 2");
    }

    @Test
    void testDateJoinAndAggregation() throws SQLException
    {
        createBenchmarkFixtures();
        String sql = "SELECT o.name, count(*) AS n, sum(o.amount) AS total FROM orders o JOIN benchmark a ON o.id = a.id JOIN benchmark b ON a.day = b.day AND a.id = b.id GROUP BY o.name HAVING count(*) > 0 ORDER BY n DESC, o.name LIMIT 2";
        MaterializedResultWithPlan actual = assertEquivalent(sql);
        assertPushedDown(actual, JoinNode.class);
        assertPushedDown(actual, AggregationNode.class);
        assertPushedDown(actual, FilterNode.class);
        assertRemoteEquivalent(sql);
    }

    @Test
    void testFullOuterJoin() throws SQLException
    {
        createFixtures();
        assertFullyPushedDown("SELECT coalesce(o.id, c.id), o.amount, c.customer_name FROM orders o FULL OUTER JOIN customers c ON o.id = c.id LIMIT 100");
    }

    @Test
    void testConditionalAggregation() throws SQLException
    {
        createFixtures();
        String sql = "SELECT name, count(*) AS n, sum(amount) AS total, sum(CASE WHEN amount > 15 THEN 1 ELSE 0 END) AS high_count FROM orders GROUP BY name HAVING count(*) > 0 ORDER BY total DESC, name LIMIT 2";
        MaterializedResultWithPlan actual = assertEquivalent(sql);
        assertThat(getPlanNodes(actual.queryPlan().orElseThrow().getRoot())).anyMatch(AggregationNode.class::isInstance);
        assertRemoteEquivalent(sql);
    }

    @Test
    void testWindowOverRemoteAggregation() throws SQLException
    {
        createFixtures();
        String sql = "WITH totals AS (SELECT name, sum(amount) AS total FROM orders GROUP BY name) SELECT name, total, rank() OVER (ORDER BY total DESC) AS ranking FROM totals ORDER BY total DESC, name LIMIT 2";
        MaterializedResultWithPlan actual = assertEquivalent(sql);
        assertPushedDown(actual, AggregationNode.class);
        assertThat(getPlanNodes(actual.queryPlan().orElseThrow().getRoot())).anyMatch(WindowNode.class::isInstance);
        assertRemoteEquivalent(sql);
    }

    @Test
    void testDerivedDistinctDateAggregationJoin() throws SQLException
    {
        createBenchmarkFixtures();
        assertFullyPushedDown("SELECT o.id, a.active_days, a.total FROM orders o JOIN (SELECT id, count(DISTINCT day) AS active_days, sum(minutes) AS total FROM benchmark GROUP BY id) a ON o.id = a.id ORDER BY a.total DESC, o.id LIMIT 2");
    }

    @Test
    void testStringDistinctNullsDuplicatesAndEmptyInput() throws SQLException
    {
        createFixtures();
        try (Connection connection = DriverManager.getConnection(jdbcUrl, ROOT_USER, ROOT_PASSWORD);
                Statement statement = connection.createStatement()) {
            statement.execute("INSERT INTO " + SCHEMA + ".orders VALUES (6, 1, 'Ali'), (7, 1, ''), (8, 1, NULL)");
        }
        assertFullyPushedDown("SELECT count(DISTINCT name), min(name), max(name), sum(amount) FROM orders");
        assertThat(trino.execute("SELECT count(DISTINCT name) FROM orders").getOnlyValue()).isEqualTo(4L);
        assertFullyPushedDown("SELECT amount, count(*) AS n, count(DISTINCT name) AS subs, sum(id) AS total, avg(id) AS average FROM orders GROUP BY amount HAVING count(*) > 0 ORDER BY subs DESC, amount LIMIT 3");
        assertFullyPushedDown("SELECT count(DISTINCT name), sum(amount), avg(amount) FROM orders WHERE id < 0");
    }

    @Test
    void testTopNNullOrdering() throws SQLException
    {
        createFixtures();
        for (String ordering : List.of("ASC NULLS FIRST", "ASC NULLS LAST", "DESC NULLS FIRST", "DESC NULLS LAST")) {
            assertFullyPushedDown("SELECT id, amount FROM orders ORDER BY amount " + ordering + ", id LIMIT 2");
        }
    }

    @Test
    void testSemiAndAntiJoinNativeQueries() throws SQLException
    {
        createFixtures();
        String semi = "SELECT id, count(*) AS n FROM orders WHERE id IN (SELECT id FROM customers) GROUP BY id ORDER BY n DESC, id LIMIT 2";
        String anti = "SELECT o.id, o.name FROM orders o WHERE NOT EXISTS (SELECT 1 FROM customers c WHERE c.id = o.id) ORDER BY o.id LIMIT 2";
        assertEquivalent(semi);
        assertEquivalent(anti);
        assertRemoteEquivalent(semi);
        assertRemoteEquivalent(anti);
    }

    @Test
    void testUnionAndPerGroupRankingNativeQueries() throws SQLException
    {
        createFixtures();
        assertRemoteEquivalent("SELECT src, id, count(*) AS n FROM (SELECT 'event' AS src, id FROM orders UNION ALL SELECT 'profile' AS src, id FROM customers) u GROUP BY src, id ORDER BY n DESC, src, id LIMIT 3");
        assertRemoteEquivalent("WITH totals AS (SELECT name, sum(amount) AS total FROM orders GROUP BY name) SELECT name, total FROM (SELECT name, total, row_number() OVER (PARTITION BY name ORDER BY total DESC) AS rn FROM totals) t WHERE rn <= 1 ORDER BY total DESC NULLS LAST, name LIMIT 3");
    }

    @Test
    void testNarrowingCastStaysLocal() throws SQLException
    {
        createFixtures();
        MaterializedResultWithPlan actual = assertEquivalent("SELECT sum(CAST(amount AS INTEGER)) FROM orders");
        assertThat(getPlanNodes(actual.queryPlan().orElseThrow().getRoot())).anyMatch(ProjectNode.class::isInstance);
    }

    @Test
    void testArithmeticFunctionPredicates() throws SQLException
    {
        createFixtures();
        for (String predicate : List.of(
                "amount + id > 20", "amount - id > 20", "amount * id > 50",
                "amount / id > 5", "amount / id = 3", "amount % id = 2", "-amount < -15",
                "nullif(amount, 10) > 15", "amount BETWEEN 10 AND 20",
                "amount IN (10, 30)")) {
            assertEquivalent("SELECT id FROM orders WHERE " + predicate);
        }
    }

    @Test
    void testStringAndNullFunctions() throws SQLException
    {
        createFixtures();
        try (Connection connection = DriverManager.getConnection(jdbcUrl, ROOT_USER, ROOT_PASSWORD);
                Statement statement = connection.createStatement()) {
            statement.execute("INSERT INTO " + SCHEMA + ".orders VALUES (6, 1, 'سلام'), (7, 1, 'é😀'), (8, 1, ' A_% '), (9, 1, '')");
        }
        for (String expression : List.of(
                "lower(name)", "upper(name)", "length(name)", "substr(name, 1, 2)",
                "concat(name, '-x')", "coalesce(name, 'missing')", "nullif(name, 'Ali')",
                "trim(name)", "regexp_like(name, '^A')", "try_cast(name AS BIGINT)")) {
            assertEquivalent("SELECT id, " + expression + " FROM orders");
        }
        for (String expression : List.of("length(name)", "coalesce(name, 'missing')", "nullif(name, 'Ali')")) {
            MaterializedResultWithPlan actual = assertEquivalent("SELECT " + expression + " FROM orders");
            assertPushedDown(actual, ProjectNode.class);
        }
        for (String predicate : List.of("name LIKE 'A%'", "name LIKE 'a%'", "name LIKE '%o_'",
                "name LIKE 'A!_%' ESCAPE '!'", "name IS NULL", "name IS NOT NULL")) {
            assertEquivalent("SELECT id FROM orders WHERE " + predicate);
        }
    }

    @Test
    void testDateFunctions() throws SQLException
    {
        createBenchmarkFixtures();
        for (String expression : List.of(
                "date_trunc('month', day)", "date_add('day', 1, day)",
                "date_diff('day', DATE '2025-12-31', day)", "year(day)",
                "CAST(day AS VARCHAR)", "CAST(day AS TIMESTAMP(6))")) {
            assertEquivalent("SELECT id, " + expression + " FROM benchmark");
        }
    }

    @Test
    void testArithmeticErrorsAreNotSilentlyConvertedToNull() throws SQLException
    {
        createFixtures();
        for (String expression : List.of("amount / (id - 3)", "amount % (id - 3)")) {
            String sql = "SELECT id FROM orders WHERE " + expression + " > 1";
            assertThatThrownBy(() -> trino.execute(sql)).hasMessageContaining("zero");
        }
    }

    @Test
    void testIntegerOverflowIsNotSilentlyWrapped() throws SQLException
    {
        createFixtures();
        try (Connection connection = DriverManager.getConnection(jdbcUrl, ROOT_USER, ROOT_PASSWORD);
                Statement statement = connection.createStatement()) {
            statement.execute("INSERT INTO " + SCHEMA + ".orders VALUES (10, 9223372036854775807, 'max'), (11, -9223372036854775808, 'min')");
        }
        for (String expression : List.of("amount + id", "amount - id", "amount * id", "-amount")) {
            assertThatThrownBy(() -> trino.execute("SELECT id FROM orders WHERE " + expression + " > 1"))
                    .hasMessageContaining("overflow");
        }
    }

    private void createBenchmarkFixtures() throws SQLException
    {
        createFixtures();
        try (Connection connection = DriverManager.getConnection(jdbcUrl, ROOT_USER, ROOT_PASSWORD);
                Statement statement = connection.createStatement()) {
            statement.execute("CREATE TABLE " + SCHEMA + ".benchmark (id BIGINT, day DATE, minutes INT) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES (\"replication_num\"=\"1\")");
            statement.execute("INSERT INTO " + SCHEMA + ".benchmark VALUES (2, '2026-01-01', 3), (3, '2026-01-02', 7), (5, '2026-01-01', NULL), (2, '2026-01-02', -2), (6, NULL, 2147483647)");
        }
    }

    private void assertFullyPushedDown(String sql)
    {
        MaterializedResultWithPlan actual = assertEquivalent(sql);
        assertPushedDown(actual, AggregationNode.class);
        assertPushedDown(actual, JoinNode.class);
        // Trino can retain a FINAL TopN to merge streams even when the remote
        // scan already orders and limits its rows. Verify the handle too.
        List<TopNNode> topNodes = getPlanNodes(actual.queryPlan().orElseThrow().getRoot())
                .filter(TopNNode.class::isInstance).map(TopNNode.class::cast).toList();
        if (!topNodes.isEmpty()) {
            assertThat(topNodes).allMatch(node -> node.getStep() == TopNNode.Step.FINAL);
            List<TableScanNode> scans = getPlanNodes(actual.queryPlan().orElseThrow().getRoot())
                    .filter(TableScanNode.class::isInstance).map(TableScanNode.class::cast).toList();
            assertThat(scans).hasSize(1);
            JdbcTableHandle handle = (JdbcTableHandle) scans.getFirst().getTable().connectorHandle();
            assertThat(handle.getSortOrder()).isPresent();
            assertThat(handle.getLimit()).hasValue(topNodes.getFirst().getCount());
        }
        assertPushedDown(actual, FilterNode.class);
        assertRowsReadFromStarRocks(actual, actual.result().getRowCount());
    }

    private MaterializedResultWithPlan assertEquivalent(String sql)
    {
        MaterializedResultWithPlan actual = trino.executeWithPlan(trino.getDefaultSession(), sql);
        Session withoutPushdown = Session.builder(trino.getDefaultSession())
                .setCatalogSessionProperty(CATALOG, "aggregation_pushdown_enabled", "false")
                .setCatalogSessionProperty(CATALOG, "join_pushdown_enabled", "false")
                .setCatalogSessionProperty(CATALOG, "topn_pushdown_enabled", "false")
                .setSystemProperty("allow_pushdown_into_connectors", "false")
                .build();
        assertThat(actual.result().getMaterializedRows())
                .as(sql).containsExactlyInAnyOrderElementsOf(trino.execute(withoutPushdown, sql).getMaterializedRows());
        return actual;
    }

    private void assertRemoteEquivalent(String sql)
    {
        // Native SQL names the remote schema, not the Trino catalog.
        String remote = sql.replaceAll("\\b(orders|customers|benchmark)\\b", SCHEMA + ".$1")
                .replace("DESC,", "DESC NULLS LAST,");
        String query = "SELECT * FROM TABLE(" + CATALOG + ".system.query(query => '" + remote.replace("'", "''") + "'))";
        MaterializedResultWithPlan actual = trino.executeWithPlan(trino.getDefaultSession(), query);
        assertThat(actual.result().getMaterializedRows())
                .containsExactlyInAnyOrderElementsOf(trino.execute(sql).getMaterializedRows());
        assertPushedDown(actual, AggregationNode.class);
        assertPushedDown(actual, JoinNode.class);
        assertPushedDown(actual, WindowNode.class);
        assertRowsReadFromStarRocks(actual, actual.result().getRowCount());
    }

    private static void assertPushedDown(MaterializedResultWithPlan executionPlan, Class<? extends PlanNode> localNodeType)
    {
        assertThat(executionPlan.queryPlan()).isPresent();
        assertThat(getPlanNodes(executionPlan.queryPlan().orElseThrow().getRoot()))
                .noneMatch(localNodeType::isInstance);
    }

    private void assertRowsReadFromStarRocks(MaterializedResultWithPlan executionPlan, long expectedCount)
    {
        assertThat(executionPlan.result().getRowCount()).isEqualTo(expectedCount);
        long rowsReadFromStarRocks = trino.getCoordinator()
                .getFullQueryInfo(executionPlan.queryId())
                .getQueryStats()
                .getPhysicalInputPositions();
        assertThat(rowsReadFromStarRocks).isEqualTo(expectedCount);
    }

    private static Stream<PlanNode> getPlanNodes(PlanNode node)
    {
        return Stream.concat(
                Stream.of(node),
                node.getSources().stream().flatMap(TestStarRocksClientPushDown::getPlanNodes));
    }

    private void createFixtures()
            throws SQLException
    {
        try (Connection connection = DriverManager.getConnection(jdbcUrl, ROOT_USER, ROOT_PASSWORD);
                Statement statement = connection.createStatement()) {
            statement.execute("CREATE DATABASE IF NOT EXISTS `" + SCHEMA + "`");
            statement.execute("""
                    CREATE TABLE `%s`.`orders` (
                        id BIGINT NOT NULL,
                        amount BIGINT NULL,
                        name VARCHAR(40) NULL
                    )
                    ENGINE=OLAP
                    DUPLICATE KEY(`id`)
                    DISTRIBUTED BY HASH(`id`) BUCKETS 1
                    PROPERTIES ("replication_num" = "1")
                    """.formatted(SCHEMA));
            statement.execute("""
                    INSERT INTO `%s`.`orders` (id, amount, name)
                    VALUES
                        (1, NULL, 'Ali'),
                        (2, 30, 'ali'),
                        (3, 10, 'Bob'),
                        (4, 20, NULL)
                    """.formatted(SCHEMA));
            statement.execute("""
                    CREATE TABLE `%s`.`customers` (
                        id BIGINT NOT NULL,
                        customer_name VARCHAR(40) NULL
                    )
                    ENGINE=OLAP
                    DUPLICATE KEY(`id`)
                    DISTRIBUTED BY HASH(`id`) BUCKETS 1
                    PROPERTIES ("replication_num" = "1")
                    """.formatted(SCHEMA));
            statement.execute("""
                    INSERT INTO `%s`.`customers` (id, customer_name)
                    VALUES
                        (2, 'Alice'),
                        (3, 'Bob'),
                        (5, 'Charlie')
                    """.formatted(SCHEMA));
        }
    }
}
