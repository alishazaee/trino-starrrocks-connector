package io.trino.plugin.starrocks;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.LimitNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.TableScanNode;
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
public class TestStarRocksClientPushDown extends SetupEnvironment {

    @AfterEach
    void tearDown()
            throws SQLException {
        try (Connection connection = DriverManager.getConnection(jdbcUrl, ROOT_USER, ROOT_PASSWORD);
                Statement statement = connection.createStatement()) {
            statement.execute("DROP DATABASE IF EXISTS `" + SCHEMA + "`");
        }
    }

    @Test
    void testLimitPushDown() throws SQLException {
        createFixtures();
        MaterializedResultWithPlan executionPlan = trino.executeWithPlan(trino.getDefaultSession(), """
                SELECT id
                FROM %s.%s.orders
                LIMIT 2""".formatted(CATALOG, SCHEMA));

        assertPlanNodes(executionPlan, LimitNode.class);
        assertRowsReadFromStarRocks(executionPlan, 2);

    }

    @Test
    void testPredicatePushDown() throws SQLException {
        createFixtures();
        MaterializedResultWithPlan executionPlan = trino.executeWithPlan(trino.getDefaultSession(), """
                SELECT id
                FROM %s.%s.orders
                where id = 2""".formatted(CATALOG, SCHEMA));

        assertPlanNodes(executionPlan, FilterNode.class);
        assertRowsReadFromStarRocks(executionPlan, 1);
    }

    @Test
    void testCountAllPushDown() throws SQLException {
        createFixtures();
        MaterializedResultWithPlan executionPlan = trino.executeWithPlan(trino.getDefaultSession(), """
                SELECT count(*)
                FROM %s.%s.orders""".formatted(CATALOG, SCHEMA));
        assertPlanNodes(executionPlan, AggregationNode.class);
        assertRowsReadFromStarRocks(executionPlan, 1);
    }

    @Test
    void testProjectionPushDown() throws SQLException {
        createFixtures();
        MaterializedResultWithPlan executionPlan = trino.executeWithPlan(trino.getDefaultSession(), """
                SELECT amount, id
                FROM %s.%s.orders""".formatted(CATALOG, SCHEMA));
        assertRowsReadFromStarRocks(executionPlan, 4);

        List<TableScanNode> scanNodeList = getPlanNodes(executionPlan.queryPlan().orElseThrow().getRoot())
                .filter(TableScanNode.class::isInstance)
                .map(TableScanNode.class::cast)
                .toList();

        // we read from only one table, so we expect the size to be one
        assertEquals(1, scanNodeList.size());

        Set<String> columns = scanNodeList.getFirst()
                .getAssignments()
                .values()
                .stream()
                .map(handle -> ((JdbcColumnHandle) handle).getColumnName())
                .collect(Collectors.toSet());
        assertEquals(columns, Set.of("id", "amount"));
    }

    @Test
    void testJoinPushDown() throws SQLException {
        createFixtures();
        MaterializedResultWithPlan executionPlan =
                trino.executeWithPlan(trino.getDefaultSession(), """
                    SELECT o.id, c.customer_name
                    FROM %s.%s.orders o
                    INNER JOIN %s.%s.customers c
                        ON o.id = c.id
                    """.formatted(CATALOG, SCHEMA, CATALOG, SCHEMA));

        assertThat(executionPlan.result().getRowCount())
                .isEqualTo(2);

        assertThat(executionPlan.result().getMaterializedRows())
                .extracting(row -> row.getField(0))
                .containsExactlyInAnyOrder(2L, 3L);

        assertPlanNodes(executionPlan, JoinNode.class);

        assertRowsReadFromStarRocks(executionPlan, 2);
    }

    @Test
    void testTableStatistics() throws SQLException {
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

    private void assertPlanNodes(MaterializedResultWithPlan executionPlan, Class<?> clazz) {
        assertThat(executionPlan.queryPlan()).isPresent();
        assertThat(getPlanNodes(executionPlan.queryPlan().orElseThrow().getRoot()))
                .noneMatch(clazz::isInstance);
    }

    private void assertRowsReadFromStarRocks(MaterializedResultWithPlan executionPlan, int expectedCount) {
        assertThat(executionPlan.result().getRowCount()).isEqualTo(expectedCount);
        long rowsReadFromStarRocks = trino.getCoordinator()
                .getFullQueryInfo(executionPlan.queryId())
                .getQueryStats()
                .getPhysicalInputPositions();
        assertThat(rowsReadFromStarRocks).isEqualTo(expectedCount);
    }

    private static Stream<PlanNode> getPlanNodes(PlanNode node) {
        return Stream.concat(
                Stream.of(node),
                node.getSources().stream().flatMap(TestStarRocksClientPushDown::getPlanNodes)
        );
    }

    private void createFixtures() throws SQLException {
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
