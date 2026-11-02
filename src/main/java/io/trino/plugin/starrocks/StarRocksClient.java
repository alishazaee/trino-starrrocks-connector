/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.trino.plugin.starrocks;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.trino.plugin.jdbc.JdbcJoinPushdownUtil.implementJoinCostAware;
import static io.trino.plugin.jdbc.PredicatePushdownController.DISABLE_PUSHDOWN;
import static io.trino.plugin.jdbc.PredicatePushdownController.FULL_PUSHDOWN;
import static io.trino.plugin.jdbc.StandardColumnMappings.bigintColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.bigintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.booleanColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.booleanWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.charReadFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.charWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.dateColumnMappingUsingLocalDate;
import static io.trino.plugin.jdbc.StandardColumnMappings.dateWriteFunctionUsingLocalDate;
import static io.trino.plugin.jdbc.StandardColumnMappings.decimalColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.doubleColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.doubleWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.integerColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.integerWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.longDecimalWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.realWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.shortDecimalWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.smallintColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.smallintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.timestampColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.timestampWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.tinyintColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.tinyintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.varbinaryReadFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.varbinaryWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.varcharReadFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.varcharWriteFunction;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.mysql.cj.jdbc.JdbcStatement;
import io.airlift.log.Logger;
import io.trino.plugin.base.aggregation.AggregateFunctionRewriter;
import io.trino.plugin.base.aggregation.AggregateFunctionRule;
import io.trino.plugin.base.expression.ConnectorExpressionRewriter;
import io.trino.plugin.base.mapping.IdentifierMapping;
import io.trino.plugin.jdbc.BaseJdbcClient;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ColumnMapping;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcExpression;
import io.trino.plugin.jdbc.JdbcJoinCondition;
import io.trino.plugin.jdbc.JdbcSortItem;
import io.trino.plugin.jdbc.JdbcStatisticsConfig;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.PreparedQuery;
import io.trino.plugin.jdbc.QueryBuilder;
import io.trino.plugin.jdbc.RemoteTableName;
import io.trino.plugin.jdbc.WriteMapping;
import io.trino.plugin.jdbc.aggregation.ImplementAvgDecimal;
import io.trino.plugin.jdbc.aggregation.ImplementAvgFloatingPoint;
import io.trino.plugin.jdbc.aggregation.ImplementCount;
import io.trino.plugin.jdbc.aggregation.ImplementCountAll;
import io.trino.plugin.jdbc.aggregation.ImplementCountDistinct;
import io.trino.plugin.jdbc.aggregation.ImplementMinMax;
import io.trino.plugin.jdbc.aggregation.ImplementStddevPop;
import io.trino.plugin.jdbc.aggregation.ImplementStddevSamp;
import io.trino.plugin.jdbc.aggregation.ImplementSum;
import io.trino.plugin.jdbc.aggregation.ImplementVariancePop;
import io.trino.plugin.jdbc.aggregation.ImplementVarianceSamp;
import io.trino.plugin.jdbc.expression.JdbcConnectorExpressionRewriterBuilder;
import io.trino.plugin.jdbc.expression.ParameterizedExpression;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.JoinCondition;
import io.trino.spi.connector.JoinStatistics;
import io.trino.spi.connector.JoinType;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.statistics.ColumnStatistics;
import io.trino.spi.statistics.Estimate;
import io.trino.spi.statistics.TableStatistics;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Stream;

/**
 * StarRocks' SQL endpoint is MySQL-wire compatible, but its metadata, types and
 * distributed execution semantics are not MySQL. This client deliberately
 * enables only pushdowns whose StarRocks and Trino semantics are equivalent.
 */
public final class StarRocksClient extends BaseJdbcClient {
    private static final Logger LOG = Logger.get(StarRocksClient.class);
    private static final int MAX_DATETIME_PRECISION = 6;
    private static final Set<String> COMPLEX_TYPES = ImmutableSet.of("array", "map", "struct", "json");

    private final ConnectorExpressionRewriter<ParameterizedExpression> predicateRewriter;
    private final AggregateFunctionRewriter<JdbcExpression, ?> aggregationRewriter;
    private final boolean statisticsEnabled;
    private final StarRocksConfig starRocksConfig;

    public StarRocksClient(
            BaseJdbcConfig config,
            ConnectionFactory connectionFactory,
            QueryBuilder queryBuilder,
            IdentifierMapping identifierMapping,
            RemoteQueryModifier queryModifier,
            JdbcStatisticsConfig statisticsConfig,
            StarRocksConfig starRocksConfig) {
        super(
                "`",
                connectionFactory,
                queryBuilder,
                config.getJdbcTypesMappedToVarchar(),
                identifierMapping,
                queryModifier,
                true);
        this.starRocksConfig = requireNonNull(starRocksConfig, "starRocksConfig is null");
        statisticsEnabled = requireNonNull(statisticsConfig, "statisticsConfig is null").isEnabled();

        predicateRewriter = JdbcConnectorExpressionRewriterBuilder.newBuilder()
                .addStandardRules(this::quoted)
                .withTypeClass(
                        "exact_numeric",
                        ImmutableSet.of("tinyint", "smallint", "integer", "bigint", "decimal", "number"))
                .withTypeClass("timestamp_type", ImmutableSet.of("timestamp"))
                .map("$equal(left: exact_numeric, right: exact_numeric)").to("left = right")
                .map("$not_equal(left: exact_numeric, right: exact_numeric)").to("left <> right")
                .map("$less_than(left: exact_numeric, right: exact_numeric)").to("left < right")
                .map("$less_than_or_equal(left: exact_numeric, right: exact_numeric)").to("left <= right")
                .map("$greater_than(left: exact_numeric, right: exact_numeric)").to("left > right")
                .map("$greater_than_or_equal(left: exact_numeric, right: exact_numeric)").to("left >= right")
                .map("year(value: timestamp_type)").to("year(value)")
                .map("month(value: timestamp_type)").to("month(value)")
                .map("day(value: timestamp_type)").to("dayofmonth(value)")
                .build();

        JdbcTypeHandle bigint = new JdbcTypeHandle(
                Types.BIGINT,
                Optional.of("bigint"),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
        aggregationRewriter = new AggregateFunctionRewriter<>(
                predicateRewriter,
                ImmutableSet.<AggregateFunctionRule<JdbcExpression, ParameterizedExpression>>builder()
                        .add(new ImplementCountAll(bigint))
                        .add(new ImplementCount(bigint))
                        // StarRocks COUNT(DISTINCT) has Trino-compatible null semantics. The rule's false collation
                        // flag keeps CHAR/VARCHAR distinct aggregation in Trino.
                        .add(new ImplementCountDistinct(bigint, false))
                        .add(new ImplementMinMax(false))
                        .add(new ImplementSum(StarRocksClient::decimalTypeHandle))
                        .add(new ImplementAvgFloatingPoint())
                        .add(new ImplementAvgDecimal())
                        .add(new ImplementStddevSamp())
                        .add(new ImplementStddevPop())
                        .add(new ImplementVarianceSamp())
                        .add(new ImplementVariancePop())
                        .build());
    }

    @Override
    public Optional<ParameterizedExpression> convertPredicate(
            ConnectorSession session,
            ConnectorExpression expression,
            Map<String, ColumnHandle> assignments) {
        return predicateRewriter.rewrite(session, expression, assignments);
    }

    @Override
    public Optional<JdbcExpression> implementAggregation(
            ConnectorSession session,
            AggregateFunction aggregate,
            Map<String, ColumnHandle> assignments) {
        return aggregationRewriter.rewrite(session, aggregate, assignments);
    }

    @Override
    public boolean supportsAggregationPushdown(
            ConnectorSession session,
            JdbcTableHandle table,
            List<AggregateFunction> aggregates,
            Map<String, ColumnHandle> assignments,
            List<List<ColumnHandle>> groupingSets) {
        return preventTextualTypeAggregationPushdown(groupingSets);
    }

    private static Optional<JdbcTypeHandle> decimalTypeHandle(DecimalType type) {
        return Optional.of(new JdbcTypeHandle(
                Types.DECIMAL,
                Optional.of("decimal"),
                Optional.of(type.getPrecision()),
                Optional.of(type.getScale()),
                Optional.empty(),
                Optional.empty()));
    }

    @Override
    public Collection<String> listSchemas(Connection connection) {
        try (ResultSet schemas = connection.getMetaData().getCatalogs()) {
            ImmutableSet.Builder<String> result = ImmutableSet.builder();
            while (schemas.next()) {
                String schema = schemas.getString("TABLE_CAT");
                if (filterSchema(schema)) {
                    result.add(schema);
                }
            }
            return result.build();
        } catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, "Failed to list StarRocks databases", e);
        }
    }

    @Override
    protected boolean filterSchema(String schema) {
        return !schema.equalsIgnoreCase("information_schema")
                && !schema.equalsIgnoreCase("sys")
                && !schema.equalsIgnoreCase("_statistics_");
    }

    @Override
    public ResultSet getTables(Connection connection, Optional<String> schemaName, Optional<String> tableName)
            throws SQLException {
        DatabaseMetaData metadata = connection.getMetaData();
        return metadata.getTables(
                schemaName.orElse(null),
                null,
                escapeObjectNameForMetadataQuery(tableName, metadata.getSearchStringEscape()).orElse(null),
                getTableTypes().map(types -> types.toArray(String[]::new)).orElse(null));
    }

    @Override
    protected ResultSet getAllTableColumns(Connection connection, Optional<String> remoteSchemaName)
            throws SQLException {
        return connection.getMetaData().getColumns(remoteSchemaName.orElse(null), null, null, null);
    }

    @Override
    protected String getTableSchemaName(ResultSet resultSet)
            throws SQLException {
        return resultSet.getString("TABLE_CAT");
    }

    @Override
    public Optional<ColumnMapping> toColumnMapping(
            ConnectorSession session,
            Connection connection,
            JdbcTypeHandle typeHandle) {
        Optional<ColumnMapping> forced = getForcedMappingToVarchar(typeHandle);
        if (forced.isPresent()) {
            return forced;
        }

        String remoteType = typeHandle.jdbcTypeName().orElse("").toLowerCase(Locale.ENGLISH);
        String baseType = remoteType.replaceAll("\\s*\\(.*", "").trim();
        // StarRocks 3.5 reports LARGEINT as BIGINT UNSIGNED with precision 39
        // through its MySQL-compatible metadata endpoint.
        if (baseType.equals("largeint")
                || (typeHandle.jdbcType() == Types.BIGINT && typeHandle.columnSize().orElse(0) > 19)) {
            // Trino 475 has no arbitrary-precision NUMBER type. LARGEINT has 39 digits,
            // one more than DECIMAL's maximum precision, so VARCHAR is the only lossless mapping.
            return Optional.of(varcharMapping(createUnboundedVarcharType()));
        }
        if (isComplexType(baseType)) {
            return Optional.of(varcharMapping(createUnboundedVarcharType()));
        }
        if (baseType.equals("datetime")) {
            return Optional.of(timestampColumnMapping(createTimestampType(timestampPrecision(typeHandle))));
        }
        if (baseType.equals("string")) {
            return Optional.of(varcharMapping(createUnboundedVarcharType()));
        }

        return switch (typeHandle.jdbcType()) {
            case Types.BOOLEAN, Types.BIT -> Optional.of(booleanColumnMapping());
            case Types.TINYINT -> Optional.of(tinyintColumnMapping());
            case Types.SMALLINT -> Optional.of(smallintColumnMapping());
            case Types.INTEGER -> Optional.of(integerColumnMapping());
            case Types.BIGINT -> Optional.of(bigintColumnMapping());
            case Types.REAL, Types.FLOAT -> Optional.of(ColumnMapping.longMapping(
                    REAL,
                    (resultSet, index) -> floatToRawIntBits(resultSet.getFloat(index)),
                    realWriteFunction(),
                    DISABLE_PUSHDOWN));
            case Types.DOUBLE -> Optional.of(doubleColumnMapping());
            case Types.NUMERIC, Types.DECIMAL -> decimalMapping(session, typeHandle);
            case Types.CHAR -> Optional.of(charMapping(min(typeHandle.requiredColumnSize(), CharType.MAX_LENGTH)));
            case Types.VARCHAR, Types.NVARCHAR, Types.LONGVARCHAR, Types.LONGNVARCHAR -> Optional.of(
                    varcharMapping(varcharType(typeHandle.columnSize().orElse(VarcharType.MAX_LENGTH))));
            case Types.BINARY, Types.VARBINARY, Types.LONGVARBINARY -> Optional.of(ColumnMapping.sliceMapping(
                    VARBINARY,
                    varbinaryReadFunction(),
                    varbinaryWriteFunction(),
                    FULL_PUSHDOWN));
            case Types.DATE -> Optional.of(dateColumnMappingUsingLocalDate());
            case Types.TIMESTAMP -> Optional.of(
                    timestampColumnMapping(createTimestampType(timestampPrecision(typeHandle))));
            default -> Optional.empty();
        };
    }

    private static Optional<ColumnMapping> decimalMapping(ConnectorSession session, JdbcTypeHandle typeHandle) {
        int scale = max(typeHandle.decimalDigits().orElse(0), 0);
        int precision = typeHandle.columnSize().orElse(Decimals.MAX_PRECISION)
                + max(-typeHandle.decimalDigits().orElse(0), 0);
        if (precision > Decimals.MAX_PRECISION || scale > precision) {
            return Optional.empty();
        }
        return Optional.of(decimalColumnMapping(createDecimalType(precision, scale)));
    }

    static int timestampPrecision(JdbcTypeHandle typeHandle) {
        return min(max(typeHandle.decimalDigits().orElse(0), 0), MAX_DATETIME_PRECISION);
    }

    static boolean isComplexType(String remoteType) {
        return COMPLEX_TYPES.stream()
                .anyMatch(type -> remoteType.equals(type) || remoteType.startsWith(type + "<"));
    }

    private static VarcharType varcharType(int length) {
        if (length <= 0 || length > VarcharType.MAX_LENGTH) {
            return createUnboundedVarcharType();
        }
        return createVarcharType(length);
    }

    private static ColumnMapping varcharMapping(VarcharType type) {
        return ColumnMapping.sliceMapping(type, varcharReadFunction(type), varcharWriteFunction(), DISABLE_PUSHDOWN);
    }

    private static ColumnMapping charMapping(int length) {
        CharType type = createCharType(length);
        return ColumnMapping.sliceMapping(type, charReadFunction(type), charWriteFunction(), DISABLE_PUSHDOWN);
    }

    @Override
    public WriteMapping toWriteMapping(ConnectorSession session, Type type) {
        if (type == BOOLEAN) {
            return WriteMapping.booleanMapping("boolean", booleanWriteFunction());
        }
        if (type == TINYINT) {
            return WriteMapping.longMapping("tinyint", tinyintWriteFunction());
        }
        if (type == SMALLINT) {
            return WriteMapping.longMapping("smallint", smallintWriteFunction());
        }
        if (type == INTEGER) {
            return WriteMapping.longMapping("int", integerWriteFunction());
        }
        if (type == BIGINT) {
            return WriteMapping.longMapping("bigint", bigintWriteFunction());
        }
        if (type == REAL) {
            return WriteMapping.longMapping("float", realWriteFunction());
        }
        if (type == DOUBLE) {
            return WriteMapping.doubleMapping("double", doubleWriteFunction());
        }
        if (type instanceof DecimalType decimal) {
            String dataType = format("decimal(%s,%s)", decimal.getPrecision(), decimal.getScale());
            return decimal.isShort()
                    ? WriteMapping.longMapping(dataType, shortDecimalWriteFunction(decimal))
                    : WriteMapping.objectMapping(dataType, longDecimalWriteFunction(decimal));
        }
        if (type == DATE) {
            return WriteMapping.longMapping("date", dateWriteFunctionUsingLocalDate());
        }
        if (type instanceof TimestampType timestamp) {
            if (timestamp.getPrecision() != 0) {
                throw new TrinoException(
                        NOT_SUPPORTED,
                        "StarRocks DATETIME writes require timestamp(0), got " + type.getDisplayName());
            }
            return WriteMapping.longMapping("datetime", timestampWriteFunction(timestamp));
        }
        if (type == VARBINARY) {
            return WriteMapping.sliceMapping("varbinary", varbinaryWriteFunction());
        }
        if (type instanceof CharType character) {
            return WriteMapping.sliceMapping("char(" + character.getLength() + ")", charWriteFunction());
        }
        if (type instanceof VarcharType varchar) {
            String dataType = varchar.isUnbounded() ? "string" : "varchar(" + varchar.getBoundedLength() + ")";
            return WriteMapping.sliceMapping(dataType, varcharWriteFunction());
        }
        throw new TrinoException(NOT_SUPPORTED, "Unsupported StarRocks write type: " + type.getDisplayName());
    }

    @Override
    protected List<String> createTableSqls(
            RemoteTableName table,
            List<String> columns,
            ConnectorTableMetadata metadata) {
        if (metadata.getColumns().isEmpty()) {
            throw new TrinoException(NOT_SUPPORTED, "StarRocks tables require at least one column");
        }
        ColumnMetadata distributionColumn = metadata.getColumns().getFirst();
        String sql = format(
                "CREATE TABLE %s (%s) ENGINE=OLAP DISTRIBUTED BY HASH (%s) "
                        + "BUCKETS %s PROPERTIES (\"replication_num\" = \"%s\")",
                quoted(table),
                String.join(", ", columns),
                quoted(distributionColumn.getName()),
                starRocksConfig.getDefaultBucketCount(),
                starRocksConfig.getDefaultReplicationNumber());
        return ImmutableList.of(sql);
    }

    @Override
    protected void addColumn(
            ConnectorSession session,
            Connection connection,
            RemoteTableName table,
            ColumnMetadata column)
            throws SQLException {
        String columnName = column.getName();
        verifyColumnName(connection.getMetaData(), columnName);
        String remoteColumnName = getIdentifierMapping()
                .toRemoteColumnName(getRemoteIdentifiers(connection), columnName);
        String sql = format(
                "ALTER TABLE %s ADD COLUMN %s",
                quoted(table),
                getColumnDefinitionSql(session, column, remoteColumnName));
        execute(session, connection, sql);
    }

    @Override
    protected Optional<BiFunction<String, Long, String>> limitFunction() {
        return Optional.of((query, limit) -> query + " LIMIT " + limit);
    }

    @Override
    public boolean isLimitGuaranteed(ConnectorSession session) {
        return true;
    }

    @Override
    public boolean supportsTopN(ConnectorSession session, JdbcTableHandle table, List<JdbcSortItem> sortOrder) {
        return sortOrder.stream()
                .map(JdbcSortItem::column)
                .map(JdbcColumnHandle::getColumnType)
                .allMatch(StarRocksClient::supportsTopSortType);
    }

    static boolean supportsTopSortType(Type type) {
        return !(type instanceof CharType) && !(type instanceof VarcharType) && !type.equals(REAL);
    }

    @Override
    protected Optional<TopNFunction> topNFunction() {
        return Optional.of(StarRocksClient::topSql);
    }

    static String topSql(String query, List<JdbcSortItem> sortItems, long limit) {
        String orderBy = sortItems.stream()
                .flatMap(item -> {
                    String column = "`" + item.column().getColumnName().replace("`", "``") + "`";
                    String ordering = item.sortOrder().isAscending() ? "ASC" : "DESC";
                    String sort = column + " " + ordering;
                    return switch (item.sortOrder()) {
                        case ASC_NULLS_FIRST, DESC_NULLS_LAST -> Stream.of(sort);
                        case ASC_NULLS_LAST -> Stream.of("ISNULL(" + column + ") ASC", sort);
                        case DESC_NULLS_FIRST -> Stream.of("ISNULL(" + column + ") DESC", sort);
                    };
                })
                .collect(joining(", "));
        return format("%s ORDER BY %s LIMIT %s", query, orderBy, limit);
    }

    @Override
    public boolean isTopNGuaranteed(ConnectorSession session) {
        return true;
    }

    @Override
    public Optional<PreparedQuery> implementJoin(
            ConnectorSession session,
            JoinType joinType,
            PreparedQuery leftSource,
            Map<JdbcColumnHandle, String> leftProjections,
            PreparedQuery rightSource,
            Map<JdbcColumnHandle, String> rightProjections,
            List<ParameterizedExpression> conditions,
            JoinStatistics statistics) {
        if (joinType == JoinType.FULL_OUTER) {
            return Optional.empty();
        }
        return implementJoinCostAware(
                session,
                joinType,
                leftSource,
                rightSource,
                statistics,
                () -> super.implementJoin(
                        session,
                        joinType,
                        leftSource,
                        leftProjections,
                        rightSource,
                        rightProjections,
                        conditions,
                        statistics));
    }

    @Override
    public Optional<PreparedQuery> legacyImplementJoin(
            ConnectorSession session,
            JoinType joinType,
            PreparedQuery leftSource,
            PreparedQuery rightSource,
            List<JdbcJoinCondition> conditions,
            Map<JdbcColumnHandle, String> rightAssignments,
            Map<JdbcColumnHandle, String> leftAssignments,
            JoinStatistics statistics) {
        // StarRocks 3.5 does not implement FULL OUTER JOIN. This guard must be
        // present on both the expression and legacy join paths because Trino
        // selects the path with the complex_join_pushdown_enabled session
        // property.
        if (joinType == JoinType.FULL_OUTER) {
            return Optional.empty();
        }
        return implementJoinCostAware(
                session,
                joinType,
                leftSource,
                rightSource,
                statistics,
                () -> super.legacyImplementJoin(
                        session,
                        joinType,
                        leftSource,
                        rightSource,
                        conditions,
                        rightAssignments,
                        leftAssignments,
                        statistics));
    }

    @Override
    protected boolean isSupportedJoinCondition(ConnectorSession session, JdbcJoinCondition condition) {
        if (condition.getOperator() == JoinCondition.Operator.IDENTICAL) {
            return false;
        }
        return Stream.of(condition.getLeftColumn(), condition.getRightColumn())
                .map(JdbcColumnHandle::getColumnType)
                .allMatch(StarRocksClient::supportsJoinType);
    }

    static boolean supportsJoinType(Type type) {
        return !(type instanceof CharType) && !(type instanceof VarcharType) && !type.equals(REAL);
    }

    @Override
    public TableStatistics getTableStatistics(ConnectorSession session, JdbcTableHandle table) {
        if (!statisticsEnabled || !table.isNamedRelation()) {
            return TableStatistics.empty();
        }

        RemoteTableName remoteTable = table.getRequiredNamedRelation().getRemoteTableName();
        try (Connection connection = connectionFactory.openConnection(session)) {
            Optional<Long> rowCount = readRowCount(connection, remoteTable);
            if (rowCount.isEmpty()) {
                return TableStatistics.empty();
            }

            TableStatistics.Builder statistics = TableStatistics.builder()
                    .setRowCount(Estimate.of(rowCount.get()));
            Map<String, RawColumnStatistics> rawColumnStatistics;
            try {
                rawColumnStatistics = readColumnStatistics(connection, remoteTable);
            } catch (SQLException e) {
                // StarRocks can run without collected column statistics. Row count remains useful to Trino's planner.
                LOG.debug(e, "Column statistics are not available for %s", remoteTable);
                return statistics.build();
            }

            for (JdbcColumnHandle column : getColumns(
                    session,
                    table.getRequiredNamedRelation().getSchemaTableName(),
                    remoteTable)) {
                RawColumnStatistics raw = rawColumnStatistics.get(column.getColumnName());
                if (raw != null) {
                    statistics.setColumnStatistics(column, raw.toColumnStatistics(rowCount.get()));
                }
            }
            return statistics.build();
        } catch (SQLException | RuntimeException e) {
            LOG.debug(e, "Table statistics are not available for %s", remoteTable);
            return TableStatistics.empty();
        }
    }

    private static Optional<Long> readRowCount(Connection connection, RemoteTableName table)
            throws SQLException {
        String schemaName = remoteSchemaName(table);
        String qualifiedTableName = schemaName + "." + table.getTableName();
        try (PreparedStatement statement = connection.prepareStatement("""
                SELECT SUM(row_count) AS row_count
                FROM _statistics_.column_statistics
                WHERE table_name = ?
                GROUP BY column_name
                LIMIT 1
                """)) {
            statement.setString(1, qualifiedTableName);
            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    long rowCount = resultSet.getLong("row_count");
                    if (!resultSet.wasNull()) {
                        return Optional.of(max(rowCount, 0));
                    }
                }
            }
        } catch (SQLException e) {
            LOG.debug(e, "Collected row count is not available for %s", table);
        }

        try (ResultSet resultSet = connection.createStatement().executeQuery(format(
                "SHOW DATA FROM `%s`.`%s`",
                schemaName.replace("`", "``"),
                table.getTableName().replace("`", "``")))) {
            while (resultSet.next()) {
                if (table.getTableName().equals(resultSet.getString("TableName"))
                        && table.getTableName().equals(resultSet.getString("IndexName"))) {
                    return Optional.of(max(resultSet.getLong("RowCount"), 0));
                }
            }
        } catch (SQLException e) {
            LOG.debug(e, "SHOW DATA row count is not available for %s", table);
        }

        try (PreparedStatement statement = connection.prepareStatement("""
                SELECT TABLE_ROWS
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND TABLE_TYPE = 'BASE TABLE'
                """)) {
            statement.setString(1, schemaName);
            statement.setString(2, table.getTableName());
            try (ResultSet resultSet = statement.executeQuery()) {
                if (!resultSet.next()) {
                    return Optional.empty();
                }
                long rowCount = resultSet.getLong("TABLE_ROWS");
                return resultSet.wasNull() ? Optional.empty() : Optional.of(max(rowCount, 0));
            }
        }
    }

    private static Map<String, RawColumnStatistics> readColumnStatistics(
            Connection connection,
            RemoteTableName table)
            throws SQLException {
        String qualifiedTableName = remoteSchemaName(table) + "." + table.getTableName();
        try (PreparedStatement statement = connection.prepareStatement("""
                SELECT
                    column_name,
                    SUM(null_count) AS null_count,
                    SUM(data_size) AS data_size,
                    HLL_CARDINALITY(HLL_UNION(ndv)) AS distinct_values
                FROM _statistics_.column_statistics
                WHERE table_name = ?
                GROUP BY column_name
                """)) {
            statement.setString(1, qualifiedTableName);
            try (ResultSet resultSet = statement.executeQuery()) {
                Map<String, RawColumnStatistics> statistics = new HashMap<>();
                while (resultSet.next()) {
                    statistics.put(
                            resultSet.getString("column_name"),
                            new RawColumnStatistics(
                                    resultSet.getLong("null_count"),
                                    resultSet.getDouble("data_size"),
                                    resultSet.getDouble("distinct_values")));
                }
                return Map.copyOf(statistics);
            }
        }
    }

    private static String remoteSchemaName(RemoteTableName table) {
        return table.getCatalogName()
                .or(() -> table.getSchemaName())
                .orElseThrow(() -> new IllegalArgumentException("Remote table has no schema: " + table));
    }

    @Override
    public PreparedStatement getPreparedStatement(Connection connection, String sql, Optional<Integer> columnCount)
            throws SQLException {
        PreparedStatement statement = connection.prepareStatement(sql);
        if (statement.isWrapperFor(JdbcStatement.class)) {
            statement.unwrap(JdbcStatement.class).enableStreamingResults();
        }
        return statement;
    }

    @Override
    public void abortReadConnection(Connection connection, ResultSet resultSet)
            throws SQLException {
        if (!resultSet.isAfterLast()) {
            connection.abort(directExecutor());
        }
    }

    private record RawColumnStatistics(long nullCount, double dataSize, double distinctValuesCount) {
        private ColumnStatistics toColumnStatistics(long rowCount) {
            long boundedNullCount = min(max(nullCount, 0), rowCount);
            double nonNullCount = rowCount - boundedNullCount;
            ColumnStatistics.Builder statistics = ColumnStatistics.builder()
                    .setDataSize(Estimate.of(max(dataSize, 0)))
                    .setDistinctValuesCount(Estimate.of(min(max(distinctValuesCount, 0), nonNullCount)));
            if (rowCount > 0) {
                statistics.setNullsFraction(Estimate.of((double) boundedNullCount / rowCount));
            }
            return statistics.build();
        }
    }
}
