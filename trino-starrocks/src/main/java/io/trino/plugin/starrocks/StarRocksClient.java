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

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.emptyToNull;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static com.starrocks.data.load.stream.StreamLoadConstants.TABLE_MODEL_PRIMARY_KEYS;
import static io.airlift.json.JsonCodec.jsonCodec;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.base.util.JsonTypeUtil.jsonParse;
import static io.trino.plugin.jdbc.DecimalConfig.DecimalMapping.ALLOW_OVERFLOW;
import static io.trino.plugin.jdbc.DecimalSessionSessionProperties.getDecimalDefaultScale;
import static io.trino.plugin.jdbc.DecimalSessionSessionProperties.getDecimalRounding;
import static io.trino.plugin.jdbc.DecimalSessionSessionProperties.getDecimalRoundingMode;
import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.trino.plugin.jdbc.JdbcJoinPushdownUtil.implementJoinCostAware;
import static io.trino.plugin.jdbc.PredicatePushdownController.DISABLE_PUSHDOWN;
import static io.trino.plugin.jdbc.PredicatePushdownController.FULL_PUSHDOWN;
import static io.trino.plugin.jdbc.StandardColumnMappings.bigintColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.bigintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.booleanColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.booleanWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.charWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.dateReadFunctionUsingLocalDate;
import static io.trino.plugin.jdbc.StandardColumnMappings.decimalColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.defaultCharColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.defaultVarcharColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.doubleColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.doubleWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.integerColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.integerWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.longDecimalWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.realWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.shortDecimalWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.smallintColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.smallintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.timeReadFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.timeWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.timestampReadFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.timestampWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.tinyintColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.tinyintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.varbinaryReadFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.varbinaryWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.varcharWriteFunction;
import static io.trino.plugin.jdbc.TypeHandlingJdbcSessionProperties.getUnsupportedTypeHandling;
import static io.trino.plugin.jdbc.UnsupportedTypeHandling.CONVERT_TO_VARCHAR;
import static io.trino.plugin.jdbc.UnsupportedTypeHandling.IGNORE;
import static io.trino.plugin.starrocks.StarRocksTableProperties.BLOOM_FILTER_COLUMNS;
import static io.trino.plugin.starrocks.StarRocksTableProperties.BLOOM_FILTER_PATTERN;
import static io.trino.plugin.starrocks.StarRocksTableProperties.BUCKETS;
import static io.trino.plugin.starrocks.StarRocksTableProperties.BUCKETS_PATTERN;
import static io.trino.plugin.starrocks.StarRocksTableProperties.BUCKET_SIZE;
import static io.trino.plugin.starrocks.StarRocksTableProperties.BUCKET_SIZE_PATTERN;
import static io.trino.plugin.starrocks.StarRocksTableProperties.DISTRIBUTED_BY;
import static io.trino.plugin.starrocks.StarRocksTableProperties.DISTRIBUTED_BY_PATTERN;
import static io.trino.plugin.starrocks.StarRocksTableProperties.ENGINE;
import static io.trino.plugin.starrocks.StarRocksTableProperties.ENGINE_PATTERN;
import static io.trino.plugin.starrocks.StarRocksTableProperties.ORDER_BY;
import static io.trino.plugin.starrocks.StarRocksTableProperties.ORDER_BY_PATTERN;
import static io.trino.plugin.starrocks.StarRocksTableProperties.PARTITION_BY;
import static io.trino.plugin.starrocks.StarRocksTableProperties.PARTITION_BY_PATTERN;
import static io.trino.plugin.starrocks.StarRocksTableProperties.PROPS_BLOCK_PATTERN;
import static io.trino.plugin.starrocks.StarRocksTableProperties.REPLICATION_NUMBER;
import static io.trino.plugin.starrocks.StarRocksTableProperties.REPLICATION_NUM_PATTERN;
import static io.trino.plugin.starrocks.StarRocksTableProperties.ROLLUP;
import static io.trino.plugin.starrocks.StarRocksTableProperties.TABLE_COMMENT;
import static io.trino.plugin.starrocks.StarRocksTableProperties.TABLE_COMMENT_PATTERN;
import static io.trino.plugin.starrocks.StarRocksTableProperties.TABLE_KEYS;
import static io.trino.plugin.starrocks.StarRocksTableProperties.TABLE_TYPE;
import static io.trino.plugin.starrocks.StarRocksTableProperties.TABLE_TYPE_PATTERN;
import static io.trino.plugin.starrocks.StarRocksWriteSessionProperties.getQueryTimeout;
import static io.trino.spi.StandardErrorCode.ALREADY_EXISTS;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimeType.createTimeType;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.String.format;
import static java.sql.DatabaseMetaData.columnNoNulls;
import static java.time.format.DateTimeFormatter.ISO_DATE;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import com.mysql.cj.jdbc.JdbcStatement;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.plugin.base.aggregation.AggregateFunctionRewriter;
import io.trino.plugin.base.aggregation.AggregateFunctionRule;
import io.trino.plugin.base.expression.ConnectorExpressionRewriter;
import io.trino.plugin.base.mapping.IdentifierMapping;
import io.trino.plugin.base.mapping.RemoteIdentifiers;
import io.trino.plugin.jdbc.BaseJdbcClient;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ColumnMapping;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcExpression;
import io.trino.plugin.jdbc.JdbcMetadata;
import io.trino.plugin.jdbc.JdbcOutputTableHandle;
import io.trino.plugin.jdbc.JdbcSortItem;
import io.trino.plugin.jdbc.JdbcStatisticsConfig;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.LongReadFunction;
import io.trino.plugin.jdbc.LongWriteFunction;
import io.trino.plugin.jdbc.ObjectReadFunction;
import io.trino.plugin.jdbc.ObjectWriteFunction;
import io.trino.plugin.jdbc.PreparedQuery;
import io.trino.plugin.jdbc.QueryBuilder;
import io.trino.plugin.jdbc.JdbcQueryRelationHandle;
import io.trino.plugin.jdbc.RemoteTableName;
import io.trino.plugin.jdbc.UnsupportedTypeHandling;
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
import io.trino.plugin.jdbc.expression.RewriteIn;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.JoinStatistics;
import io.trino.spi.connector.JoinType;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.statistics.ColumnStatistics;
import io.trino.spi.statistics.Estimate;
import io.trino.spi.statistics.TableStatistics;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.VarcharType;
import io.trino.spi.predicate.TupleDomain;
import java.util.OptionalLong;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.sql.Statement;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.statement.UnableToExecuteStatementException;

public class StarRocksClient
        extends BaseJdbcClient
{
    private static final Logger log = Logger.get(StarRocksClient.class);
    private static final DateTimeFormatter SQL_DATETIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss[.SSSSSS]");
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final TypeReference<List<Object>> LIST_TYPE_REF = new TypeReference<>() {};
    private static final int MAX_SUPPORTED_DATE_TIME_PRECISION = 6;
    private static final int ZERO_PRECISION_TIMESTAMP_COLUMN_SIZE = 19;
    private static final int ZERO_PRECISION_TIME_COLUMN_SIZE = 8;
    private static final String NO_COMMENT = "";
    public static final String SQL_STATE_ER_TABLE_EXISTS_ERROR = "42S01";
    public static final Integer ER_UNKNOWN_TABLE = 1109;
    private static final JsonCodec<ColumnHistogram> HISTOGRAM_CODEC = jsonCodec(ColumnHistogram.class);
    private final Type jsonType;
    private final boolean statisticsEnabled;
    private final ConnectorExpressionRewriter<ParameterizedExpression> connectorExpressionRewriter;
    private final AggregateFunctionRewriter<JdbcExpression, ?> aggregateFunctionRewriter;
    private final IdentifierMapping identifierMapping;

    @Inject
    public StarRocksClient(
            BaseJdbcConfig config,
            JdbcStatisticsConfig statisticsConfig,
            ConnectionFactory connectionFactory,
            QueryBuilder queryBuilder,
            TypeManager typeManager,
            IdentifierMapping identifierMapping,
            RemoteQueryModifier queryModifier)
    {
        super("`", connectionFactory, queryBuilder, config.getJdbcTypesMappedToVarchar(), identifierMapping, queryModifier, false);
        this.jsonType = typeManager.fromSqlType(StandardTypes.JSON);
        this.statisticsEnabled = requireNonNull(statisticsConfig, "statisticsConfig is null").isEnabled();
        this.identifierMapping = identifierMapping;

        this.connectorExpressionRewriter = JdbcConnectorExpressionRewriterBuilder.newBuilder()
                .addStandardRules(this::quoted)
                .add(new RewriteIn())
                .withTypeClass("supported_type", ImmutableSet.of("tinyint", "smallint", "integer", "bigint", "decimal", "double", "char", "varchar"))
                .withTypeClass("numeric_type", ImmutableSet.of("tinyint", "smallint", "integer", "bigint", "decimal", "double"))
                .map("$equal(left: supported_type, right: supported_type)").to("left = right")
                .map("$not_equal(left: supported_type, right: supported_type)").to("left <> right")
                .map("$less_than(left: supported_type, right: supported_type)").to("left < right")
                .map("$less_than_or_equal(left: supported_type, right: supported_type)").to("left <= right")
                .map("$greater_than(left: supported_type, right: supported_type)").to("left > right")
                .map("$greater_than_or_equal(left: supported_type, right: supported_type)").to("left >= right")
                .map("$add(left: numeric_type, right: numeric_type)").to("left + right")
                .map("$subtract(left: numeric_type, right: numeric_type)").to("left - right")
                .map("$multiply(left: numeric_type, right: numeric_type)").to("multiply(left, right)")
                .map("$divide(left: numeric_type, right: numeric_type)").to("divide(left, right)")
                .map("$modulus(left: numeric_type, right: numeric_type)").to("mod(left, right)")
                .map("$negate(value: numeric_type)").to("negative(value)")
                .map("$like(value: varchar, pattern: varchar): boolean").to("value LIKE pattern")
                .map("$not(value: boolean)").to("NOT value")
                .map("$nullif(first, second)").to("nullif(first, second)")
                .build();

        JdbcTypeHandle bigintTypeHandle = new JdbcTypeHandle(Types.BIGINT, Optional.of("bigint"), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
        this.aggregateFunctionRewriter = new AggregateFunctionRewriter<>(
                this.connectorExpressionRewriter,
                ImmutableSet.<AggregateFunctionRule<JdbcExpression, ParameterizedExpression>>builder()
                        .add(new ImplementCountAll(bigintTypeHandle))
                        .add(new ImplementCount(bigintTypeHandle))
                        .add(new ImplementCountDistinct(bigintTypeHandle, false))
                        .add(new ImplementMinMax(false))
                        .add(new ImplementSum(StarRocksClient::toTypeHandle))
                        .add(new ImplementAvgFloatingPoint())
                        .add(new ImplementAvgDecimal())
                        .add(new ImplementAvgBigint())
                        .add(new ImplementStddevSamp())
                        .add(new ImplementStddevPop())
                        .add(new ImplementVarianceSamp())
                        .add(new ImplementVariancePop())
                        .build());
    }

    @Override
    public Optional<JdbcExpression> implementAggregation(ConnectorSession session, AggregateFunction aggregate, Map<String, ColumnHandle> assignments)
    {
        return aggregateFunctionRewriter.rewrite(session, aggregate, assignments);
    }

    @Override
    public boolean supportsAggregationPushdown(ConnectorSession session, JdbcTableHandle table, List<AggregateFunction> aggregates, Map<String, ColumnHandle> assignments, List<List<ColumnHandle>> groupingSets)
    {
        return true;
    }

    @Override
    public Collection<String> listSchemas(Connection connection)
    {
        try (ResultSet resultSet = connection.getMetaData().getCatalogs()) {
            ImmutableSet.Builder<String> schemaNames = ImmutableSet.builder();
            while (resultSet.next()) {
                String schemaName = resultSet.getString("TABLE_CAT");
                // skip internal schemas
                if (filterRemoteSchema(schemaName)) {
                    schemaNames.add(schemaName);
                }
            }
            return schemaNames.build();
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    @Override
    protected boolean filterRemoteSchema(String schemaName)
    {
        if (schemaName.equalsIgnoreCase("sys")) {
            return false;
        }
        return super.filterRemoteSchema(schemaName);
    }

    @Override
    public void abortReadConnection(Connection connection, ResultSet resultSet)
            throws SQLException
    {
        if (!resultSet.isAfterLast()) {
            connection.abort(directExecutor());
        }
    }

    @Override
    public PreparedStatement getPreparedStatement(Connection connection, String sql, Optional<Integer> columnCount)
            throws SQLException
    {
        PreparedStatement statement = connection.prepareStatement(sql);
        if (statement.isWrapperFor(JdbcStatement.class)) {
            statement.unwrap(JdbcStatement.class).enableStreamingResults();
        }
        return statement;
    }

    // Avoid executing pass-through queries during analyze() by using a metadata-only wrapper.
    // This defers actual execution to the RecordCursor phase where cancellation is supported.
    @Override
    public JdbcTableHandle getTableHandle(ConnectorSession session, PreparedQuery preparedQuery)
    {
        String originalQuery = preparedQuery.query();
        PreparedQuery metadataQuery = new PreparedQuery(
                "SELECT * FROM (" + originalQuery + ") _sr_meta WHERE 1=0",
                preparedQuery.parameters());
        try (Connection connection = connectionFactory.openConnection(session);
                PreparedStatement preparedStatement = queryBuilder.prepareStatement(this, session, connection, metadataQuery, Optional.empty())) {
            ResultSetMetaData metadata = preparedStatement.getMetaData();
            if (metadata == null) {
                throw new TrinoException(NOT_SUPPORTED, "Query not supported: ResultSetMetaData not available for query: " + preparedQuery.query());
            }
            return new JdbcTableHandle(
                    new JdbcQueryRelationHandle(preparedQuery),
                    TupleDomain.all(),
                    ImmutableList.of(),
                    Optional.empty(),
                    OptionalLong.empty(),
                    Optional.of(getColumns(session, connection, metadata)),
                    // The query is opaque, so we don't know referenced tables
                    Optional.empty(),
                    0,
                    Optional.empty(),
                    ImmutableList.of());
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, "Failed to get table handle for prepared query. " + (e.getMessage() != null ? e.getMessage() : e), e);
        }
    }

    @Override
    public ResultSet getTables(Connection connection, Optional<String> schemaName, Optional<String> tableName)
            throws SQLException
    {
        DatabaseMetaData metadata = connection.getMetaData();
        return metadata.getTables(
                schemaName.orElse(null),
                null,
                escapeObjectNameForMetadataQuery(tableName, metadata.getSearchStringEscape()).orElse(null),
                getTableTypes().map(types -> types.toArray(String[]::new)).orElse(null));
    }

    @Override
    public List<JdbcColumnHandle> getColumns(ConnectorSession session, SchemaTableName schemaTableName, RemoteTableName remoteTableName)
    {
        try (Connection connection = connectionFactory.openConnection(session)) {
            Map<String, String> columnTypes = getColumnTypesFromInformationSchema(connection, remoteTableName); // Fetch full column_type for arrays
            try (ResultSet resultSet = getColumns(remoteTableName, connection.getMetaData())) {
                int allColumns = 0;
                List<JdbcColumnHandle> columns = new ArrayList<>();
                while (resultSet.next()) {
                    allColumns++;
                    String columnName = resultSet.getString("COLUMN_NAME");
                    String typeName = resultSet.getString("TYPE_NAME");
                    int dataType = resultSet.getInt("DATA_TYPE");
                    Optional<Integer> arrayDimensions = Optional.empty();
                    // check for struct type
                    if (columnTypes.get(columnName).contains("struct")) {
                        typeName = columnTypes.getOrDefault(columnName, typeName);
                        dataType = Types.STRUCT;
                    }
                    // check for array type
                    else if (columnTypes.get(columnName).startsWith("array")) {
                        typeName = columnTypes.getOrDefault(columnName, typeName);
                        dataType = Types.ARRAY;
                        arrayDimensions = Optional.of(countArrayDimensions(typeName));
                    }
                    // check for boolean type
                    else if (columnTypes.get(columnName).startsWith("tinyint(1)")) {
                        typeName = columnTypes.getOrDefault(columnName, typeName);
                        dataType = Types.BIT;
                    }
                    JdbcTypeHandle typeHandle = new JdbcTypeHandle(
                            dataType,
                            Optional.of(typeName),
                            getInteger(resultSet, "COLUMN_SIZE"),
                            getInteger(resultSet, "DECIMAL_DIGITS"),
                            arrayDimensions,
                            Optional.empty());
                    Optional<ColumnMapping> columnMapping = toColumnMapping(session, connection, typeHandle);
                    log.debug("Mapping data type of '%s' column '%s': %s mapped to %s", schemaTableName, columnName,
                            typeHandle, columnMapping);
                    // skip unsupported column types
                    if (columnMapping.isPresent()) {
                        boolean nullable = (resultSet.getInt("NULLABLE") != columnNoNulls);
                        Optional<String> comment = Optional.ofNullable(resultSet.getString("REMARKS"));
                        columns.add(JdbcColumnHandle.builder()
                                .setColumnName(columnName)
                                .setJdbcTypeHandle(typeHandle)
                                .setColumnType(columnMapping.get().getType())
                                .setNullable(nullable)
                                .setComment(comment)
                                .build());
                    }
                    if (columnMapping.isEmpty()) {
                        UnsupportedTypeHandling unsupportedTypeHandling = getUnsupportedTypeHandling(session);
                        verify(
                                unsupportedTypeHandling == IGNORE,
                                "Unsupported type handling is set to %s, but toColumnMapping() returned empty for %s",
                                unsupportedTypeHandling,
                                typeHandle);
                    }
                }
                if (columns.isEmpty()) {
                    throw new TableNotFoundException(
                            schemaTableName,
                            format("Table '%s' has no supported columns (all %s columns are not supported)",
                                    schemaTableName, allColumns));
                }
                return ImmutableList.copyOf(columns);
            }
        } catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    private Map<String, String> getColumnTypesFromInformationSchema(Connection connection, RemoteTableName remoteTableName) throws SQLException
    {
        String sql = "SELECT column_name, column_type FROM information_schema.columns WHERE table_name = ?";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, remoteTableName.getTableName());
            Map<String, String> columnTypes = new HashMap<>();
            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    columnTypes.put(resultSet.getString("column_name"), resultSet.getString("column_type"));
                }
            }
            return columnTypes;
        }
    }

    private static int countArrayDimensions(String typeName)
    {
        typeName = typeName.trim().toLowerCase();
        if (!typeName.startsWith("array<") || !typeName.endsWith(">")) {
            return 0;
        }
        int dimensions = 1;
        String inner = typeName.substring(6, typeName.length() - 1).trim();
        dimensions += countArrayDimensions(inner);
        return dimensions;
    }

    private Optional<ColumnMapping> arrayToTrinoType(ConnectorSession session, Connection connection, JdbcTypeHandle typeHandle)
    {
        checkArgument(typeHandle.jdbcType() == Types.ARRAY, "Not array type");

        // resolve and map base array element type
        JdbcTypeHandle baseElementTypeHandle = getArrayElementTypeHandle(connection, typeHandle);
        String baseElementTypeName = baseElementTypeHandle.jdbcTypeName()
                .orElseThrow(() -> new TrinoException(JDBC_ERROR, "Element type name is missing: " + baseElementTypeHandle));
        Optional<ColumnMapping> baseElementMapping = toColumnMapping(session, connection, baseElementTypeHandle);

        if (baseElementMapping.isEmpty()) {
            return Optional.empty();
        }
        if (typeHandle.arrayDimensions().isEmpty()) {
            return Optional.empty();
        }

        ColumnMapping elementMapping = baseElementMapping.get();
        ArrayType trinoArrayType = new ArrayType(elementMapping.getType());
        ColumnMapping arrayColumnMapping = arrayColumnMapping(trinoArrayType, elementMapping, baseElementTypeName);

        int arrayDimensions = typeHandle.arrayDimensions().get();
        for (int i = 1; i < arrayDimensions; i++) {
            trinoArrayType = new ArrayType(trinoArrayType);
            arrayColumnMapping = arrayColumnMapping(trinoArrayType, arrayColumnMapping, baseElementTypeName);
        }
        return Optional.of(arrayColumnMapping);
    }

    // Parse the array element type handle by recursing through nested arrays
    private JdbcTypeHandle getArrayElementTypeHandle(Connection connection, JdbcTypeHandle typeHandle)
    {
        String typeName = typeHandle.jdbcTypeName()
                .orElseThrow(() -> new TrinoException(JDBC_ERROR, "Type name is missing: " + typeHandle));
        if (!typeName.toLowerCase().startsWith("array<") || !typeName.endsWith(">")) {
            throw new TrinoException(JDBC_ERROR, "Not a valid array type name: " + typeName);
        }
        String elementTypeName = typeName.substring(6, typeName.length() - 1).trim();
        if (elementTypeName.toLowerCase().startsWith("array<")) {
            return getArrayElementTypeHandle(connection,
                    new JdbcTypeHandle(Types.ARRAY, Optional.of(elementTypeName), Optional.empty(), Optional.empty(),
                            Optional.of(1), Optional.empty()));
        }
        return createTypeHandle(elementTypeName);
    }

    private JdbcTypeHandle createTypeHandle(String typeName)
    {
        typeName = typeName.toLowerCase().trim();

        // Handle parameterized types (e.g., char(n), varchar(n), decimal(p,s))
        String baseType = typeName;
        String parameters = "";
        int paramStart = typeName.indexOf('(');
        if (paramStart >= 0 && typeName.endsWith(")")) {
            baseType = typeName.substring(0, paramStart);
            parameters = typeName.substring(paramStart + 1, typeName.length() - 1).trim();
        }

        switch (baseType) {
            case "boolean":
                return new JdbcTypeHandle(Types.BOOLEAN, Optional.of(typeName), Optional.empty(), Optional.empty(),
                        Optional.empty(), Optional.empty());
            case "tinyint":
                return new JdbcTypeHandle(Types.TINYINT, Optional.of(typeName), Optional.of(3), Optional.of(0),
                        Optional.empty(), Optional.empty());
            case "smallint":
                return new JdbcTypeHandle(Types.SMALLINT, Optional.of(typeName), Optional.of(5), Optional.of(0),
                        Optional.empty(), Optional.empty());
            case "int":
            case "integer":
                return new JdbcTypeHandle(Types.INTEGER, Optional.of(typeName), Optional.of(10), Optional.of(0),
                        Optional.empty(), Optional.empty());
            case "bigint":
                return new JdbcTypeHandle(Types.BIGINT, Optional.of(typeName), Optional.of(19), Optional.of(0),
                        Optional.empty(), Optional.empty());
            case "float":
            case "real":
                return new JdbcTypeHandle(Types.REAL, Optional.of(typeName), Optional.empty(), Optional.empty(),
                        Optional.empty(), Optional.empty());
            case "double":
                return new JdbcTypeHandle(Types.DOUBLE, Optional.of(typeName), Optional.empty(), Optional.empty(),
                        Optional.empty(), Optional.empty());
            case "decimal":
            case "numeric": {
                int precision = 38; // Default precision
                int scale = 0;    // Default scale
                if (!parameters.isEmpty()) {
                    String[] parts = parameters.split(",");
                    precision = Integer.parseInt(parts[0].trim());
                    if (parts.length > 1) {
                        scale = Integer.parseInt(parts[1].trim());
                    }
                }
                return new JdbcTypeHandle(Types.DECIMAL, Optional.of(typeName), Optional.of(precision),
                        Optional.of(scale), Optional.empty(), Optional.empty());
            }
            case "varchar":
            case "string": {
                int length = VarcharType.UNBOUNDED_LENGTH;
                if (!parameters.isEmpty()) {
                    length = Integer.parseInt(parameters);
                }
                return new JdbcTypeHandle(Types.VARCHAR, Optional.of(typeName), Optional.of(length), Optional.empty(),
                        Optional.empty(), Optional.empty());
            }
            case "char": {
                int length = 1;
                if (!parameters.isEmpty()) {
                    length = Integer.parseInt(parameters);
                }
                return new JdbcTypeHandle(Types.CHAR, Optional.of(typeName), Optional.of(length), Optional.empty(),
                        Optional.empty(), Optional.empty());
            }
            case "date":
                return new JdbcTypeHandle(Types.DATE, Optional.of(typeName), Optional.of(10), Optional.of(0),
                        Optional.empty(), Optional.empty());
            case "datetime":
            case "timestamp":
                // StarRocks DATETIME/TIMESTAMP supports up to microsecond precision (6 digits),
                // but precision is not specified in the type definition.
                // Use fixed precision of 6 to match toColumnMapping's assumption.
                int precision = 6;
                int columnSize = ZERO_PRECISION_TIMESTAMP_COLUMN_SIZE + precision + 1; // 19 + 1 + 6 = 26
                return new JdbcTypeHandle(Types.TIMESTAMP, Optional.of(typeName), Optional.of(columnSize),
                        Optional.of(precision), Optional.empty(), Optional.empty());
            default:
                throw new TrinoException(JDBC_ERROR, "Unsupported array element type: " + typeName);
        }
    }

    private static ColumnMapping arrayColumnMapping(ArrayType arrayType, ColumnMapping arrayElementMapping, String baseElementJdbcTypeName)
    {
        return ColumnMapping.objectMapping(
                arrayType,
                arrayReadFunction(arrayType.getElementType()),
                unsupportedArrayWriteFunction());
    }

    private static ObjectReadFunction arrayReadFunction(Type elementType)
    {
        return ObjectReadFunction.of(Block.class, (resultSet, columnIndex) -> {
            String arrayString = resultSet.getString(columnIndex);
            if (arrayString == null) {
                return null;
            }
            try {
                List<Object> parsedElements = OBJECT_MAPPER.readValue(arrayString, LIST_TYPE_REF);
                ArrayType topLevelArrayType = new ArrayType(elementType);
                return buildArrayBlock(topLevelArrayType, parsedElements);
            } catch (JsonProcessingException e) {
                throw new TrinoException(JDBC_ERROR, "Failed to parse StarRocks array JSON: " + arrayString, e);
            }
        });
    }

    @SuppressWarnings("unchecked")
    private static Block buildArrayBlock(ArrayType arrayType, List<Object> parsedElements)
    {
        Type elementType = arrayType.getElementType();
        BlockBuilder builder = elementType.createBlockBuilder(null, parsedElements.size());

        for (Object element : parsedElements) {
            if (element == null) {
                builder.appendNull();
                continue;
            }
            if (elementType instanceof ArrayType nestedArrayType) {
                if (!(element instanceof List)) {
                    throw new TrinoException(JDBC_ERROR, "Expected nested list for array type, but got: " + element.getClass());
                }
                Block innerBlock = buildArrayBlock(nestedArrayType, (List<Object>) element);
                elementType.writeObject(builder, innerBlock);
            }
            else {
                writeLeafElement(builder, elementType, element);
            }
        }
        return builder.build();
    }

    private static void writeLeafElement(BlockBuilder builder, Type elementType, Object value)
    {
        String strValue = value.toString();

        if (elementType.getJavaType() == boolean.class) {
            boolean val = value instanceof Boolean ? (Boolean) value : Boolean.parseBoolean(strValue);
            elementType.writeBoolean(builder, val);
        }
        else if (elementType.getJavaType() == long.class) {
            if (elementType == DATE) {
                elementType.writeLong(builder, LocalDate.parse(strValue).toEpochDay());
            }
            else if (elementType instanceof TimestampType) {
                LocalDateTime dateTime = LocalDateTime.parse(strValue, SQL_DATETIME_FORMATTER);
                long micros = dateTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli() * 1000L;
                elementType.writeLong(builder, micros);
            }
            else {
                elementType.writeLong(builder, Long.parseLong(strValue));
            }
        }
        else if (elementType.getJavaType() == double.class) {
            elementType.writeDouble(builder, Double.parseDouble(strValue));
        }
        else if (elementType.getJavaType() == Slice.class) {
            elementType.writeSlice(builder, Slices.utf8Slice(strValue));
        }
        else {
            throw new TrinoException(JDBC_ERROR, "Unsupported leaf element type: " + elementType);
        }
    }

    private static ObjectWriteFunction unsupportedArrayWriteFunction()
    {
        return ObjectWriteFunction.of(Block.class, (statement, index, value) -> {
            throw new TrinoException(NOT_SUPPORTED, "Writing array columns to StarRocks is not supported.");
        });
    }

    @Override
    public Optional<String> getTableComment(ResultSet resultSet)
            throws SQLException
    {
        return Optional.ofNullable(emptyToNull(resultSet.getString("REMARKS")));
    }

    @Override
    public void setTableComment(ConnectorSession session, JdbcTableHandle handle, Optional<String> comment)
    {
        String sql = format(
                "ALTER TABLE %s COMMENT = %s",
                quoted(handle.asPlainTable().getRemoteTableName()),
                starRocksVarcharLiteral(comment.orElse(NO_COMMENT)));
        execute(session, sql);
    }

    private static String starRocksVarcharLiteral(String value)
    {
        requireNonNull(value, "value is null");
        return "'" + value.replace("'", "''").replace("\\", "\\\\") + "'";
    }

    @Override
    protected String getTableRemoteSchemaName(ResultSet resultSet)
            throws SQLException
    {
        return resultSet.getString("TABLE_CAT");
    }

    public String buildCreateTableProperties(Map<String, Object> properties) {
        StringBuilder result = new StringBuilder();
        for (Entry<String, Object> entry : properties.entrySet()) {
            result.append("\"").append(entry.getKey()).append("\"=\"").append(entry.getValue()).append("\",");
        }
        result.setLength(result.length() - 1);
        return result.toString();
    }

    @Override
    protected List<String> createTableSqls(RemoteTableName remoteTableName, List<String> columns,
            ConnectorTableMetadata tableMetadata) {
        Map<String, Object> properties = tableMetadata.getProperties();

        StringBuilder createTableSql = new StringBuilder();
        createTableSql.append(format("CREATE TABLE %s (%s)", quoted(remoteTableName), String.join(", ", columns)));


        if (properties.containsKey(ENGINE)) {
            String engine = (String) properties.get(ENGINE);
            createTableSql.append(String.format(" ENGINE = %s", engine));
        }


        if (properties.containsKey(TABLE_TYPE)) {
            if (!properties.containsKey(TABLE_KEYS)) {
                throw new IllegalArgumentException("please give keys columns.");
            }
            String tableType = (String) properties.get(TABLE_TYPE);
            String keys = (String) properties.get(TABLE_KEYS);
            createTableSql.append(String.format(" %s KEY (%s)", tableType.toUpperCase(), keys));
        }


        if (properties.containsKey(TABLE_COMMENT)) {
            String comment = (String) properties.get(TABLE_COMMENT);
            createTableSql.append(format(" COMMENT %s", starRocksVarcharLiteral(comment)));
        }

        if (properties.containsKey(PARTITION_BY)) {
            String partitionBy = (String) properties.get(PARTITION_BY);
            createTableSql.append(format(" PARTITION BY (%s)", partitionBy));
        }

        if (properties.containsKey(DISTRIBUTED_BY)) {
            String distributionColumns = (String) properties.get(DISTRIBUTED_BY);
            createTableSql.append(format(" DISTRIBUTED BY HASH(%s)", distributionColumns));
            if (properties.containsKey(BUCKETS)) {
                String buckets = (String) properties.get(BUCKETS);
                createTableSql.append(format(" BUCKETS %s", buckets));
            }
        } else {
            createTableSql.append(" DISTRIBUTED BY RANDOM");
            if (properties.containsKey(BUCKETS)) {
                String buckets = (String) properties.get(BUCKETS);
                createTableSql.append(format(" BUCKETS %s", buckets));
            }
        }

        if (properties.containsKey(ORDER_BY)) {
            String orderBy = (String) properties.get(ORDER_BY);
            createTableSql.append(format(" ORDER BY (%s)", orderBy));
        }

        if (properties.containsKey(ROLLUP)) {
            String rollupBy = (String) properties.get(ROLLUP);
            createTableSql.append(format(" ROLLUP (%s)", rollupBy));
        }

        Map<String, Object> mutableProperties = new HashMap<>(properties);
        List<String> keysToRemove = Arrays.asList(ENGINE, TABLE_TYPE, TABLE_KEYS, PARTITION_BY, DISTRIBUTED_BY, BUCKETS,
                ROLLUP,
                ORDER_BY, TABLE_COMMENT);
        keysToRemove.forEach(mutableProperties.keySet()::remove);
        if (!mutableProperties.isEmpty()) {
            createTableSql.append(format(" PROPERTIES (%s)", buildCreateTableProperties(mutableProperties)));

        }

        return ImmutableList.of(createTableSql.toString());
    }

    @Override
    protected void renameSchema(ConnectorSession session, Connection connection, String remoteSchemaName, String newRemoteSchemaName)
            throws SQLException
    {
        execute(session, connection, "ALTER DATABASE " + quoted(remoteSchemaName) + " RENAME " + quoted(newRemoteSchemaName));
    }

    @Override
    protected void renameTable(ConnectorSession session, Connection connection, String catalogName, String remoteSchemaName, String remoteTableName, String newRemoteSchemaName, String newRemoteTableName)
            throws SQLException
    {
        if (!remoteSchemaName.equals(newRemoteSchemaName)) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support renaming tables across schemas");
        }

        execute(session, connection, format(
                "ALTER TABLE %s RENAME %s",
                quoted(catalogName, remoteSchemaName, remoteTableName),
                quoted(newRemoteTableName)));
    }

    @Override
    protected void renameColumn(ConnectorSession session, Connection connection, RemoteTableName remoteTableName, String remoteColumnName, String newRemoteColumnName)
            throws SQLException
    {
        String sql = format(
                "ALTER TABLE %s RENAME COLUMN %s TO %s",
                quoted(remoteTableName.getCatalogName().orElse(null), remoteTableName.getSchemaName().orElse(null), remoteTableName.getTableName()),
                quoted(remoteColumnName),
                quoted(newRemoteColumnName));
        execute(session, connection, sql);
    }

    @Override
    public Optional<ColumnMapping> toColumnMapping(ConnectorSession session, Connection connection, JdbcTypeHandle typeHandle)
    {
        String jdbcTypeName = typeHandle.jdbcTypeName()
                .orElseThrow(() -> new TrinoException(JDBC_ERROR, "Type name is missing: " + typeHandle));

        Optional<ColumnMapping> mapping = getForcedMappingToVarchar(typeHandle);
        if (mapping.isPresent()) {
            return mapping;
        }

        switch (jdbcTypeName.toLowerCase(ENGLISH)) {
            case "tinyint unsigned":
                return Optional.of(smallintColumnMapping());
            case "smallint unsigned":
                return Optional.of(integerColumnMapping());
            case "int unsigned":
                return Optional.of(bigintColumnMapping());
            case "bigint unsigned":
                return Optional.of(decimalColumnMapping(createDecimalType(20)));
            case "json":
                return Optional.of(jsonColumnMapping());
            case "enum":
                return Optional.of(defaultVarcharColumnMapping(typeHandle.requiredColumnSize(), false));
        }

        switch (typeHandle.jdbcType()) {
            case Types.BIT:
                return Optional.of(booleanColumnMapping());

            case Types.TINYINT:
                return Optional.of(tinyintColumnMapping());

            case Types.SMALLINT:
                return Optional.of(smallintColumnMapping());

            case Types.INTEGER:
                return Optional.of(integerColumnMapping());

            case Types.BIGINT:
                return Optional.of(bigintColumnMapping());

            case Types.REAL:
                // Disable pushdown because floating-point values are approximate and not stored as exact values,
                // attempts to treat them as exact in comparisons may lead to problems
                return Optional.of(ColumnMapping.longMapping(
                        REAL,
                        (resultSet, columnIndex) -> floatToRawIntBits(resultSet.getFloat(columnIndex)),
                        realWriteFunction(),
                        DISABLE_PUSHDOWN));

            case Types.DOUBLE:
                return Optional.of(doubleColumnMapping());

            case Types.DECIMAL:
                int decimalDigits = typeHandle.decimalDigits().orElseThrow(() -> new IllegalStateException("decimal digits not present"));
                int precision = typeHandle.requiredColumnSize();
                if (getDecimalRounding(session) == ALLOW_OVERFLOW && precision > Decimals.MAX_PRECISION) {
                    int scale = min(decimalDigits, getDecimalDefaultScale(session));
                    return Optional.of(decimalColumnMapping(createDecimalType(Decimals.MAX_PRECISION, scale), getDecimalRoundingMode(session)));
                }
                precision = precision + max(-decimalDigits, 0); // Map decimal(p, -s) (negative scale) to decimal(p+s, 0).
                if (precision > Decimals.MAX_PRECISION) {
                    break;
                }
                return Optional.of(decimalColumnMapping(createDecimalType(precision, max(decimalDigits, 0))));

            case Types.CHAR:
                return Optional.of(defaultCharColumnMapping(typeHandle.requiredColumnSize(), true));

            // TODO not all these type constants are necessarily used by the JDBC driver
            case Types.VARCHAR:
            case Types.NVARCHAR:
            case Types.LONGVARCHAR:
            case Types.LONGNVARCHAR: {
                // StarRocks often reports ARRAY exprs as VARCHAR(n) where n == element width (e.g., 20).
                // For pass-through queries(system.query) this causes length check failures on JSON-like array text.
                // Map to unbounded to avoid spurious length exceptions.
                return Optional.of(defaultVarcharColumnMapping(Integer.MAX_VALUE, true));
            }

            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                return Optional.of(ColumnMapping.sliceMapping(VARBINARY, varbinaryReadFunction(), varbinaryWriteFunction(), FULL_PUSHDOWN));

            case Types.DATE:
                return Optional.of(ColumnMapping.longMapping(
                        DATE,
                        dateReadFunctionUsingLocalDate(),
                        starRocksDateWriteFunctionUsingLocalDate()));

            case Types.TIME:
                TimeType timeType = createTimeType(getTimePrecision(typeHandle.requiredColumnSize()));
                requireNonNull(timeType, "timeType is null");
                checkArgument(timeType.getPrecision() <= 9, "Unsupported type precision: %s", timeType);
                return Optional.of(ColumnMapping.longMapping(
                        timeType,
                        starRocksTimeReadFunction(timeType),
                        timeWriteFunction(timeType.getPrecision())));

            // StarRocks supports DATETIME with millisecond and microsecond precision internally,
            // but it does not store or expose the precision as part of column metadata (e.g., COLUMN_SIZE).
            // Therefore, we explicitly use fixed precision instead of deriving it from JDBC metadata.
            case Types.TIMESTAMP:
                TimestampType timestampType = createTimestampType(6);
                checkArgument(timestampType.getPrecision() <= TimestampType.MAX_SHORT_PRECISION, "Precision is out of range: %s", timestampType.getPrecision());
                return Optional.of(ColumnMapping.longMapping(
                        timestampType,
                        starRocksTimestampReadFunction(timestampType),
                        timestampWriteFunction(timestampType)));

            case Types.ARRAY:
                return arrayToTrinoType(session, connection, typeHandle);
        }

        if (getUnsupportedTypeHandling(session) == CONVERT_TO_VARCHAR) {
            return mapToUnboundedVarchar(typeHandle);
        }
        return Optional.empty();
    }

    private ColumnMapping jsonColumnMapping()
    {
        return ColumnMapping.sliceMapping(
                jsonType,
                (resultSet, columnIndex) -> jsonParse(utf8Slice(resultSet.getString(columnIndex))),
                varcharWriteFunction(),
                DISABLE_PUSHDOWN);
    }

    private static Optional<JdbcTypeHandle> toTypeHandle(DecimalType decimalType)
    {
        return Optional.of(new JdbcTypeHandle(Types.NUMERIC, Optional.of("decimal"), Optional.of(decimalType.getPrecision()), Optional.of(decimalType.getScale()), Optional.empty(), Optional.empty()));
    }

    private LongWriteFunction starRocksDateWriteFunctionUsingLocalDate()
    {
        return new LongWriteFunction() {
            @Override
            public String getBindExpression()
            {
                return "CAST(? AS DATE)";
            }

            @Override
            public void set(PreparedStatement statement, int index, long epochDay)
                    throws SQLException
            {
                statement.setString(index, LocalDate.ofEpochDay(epochDay).format(ISO_DATE));
            }
        };
    }

    private static LongReadFunction starRocksTimestampReadFunction(TimestampType timestampType)
    {
        return new LongReadFunction()
        {
            @Override
            public boolean isNull(ResultSet resultSet, int columnIndex)
                    throws SQLException
            {
                // super calls ResultSet#getObject(), which for TIMESTAMP type returns java.sql.Timestamp, for which the conversion can fail if the value isn't a valid instant in server's time zone.
                resultSet.getObject(columnIndex, LocalDateTime.class);
                return resultSet.wasNull();
            }

            @Override
            public long readLong(ResultSet resultSet, int columnIndex)
                    throws SQLException
            {
                return timestampReadFunction(timestampType).readLong(resultSet, columnIndex);
            }
        };
    }

    private static LongReadFunction starRocksTimeReadFunction(TimeType timeType)
    {
        return new LongReadFunction()
        {
            @Override
            public boolean isNull(ResultSet resultSet, int columnIndex)
                    throws SQLException
            {
                // super calls ResultSet#getObject(), which for TIME type returns java.sql.Time, for which the conversion can fail if the value isn't a valid instant in server's time zone.
                resultSet.getObject(columnIndex, String.class);
                return resultSet.wasNull();
            }

            @Override
            public long readLong(ResultSet resultSet, int columnIndex)
                    throws SQLException
            {
                return timeReadFunction(timeType).readLong(resultSet, columnIndex);
            }
        };
    }

    private static int getTimestampPrecision(int timestampColumnSize)
    {
        if (timestampColumnSize == ZERO_PRECISION_TIMESTAMP_COLUMN_SIZE) {
            return 0;
        }
        int timestampPrecision = timestampColumnSize - ZERO_PRECISION_TIMESTAMP_COLUMN_SIZE - 1;
        verify(1 <= timestampPrecision && timestampPrecision <= MAX_SUPPORTED_DATE_TIME_PRECISION, "Unexpected timestamp precision %s calculated from timestamp column size %s", timestampPrecision, timestampColumnSize);
        return timestampPrecision;
    }

    private static int getTimePrecision(int timeColumnSize)
    {
        if (timeColumnSize == ZERO_PRECISION_TIME_COLUMN_SIZE) {
            return 0;
        }
        int timePrecision = timeColumnSize - ZERO_PRECISION_TIME_COLUMN_SIZE - 1;
        verify(1 <= timePrecision && timePrecision <= MAX_SUPPORTED_DATE_TIME_PRECISION, "Unexpected time precision %s calculated from time column size %s", timePrecision, timeColumnSize);
        return timePrecision;
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        try {
            createTable(session, tableMetadata, tableMetadata.getTable().getTableName());
        }
        catch (SQLException e) {
            boolean exists = SQL_STATE_ER_TABLE_EXISTS_ERROR.equals(e.getSQLState());
            throw new TrinoException(exists ? ALREADY_EXISTS : JDBC_ERROR, e);
        }
    }

    @Override
    protected JdbcOutputTableHandle createTable(
            ConnectorSession session,
            Connection connection,
            ConnectorTableMetadata tableMetadata,
            RemoteIdentifiers remoteIdentifiers,
            String catalog,
            String remoteSchema,
            String remoteTable,
            String remoteTargetTableName,
            Optional<ColumnMetadata> pageSinkIdColumn)
            throws SQLException
    {
        List<ColumnMetadata> columns = tableMetadata.getColumns();
        ImmutableList.Builder<String> columnNames = ImmutableList.builderWithExpectedSize(columns.size());
        ImmutableList.Builder<Type> columnTypes = ImmutableList.builderWithExpectedSize(columns.size());
        // columnList is only used for createTableSql - the extraColumns are not included on the JdbcOutputTableHandle
        ImmutableList.Builder<String> columnList = ImmutableList.builderWithExpectedSize(columns.size() + (pageSinkIdColumn.isPresent() ? 1 : 0));

        for (ColumnMetadata column : columns) {
            String columnName = identifierMapping.toRemoteColumnName(remoteIdentifiers, column.getName());
            verifyColumnName(connection.getMetaData(), columnName);
            columnNames.add(columnName);
            columnTypes.add(column.getType());
            columnList.add(getColumnDefinitionSql(session, column, columnName));
        }

        Optional<String> pageSinkIdColumnName = Optional.empty();
        if (pageSinkIdColumn.isPresent()) {
            String columnName = identifierMapping.toRemoteColumnName(remoteIdentifiers, pageSinkIdColumn.get().getName());
            pageSinkIdColumnName = Optional.of(columnName);
            verifyColumnName(connection.getMetaData(), columnName);
            columnList.add(getColumnDefinitionSql(session, pageSinkIdColumn.get(), columnName));
        }

        boolean isPkTable = isPkTable(connection, remoteSchema, remoteTable);

        RemoteTableName remoteTableName = new RemoteTableName(Optional.ofNullable(catalog), Optional.ofNullable(remoteSchema), remoteTargetTableName);
        for (String sql : createTableSqls(remoteTableName, columnList.build(), tableMetadata)) {
            execute(session, connection, sql);
        }

        return new StarRocksOutputTableHandle(
                new RemoteTableName(Optional.ofNullable(catalog), Optional.ofNullable(remoteSchema), remoteTable),
                columnNames.build(),
                columnTypes.build(),
                Optional.empty(),
                Optional.of(remoteTargetTableName),
                pageSinkIdColumnName,
                isPkTable);
    }

    @Override
    protected void copyTableSchema(ConnectorSession session, Connection connection, String catalogName, String schemaName, String tableName, String newTableName, List<String> columnNames)
    {
        String tableCopyFormat = "CREATE TABLE %s LIKE %s";
        String sql = format(
                tableCopyFormat,
                quoted(catalogName, schemaName, newTableName),
                quoted(catalogName, schemaName, tableName));
        try {
            execute(session, connection, sql);
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    @Override
    protected void addColumn(ConnectorSession session, Connection connection, RemoteTableName table, ColumnMetadata column)
            throws SQLException
    {
        String columnName = column.getName();
        verifyColumnName(connection.getMetaData(), columnName);
        String remoteColumnName = getIdentifierMapping().toRemoteColumnName(getRemoteIdentifiers(connection), columnName);
        String sql = format(
                "ALTER TABLE %s ADD COLUMN %s",
                quoted(table),
                getColumnDefinitionSql(session, column, remoteColumnName));
        execute(session, connection, sql);
    }

    @Override
    public WriteMapping toWriteMapping(ConnectorSession session, Type type)
    {
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
            return WriteMapping.longMapping("integer", integerWriteFunction());
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

        if (type instanceof DecimalType decimalType) {
            String dataType = format("decimal(%s, %s)", decimalType.getPrecision(), decimalType.getScale());
            if (decimalType.isShort()) {
                return WriteMapping.longMapping(dataType, shortDecimalWriteFunction(decimalType));
            }
            return WriteMapping.objectMapping(dataType, longDecimalWriteFunction(decimalType));
        }

        if (type == DATE) {
            return WriteMapping.longMapping("date", starRocksDateWriteFunctionUsingLocalDate());
        }

        if (type instanceof TimestampType timestampType) {
            if (timestampType.getPrecision() <= MAX_SUPPORTED_DATE_TIME_PRECISION) {
                return WriteMapping.longMapping("datetime", timestampWriteFunction(timestampType));
            }
            throw new TrinoException(NOT_SUPPORTED, "Unsupported column type: " + type.getDisplayName());
        }

        if (VARBINARY.equals(type)) {
            return WriteMapping.sliceMapping("varbinary", varbinaryWriteFunction());
        }

        if (type instanceof CharType charType) {
            return WriteMapping.sliceMapping("char(" + charType.getLength() + ")", charWriteFunction());
        }

        if (type instanceof VarcharType varcharType) {
            String dataType = varcharType.getLength().map(l -> "varchar(" + l + ")").orElse("string");
            return WriteMapping.sliceMapping(dataType, varcharWriteFunction());
        }

        if (type.equals(jsonType)) {
            return WriteMapping.sliceMapping("json", varcharWriteFunction());
        }

        throw new TrinoException(NOT_SUPPORTED, "Unsupported column type: " + type.getDisplayName());
    }

    @Override
    protected Optional<BiFunction<String, Long, String>> limitFunction()
    {
        return Optional.of((sql, limit) -> sql + " LIMIT " + limit);
    }

    @Override
    public boolean isLimitGuaranteed(ConnectorSession session)
    {
        return true;
    }

    @Override
    public boolean supportsTopN(ConnectorSession session, JdbcTableHandle handle, List<JdbcSortItem> sortOrder)
    {
        return true;
    }

    @Override
    protected Optional<TopNFunction> topNFunction()
    {
        return Optional.of((query, sortItems, limit) -> {
            String orderBy = sortItems.stream()
                    .flatMap(sortItem -> {
                        String ordering = sortItem.sortOrder().isAscending() ? "ASC" : "DESC";
                        String columnSorting = format("%s %s", quoted(sortItem.column().getColumnName()), ordering);

                        switch (sortItem.sortOrder()) {
                            case ASC_NULLS_FIRST:
                            case DESC_NULLS_LAST:
                                return Stream.of(columnSorting);

                            case ASC_NULLS_LAST:
                                return Stream.of(
                                        format("ISNULL(%s) ASC", quoted(sortItem.column().getColumnName())),
                                        columnSorting);
                            case DESC_NULLS_FIRST:
                                return Stream.of(
                                        format("ISNULL(%s) DESC", quoted(sortItem.column().getColumnName())),
                                        columnSorting);
                        }
                        throw new UnsupportedOperationException("Unsupported sort order: " + sortItem.sortOrder());
                    })
                    .collect(joining(", "));
            return format("%s ORDER BY %s LIMIT %s", query, orderBy, limit);
        });
    }

    @Override
    public boolean isTopNGuaranteed(ConnectorSession session)
    {
        return true;
    }

    @Override
    public Optional<ParameterizedExpression> convertPredicate(ConnectorSession session, ConnectorExpression expression, Map<String, ColumnHandle> assignments)
    {
        return connectorExpressionRewriter.rewrite(session, expression, assignments);
    }

    @Override
    public Optional<PreparedQuery> implementJoin(
            ConnectorSession session,
            JoinType joinType,
            PreparedQuery leftSource,
            Map<JdbcColumnHandle, String> leftProjections,
            PreparedQuery rightSource,
            Map<JdbcColumnHandle, String> rightProjections,
            List<ParameterizedExpression> joinConditions,
            JoinStatistics statistics)
    {
        if (joinType == JoinType.FULL_OUTER) {
            // Not supported in MySQL
            return Optional.empty();
        }
        return implementJoinCostAware(
                session,
                joinType,
                leftSource,
                rightSource,
                statistics,
                () -> super.implementJoin(session, joinType, leftSource, leftProjections, rightSource, rightProjections, joinConditions, statistics));
    }

    @Override
    public TableStatistics getTableStatistics(ConnectorSession session, JdbcTableHandle handle)
    {
        if (!statisticsEnabled) {
            return TableStatistics.empty();
        }
        if (!handle.isNamedRelation()) {
            return TableStatistics.empty();
        }
        try {
            return readTableStatistics(session, handle);
        }
        catch (SQLException | RuntimeException e) {
            throwIfInstanceOf(e, TrinoException.class);
            throw new TrinoException(JDBC_ERROR, "Failed fetching statistics for table: " + handle, e);
        }
    }

    private TableStatistics readTableStatistics(ConnectorSession session, JdbcTableHandle table)
            throws SQLException
    {
        checkArgument(table.isNamedRelation(), "Relation is not a table: %s", table);

        log.debug("Reading statistics for %s", table);
        try (Connection connection = connectionFactory.openConnection(session);
                Handle handle = Jdbi.open(connection)) {
            StatisticsDao statisticsDao = new StatisticsDao(handle);

            Long rowCount = statisticsDao.getRowCount(table);
            log.debug("Estimated row count of table %s is %s", table, rowCount);

            if (rowCount == null) {
                // Table not found, or is a view.
                return TableStatistics.empty();
            }

            TableStatistics.Builder tableStatistics = TableStatistics.builder();
            tableStatistics.setRowCount(Estimate.of(rowCount));

            Map<String, RawColumnStats> rawColumnStats = statisticsDao.getRawColumnStats(table);

            if (rawColumnStats.isEmpty()) {
                log.debug("No raw column statistics read");
                return tableStatistics.build();
            }

            for (JdbcColumnHandle column : JdbcMetadata.getColumns(session, this, table)) {
                ColumnStatistics.Builder columnStatisticsBuilder = ColumnStatistics.builder();

                String columnName = column.getColumnName();
                RawColumnStats rawStats = rawColumnStats.get(columnName);
                if (rawStats != null) {

                    rawStats.nullCount().ifPresent(nullCountValue -> {
                        if (rowCount > 0) {
                            double nullsFraction = (double) nullCountValue / rowCount;
                            columnStatisticsBuilder.setNullsFraction(Estimate.of(nullsFraction));
                        }
                    });

                    rawStats.dataSize().ifPresent(size ->
                            columnStatisticsBuilder.setDataSize(Estimate.of(size)));
                }

                tableStatistics.setColumnStatistics(column, columnStatisticsBuilder.build());
            }

            tableStatistics.setRowCount(Estimate.of(rowCount));
            return tableStatistics.build();
        }
    }

    private static Optional<ColumnHistogram> getColumnHistogram(Map<String, String> columnHistograms, String columnName)
    {
        return Optional.ofNullable(columnHistograms.get(columnName))
                .flatMap(histogramJson -> {
                    try {
                        return Optional.of(HISTOGRAM_CODEC.fromJson(histogramJson));
                    }
                    catch (RuntimeException e) {
                        log.warn(e, "Failed to parse column statistics histogram: %s", histogramJson);
                        return Optional.empty();
                    }
                });
    }

    private static void updateColumnStatisticsFromIndexStatistics(JdbcTableHandle table, String columnName, ColumnStatistics.Builder columnStatistics, ColumnIndexStatistics columnIndexStatistics)
    {
        // Prefer CARDINALITY from index statistics over NDV from a histogram.
        // Index column might be NULLABLE. Then CARDINALITY includes all
        columnStatistics.setDistinctValuesCount(Estimate.of(columnIndexStatistics.cardinality()));

        if (!columnIndexStatistics.nullable) {
            double knownNullFraction = columnStatistics.build().getNullsFraction().getValue();
            if (knownNullFraction > 0) {
                log.warn("Inconsistent statistics, null fraction for a column %s, %s, that is not nullable according to index statistics: %s", table, columnName, knownNullFraction);
            }
            columnStatistics.setNullsFraction(Estimate.zero());
        }
    }

    private record StatisticsDao(Handle handle) {

        private StatisticsDao(Handle handle) {
            this.handle = requireNonNull(handle, "handle is null");
        }

        Long getRowCount(JdbcTableHandle table) {
            RemoteTableName remoteTableName = table.getRequiredNamedRelation().getRemoteTableName();
            return handle.createQuery("" +
                            "SELECT TABLE_ROWS FROM INFORMATION_SCHEMA.TABLES " +
                            "WHERE TABLE_SCHEMA = :schema AND TABLE_NAME = :table_name " +
                            "AND TABLE_TYPE = 'BASE TABLE' ")
                    .bind("schema", remoteTableName.getCatalogName().orElse(null))
                    .bind("table_name", remoteTableName.getTableName())
                    .mapTo(Long.class)
                    .findOne()
                    .orElse(null);
        }

        Map<String, RawColumnStats> getRawColumnStats(JdbcTableHandle table) {
            RemoteTableName remoteTableName = table.getRequiredNamedRelation().getRemoteTableName();
            String tableNameWithSchema = remoteTableName.getCatalogName().orElseThrow() + "." + remoteTableName.getTableName();
            log.debug("Querying StarRocks stats for fully qualified table: %s", tableNameWithSchema);

            return handle.createQuery("" +
                            "SELECT " +
                            "  column_name, " +
                            "  SUM(null_count) AS null_count, " +
                            "  SUM(data_size) AS data_size, " +
                            "  MAX(max) AS max_value, " +
                            "  MIN(min) AS min_value " +
                            "FROM _statistics_.column_statistics " +
                            "WHERE table_name = :tableNameWithSchema " +
                            "GROUP BY column_name")
                    .bind("tableNameWithSchema", tableNameWithSchema)
                    .map((rs, ctx) -> {
                        String columnName = rs.getString("column_name");

                        long nullCountValue = rs.getLong("null_count");
                        Optional<Long> nullCount = rs.wasNull() ? Optional.empty() : Optional.of(nullCountValue);

                        long dataSizeValue = rs.getLong("data_size");
                        Optional<Long> dataSize = rs.wasNull() ? Optional.empty() : Optional.of(dataSizeValue);

                        String max = rs.getString("max_value");
                        Optional<String> maxOpt = rs.wasNull() ? Optional.empty() : Optional.of(max);

                        String min = rs.getString("min_value");
                        Optional<String> minOpt = rs.wasNull() ? Optional.empty() : Optional.of(min);

                        return new SimpleEntry<>(columnName, new RawColumnStats(
                                nullCount,
                                maxOpt,
                                minOpt,
                                dataSize));
                    })
                    .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
        }

        Map<String, ColumnIndexStatistics> getColumnIndexStatistics(JdbcTableHandle table) {
            RemoteTableName remoteTableName = table.getRequiredNamedRelation().getRemoteTableName();
            return handle.createQuery("" +
                            "SELECT " +
                            "  COLUMN_NAME, " +
                            "  MAX(NULLABLE) AS NULLABLE, " +
                            "  MAX(CARDINALITY) AS CARDINALITY " +
                            "FROM INFORMATION_SCHEMA.STATISTICS " +
                            "WHERE TABLE_SCHEMA = :schema AND TABLE_NAME = :table_name " +
                            "AND SEQ_IN_INDEX = 1 " + // first column in the index
                            "AND SUB_PART IS NULL " + // ignore cases where only a column prefix is indexed
                            "AND CARDINALITY IS NOT NULL " +
                            // CARDINALITY might be null (https://stackoverflow.com/a/42242729/65458)
                            "GROUP BY COLUMN_NAME") // there might be multiple indexes on a column
                    .bind("schema", remoteTableName.getCatalogName().orElse(null))
                    .bind("table_name", remoteTableName.getTableName())
                    .map((rs, ctx) -> {
                        String columnName = rs.getString("COLUMN_NAME");

                        boolean nullable = rs.getString("NULLABLE").equalsIgnoreCase("YES");
                        checkState(!rs.wasNull(), "NULLABLE is null");

                        long cardinality = rs.getLong("CARDINALITY");
                        checkState(!rs.wasNull(), "CARDINALITY is null");

                        return new SimpleEntry<>(columnName, new ColumnIndexStatistics(nullable, cardinality));
                    })
                    .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
        }

        // INFORMATION_SCHEMA.COLUMN_STATISTICS is not available in StarRocks.
        // Additionally, _statistics_.column_statistics does not include a histogram column.
        Map<String, String> getColumnHistograms(JdbcTableHandle table) {
            try {
                handle.execute("SELECT 1 FROM _statistics_.column_statistics WHERE 0=1");
            } catch (UnableToExecuteStatementException e) {
                if (e.getCause() instanceof SQLSyntaxErrorException
                        && ((SQLSyntaxErrorException) e.getCause()).getErrorCode() == ER_UNKNOWN_TABLE) {
                    log.debug("_statistics_.column_statistics table is not available: %s", e);
                    return ImmutableMap.of();
                }
            }

            RemoteTableName remoteTableName = table.getRequiredNamedRelation().getRemoteTableName();
            return handle.createQuery("" +
                            "SELECT COLUMN_NAME, HISTOGRAM FROM INFORMATION_SCHEMA.COLUMN_STATISTICS " +
                            "WHERE SCHEMA_NAME = :schema AND TABLE_NAME = :table_name")
                    .bind("schema", remoteTableName.getCatalogName().orElse(null))
                    .bind("table_name", remoteTableName.getTableName())
                    .map((rs, ctx) -> new SimpleEntry<>(rs.getString("COLUMN_NAME"), rs.getString("HISTOGRAM")))
                    .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
        }
    }

    private record RawColumnStats(
            Optional<Long> nullCount,
            Optional<String> max,
            Optional<String> min,
            Optional<Long> dataSize)
    {
        @Override
        public String toString() {
        return toStringHelper(this)
                .add("nullCount", nullCount)
                .add("max",   max)
                .add("min",   min)
                .add("dataSize", dataSize)
                .toString();
    }
    }

    private record ColumnIndexStatistics(boolean nullable, long cardinality) {

        @Override
        public String toString() {
            return toStringHelper(this)
                    .add("cardinality", cardinality())
                    .add("nullable", nullable)
                    .toString();
        }
    }

    public static class ColumnHistogram {

        private final Optional<Double> nullFraction;
        private final Optional<String> histogramType;
        private final Optional<List<List<Object>>> buckets;

        @JsonCreator
        public ColumnHistogram(
                @JsonProperty("null-values") Optional<Double> nullFraction,
                @JsonProperty("histogram-type") Optional<String> histogramType,
                @JsonProperty("buckets") Optional<List<List<Object>>> buckets)
        {
            this.nullFraction = nullFraction;
            this.histogramType = histogramType;
            this.buckets = buckets;
        }

        public void updateColumnStatistics(ColumnStatistics.Builder columnStatistics)
        {
            nullFraction.map(Estimate::of).ifPresent(columnStatistics::setNullsFraction);
            getDistinctValuesCount().map(Estimate::of).ifPresent(columnStatistics::setDistinctValuesCount);
        }

        private Optional<Long> getDistinctValuesCount()
        {
            if (histogramType.isPresent() && buckets.isPresent()) {
                switch (histogramType.get()) {
                    case "singleton":
                        return Optional.of((long) buckets.get().size());

                    case "equi-height":
                        long distinctValues = 0;
                        for (List<?> bucket : buckets.get()) {
                            distinctValues += ((Number) bucket.get(3)).longValue();
                        }
                        return Optional.of(distinctValues);

                    default:
                        log.debug("Unsupported histogram type: %s", histogramType.get());
                }
            }
            else {
                log.debug("Unsupported histogram: type: %s, bucket count: %s", histogramType, buckets.map(List::size));
            }
            return Optional.empty();
        }

        public long getUpdateRowCount(long rowCount)
        {
            return getDistinctValuesCount()
                    .map(distinctValuesCount -> max(rowCount, distinctValuesCount))
                    .orElse(rowCount);
        }
    }

    private boolean isPkTable(Connection connection, String schemaName, String tableName)
    {
        try (java.sql.Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(String.format("SELECT TABLE_MODEL FROM information_schema.tables_config WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s'", schemaName, tableName))) {
            if (resultSet.next()) {
                return resultSet.getString("TABLE_MODEL").equalsIgnoreCase(TABLE_MODEL_PRIMARY_KEYS);
            }
            return false;
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    @Override
    protected void execute(ConnectorSession session, Connection connection, String query)
            throws SQLException
    {
        try (Statement statement = connection.createStatement()) {
            statement.setQueryTimeout(getQueryTimeout(session));
            String modifiedQuery = queryModifier.apply(session, query);
            log.debug("Execute: %s", modifiedQuery);
            statement.execute(modifiedQuery);
        }
        catch (SQLException e) {
            e.addSuppressed(new RuntimeException("Query: " + query));
            throw e;
        }
    }

    @Override
    public Map<String, Object> getTableProperties(ConnectorSession session, JdbcTableHandle handle) {
        Map<String, Object> properties = new HashMap<>(super.getTableProperties(session, handle));

        RemoteTableName remoteTableName = handle.getRequiredNamedRelation().getRemoteTableName();

        String query = format("SHOW CREATE TABLE %s", quoted(remoteTableName));

        try (Connection connection = connectionFactory.openConnection(session);
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(query)) {

            if (resultSet.next()) {
                String createTableSql = resultSet.getString(2);

                extractProperty(createTableSql, ENGINE_PATTERN)
                        .ifPresent(v -> properties.put(ENGINE, v));

                Optional<String> tableType = extractProperty(
                        createTableSql,
                        TABLE_TYPE_PATTERN,
                        1
                );
                tableType.ifPresent(v -> properties.put(TABLE_TYPE, v));

                Optional<String> tableKeys = extractProperty(
                        createTableSql,
                        TABLE_TYPE_PATTERN,
                        2
                );
                tableKeys.ifPresent(v -> properties.put(TABLE_KEYS, v));

                extractProperty(createTableSql,
                        PARTITION_BY_PATTERN, 1)
                        .map(String::trim)
                        .ifPresent(v -> properties.put(PARTITION_BY, v));

                Optional<String> distributedByMatch = extractProperty(createTableSql, DISTRIBUTED_BY_PATTERN,
                        1);
                distributedByMatch.ifPresent(v -> properties.put(DISTRIBUTED_BY, v));

                extractProperty(createTableSql, BUCKETS_PATTERN)
                        .ifPresent(v -> properties.put(BUCKETS, v));

                extractProperty(createTableSql, ORDER_BY_PATTERN, 1)
                        .ifPresent(v -> properties.put(ORDER_BY, v));

                extractProperty(createTableSql, TABLE_COMMENT_PATTERN, 1)
                        .ifPresent(v -> properties.put(TABLE_COMMENT, v));

                Optional<String> propsBlock = extractProperty(createTableSql, PROPS_BLOCK_PATTERN, 1);

                if (propsBlock.isPresent()) {
                    String block = propsBlock.get();
                    extractPropertyFromBlock(block, REPLICATION_NUM_PATTERN)
                            .ifPresent(v -> properties.put(REPLICATION_NUMBER, v));

                    extractPropertyFromBlock(block, BUCKET_SIZE_PATTERN)
                            .ifPresent(v -> properties.put(BUCKET_SIZE, v));

                    extractPropertyFromBlock(block, BLOOM_FILTER_PATTERN)
                            .ifPresent(v -> properties.put(BLOOM_FILTER_COLUMNS, v));
                }
            }
        } catch (SQLException e) {
            log.warn(e, "Failed to retrieve StarRocks table properties for %s", remoteTableName);
        }
        return properties;
    }

    private static Optional<String> extractProperty(String sql, Pattern pattern, int groupIndex) {
        Matcher matcher = pattern.matcher(sql);
        if (matcher.find() && matcher.groupCount() >= groupIndex) {
            return Optional.of(matcher.group(groupIndex).trim());
        }
        return Optional.empty();
    }

    private static Optional<String> extractProperty(String sql, Pattern pattern) {
        return extractProperty(sql, pattern, 1);
    }

    private static Optional<String> extractPropertyFromBlock(String block, Pattern pattern) {
        Matcher matcher = pattern.matcher(block);
        if (matcher.find()) {
            return Optional.of(matcher.group(1));
        }
        return Optional.empty();
    }
}
