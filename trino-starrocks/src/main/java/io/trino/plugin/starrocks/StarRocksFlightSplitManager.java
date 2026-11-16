package io.trino.plugin.starrocks;

import com.google.inject.Inject;
import io.trino.plugin.jdbc.*;
import io.trino.spi.connector.*;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public final class StarRocksFlightSplitManager implements ConnectorSplitManager
{
    private final JdbcClient jdbc;
    private final StarRocksFlightClient flight;
    private final JdbcSplitManager fallback;
    private final StarRocksFlightConfig config;

    @Inject
    public StarRocksFlightSplitManager(JdbcClient jdbc, StarRocksFlightClient flight, StarRocksFlightConfig config)
    {
        this.jdbc = jdbc;
        this.flight = flight;
        this.config = config;
        fallback = new JdbcSplitManager(jdbc);
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transaction, ConnectorSession session,
            ConnectorTableHandle handle, Set<ColumnHandle> dynamicFilterColumns, Constraint constraint)
    {
        if (handle instanceof JdbcTableHandle table && table.getAuthorization().isEmpty() && table.getUpdateAssignments().isEmpty()) {
            // Pushed-down relations already carry their output columns. Avoid a
            // second information_schema lookup, which is both unnecessary and
            // can block behind the distributed query being planned.
            List<JdbcColumnHandle> columns = table.getColumns()
                    .orElseGet(() -> JdbcMetadata.getColumns(session, jdbc, table));
            if (!columns.isEmpty() && columns.stream().allMatch(c -> StarRocksArrowReader.supports(c.getColumnType())) &&
                    columns.stream().noneMatch(c -> c.getColumnName().equals("$merge_row_id"))) {
                PreparedQuery query = jdbc.prepareQuery(session, table, Optional.empty(), columns, Map.of());
                // StarRocks Flight 3.5 does not support bound parameters. Keep their semantics on JDBC.
                if (query.parameters().isEmpty()) {
                    return new FixedSplitSource(flight.execute(session, query.query(), columns));
                }
            }
        }
        if (config.getLocality() == StarRocksFlightConfig.Locality.REQUIRED) {
            throw new io.trino.spi.TrinoException(io.trino.spi.StandardErrorCode.NOT_SUPPORTED,
                    "Required Flight locality cannot use JDBC fallback for this scan (parameters, types, or operation unsupported)");
        }
        return fallback.getSplits(transaction, session, handle, dynamicFilterColumns, constraint);
    }
}
