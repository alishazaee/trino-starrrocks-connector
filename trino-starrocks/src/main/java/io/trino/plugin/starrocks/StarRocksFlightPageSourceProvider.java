package io.trino.plugin.starrocks;

import com.google.inject.Inject;
import io.trino.plugin.jdbc.ForJdbcClient;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcPageSourceProvider;
import io.trino.spi.HostAddress;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.*;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;

import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;

public final class StarRocksFlightPageSourceProvider implements ConnectorPageSourceProvider
{
    private static final io.airlift.log.Logger LOG = io.airlift.log.Logger.get(StarRocksFlightPageSourceProvider.class);
    private final JdbcPageSourceProvider jdbc;
    private final StarRocksFlightClient flight;
    private final ExecutorService executor;
    private final StarRocksFlightConfig config;
    private final io.trino.spi.NodeManager nodes;

    @Inject
    public StarRocksFlightPageSourceProvider(JdbcPageSourceProvider jdbc, StarRocksFlightClient flight,
            @ForJdbcClient ExecutorService executor, StarRocksFlightConfig config, io.trino.spi.NodeManager nodes)
    {
        this.jdbc = jdbc;
        this.flight = flight;
        this.executor = executor;
        this.config = config;
        this.nodes = nodes;
    }

    @Override
    public ConnectorPageSource createPageSource(ConnectorTransactionHandle transaction, ConnectorSession session,
            ConnectorSplit split, ConnectorTableHandle table, Optional<ConnectorTableCredentials> credentials,
            List<ColumnHandle> columns, DynamicFilter dynamicFilter)
    {
        if (!(split instanceof StarRocksFlightSplit remote)) {
            return jdbc.createPageSource(transaction, session, split, table, credentials, columns, dynamicFilter);
        }
        verifyRequiredLocality(config.getLocality(), remote, nodes.getCurrentNode().getHostAndPort());
        List<JdbcColumnHandle> requested = columns.stream().map(JdbcColumnHandle.class::cast).toList();
        List<Integer> channels = requested.stream().map(remote.getColumns()::indexOf).toList();
        if (channels.contains(-1) || !config.getBackendWorkers().containsKey(remote.getEndpoint())) {
            throw new IllegalArgumentException("Invalid Flight column selection or unconfigured backend endpoint");
        }
        LOG.debug("Flight read query=%s worker=%s endpoint=%s preferredWorkers=%s", session.getQueryId(),
                nodes.getCurrentNode().getHostAndPort(), remote.getEndpoint(), remote.getAddresses());
        return new StarRocksFlightPageSource(remote, requested.stream().map(JdbcColumnHandle::getColumnType).toList(),
                channels, flight.newReadAllocator(), executor, config.getTimeoutSeconds());
    }

    static void verifyRequiredLocality(StarRocksFlightConfig.Locality locality, StarRocksFlightSplit split, HostAddress currentWorker)
    {
        if (locality == StarRocksFlightConfig.Locality.REQUIRED && !split.getAddresses().contains(currentWorker)) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR,
                    "Flight locality violation: endpoint %s was scheduled on %s instead of %s"
                            .formatted(split.getEndpoint(), currentWorker, split.getAddresses()));
        }
    }
}
