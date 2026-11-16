package io.trino.plugin.starrocks;

import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcQueryEventListener;
import io.trino.plugin.jdbc.credential.CredentialProvider;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import jakarta.annotation.PreDestroy;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.apache.arrow.flight.*;
import org.apache.arrow.flight.grpc.CredentialCallOption;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;

/** Keeps the submitting FE session alive until all distributed readers finish. */
public final class StarRocksFlightClient implements JdbcQueryEventListener
{
    private static final Logger LOG = Logger.get(StarRocksFlightClient.class);
    private final StarRocksFlightConfig config;
    private final CredentialProvider credentials;
    private final BufferAllocator allocator;
    private final ConcurrentHashMap<String, List<RemoteQuery>> queries = new ConcurrentHashMap<>();

    @Inject
    public StarRocksFlightClient(StarRocksFlightConfig config, CredentialProvider credentials)
    {
        this.config = config;
        this.credentials = credentials;
        StarRocksFlightConfig.validateEndpoint(config.getEndpoint());
        if (config.getBackendWorkers().isEmpty()) {
            throw new IllegalArgumentException("Flight requires starrocks.flight.backend-workers");
        }
        allocator = new RootAllocator(config.getMaxMemoryBytes());
    }

    BufferAllocator newReadAllocator()
    {
        return allocator.newChildAllocator("starrocks-flight-read", 0, config.getMaxMemoryBytes());
    }

    @Override public void beginQuery(ConnectorSession session)
    {
        queries.putIfAbsent(session.getQueryId(), new ArrayList<>());
    }

    List<StarRocksFlightSplit> execute(ConnectorSession session, String sql, List<JdbcColumnHandle> columns)
    {
        RemoteQuery remote = new RemoteQuery();
        try {
            remote.client = FlightClient.builder(allocator, new Location(config.getEndpoint())).build();
            remote.auth = remote.client.authenticateBasicToken(
                    credentials.getConnectionUser(Optional.of(session.getIdentity())).orElse(""),
                    credentials.getConnectionPassword(Optional.of(session.getIdentity())).orElse(""))
                    .orElseThrow(() -> new IllegalStateException("StarRocks Flight did not return a bearer credential"));
            FlightInfo info = new FlightSqlClient(remote.client).execute(sql, remote.auth,
                    CallOptions.timeout(config.getTimeoutSeconds(), TimeUnit.SECONDS));
            List<StarRocksFlightSplit> splits = splits(info, columns, config);
            boolean[] registered = {false};
            queries.computeIfPresent(session.getQueryId(), (id, active) -> {
                active.add(remote);
                registered[0] = true;
                return active;
            });
            if (!registered[0]) {
                throw new IllegalStateException("Trino query ended before Flight planning completed");
            }
            return splits;
        }
        catch (Exception e) {
            remote.close();
            throw new TrinoException(JDBC_ERROR, "StarRocks Flight planning failed; verify direct BE endpoints and backend-workers mapping", e);
        }
    }

    static List<StarRocksFlightSplit> splits(FlightInfo info, List<JdbcColumnHandle> columns, StarRocksFlightConfig config)
    {
        if (info.getOrdered() && info.getEndpoints().size() > 1) {
            throw new IllegalArgumentException("Ordered multi-endpoint Flight results cannot be scheduled independently");
        }
        if (info.getSchema().getFields().size() != columns.size()) {
            throw new IllegalArgumentException("Flight output does not match planned columns");
        }
        List<StarRocksFlightSplit> result = new ArrayList<>();
        for (FlightEndpoint endpoint : info.getEndpoints()) {
            // Multiple locations in ONE endpoint are alternatives, not independent result partitions.
            String location = endpoint.getLocations().stream()
                    .map(l -> StarRocksFlightConfig.validateEndpoint(l.getUri().toString()))
                    .filter(config.getBackendWorkers()::containsKey)
                    .findFirst()
                    .orElseThrow(() -> new IllegalArgumentException("No explicitly mapped direct backend for Flight endpoint"));
            result.add(new StarRocksFlightSplit(location, endpoint.getTicket().getBytes(), columns,
                    config.getLocality() == StarRocksFlightConfig.Locality.OFF ? List.of() : List.of(config.getBackendWorkers().get(location)),
                    config.getLocality() != StarRocksFlightConfig.Locality.REQUIRED));
        }
        return List.copyOf(result);
    }

    @Override public void cleanupQuery(ConnectorSession session)
    {
        List<RemoteQuery> active = queries.remove(session.getQueryId());
        if (active != null) { active.forEach(RemoteQuery::close); }
    }

    @PreDestroy public void shutdown()
    {
        queries.keySet().forEach(id -> {
            List<RemoteQuery> active = queries.remove(id);
            if (active != null) { active.forEach(RemoteQuery::close); }
        });
        allocator.close();
    }

    private static final class RemoteQuery
    {
        private FlightClient client;
        private CredentialCallOption auth;

        void close()
        {
            if (client == null) { return; }
            try {
                if (auth != null) {
                    client.closeSession(new CloseSessionRequest(), auth, CallOptions.timeout(5, TimeUnit.SECONDS));
                }
            }
            catch (RuntimeException e) { LOG.warn("Unable to close StarRocks Flight session (%s)", e.getClass().getSimpleName()); }
            finally {
                try { client.close(); }
                catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            }
        }
    }
}
