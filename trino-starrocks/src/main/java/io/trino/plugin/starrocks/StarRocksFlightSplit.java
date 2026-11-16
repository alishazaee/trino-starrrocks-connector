package io.trino.plugin.starrocks;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.spi.HostAddress;
import io.trino.spi.connector.ConnectorSplit;
import java.util.List;
import static java.util.Objects.requireNonNull;

public final class StarRocksFlightSplit implements ConnectorSplit
{
    private final String endpoint;
    private final byte[] ticket;
    private final List<JdbcColumnHandle> columns;
    private final List<HostAddress> addresses;
    private final boolean remotelyAccessible;

    @JsonCreator
    public StarRocksFlightSplit(
            @JsonProperty("endpoint") String endpoint,
            @JsonProperty("ticket") byte[] ticket,
            @JsonProperty("columns") List<JdbcColumnHandle> columns,
            @JsonProperty("addresses") List<HostAddress> addresses,
            @JsonProperty("remotelyAccessible") boolean remotelyAccessible)
    {
        this.endpoint = StarRocksFlightConfig.validateEndpoint(endpoint);
        this.ticket = requireNonNull(ticket, "ticket is null").clone();
        this.columns = List.copyOf(columns);
        this.addresses = List.copyOf(addresses);
        this.remotelyAccessible = remotelyAccessible;
        if (!remotelyAccessible && addresses.isEmpty()) {
            throw new IllegalArgumentException("Required locality needs a worker address");
        }
    }

    @JsonProperty public String getEndpoint() { return endpoint; }
    @JsonProperty public byte[] getTicket() { return ticket.clone(); }
    @JsonProperty public List<JdbcColumnHandle> getColumns() { return columns; }
    @Override @JsonProperty public List<HostAddress> getAddresses() { return addresses; }
    @Override @JsonProperty public boolean isRemotelyAccessible() { return remotelyAccessible; }
    @Override public long getRetainedSizeInBytes()
    {
        return 128L + endpoint.length() * 2L + ticket.length + columns.stream().mapToLong(JdbcColumnHandle::getRetainedSizeInBytes).sum() +
                addresses.stream().mapToLong(address -> 64L + address.toString().length() * 2L).sum();
    }
    @Override public String toString() { return "StarRocksFlightSplit{endpoint=" + endpoint + ", locality=" + addresses + "}"; }
}
