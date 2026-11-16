package io.trino.plugin.starrocks;

import io.airlift.configuration.Config;
import io.trino.spi.HostAddress;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import java.net.URI;
import java.util.LinkedHashMap;
import java.util.Map;

/** Flight is opt-in. Endpoint mappings also restrict which servers may receive tickets. */
public class StarRocksFlightConfig
{
    public enum Locality { OFF, PREFERRED, REQUIRED }

    private boolean enabled;
    private String endpoint = "";
    private Locality locality = Locality.PREFERRED;
    private Map<String, HostAddress> backendWorkers = Map.of();
    private long maxMemoryBytes = 268435456;
    private int timeoutSeconds = 300;

    public boolean isEnabled() { return enabled; }
    @Config("starrocks.flight.enabled")
    public StarRocksFlightConfig setEnabled(boolean enabled) { this.enabled = enabled; return this; }

    @NotNull
    public String getEndpoint() { return endpoint; }
    @Config("starrocks.flight.endpoint")
    public StarRocksFlightConfig setEndpoint(String endpoint) { this.endpoint = endpoint; return this; }

    @NotNull
    public Locality getLocality() { return locality; }
    @Config("starrocks.flight.locality")
    public StarRocksFlightConfig setLocality(Locality locality) { this.locality = locality; return this; }

    public Map<String, HostAddress> getBackendWorkers() { return backendWorkers; }
    @Config("starrocks.flight.backend-workers")
    public StarRocksFlightConfig setBackendWorkers(String value)
    {
        Map<String, HostAddress> mappings = new LinkedHashMap<>();
        if (!value.isBlank()) {
            for (String entry : value.split(",")) {
                String[] parts = entry.trim().split("=", 2);
                if (parts.length != 2 || parts[0].isBlank() || parts[1].isBlank()) {
                    throw new IllegalArgumentException("Expected backend Flight URI=Trino worker host[:port]");
                }
                String uri = validateEndpoint(parts[0].trim());
                if (mappings.put(uri, HostAddress.fromString(parts[1].trim())) != null) {
                    throw new IllegalArgumentException("Duplicate backend Flight URI: " + uri);
                }
            }
        }
        backendWorkers = Map.copyOf(mappings);
        return this;
    }

    @Min(1)
    public long getMaxMemoryBytes() { return maxMemoryBytes; }
    @Config("starrocks.flight.max-memory-bytes")
    public StarRocksFlightConfig setMaxMemoryBytes(long value) { maxMemoryBytes = value; return this; }

    @Min(1)
    public int getTimeoutSeconds() { return timeoutSeconds; }
    @Config("starrocks.flight.timeout-seconds")
    public StarRocksFlightConfig setTimeoutSeconds(int value) { timeoutSeconds = value; return this; }

    static String validateEndpoint(String value)
    {
        URI uri = URI.create(value);
        if (!("grpc".equals(uri.getScheme()) || "grpc+tcp".equals(uri.getScheme()) || "grpc+tls".equals(uri.getScheme())) ||
                uri.getHost() == null || uri.getPort() < 1 || uri.getPort() > 65535 || uri.getUserInfo() != null ||
                uri.getQuery() != null || uri.getFragment() != null || !uri.getPath().isEmpty()) {
            throw new IllegalArgumentException("Expected grpc://host:port or grpc+tls://host:port");
        }
        // Arrow treats grpc and grpc+tcp as equivalent transports.
        return value.replaceFirst("^grpc://", "grpc+tcp://");
    }
}
