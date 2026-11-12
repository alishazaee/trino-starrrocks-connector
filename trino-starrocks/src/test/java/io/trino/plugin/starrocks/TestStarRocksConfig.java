package io.trino.plugin.starrocks;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import java.util.Set;
import org.junit.jupiter.api.Test;

class TestStarRocksConfig
{
    @Test
    void testClientDefaults()
    {
        assertRecordedDefaults(recordDefaults(StarRocksConfig.class)
                .setLoadUrls("")
                .setLabelPrefix("trino-")
                .setMaxCacheBytes(268_435_456L)
                .setConnectTimeout(30_000)
                .setChunkLimit(Long.MAX_VALUE));
    }

    @Test
    void testClientExplicitPropertyMappings()
    {
        assertFullMapping(
                ImmutableMap.<String, String>builder()
                        .put("starrocks.client.load-url", "fe-1:8030, fe-2:8030")
                        .put("starrocks.client.label-prefix", "test-")
                        .put("starrocks.client.max-cache-bytes", "1048576")
                        .put("starrocks.client.connect-timeout", "5000")
                        .put("starrocks.client.chunk-limit", "8192")
                        .buildOrThrow(),
                new StarRocksConfig()
                        .setLoadUrls("fe-1:8030,fe-2:8030")
                        .setLabelPrefix("test-")
                        .setMaxCacheBytes(1_048_576)
                        .setConnectTimeout(5_000)
                        .setChunkLimit(8_192));
    }

    @Test
    void testJdbcProperties()
    {
        StarRocksJdbcConfig expected = new StarRocksJdbcConfig();
        expected.setConnectionUrl("jdbc:mysql://localhost:9030/");
        expected.setAutoReconnect(false)
                .setMaxReconnects(7)
                .setConnectionTimeout(new Duration(15, SECONDS))
                .setDriverUseInformationSchema(false);

        assertFullMapping(
                ImmutableMap.<String, String>builder()
                        .put("connection-url", "jdbc:mysql://localhost:9030/")
                        .put("starrocks.auto-reconnect", "false")
                        .put("starrocks.max-reconnects", "7")
                        .put("starrocks.connection-timeout", "15s")
                        .put("starrocks.jdbc.use-information-schema", "false")
                        .buildOrThrow(),
                expected,
                Set.of(
                        "jdbc-types-mapped-to-varchar",
                        "metadata.cache-maximum-size",
                        "metadata.cache-missing",
                        "metadata.cache-ttl",
                        "metadata.schemas.cache-ttl",
                        "metadata.statistics.cache-ttl",
                        "metadata.tables.cache-ttl"));
    }

    @Test
    void testWriteProperties()
    {
        assertRecordedDefaults(recordDefaults(StarRocksWriteConfig.class)
                .setQueryTimeout(3_000));
        assertFullMapping(
                ImmutableMap.of("write.query-timeout", "42"),
                new StarRocksWriteConfig().setQueryTimeout(42));
    }

    @Test
    void testJdbcUrlValidation()
    {
        StarRocksJdbcConfig rootUrl = new StarRocksJdbcConfig();
        rootUrl.setConnectionUrl("jdbc:mysql://localhost:9030/");
        assertThat(rootUrl.isUrlValid()).isTrue();
        assertThat(rootUrl.isUrlWithoutDatabase()).isTrue();

        StarRocksJdbcConfig databaseUrl = new StarRocksJdbcConfig();
        databaseUrl.setConnectionUrl("jdbc:mysql://localhost:9030/analytics");
        assertThat(databaseUrl.isUrlValid()).isTrue();
        assertThat(databaseUrl.isUrlWithoutDatabase()).isFalse();
    }
}
