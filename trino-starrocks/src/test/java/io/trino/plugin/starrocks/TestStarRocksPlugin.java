package io.trino.plugin.starrocks;

import static org.assertj.core.api.Assertions.assertThat;

import io.trino.spi.connector.ConnectorFactory;
import org.junit.jupiter.api.Test;

class TestStarRocksPlugin
{
    @Test
    void testConnectorFactory()
    {
        assertThat(new StarRocksPlugin().getConnectorFactories())
                .extracting(ConnectorFactory::getName)
                .containsExactly("starrocks");
    }
}
