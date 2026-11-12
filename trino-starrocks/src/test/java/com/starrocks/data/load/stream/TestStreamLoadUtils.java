package com.starrocks.data.load.stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class TestStreamLoadUtils
{
    @Test
    void testIdentifiersAndUrl()
    {
        assertThat(StreamLoadUtils.getTableUniqueKey("sales", "orders")).isEqualTo("sales-orders");
        assertThat(StreamLoadUtils.getSendUrl("http://fe:8030", "sales", "orders"))
                .isEqualTo("http://fe:8030/api/sales/orders/_stream_load");
        assertThatThrownBy(() -> StreamLoadUtils.getSendUrl(null, "sales", "orders"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("load_url");
    }

    @Test
    void testBasicAuthenticationHeader()
    {
        assertThat(StreamLoadUtils.getBasicAuthHeader("root", "")).isEqualTo("Basic cm9vdDo=");
        assertThat(StreamLoadUtils.getBasicAuthHeader("user", "secret")).isEqualTo("Basic dXNlcjpzZWNyZXQ=");
    }

    @Test
    void testEpochMicrosecondHelpers()
    {
        assertThat(StreamLoadUtils.scaleEpochMicrosToSeconds(1_234_567)).isEqualTo(1);
        assertThat(StreamLoadUtils.getMicrosOfSecond(1_234_567)).isEqualTo(234_567);
        assertThat(StreamLoadUtils.scaleEpochMicrosToSeconds(-1)).isEqualTo(-1);
        assertThat(StreamLoadUtils.getMicrosOfSecond(-1)).isEqualTo(999_999);
    }
}
