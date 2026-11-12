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

import static java.util.Objects.requireNonNull;

import com.google.inject.Inject;
import com.starrocks.data.load.stream.properties.StreamLoadProperties;
import io.trino.plugin.jdbc.JdbcOutputTableHandle;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorPageSinkId;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableCredentials;
import io.trino.spi.connector.ConnectorTransactionHandle;
import java.util.Optional;
import org.apache.http.impl.client.DefaultRedirectStrategy;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;

public class StarRocksPageSinkProvider
        implements ConnectorPageSinkProvider
{
    private final StreamLoadProperties streamLoadProperties;
    private final HttpClientBuilder clientBuilder;

    @Inject
    public StarRocksPageSinkProvider(StreamLoadProperties streamLoadProperties)
    {
        this.streamLoadProperties = requireNonNull(streamLoadProperties, "streamLoadProperties is null");
        this.clientBuilder = HttpClients.custom()
                .setRedirectStrategy(new DefaultRedirectStrategy()
                {
                    @Override
                    protected boolean isRedirectable(String method)
                    {
                        return true;
                    }
                });
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorOutputTableHandle tableHandle, Optional<ConnectorTableCredentials> credentials, ConnectorPageSinkId pageSinkId)
    {
        StarRocksOutputTableHandle starRocksOutputTableHandle = (StarRocksOutputTableHandle) tableHandle;
        StarRocksOperationApplier applier = new StarRocksOperationApplier(
                starRocksOutputTableHandle.getRemoteTableName().getSchemaName().get(), starRocksOutputTableHandle.getRemoteTableName().getTableName(), starRocksOutputTableHandle.getTemporaryTableName(), starRocksOutputTableHandle.getColumnNames(), starRocksOutputTableHandle.getIsPkTable(), streamLoadProperties, clientBuilder);
        return new StarRocksPageSink(starRocksOutputTableHandle, applier, pageSinkId);
    }

    /**
     * Creates a {@link ConnectorPageSink} for inserting data into StarRocks.
     *
     * <p>The original implementation attempted to cast {@code tableHandle} directly to
     * {@link StarRocksOutputTableHandle}, but in this context the handle is actually an
     * instance of {@link JdbcOutputTableHandle}. Since {@code StarRocksOutputTableHandle}
     * extends {@code JdbcOutputTableHandle} and introduces an additional field
     * ({@code isPkTable}), a direct cast would result in a {@link ClassCastException}.
     *
     * <p>To avoid this, we reconstruct a new {@code StarRocksOutputTableHandle} manually
     * using properties from the {@code JdbcOutputTableHandle}. The {@code isPkTable} field
     * is set to {@code false} because this information is not present in
     * {@code JdbcOutputTableHandle} and is not required for the current insert operation.
     * This approach maintains runtime safety while ensuring compatibility with
     * {@link StarRocksPageSink}.
     */
    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorInsertTableHandle tableHandle, Optional<ConnectorTableCredentials> credentials, ConnectorPageSinkId pageSinkId)
    {
        StarRocksOutputTableHandle starRocksOutputTableHandle = getStarRocksOutputTableHandle(
                (JdbcOutputTableHandle) tableHandle);
        StarRocksOperationApplier applier = new StarRocksOperationApplier(
                starRocksOutputTableHandle.getRemoteTableName().getSchemaName().get(), starRocksOutputTableHandle.getRemoteTableName().getTableName(), starRocksOutputTableHandle.getTemporaryTableName(), starRocksOutputTableHandle.getColumnNames(), starRocksOutputTableHandle.getIsPkTable(), streamLoadProperties, clientBuilder);
        return new StarRocksPageSink(starRocksOutputTableHandle, applier, pageSinkId);
    }

    private static StarRocksOutputTableHandle getStarRocksOutputTableHandle(JdbcOutputTableHandle tableHandle) {

        return new StarRocksOutputTableHandle(
                tableHandle.getRemoteTableName(),
                tableHandle.getColumnNames(),
                tableHandle.getColumnTypes(),
                tableHandle.getJdbcColumnTypes(),
                tableHandle.getTemporaryTableName(),
                tableHandle.getPageSinkIdColumnName(),
                false
        );
    }
}
