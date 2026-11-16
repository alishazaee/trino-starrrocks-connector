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

import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;

import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.google.inject.multibindings.Multibinder;
import com.mysql.cj.jdbc.Driver;
import com.starrocks.data.load.stream.StreamLoadDataFormat;
import com.starrocks.data.load.stream.properties.StreamLoadProperties;
import com.starrocks.data.load.stream.properties.StreamLoadTableProperties;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.opentelemetry.api.OpenTelemetry;
import io.trino.plugin.base.session.SessionPropertiesProvider;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.DecimalModule;
import io.trino.plugin.jdbc.DriverConnectionFactory;
import io.trino.plugin.jdbc.ForBaseJdbc;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcJoinPushdownConfig;
import io.trino.plugin.jdbc.JdbcJoinPushdownSupportModule;
import io.trino.plugin.jdbc.JdbcStatisticsConfig;
import io.trino.plugin.jdbc.JoinPushdownStrategy;
import io.trino.plugin.jdbc.TablePropertiesProvider;
import io.trino.plugin.jdbc.credential.CredentialConfig;
import io.trino.plugin.jdbc.credential.CredentialProvider;
import io.trino.plugin.jdbc.ptf.Query;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.function.table.ConnectorTableFunction;
import java.sql.SQLException;
import java.util.Properties;

public class StarRocksClientModule
        extends AbstractConfigurationAwareModule
{
    public static final String EMPTY = "";

    @Override
    protected void setup(Binder binder)
    {
        binder.bind(JdbcClient.class).annotatedWith(ForBaseJdbc.class).to(StarRocksClient.class).in(Scopes.SINGLETON);
        newSetBinder(binder, TablePropertiesProvider.class).addBinding().to(StarRocksTableProperties.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(StarRocksJdbcConfig.class);
        configBinder(binder).bindConfig(StarRocksConfig.class);
        configBinder(binder).bindConfig(StarRocksFlightConfig.class);
        if (buildConfigObject(StarRocksFlightConfig.class).isEnabled()) {
            binder.bind(io.trino.plugin.jdbc.JdbcPageSourceProvider.class).in(Scopes.SINGLETON);
            binder.bind(StarRocksFlightClient.class).in(Scopes.SINGLETON);
            newSetBinder(binder, io.trino.plugin.jdbc.JdbcQueryEventListener.class)
                    .addBinding().to(StarRocksFlightClient.class);
            newOptionalBinder(binder, com.google.inject.Key.get(io.trino.spi.connector.ConnectorSplitManager.class,
                    io.trino.plugin.jdbc.ForJdbcDynamicFiltering.class))
                    .setBinding().to(StarRocksFlightSplitManager.class).in(Scopes.SINGLETON);
            newOptionalBinder(binder, io.trino.spi.connector.ConnectorPageSourceProvider.class)
                    .setBinding().to(StarRocksFlightPageSourceProvider.class).in(Scopes.SINGLETON);
        }
        configBinder(binder).bindConfig(StarRocksWriteConfig.class);
        configBinder(binder).bindConfig(JdbcStatisticsConfig.class);
        bindSessionPropertiesProvider(binder, StarRocksWriteSessionProperties.class);
        install(new DecimalModule());
        install(new JdbcJoinPushdownSupportModule());
        // Prefer remote execution for every supported same-catalog join, even
        // when StarRocks statistics are unavailable. Explicit catalog/session
        // settings can still select AUTOMATIC or disable join pushdown.
        configBinder(binder).bindConfigDefaults(JdbcJoinPushdownConfig.class,
                config -> config.setJoinPushdownStrategy(JoinPushdownStrategy.EAGER));
        newSetBinder(binder, ConnectorTableFunction.class).addBinding().toProvider(Query.class).in(Scopes.SINGLETON);
        newOptionalBinder(binder, ConnectorPageSinkProvider.class).setBinding().to(StarRocksPageSinkProvider.class).in(Scopes.SINGLETON);
    }

    public static Multibinder<SessionPropertiesProvider> sessionPropertiesProviderBinder(Binder binder)
    {
        return newSetBinder(binder, SessionPropertiesProvider.class);
    }

    public static void bindSessionPropertiesProvider(Binder binder, Class<? extends SessionPropertiesProvider> type)
    {
        sessionPropertiesProviderBinder(binder).addBinding().to(type).in(Scopes.SINGLETON);
    }

    @Provides
    @Singleton
    @ForBaseJdbc
    public static ConnectionFactory createConnectionFactory(BaseJdbcConfig config, CredentialProvider credentialProvider, OpenTelemetry openTelemetry, StarRocksJdbcConfig starRocksJdbcConfig)
            throws SQLException
    {
        return DriverConnectionFactory.builder(new Driver(), config.getConnectionUrl(), credentialProvider)
                .setConnectionProperties(new Properties())
                .setOpenTelemetry(openTelemetry)
                .build();
    }

    @Provides
    @Singleton
    public static StreamLoadProperties getStreamLoadProperties(StarRocksConfig starRocksConfig, BaseJdbcConfig baseJdbcConfig, CredentialConfig credentialConfig)
    {
        StreamLoadTableProperties streamLoadTableProperties = StreamLoadTableProperties.builder()
                .database(EMPTY)
                .table(EMPTY)
                .streamLoadDataFormat(StreamLoadDataFormat.JSON)
                .chunkLimit(starRocksConfig.getChunkLimit())
                .enableUpsertDelete(Boolean.TRUE)
                .addProperty("format", "json")
                .addProperty("strict_mode", "true")
                .addProperty("Expect", "100-continue")
                .addProperty("strip_outer_array", "true")
                .build();
        return StreamLoadProperties.builder()
                .loadUrls(starRocksConfig.getLoadUrls().toArray(new String[0]))
                .jdbcUrl(baseJdbcConfig.getConnectionUrl())
                .tableProperties(streamLoadTableProperties)
                .cacheMaxBytes(starRocksConfig.getMaxCacheBytes())
                .connectTimeout(starRocksConfig.getConnectTimeout())
                .labelPrefix(starRocksConfig.getLabelPrefix())
                .username(credentialConfig.getConnectionUser().orElse(EMPTY))
                .password(credentialConfig.getConnectionPassword().orElse(EMPTY))
                .build();
    }
}
