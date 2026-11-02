package io.trino.plugin.starrocks;

import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;

import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.mysql.cj.jdbc.Driver;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.opentelemetry.api.OpenTelemetry;
import io.trino.plugin.base.mapping.IdentifierMapping;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.DecimalModule;
import io.trino.plugin.jdbc.DriverConnectionFactory;
import io.trino.plugin.jdbc.ForBaseJdbc;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcJoinPushdownSupportModule;
import io.trino.plugin.jdbc.JdbcMetadataConfig;
import io.trino.plugin.jdbc.JdbcStatisticsConfig;
import io.trino.plugin.jdbc.QueryBuilder;
import io.trino.plugin.jdbc.RemoteQueryCancellationModule;
import io.trino.plugin.jdbc.TimestampTimeZoneDomain;
import io.trino.plugin.jdbc.credential.CredentialProvider;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import java.sql.SQLException;
import java.util.Properties;

/** Wires the StarRocks-specific JDBC client into Trino's JDBC connector framework. */
public final class StarRocksClientModule extends AbstractConfigurationAwareModule {
    @Override
    protected void setup(Binder binder) {
        configBinder(binder).bindConfigDefaults(JdbcMetadataConfig.class, config -> config.setBulkListColumns(true));
        configBinder(binder).bindConfig(JdbcStatisticsConfig.class);
        configBinder(binder).bindConfig(StarRocksConfig.class);
        newOptionalBinder(binder, TimestampTimeZoneDomain.class)
                .setBinding()
                .toInstance(TimestampTimeZoneDomain.UTC_ONLY);
        install(new DecimalModule());
        install(new JdbcJoinPushdownSupportModule());
        install(new RemoteQueryCancellationModule());
    }

    @Provides
    @Singleton
    @ForBaseJdbc
    public static JdbcClient createJdbcClient(
            BaseJdbcConfig config,
            @ForBaseJdbc ConnectionFactory connectionFactory,
            QueryBuilder queryBuilder,
            IdentifierMapping identifierMapping,
            RemoteQueryModifier queryModifier,
            JdbcStatisticsConfig statisticsConfig,
            StarRocksConfig starRocksConfig) {
        return new StarRocksClient(
                config,
                connectionFactory,
                queryBuilder,
                identifierMapping,
                queryModifier,
                statisticsConfig,
                starRocksConfig);
    }

    @Provides
    @Singleton
    @ForBaseJdbc
    public static ConnectionFactory createConnectionFactory(
            BaseJdbcConfig config,
            CredentialProvider credentialProvider,
            StarRocksConfig starRocksConfig,
            OpenTelemetry openTelemetry)
            throws SQLException {
        Properties properties = connectionProperties(starRocksConfig);
        return DriverConnectionFactory.builder(new Driver(), config.getConnectionUrl(), credentialProvider)
                .setConnectionProperties(properties)
                .setOpenTelemetry(openTelemetry)
                .build();
    }

    static Properties connectionProperties(StarRocksConfig starRocksConfig) {
        Properties properties = new Properties();
        properties.setProperty("useInformationSchema", "true");
        properties.setProperty("useUnicode", "true");
        properties.setProperty("characterEncoding", "utf8");
        properties.setProperty("tinyInt1isBit", "false");
        properties.setProperty("useServerPrepStmts", "false");
        properties.setProperty("useCursorFetch", "false");
        properties.setProperty("connectionTimeZone", "LOCAL");
        if (starRocksConfig.isAutoReconnect()) {
            properties.setProperty("autoReconnect", "true");
            properties.setProperty("maxReconnects", Integer.toString(starRocksConfig.getMaxReconnects()));
        }
        return properties;
    }
}
