package io.trino.plugin.starrocks;

import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

import io.trino.testing.DistributedQueryRunner;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

@TestInstance(PER_CLASS)
class SetupEnvironment
{
    protected static final int STARROCKS_PORT = 9030;
    protected static final int STARROCKS_HTTP_PORT = 8030;
    protected static final int STARROCKS_BACKEND_HTTP_PORT = 8040;
    protected static final String ROOT_USER = "root";
    protected static final String ROOT_PASSWORD = "";
    protected static final String SCHEMA = "sample_schema";
    protected static final String CATALOG = "starrocks";

    protected String jdbcUrl;
    protected GenericContainer<?> starrocks;
    protected DistributedQueryRunner trino;

    @BeforeAll
    void setUp()
            throws Exception
    {
        // Stream load starts at the FE HTTP endpoint and is redirected to the BE's
        // advertised 127.0.0.1:8040 endpoint. Keep that one port fixed so the
        // redirected URL remains reachable from the test JVM.
        starrocks = new StarRocksContainer(DockerImageName.parse("starrocks/allin1-ubuntu:3.5.21"))
                .withFixedBackendHttpPort()
                .withExposedPorts(STARROCKS_PORT, STARROCKS_HTTP_PORT)
                .waitingFor(Wait.forListeningPort().withStartupTimeout(Duration.ofMinutes(5)));
        starrocks.start();

        jdbcUrl = "jdbc:mysql://%s:%s/".formatted(starrocks.getHost(), starrocks.getMappedPort(STARROCKS_PORT));
        awaitBackendReady();

        trino = DistributedQueryRunner.builder(testSessionBuilder()
                        .setCatalog(CATALOG)
                        .setSchema(SCHEMA)
                        .build())
                .build();

        trino.installPlugin(new StarRocksPlugin());
        trino.createCatalog(CATALOG, "starrocks", Map.of(
                "connection-url", jdbcUrl,
                "connection-user", ROOT_USER,
                "connection-password", ROOT_PASSWORD,
                "starrocks.client.load-url", "%s:%s".formatted(starrocks.getHost(), starrocks.getMappedPort(STARROCKS_HTTP_PORT)),
                "statistics.enabled", "true"));
    }

    @AfterAll
    void closeEnvironment()
    {
        if (trino != null) {
            trino.close();
        }
        if (starrocks != null) {
            starrocks.close();
        }
    }

    private void awaitBackendReady()
            throws InterruptedException
    {
        SQLException lastFailure = null;
        for (int attempt = 0; attempt < 60; attempt++) {
            try (Connection connection = DriverManager.getConnection(jdbcUrl, ROOT_USER, ROOT_PASSWORD);
                    Statement statement = connection.createStatement();
                    ResultSet backends = statement.executeQuery("SHOW BACKENDS")) {
                if (backends.next() && backends.getBoolean("Alive")) {
                    return;
                }
            }
            catch (SQLException e) {
                lastFailure = e;
            }
            Thread.sleep(1_000);
        }
        throw new IllegalStateException("StarRocks did not become ready", lastFailure);
    }

    private static final class StarRocksContainer
            extends GenericContainer<StarRocksContainer>
    {
        private StarRocksContainer(DockerImageName imageName)
        {
            super(imageName);
        }

        private StarRocksContainer withFixedBackendHttpPort()
        {
            addFixedExposedPort(STARROCKS_BACKEND_HTTP_PORT, STARROCKS_BACKEND_HTTP_PORT);
            return this;
        }
    }
}
