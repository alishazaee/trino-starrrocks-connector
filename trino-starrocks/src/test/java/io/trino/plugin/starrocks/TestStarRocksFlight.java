package io.trino.plugin.starrocks;

import io.airlift.json.JsonCodec;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.spi.HostAddress;
import io.trino.spi.TrinoException;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.type.Type;
import java.math.BigDecimal;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.arrow.flight.*;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.TimeStampMicroTZVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.message.IpcOption;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.*;

class TestStarRocksFlight
{
    @Test
    void configDefaultsAndMappingParsing()
    {
        var config = new StarRocksFlightConfig();
        assertThat(config.isEnabled()).isFalse();
        assertThat(config.getLocality()).isEqualTo(StarRocksFlightConfig.Locality.PREFERRED);
        assertThat(config.getBackendWorkers()).isEmpty();
        assertThat(config.getMaxMemoryBytes()).isEqualTo(256L * 1024 * 1024);
        assertThat(config.getTimeoutSeconds()).isEqualTo(300);

        config.setEnabled(true)
                .setEndpoint("grpc+tls://fe.example:9408")
                .setBackendWorkers(" grpc://be-a:9419 = worker-a:8080 , grpc+tls://be-b:9420=worker-b ")
                .setLocality(StarRocksFlightConfig.Locality.REQUIRED)
                .setMaxMemoryBytes(1024)
                .setTimeoutSeconds(7);
        assertThat(config.isEnabled()).isTrue();
        assertThat(config.getEndpoint()).isEqualTo("grpc+tls://fe.example:9408");
        assertThat(config.getBackendWorkers()).containsExactlyInAnyOrderEntriesOf(Map.of(
                "grpc+tcp://be-a:9419", HostAddress.fromString("worker-a:8080"),
                "grpc+tls://be-b:9420", HostAddress.fromString("worker-b")));
        assertThat(config.getLocality()).isEqualTo(StarRocksFlightConfig.Locality.REQUIRED);
        assertThat(config.getMaxMemoryBytes()).isEqualTo(1024);
        assertThat(config.getTimeoutSeconds()).isEqualTo(7);
        assertThatThrownBy(() -> config.getBackendWorkers().put("grpc+tcp://other:1", HostAddress.fromString("other")))
                .isInstanceOf(UnsupportedOperationException.class);
        config.setBackendWorkers("  ");
        assertThat(config.getBackendWorkers()).isEmpty();
    }

    @Test
    void endpointValidationRejectsUnsafeOrAmbiguousUris()
    {
        assertThat(StarRocksFlightConfig.validateEndpoint("grpc://be-a:9419")).isEqualTo("grpc+tcp://be-a:9419");
        assertThat(StarRocksFlightConfig.validateEndpoint("grpc+tcp://be-a:9419")).isEqualTo("grpc+tcp://be-a:9419");
        assertThat(StarRocksFlightConfig.validateEndpoint("grpc+tls://be-a:9419")).isEqualTo("grpc+tls://be-a:9419");

        for (String endpoint : List.of(
                "http://be-a:9419",
                "grpc://be-a",
                "grpc://be-a:0",
                "grpc://be-a:65536",
                "grpc://user@be-a:9419",
                "grpc://be-a:9419/path",
                "grpc://be-a:9419?x=1",
                "grpc://be-a:9419#fragment")) {
            assertThatThrownBy(() -> StarRocksFlightConfig.validateEndpoint(endpoint))
                    .as(endpoint)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("grpc://host:port");
        }
        assertThatThrownBy(() -> new StarRocksFlightConfig().setBackendWorkers("missing-equals"))
                .hasMessageContaining("URI=Trino worker");
        assertThatThrownBy(() -> new StarRocksFlightConfig().setBackendWorkers("grpc://be-a:9419="))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void splitRoundTripDoesNotExposeTicketInToString()
    {
        var split = new StarRocksFlightSplit("grpc://be-a:9419", new byte[] {1, 2, 3}, List.of(),
                List.of(HostAddress.fromString("worker-a:8080")), false);
        var codec = JsonCodec.jsonCodec(StarRocksFlightSplit.class);
        var copy = codec.fromJson(codec.toJson(split));
        assertThat(copy.getTicket()).containsExactly(1, 2, 3);
        assertThat(copy.getEndpoint()).isEqualTo("grpc+tcp://be-a:9419");
        assertThat(copy.getAddresses()).containsExactly(HostAddress.fromString("worker-a:8080"));
        assertThat(copy.isRemotelyAccessible()).isFalse();
        byte[] ticket = copy.getTicket();
        ticket[0] = 99;
        assertThat(copy.getTicket()[0]).isEqualTo((byte) 1);
        assertThat(copy.toString()).doesNotContain("ticket");
    }

    @Test
    void splitDefensivelyCopiesAllMutableInputs()
    {
        byte[] ticket = {1, 2};
        List<JdbcColumnHandle> columns = new ArrayList<>();
        List<HostAddress> workers = new ArrayList<>(List.of(HostAddress.fromString("worker-a:8080")));
        var split = new StarRocksFlightSplit("grpc://be-a:9419", ticket, columns, workers, false);

        ticket[0] = 9;
        columns.add(bigintColumn("later"));
        workers.clear();
        assertThat(split.getTicket()).containsExactly(1, 2);
        assertThat(split.getColumns()).isEmpty();
        assertThat(split.getAddresses()).containsExactly(HostAddress.fromString("worker-a:8080"));
        assertThatThrownBy(() -> split.getColumns().add(bigintColumn("x")))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> split.getAddresses().clear())
                .isInstanceOf(UnsupportedOperationException.class);
        assertThat(split.getRetainedSizeInBytes()).isPositive();
    }

    @Test
    void endpointAlternativesProduceOneSplitAndDifferentEndpointsProduceTwo() throws Exception
    {
        var config = new StarRocksFlightConfig().setBackendWorkers("grpc://be-a:9419=worker-a:8080,grpc://be-b:9419=worker-b:8080");
        var info = new FlightInfo(new Schema(List.of()), FlightDescriptor.command(new byte[] {0}), List.of(
                new FlightEndpoint(new Ticket(new byte[] {1}), new Location("grpc://be-a:9419"), new Location("grpc://be-b:9419")),
                new FlightEndpoint(new Ticket(new byte[] {2}), new Location("grpc://be-b:9419"))), -1, -1);
        var splits = StarRocksFlightClient.splits(info, List.of(), config);
        assertThat(splits).hasSize(2);
        assertThat(splits.getFirst().getEndpoint()).isEqualTo("grpc+tcp://be-a:9419");
        assertThat(splits.getFirst().getAddresses()).containsExactly(HostAddress.fromString("worker-a:8080"));
        assertThat(splits.get(1).getEndpoint()).isEqualTo("grpc+tcp://be-b:9419");
        assertThat(splits.get(1).getAddresses()).containsExactly(HostAddress.fromString("worker-b:8080"));
        assertThat(splits.getFirst().isRemotelyAccessible()).isTrue();
        config.setLocality(StarRocksFlightConfig.Locality.REQUIRED);
        assertThat(StarRocksFlightClient.splits(info, List.of(), config).getFirst().isRemotelyAccessible()).isFalse();
        config.setLocality(StarRocksFlightConfig.Locality.OFF);
        assertThat(StarRocksFlightClient.splits(info, List.of(), config).getFirst().getAddresses()).isEmpty();
    }

    @Test
    void endpointPlanningSelectsMappedAlternativeAndPreservesTickets() throws Exception
    {
        var config = new StarRocksFlightConfig().setBackendWorkers("grpc://be-b:9419=worker-b:8080");
        var endpoint = new FlightEndpoint(new Ticket(new byte[] {4, 5}),
                new Location("grpc://unmapped:9419"), new Location("grpc://be-b:9419"));
        var info = new FlightInfo(new Schema(List.of()), FlightDescriptor.command(new byte[] {0}), List.of(endpoint), 12, 34);

        var splits = StarRocksFlightClient.splits(info, List.of(), config);
        assertThat(splits).singleElement().satisfies(split -> {
            assertThat(split.getEndpoint()).isEqualTo("grpc+tcp://be-b:9419");
            assertThat(split.getTicket()).containsExactly(4, 5);
            assertThat(split.getAddresses()).containsExactly(HostAddress.fromString("worker-b:8080"));
        });
    }

    @Test
    void endpointPlanningRejectsOrderedParallelResultsAndSchemaMismatch() throws Exception
    {
        var config = new StarRocksFlightConfig().setBackendWorkers(
                "grpc://be-a:9419=worker-a:8080,grpc://be-b:9419=worker-b:8080");
        var endpoints = List.of(
                new FlightEndpoint(new Ticket(new byte[] {1}), new Location("grpc://be-a:9419")),
                new FlightEndpoint(new Ticket(new byte[] {2}), new Location("grpc://be-b:9419")));
        var ordered = new FlightInfo(new Schema(List.of()), FlightDescriptor.command(new byte[] {0}), endpoints,
                -1, -1, true, IpcOption.DEFAULT);
        assertThatThrownBy(() -> StarRocksFlightClient.splits(ordered, List.of(), config))
                .hasMessageContaining("Ordered multi-endpoint");

        var field = new Field("id", FieldType.nullable(new ArrowType.Int(64, true)), List.of());
        var schemaMismatch = new FlightInfo(new Schema(List.of(field)), FlightDescriptor.command(new byte[] {0}),
                List.of(endpoints.getFirst()), -1, -1);
        assertThatThrownBy(() -> StarRocksFlightClient.splits(schemaMismatch, List.of(), config))
                .hasMessageContaining("output does not match planned columns");

        var orderedSingle = new FlightInfo(new Schema(List.of()), FlightDescriptor.command(new byte[] {0}),
                List.of(endpoints.getFirst()), -1, -1, true, IpcOption.DEFAULT);
        assertThat(StarRocksFlightClient.splits(orderedSingle, List.of(), config)).hasSize(1);
        var noEndpoints = new FlightInfo(new Schema(List.of()), FlightDescriptor.command(new byte[] {0}), List.of(), -1, -1);
        assertThat(StarRocksFlightClient.splits(noEndpoints, List.of(), config)).isEmpty();
    }

    @Test
    void requiredLocalityRejectsTheWrongWorker()
    {
        var splitA = new StarRocksFlightSplit("grpc://be-a:9419", new byte[] {1}, List.of(),
                List.of(HostAddress.fromString("worker-a:8080")), false);
        var splitB = new StarRocksFlightSplit("grpc://be-b:9419", new byte[] {2}, List.of(),
                List.of(HostAddress.fromString("worker-b:8080")), false);

        StarRocksFlightPageSourceProvider.verifyRequiredLocality(
                StarRocksFlightConfig.Locality.REQUIRED, splitA, HostAddress.fromString("worker-a:8080"));
        StarRocksFlightPageSourceProvider.verifyRequiredLocality(
                StarRocksFlightConfig.Locality.REQUIRED, splitB, HostAddress.fromString("worker-b:8080"));
        assertThatThrownBy(() -> StarRocksFlightPageSourceProvider.verifyRequiredLocality(
                StarRocksFlightConfig.Locality.REQUIRED, splitA, HostAddress.fromString("worker-b:8080")))
                .hasMessageContaining("endpoint grpc+tcp://be-a:9419 was scheduled on worker-b:8080 instead of [worker-a:8080]");
        StarRocksFlightPageSourceProvider.verifyRequiredLocality(
                StarRocksFlightConfig.Locality.PREFERRED, splitA, HostAddress.fromString("worker-b:8080"));
        StarRocksFlightPageSourceProvider.verifyRequiredLocality(
                StarRocksFlightConfig.Locality.OFF, splitA, HostAddress.fromString("worker-b:8080"));
    }

    @Test
    void rejectsUnmappedProxyAndInvalidMappings() throws Exception
    {
        var config = new StarRocksFlightConfig().setBackendWorkers("grpc://be-a:9419=worker-a");
        var info = new FlightInfo(new Schema(List.of()), FlightDescriptor.command(new byte[] {0}), List.of(
                new FlightEndpoint(new Ticket(new byte[] {1}), new Location("grpc://fe:9408"))), -1, -1);
        assertThatThrownBy(() -> StarRocksFlightClient.splits(info, List.of(), config)).hasMessageContaining("No explicitly mapped");
        assertThatThrownBy(() -> config.setBackendWorkers("http://be-a:9419=worker-a")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> config.setBackendWorkers("grpc://be-a:9419=a,grpc+tcp://be-a:9419=b")).hasMessageContaining("Duplicate");
        assertThatThrownBy(() -> new StarRocksFlightSplit("grpc://be:9419", new byte[0], List.of(), List.of(), false))
                .hasMessageContaining("worker address");
    }

    @Test
    void arrowTypeWhitelistKeepsUnsupportedTypesOnJdbc()
    {
        assertThat(List.of(BOOLEAN, TINYINT, SMALLINT, INTEGER, BIGINT, REAL, DOUBLE, DATE, VARBINARY,
                io.trino.spi.type.VarcharType.createVarcharType(20),
                io.trino.spi.type.CharType.createCharType(5),
                io.trino.spi.type.DecimalType.createDecimalType(18, 2),
                io.trino.spi.type.TimestampType.createTimestampType(6)))
                .allMatch(StarRocksArrowReader::supports);
        assertThat(StarRocksArrowReader.supports(io.trino.spi.type.TimestampType.createTimestampType(9))).isFalse();
        assertThat(StarRocksArrowReader.supports(io.trino.spi.type.TimeType.createTimeType(6))).isFalse();
        assertThat(StarRocksArrowReader.supports(new io.trino.spi.type.ArrayType(BIGINT))).isFalse();
    }

    @Test
    void arrowReaderConvertsValuesNullsAndRejectsUnsafeValues()
    {
        try (var allocator = new RootAllocator(8 * 1024 * 1024);
                var bools = new BitVector("flag", allocator);
                var floats = new Float4Vector("real", allocator);
                var decimal = new DecimalVector("amount", allocator, 10, 2);
                var text = new VarCharVector("text", allocator);
                var binary = new VarBinaryVector("binary", allocator);
                var date = new DateDayVector("date", allocator);
                var utcTimestamp = new TimeStampMicroTZVector("timestamp", allocator, "UTC");
                var zonedTimestamp = new TimeStampMicroTZVector("timestamp", allocator, "America/New_York")) {
            bools.allocateNew(2);
            bools.setSafe(0, 1);
            floats.allocateNew(1);
            floats.setSafe(0, 1.25f);
            decimal.allocateNew(1);
            decimal.setSafe(0, new BigDecimal("123.45"));
            text.allocateNew();
            text.setSafe(0, "abc  ".getBytes(java.nio.charset.StandardCharsets.UTF_8));
            binary.allocateNew();
            binary.setSafe(0, new byte[] {1, 2, 3});
            date.allocateNew(1);
            date.setSafe(0, 20_000);
            utcTimestamp.allocateNew(1);
            utcTimestamp.setSafe(0, 1_234_567L);
            zonedTimestamp.allocateNew(1);
            zonedTimestamp.setSafe(0, 1L);

            assertThat(readBoolean(BOOLEAN, bools, 0)).isTrue();
            assertThat(readNull(BOOLEAN, bools, 1)).isTrue();
            assertThat(readLong(REAL, floats, 0)).isEqualTo(Float.floatToRawIntBits(1.25f));
            assertThat(readLong(io.trino.spi.type.DecimalType.createDecimalType(5, 2), decimal, 0)).isEqualTo(12_345);
            assertThat(readSlice(io.trino.spi.type.CharType.createCharType(5), text, 0)).isEqualTo("abc");
            assertThat(readSlice(VARBINARY, binary, 0).getBytes(java.nio.charset.StandardCharsets.ISO_8859_1))
                    .containsExactly(1, 2, 3);
            assertThat(readLong(DATE, date, 0)).isEqualTo(20_000);
            assertThat(readLong(io.trino.spi.type.TimestampType.createTimestampType(6), utcTimestamp, 0)).isEqualTo(1_234_567);
            assertThatThrownBy(() -> writeValue(io.trino.spi.type.TimestampType.createTimestampType(6), zonedTimestamp, 0))
                    .hasMessageContaining("Unexpected timezone");
            assertThatThrownBy(() -> writeValue(io.trino.spi.type.DecimalType.createDecimalType(4, 2), decimal, 0))
                    .isInstanceOf(ArithmeticException.class)
                    .hasMessageContaining("exceeds Trino precision");
        }
    }

    @Test
    void readsBatchesAcrossPageBoundariesAndReleasesMemory() throws Exception
    {
        try (var allocator = new RootAllocator(64 * 1024 * 1024);
                var executor = Executors.newSingleThreadExecutor()) {
            var producer = new NoOpFlightProducer() {
                @Override public void getStream(CallContext context, Ticket ticket, ServerStreamListener listener)
                {
                    assertThat(ticket.getBytes()).containsExactly(7);
                    try (var vector = new BigIntVector("id", allocator);
                            var root = new VectorSchemaRoot(List.of(vector.getField()), List.of(vector), 0)) {
                        vector.allocateNew(2500);
                        for (int i = 0; i < 2500; i++) { vector.setSafe(i, i); }
                        vector.setNull(123);
                        root.setRowCount(2500);
                        listener.start(root);
                        listener.putNext();
                        listener.completed();
                    }
                    catch (Throwable failure) {
                        failure.printStackTrace();
                        listener.error(failure);
                    }
                }
            };
            try (var server = FlightServer.builder(allocator, Location.forGrpcInsecure("127.0.0.1", 0), producer).build().start()) {
                var column = new JdbcColumnHandle("id", new JdbcTypeHandle(Types.BIGINT, Optional.of("BIGINT"), Optional.empty(),
                        Optional.empty(), Optional.empty(), Optional.empty()), BIGINT);
                var split = new StarRocksFlightSplit(server.getLocation().getUri().toString(), new byte[] {7}, List.of(column), List.of(), true);
                var child = allocator.newChildAllocator("reader", 0, 32 * 1024 * 1024);
                List<Long> values = new ArrayList<>();
                int pages = 0;
                try (var source = new StarRocksFlightPageSource(split, List.of(BIGINT), List.of(0), child, executor, 10)) {
                    while (!source.isFinished()) {
                        source.isBlocked().get(10, TimeUnit.SECONDS);
                        SourcePage page = source.getNextSourcePage();
                        if (page == null) { continue; }
                        pages++;
                        for (int i = 0; i < page.getPositionCount(); i++) {
                            values.add(page.getBlock(0).isNull(i) ? null : BIGINT.getLong(page.getBlock(0), i));
                        }
                    }
                    assertThat(source.getCompletedBytes()).isPositive();
                    assertThat(source.getReadTimeNanos()).isPositive();
                }
                assertThat(pages).isEqualTo(3);
                assertThat(values).hasSize(2500);
                assertThat(values.get(123)).isNull();
                assertThat(values.getLast()).isEqualTo(2499L);
                assertThat(child.getAllocatedMemory()).isZero();
            }
        }
    }

    @Test
    void pageSourceReportsSchemaChangesAndReleasesMemory() throws Exception
    {
        try (var allocator = new RootAllocator(32 * 1024 * 1024);
                var executor = Executors.newSingleThreadExecutor()) {
            var producer = new NoOpFlightProducer() {
                @Override public void getStream(CallContext context, Ticket ticket, ServerStreamListener listener)
                {
                    try (var vector = new BigIntVector("unexpected", allocator);
                            var root = new VectorSchemaRoot(List.of(vector.getField()), List.of(vector), 0)) {
                        vector.allocateNew(1);
                        vector.setSafe(0, 1);
                        root.setRowCount(1);
                        listener.start(root);
                        listener.putNext();
                        listener.completed();
                    }
                }
            };
            try (var server = FlightServer.builder(allocator, Location.forGrpcInsecure("127.0.0.1", 0), producer).build().start()) {
                var split = new StarRocksFlightSplit(server.getLocation().getUri().toString(), new byte[] {1}, List.of(), List.of(), true);
                var child = allocator.newChildAllocator("failing-reader", 0, 8 * 1024 * 1024);
                try (var source = new StarRocksFlightPageSource(split, List.of(), List.of(), child, executor, 10)) {
                    assertThatThrownBy(() -> source.isBlocked().get(10, TimeUnit.SECONDS))
                            .hasRootCauseMessage("StarRocks Flight output column count changed");
                    assertThatThrownBy(source::getNextSourcePage)
                            .isInstanceOf(TrinoException.class)
                            .hasMessageContaining("Flight read failed")
                            .hasRootCauseMessage("StarRocks Flight output column count changed");
                }
                assertThat(child.getAllocatedMemory()).isZero();
            }
        }
    }

    private static JdbcColumnHandle bigintColumn(String name)
    {
        return new JdbcColumnHandle(name, new JdbcTypeHandle(Types.BIGINT, Optional.of("BIGINT"), Optional.empty(),
                Optional.empty(), Optional.empty(), Optional.empty()), BIGINT);
    }

    private static BlockBuilder writeValue(Type type, org.apache.arrow.vector.FieldVector vector, int row)
    {
        BlockBuilder builder = type.createBlockBuilder(null, 1);
        StarRocksArrowReader.write(type, vector, row, builder);
        return builder;
    }

    private static long readLong(Type type, org.apache.arrow.vector.FieldVector vector, int row)
    {
        return type.getLong(writeValue(type, vector, row).build(), 0);
    }

    private static boolean readBoolean(Type type, org.apache.arrow.vector.FieldVector vector, int row)
    {
        return type.getBoolean(writeValue(type, vector, row).build(), 0);
    }

    private static boolean readNull(Type type, org.apache.arrow.vector.FieldVector vector, int row)
    {
        return writeValue(type, vector, row).build().isNull(0);
    }

    private static String readSlice(Type type, org.apache.arrow.vector.FieldVector vector, int row)
    {
        return type.getSlice(writeValue(type, vector, row).build(), 0).toStringUtf8();
    }
}
