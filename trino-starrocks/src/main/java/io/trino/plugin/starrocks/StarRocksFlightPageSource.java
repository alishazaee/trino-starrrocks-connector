package io.trino.plugin.starrocks;

import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.type.Type;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import org.apache.arrow.flight.*;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;

final class StarRocksFlightPageSource implements ConnectorPageSource
{
    private final StarRocksFlightSplit split;
    private final List<Type> types;
    private final List<Integer> channels;
    private final BufferAllocator allocator;
    private final Executor executor;
    private final int timeout;
    private FlightClient client;
    private volatile FlightStream stream;
    private volatile boolean closed;
    private volatile boolean finished;
    private volatile long completedBytes;
    private volatile long readNanos;
    private int row;
    private VectorSchemaRoot batch;
    private CompletableFuture<Page> pending;
    private boolean released;

    StarRocksFlightPageSource(StarRocksFlightSplit split, List<Type> types, List<Integer> channels,
            BufferAllocator allocator, Executor executor, int timeout)
    {
        this.split = split;
        this.types = List.copyOf(types);
        this.channels = List.copyOf(channels);
        this.allocator = allocator;
        this.executor = executor;
        this.timeout = timeout;
    }

    @Override public long getCompletedBytes() { return completedBytes; }
    @Override public long getReadTimeNanos() { return readNanos; }
    @Override public long getMemoryUsage() { return allocator.getAllocatedMemory(); }
    @Override public boolean isFinished() { return closed || (finished && pending == null); }
    @Override public CompletableFuture<?> isBlocked()
    {
        startRead();
        return pending == null ? NOT_BLOCKED : pending;
    }

    private void startRead()
    {
        if (pending == null && !closed && !finished) {
            pending = CompletableFuture.supplyAsync(this::readPage, executor);
        }
    }

    @Override public SourcePage getNextSourcePage()
    {
        startRead();
        if (pending == null || !pending.isDone() || closed) {
            return null;
        }
        try {
            Page page = pending.join();
            pending = null;
            return page == null ? null : SourcePage.create(page);
        }
        catch (RuntimeException e) {
            close();
            throw new TrinoException(JDBC_ERROR, "StarRocks Flight read failed", e);
        }
    }

    private Page readPage()
    {
        long start = System.nanoTime();
        try {
            if (closed) {
                return null;
            }
            if (stream == null) {
                client = FlightClient.builder(allocator, new Location(split.getEndpoint())).build();
                stream = client.getStream(new Ticket(split.getTicket()), CallOptions.timeout(timeout, TimeUnit.SECONDS));
            }
            while (!closed && (batch == null || row == batch.getRowCount())) {
                if (!stream.next()) {
                    finished = true;
                    release();
                    return null;
                }
                batch = stream.getRoot();
                row = 0;
                if (batch.getFieldVectors().size() != split.getColumns().size()) {
                    throw new IllegalStateException("StarRocks Flight output column count changed");
                }
                completedBytes += batch.getFieldVectors().stream().mapToLong(v -> v.getBufferSize()).sum();
            }
            if (closed) {
                return null;
            }
            PageBuilder builder = new PageBuilder(types);
            int end = Math.min(batch.getRowCount(), row + 1024);
            while (row < end && !builder.isFull()) {
                builder.declarePosition();
                for (int channel = 0; channel < types.size(); channel++) {
                    StarRocksArrowReader.write(types.get(channel), batch.getVector(channels.get(channel)), row, builder.getBlockBuilder(channel));
                }
                row++;
            }
            return builder.build();
        }
        catch (Exception e) {
            release();
            throw new TrinoException(JDBC_ERROR, "Cannot read StarRocks Flight result", e);
        }
        finally {
            readNanos += System.nanoTime() - start;
        }
    }

    @Override public void close()
    {
        if (closed) {
            return;
        }
        closed = true;
        FlightStream active = stream;
        if (active != null) {
            active.cancel("Trino page source closed", null);
        }
        if (pending != null) {
            pending.whenComplete((ignored, failure) -> release());
        }
        else {
            release();
        }
    }

    private synchronized void release()
    {
        if (released) { return; }
        released = true;
        try {
            if (stream != null) { stream.close(); }
        }
        catch (Exception ignored) { }
        finally { stream = null; }
        try {
            if (client != null) { client.close(); }
        }
        catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        finally { client = null; }
        allocator.close();
    }
}
