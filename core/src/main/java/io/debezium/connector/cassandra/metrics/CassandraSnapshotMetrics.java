/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra.metrics;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import io.debezium.annotation.ThreadSafe;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.metrics.Metrics;

/**
 * Standard snapshot metrics for Cassandra connector that follows Debezium naming conventions.
 *
 * JMX ObjectName pattern: debezium.cassandra:type=connector-metrics,context=snapshot,server=<logical-name>
 */
@ThreadSafe
public class CassandraSnapshotMetrics extends Metrics implements CassandraSnapshotMetricsMXBean {

    private final AtomicInteger totalTableCount = new AtomicInteger();
    private final AtomicInteger remainingTableCount = new AtomicInteger();
    private final AtomicBoolean snapshotRunning = new AtomicBoolean();
    private final AtomicBoolean snapshotCompleted = new AtomicBoolean();
    private final AtomicBoolean snapshotAborted = new AtomicBoolean();
    private final AtomicLong snapshotStartTime = new AtomicLong();
    private final AtomicLong snapshotCompletedTime = new AtomicLong();
    private final AtomicLong snapshotAbortedTime = new AtomicLong();
    private final ConcurrentMap<String, Long> rowsScanned = new ConcurrentHashMap<>();

    public CassandraSnapshotMetrics(CdcSourceTaskContext taskContext) {
        super(taskContext, "snapshot");
    }

    public void reset() {
        totalTableCount.set(0);
        remainingTableCount.set(0);
        snapshotRunning.set(false);
        snapshotCompleted.set(false);
        snapshotAborted.set(false);
        snapshotStartTime.set(0L);
        snapshotCompletedTime.set(0L);
        snapshotAbortedTime.set(0L);
        rowsScanned.clear();
    }

    public void registerMetrics() {
        register();
    }

    public void unregisterMetrics() {
        unregister();
    }

    // Cassandra-specific snapshot methods (same interface as legacy SnapshotProcessorMetrics)
    @Override
    public int getTotalTableCount() {
        return totalTableCount.get();
    }

    @Override
    public int getRemainingTableCount() {
        return remainingTableCount.get();
    }

    @Override
    public boolean getSnapshotCompleted() {
        return snapshotCompleted.get();
    }

    @Override
    public boolean getSnapshotRunning() {
        return snapshotRunning.get();
    }

    @Override
    public boolean getSnapshotAborted() {
        return snapshotAborted.get();
    }

    @Override
    public Map<String, Long> getRowsScanned() {
        return rowsScanned;
    }

    @Override
    public long getSnapshotDurationInSeconds() {
        return snapshotDurationInSeconds();
    }

    // Legacy method names for backward compatibility
    public boolean snapshotCompleted() {
        return getSnapshotCompleted();
    }

    public boolean snapshotRunning() {
        return getSnapshotRunning();
    }

    public boolean snapshotAborted() {
        return getSnapshotAborted();
    }

    public Map<String, Long> rowsScanned() {
        return getRowsScanned();
    }

    public long snapshotDurationInSeconds() {
        long startMillis = snapshotStartTime.get();
        if (startMillis == 0L) {
            return 0;
        }
        long stopMillis = snapshotCompletedTime.get();
        if (snapshotAbortedTime.get() > 0L) {
            stopMillis = snapshotAbortedTime.get();
        }
        if (stopMillis <= 0L) {
            stopMillis = System.currentTimeMillis();
        }
        return (stopMillis - startMillis) / 1000L;
    }

    public void setTableCount(int count) {
        totalTableCount.set(count);
        remainingTableCount.set(count);
    }

    public void completeTable() {
        remainingTableCount.decrementAndGet();
    }

    public void startSnapshot() {
        snapshotRunning.set(true);
        snapshotCompleted.set(false);
        snapshotAborted.set(false);
        snapshotStartTime.set(System.currentTimeMillis());
        snapshotCompletedTime.set(0L);
        snapshotAbortedTime.set(0L);
    }

    public void stopSnapshot() {
        snapshotCompleted.set(true);
        snapshotAborted.set(false);
        snapshotRunning.set(false);
        snapshotCompletedTime.set(System.currentTimeMillis());
    }

    public void abortSnapshot() {
        snapshotCompleted.set(false);
        snapshotAborted.set(true);
        snapshotRunning.set(false);
        snapshotAbortedTime.set(System.currentTimeMillis());
    }

    public void setRowsScanned(String key, Long value) {
        rowsScanned.put(key, value);
    }
}
