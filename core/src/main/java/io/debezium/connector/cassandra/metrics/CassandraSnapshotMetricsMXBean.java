/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra.metrics;

import java.util.Map;

/**
 * MXBean interface for Cassandra snapshot metrics to ensure JMX compliance.
 */
public interface CassandraSnapshotMetricsMXBean {

    int getTotalTableCount();

    int getRemainingTableCount();

    boolean getSnapshotCompleted();

    boolean getSnapshotRunning();

    boolean getSnapshotAborted();

    Map<String, Long> getRowsScanned();

    long getSnapshotDurationInSeconds();
}