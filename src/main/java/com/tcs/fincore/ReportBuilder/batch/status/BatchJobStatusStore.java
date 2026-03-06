package com.tcs.fincore.ReportBuilder.batch.status;

import com.tcs.fincore.ReportBuilder.batch.dto.BatchJobStatus;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Optional;

public interface BatchJobStatusStore {

    void save(BatchJobStatus status, Duration ttl);

    Optional<BatchJobStatus> find(String jobId);

    void delete(String jobId);

    int evictStaleTerminal(LocalDateTime cutoff);
}
