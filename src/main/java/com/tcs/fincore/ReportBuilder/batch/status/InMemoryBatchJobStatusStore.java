package com.tcs.fincore.ReportBuilder.batch.status;

import com.tcs.fincore.ReportBuilder.batch.dto.BatchJobStatus;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

@Component
@ConditionalOnProperty(name = "report.batch.status-store", havingValue = "memory", matchIfMissing = true)
public class InMemoryBatchJobStatusStore implements BatchJobStatusStore {

    private final Map<String, BatchJobStatus> statuses = new ConcurrentHashMap<>();

    @Override
    public void save(BatchJobStatus status, Duration ttl) {
        statuses.put(status.getJobId(), status);
    }

    @Override
    public Optional<BatchJobStatus> find(String jobId) {
        return Optional.ofNullable(statuses.get(jobId));
    }

    @Override
    public void delete(String jobId) {
        statuses.remove(jobId);
    }

    @Override
    public int evictStaleTerminal(LocalDateTime cutoff) {
        int before = statuses.size();
        statuses.entrySet().removeIf(entry -> {
            BatchJobStatus s = entry.getValue();
            boolean terminal = s.getStatus() == BatchJobStatus.JobState.COMPLETED
                    || s.getStatus() == BatchJobStatus.JobState.FAILED
                    || s.getStatus() == BatchJobStatus.JobState.CANCELLED;
            return terminal && s.getCompletionTime() != null && s.getCompletionTime().isBefore(cutoff);
        });
        return before - statuses.size();
    }
}