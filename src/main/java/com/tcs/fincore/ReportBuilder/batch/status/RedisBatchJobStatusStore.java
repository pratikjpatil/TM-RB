package com.tcs.fincore.ReportBuilder.batch.status;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tcs.fincore.ReportBuilder.dto.BatchJobStatus;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

@Component
@ConditionalOnProperty(name = "report.batch.status-store", havingValue = "redis")
public class RedisBatchJobStatusStore implements BatchJobStatusStore {

    private static final String STATUS_KEY = "rb:batch:status:%s";

    private final StringRedisTemplate redisTemplate;
    private final ObjectMapper objectMapper;

    public RedisBatchJobStatusStore(StringRedisTemplate redisTemplate, ObjectMapper objectMapper) {
        this.redisTemplate = redisTemplate;
        this.objectMapper = objectMapper;
    }

    @Override
    public void save(BatchJobStatus status, Duration ttl) {
        String key = String.format(STATUS_KEY, status.getJobId());
        redisTemplate.opsForValue().set(key, toJson(status), ttl);
    }

    @Override
    public Optional<BatchJobStatus> find(String jobId) {
        String payload = redisTemplate.opsForValue().get(String.format(STATUS_KEY, jobId));
        if (payload == null || payload.isBlank()) {
            return Optional.empty();
        }
        return Optional.of(fromJson(payload));
    }

    @Override
    public void delete(String jobId) {
        redisTemplate.delete(String.format(STATUS_KEY, jobId));
    }

    @Override
    public int evictStaleTerminal(LocalDateTime cutoff) {
        return 0;
    }

    private String toJson(BatchJobStatus status) {
        try {
            return objectMapper.writeValueAsString(snapshot(status));
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize batch job status", e);
        }
    }

    private BatchJobStatus snapshot(BatchJobStatus status) {
        BatchJobStatus.ProgressInfo progress = status.getProgress();
        BatchJobStatus.ErrorInfo errors = status.getErrors();

        BatchJobStatus.ProgressInfo progressSnapshot;
        if (progress == null) {
            progressSnapshot = null;
        } else {
            synchronized (progress) {
                Set<String> completedValues = progress.getCompletedValues() == null
                        ? Set.of()
                        : new HashSet<>(progress.getCompletedValues());
                Set<String> failedValues = progress.getFailedValues() == null
                        ? Set.of()
                        : new HashSet<>(progress.getFailedValues());

                progressSnapshot = BatchJobStatus.ProgressInfo.builder()
                        .totalReports(progress.getTotalReports())
                        .completedReports(progress.getCompletedReports())
                        .failedReports(progress.getFailedReports())
                        .skippedReports(progress.getSkippedReports())
                        .percentageComplete(progress.getPercentageComplete())
                        .materializedRows(progress.getMaterializedRows())
                        .completedValues(completedValues)
                        .failedValues(failedValues)
                        .reportsPerMinute(progress.getReportsPerMinute())
                        .averageReportTimeMs(progress.getAverageReportTimeMs())
                        .build();
            }
        }

        BatchJobStatus.ErrorInfo errorSnapshot;
        if (errors == null) {
            errorSnapshot = null;
        } else {
            synchronized (errors) {
                errorSnapshot = BatchJobStatus.ErrorInfo.builder()
                        .totalErrors(errors.getTotalErrors())
                        .recentErrors(errors.getRecentErrors() == null
                                ? new ArrayList<>()
                                : new ArrayList<>(errors.getRecentErrors()))
                        .lastErrorMessage(errors.getLastErrorMessage())
                        .lastErrorTime(errors.getLastErrorTime())
                        .build();
            }
        }

        return BatchJobStatus.builder()
                .jobId(status.getJobId())
                .templateId(status.getTemplateId())
                .variantCode(status.getVariantCode())
                .currentPhase(status.getCurrentPhase())
                .status(status.getStatus())
                .progress(progressSnapshot)
                .errors(errorSnapshot)
                .startTime(status.getStartTime())
                .batchProcessStartTime(status.getBatchProcessStartTime())
                .lastUpdateTime(status.getLastUpdateTime())
                .completionTime(status.getCompletionTime())
                .estimatedTimeRemainingSeconds(status.getEstimatedTimeRemainingSeconds())
                .build();
    }

    private BatchJobStatus fromJson(String payload) {
        try {
            return objectMapper.readValue(payload, BatchJobStatus.class);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to deserialize batch job status", e);
        }
    }
}
