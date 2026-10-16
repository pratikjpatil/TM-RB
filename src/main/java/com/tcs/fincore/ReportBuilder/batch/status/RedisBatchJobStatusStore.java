package com.tcs.fincore.ReportBuilder.batch.status;

import com.tcs.fincore.ReportBuilder.batch.dto.BatchJobStatus;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.json.JsonMapper;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.LinkedList;
import java.util.Optional;
import java.util.Set;

@Component
@ConditionalOnProperty(name = "report.batch.status-store", havingValue = "redis")
public class RedisBatchJobStatusStore implements BatchJobStatusStore {

    private static final String STATUS_KEY = "rb:batch:status:%s";

    private final StringRedisTemplate redisTemplate;
    private final JsonMapper jsonMapper;

    public RedisBatchJobStatusStore(StringRedisTemplate redisTemplate, JsonMapper jsonMapper) {
        this.redisTemplate = redisTemplate;
        this.jsonMapper = jsonMapper;
    }

    @Override
    public void save(BatchJobStatus status, Duration ttl) {
        String key = String.format(STATUS_KEY, status.getJobId());
        redisTemplate.opsForValue().set(key, toJson(snapshot(status)), ttl);
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


    private BatchJobStatus snapshot(BatchJobStatus status) {
        BatchJobStatus.ProgressInfo progress = status.getProgress();
        BatchJobStatus.ProgressInfo progressSnapshot = progress;
        if (progress != null) {
            synchronized (progress) {
                progressSnapshot = BatchJobStatus.ProgressInfo.builder()
                        .totalReports(progress.getTotalReports())
                        .completedReports(progress.getCompletedReports())
                        .failedReports(progress.getFailedReports())
                        .skippedReports(progress.getSkippedReports())
                        .percentageComplete(progress.getPercentageComplete())
                        .materializedRows(progress.getMaterializedRows())
                        .completedValues(progress.getCompletedValues() == null
                                ? Set.of() : Set.copyOf(progress.getCompletedValues()))
                        .failedValues(progress.getFailedValues() == null
                                ? Set.of() : Set.copyOf(progress.getFailedValues()))
                        .reportsPerMinute(progress.getReportsPerMinute())
                        .averageReportTimeMs(progress.getAverageReportTimeMs())
                        .build();
            }
        }

        BatchJobStatus.ErrorInfo errors = status.getErrors();
        BatchJobStatus.ErrorInfo errorsSnapshot = errors;
        if (errors != null) {
            synchronized (errors) {
                errorsSnapshot = BatchJobStatus.ErrorInfo.builder()
                        .totalErrors(errors.getTotalErrors())
                        .recentErrors(errors.getRecentErrors() == null
                                ? new LinkedList<>() : new LinkedList<>(errors.getRecentErrors()))
                        .lastErrorMessage(errors.getLastErrorMessage())
                        .lastErrorTime(errors.getLastErrorTime())
                        .build();
            }
        }

        return BatchJobStatus.builder()
                .jobId(status.getJobId())
                .reportId(status.getReportId())
                .templateId(status.getTemplateId())
                .variantCode(status.getVariantCode())
                .currentPhase(status.getCurrentPhase())
                .status(status.getStatus())
                .progress(progressSnapshot)
                .errors(errorsSnapshot)
                .startTime(status.getStartTime())
                .batchProcessStartTime(status.getBatchProcessStartTime())
                .lastUpdateTime(status.getLastUpdateTime())
                .completionTime(status.getCompletionTime())
                .estimatedTimeRemainingSeconds(status.getEstimatedTimeRemainingSeconds())
                .build();
    }

    private String toJson(BatchJobStatus status) {
        try {
            return jsonMapper.writeValueAsString(status);
        } catch (JacksonException e) {
            throw new RuntimeException(e);
        }
    }

    private BatchJobStatus fromJson(String payload) {
        try {
            return jsonMapper.readValue(payload, BatchJobStatus.class);
        } catch (JacksonException e) {
            throw new RuntimeException(e);
        }
    }
}