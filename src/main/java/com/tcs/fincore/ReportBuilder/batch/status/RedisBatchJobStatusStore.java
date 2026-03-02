package com.tcs.fincore.ReportBuilder.batch.status;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tcs.fincore.ReportBuilder.dto.BatchJobStatus;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Optional;

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
            return objectMapper.writeValueAsString(status);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize batch job status", e);
        }
    }

    private BatchJobStatus fromJson(String payload) {
        try {
            return objectMapper.readValue(payload, BatchJobStatus.class);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to deserialize batch job status", e);
        }
    }
}
