package com.tcs.fincore.ReportBuilder.batch.store;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@Component
@ConditionalOnProperty(name = "report.batch.result-store", havingValue = "redis")
public class RedisBatchResultStore implements BatchResultStore {

    private static final String JOB_INDEX = "rb:batch:%s:keys";
    private static final String PATTERN_INDEX = "rb:batch:%s:patterns";
    private static final String DATA_KEY = "rb:batch:%s:%s:%s";
    private static final int DEFAULT_MAX_RETRIES = 2;

    private final StringRedisTemplate redisTemplate;
    private final ObjectMapper objectMapper;
    private final Counter rowsBufferedCounter;
    private final Counter pipelineFlushCounter;
    private final Counter redisWriteFailureCounter;
    private final Counter redisWriteRetryCounter;
    private final Timer pipelineFlushLatency;

    public RedisBatchResultStore(StringRedisTemplate redisTemplate,
                                 ObjectMapper objectMapper,
                                 MeterRegistry meterRegistry) {
        this.redisTemplate = redisTemplate;
        this.objectMapper = objectMapper;
        this.rowsBufferedCounter = meterRegistry.counter("report.batch.redis.rows.buffered");
        this.pipelineFlushCounter = meterRegistry.counter("report.batch.redis.pipeline.flush.count");
        this.redisWriteFailureCounter = meterRegistry.counter("report.batch.redis.write.failures");
        this.redisWriteRetryCounter = meterRegistry.counter("report.batch.redis.write.retries");
        this.pipelineFlushLatency = Timer.builder("report.batch.redis.pipeline.flush.latency")
                .description("Latency for batched Redis pipeline writes")
                .register(meterRegistry);
    }

    @Override
    public void initializeJob(String jobId, Duration ttl) {
        redisTemplate.expire(String.format(PATTERN_INDEX, jobId), ttl);
        redisTemplate.expire(String.format(JOB_INDEX, jobId), ttl);
    }

    @Override
    public void storeValue(String jobId, String patternId, String dimensionValue, String bulkKey, Object value) {
        String dataKey = String.format(DATA_KEY, jobId, patternId, dimensionValue);
        redisTemplate.opsForHash().put(dataKey, bulkKey, serializeValue(value));
        redisTemplate.opsForSet().add(String.format(JOB_INDEX, jobId), dataKey);
    }

    @Override
    public void storeValues(String jobId, String patternId, List<BatchWrite> writes, Set<String> indexDataKeys) {
        if (CollectionUtils.isEmpty(writes)) {
            return;
        }

        rowsBufferedCounter.increment(writes.size());
        long startNanos = System.nanoTime();

        String jobIndex = String.format(JOB_INDEX, jobId);
        executePipelinedWithRetry(jobId, patternId, writes, indexDataKeys, jobIndex);

        pipelineFlushCounter.increment();
        pipelineFlushLatency.record(System.nanoTime() - startNanos, TimeUnit.NANOSECONDS);
    }

    @Override
    public Map<String, Object> fetchValues(String jobId, String patternId, String dimensionValue, Set<String> keys) {
        String dataKey = String.format(DATA_KEY, jobId, patternId, dimensionValue);
        if (keys == null || keys.isEmpty()) {
            Map<Object, Object> entries = redisTemplate.opsForHash().entries(dataKey);
            Map<String, Object> all = new HashMap<>(entries.size());
            entries.forEach((k, v) -> all.put(String.valueOf(k), deserializeValue(String.valueOf(v))));
            return all;
        }

        List<Object> raw = redisTemplate.opsForHash().multiGet(dataKey, keys.stream().map(k -> (Object) k).toList());
        Map<String, Object> map = new HashMap<>();
        int i = 0;
        for (String key : keys) {
            Object val = raw.get(i++);
            if (val != null) {
                map.put(key, deserializeValue(String.valueOf(val)));
            }
        }
        return map;
    }

    @Override
    public boolean hasPattern(String jobId, String patternId) {
        return Boolean.TRUE.equals(redisTemplate.opsForSet().isMember(String.format(PATTERN_INDEX, jobId), patternId));
    }

    @Override
    public void markPatternAvailable(String jobId, String patternId) {
        redisTemplate.opsForSet().add(String.format(PATTERN_INDEX, jobId), patternId);
    }

    @Override
    public void cleanupJob(String jobId) {
        String jobIndex = String.format(JOB_INDEX, jobId);
        Set<String> keys = redisTemplate.opsForSet().members(jobIndex);
        if (keys != null && !keys.isEmpty()) {
            redisTemplate.delete(keys);
        }
        redisTemplate.delete(jobIndex);
        redisTemplate.delete(String.format(PATTERN_INDEX, jobId));
    }

    private void executePipelinedWithRetry(String jobId,
                                                   String patternId,
                                                   List<BatchWrite> writes,
                                                   Set<String> indexDataKeys,
                                                   String jobIndex) {
        RuntimeException lastError = null;
        for (int attempt = 0; attempt <= DEFAULT_MAX_RETRIES; attempt++) {
            try {
                redisTemplate.executePipelined((RedisConnection connection) -> {
                    for (BatchWrite write : writes) {
                        String dataKey = String.format(DATA_KEY, jobId, patternId, write.dimensionValue());
                        connection.hashCommands().hSet(
                                redisTemplate.getStringSerializer().serialize(dataKey),
                                redisTemplate.getStringSerializer().serialize(write.bulkKey()),
                                redisTemplate.getStringSerializer().serialize(serializeValue(write.value()))
                        );
                    }

                    if (!CollectionUtils.isEmpty(indexDataKeys)) {
                        byte[] indexKey = redisTemplate.getStringSerializer().serialize(jobIndex);
                        byte[][] members = indexDataKeys.stream()
                                .map(redisTemplate.getStringSerializer()::serialize)
                                .toArray(byte[][]::new);
                        connection.setCommands().sAdd(indexKey, members);
                    }
                    return null;
                });
                return;
            } catch (RuntimeException ex) {
                redisWriteFailureCounter.increment();
                lastError = ex;
                if (attempt < DEFAULT_MAX_RETRIES) {
                    redisWriteRetryCounter.increment();
                }
            }
        }

        throw new IllegalStateException("Failed to write batched values to redis after retries", lastError);
    }

    private String serializeValue(Object value) {
        if (value == null) {
            return "null";
        }
        if (value instanceof Number || value instanceof Boolean) {
            return String.valueOf(value);
        }

        try {
            return objectMapper.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize batch value", e);
        }
    }

    private Object deserializeValue(String value) {
        try {
            return objectMapper.readValue(value, Object.class);
        } catch (JsonProcessingException e) {
            return value;
        }
    }
}
