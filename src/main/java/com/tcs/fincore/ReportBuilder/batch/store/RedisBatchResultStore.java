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
    private static final String NULL_PREFIX = "NUL:";
    private static final String NUMBER_PREFIX = "NUM:";
    private static final String BOOLEAN_PREFIX = "BOOL:";
    private static final String STRING_PREFIX = "STR:";
    private static final String JSON_PREFIX = "JSON:";

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
            return NULL_PREFIX;
        }
        if (value instanceof Number) {
            return NUMBER_PREFIX + value;
        }
        if (value instanceof Boolean) {
            return BOOLEAN_PREFIX + value;
        }
        if (value instanceof String str) {
            return STRING_PREFIX + str;
        }

        try {
            return JSON_PREFIX + objectMapper.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize batch value", e);
        }
    }

    private Object deserializeValue(String value) {
        if (value == null || value.startsWith(NULL_PREFIX)) {
            return null;
        }
        if (value.startsWith(NUMBER_PREFIX)) {
            return parseNumber(value.substring(NUMBER_PREFIX.length()));
        }
        if (value.startsWith(BOOLEAN_PREFIX)) {
            return Boolean.parseBoolean(value.substring(BOOLEAN_PREFIX.length()));
        }
        if (value.startsWith(STRING_PREFIX)) {
            return value.substring(STRING_PREFIX.length());
        }
        if (value.startsWith(JSON_PREFIX)) {
            return deserializeJsonValue(value.substring(JSON_PREFIX.length()));
        }

        // Backward compatibility with older jobs that stored raw values.
        if ("null".equalsIgnoreCase(value)) {
            return null;
        }
        Object parsedLegacyNumber = parseNumber(value);
        if (parsedLegacyNumber != null) {
            return parsedLegacyNumber;
        }
        if ("true".equalsIgnoreCase(value) || "false".equalsIgnoreCase(value)) {
            return Boolean.parseBoolean(value);
        }

        return deserializeJsonValue(value);
    }

    private Object deserializeJsonValue(String value) {
        try {
            return objectMapper.readValue(value, Object.class);
        } catch (JsonProcessingException e) {
            return value;
        }
    }

    private Number parseNumber(String value) {
        if (value == null || value.isBlank()) {
            return null;
        }
        try {
            if (value.indexOf('.') >= 0 || value.indexOf('e') >= 0 || value.indexOf('E') >= 0) {
                return Double.parseDouble(value);
            }
            return Long.parseLong(value);
        } catch (NumberFormatException ignored) {
            return null;
        }
    }
}