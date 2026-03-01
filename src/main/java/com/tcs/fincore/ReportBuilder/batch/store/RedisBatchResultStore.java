package com.tcs.fincore.ReportBuilder.batch.store;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Component
@ConditionalOnProperty(name = "report.batch.result-store", havingValue = "redis")
public class RedisBatchResultStore implements BatchResultStore {

    private static final String JOB_INDEX = "rb:batch:%s:keys";
    private static final String PATTERN_INDEX = "rb:batch:%s:patterns";
    private static final String DATA_KEY = "rb:batch:%s:%s:%s";

    private final StringRedisTemplate redisTemplate;
    private final ObjectMapper objectMapper;

    public RedisBatchResultStore(StringRedisTemplate redisTemplate, ObjectMapper objectMapper) {
        this.redisTemplate = redisTemplate;
        this.objectMapper = objectMapper;
    }

    @Override
    public void initializeJob(String jobId, Duration ttl) {
        redisTemplate.expire(String.format(PATTERN_INDEX, jobId), ttl);
        redisTemplate.expire(String.format(JOB_INDEX, jobId), ttl);
    }

    @Override
    public void storeValue(String jobId, String patternId, String dimensionValue, String bulkKey, Object value) {
        String dataKey = String.format(DATA_KEY, jobId, patternId, dimensionValue);
        redisTemplate.opsForHash().put(dataKey, bulkKey, toJson(value));
        redisTemplate.opsForSet().add(String.format(JOB_INDEX, jobId), dataKey);
    }

    @Override
    public Map<String, Object> fetchValues(String jobId, String patternId, String dimensionValue, Set<String> keys) {
        String dataKey = String.format(DATA_KEY, jobId, patternId, dimensionValue);
        if (keys == null || keys.isEmpty()) {
            Map<Object, Object> entries = redisTemplate.opsForHash().entries(dataKey);
            Map<String, Object> all = new HashMap<>(entries.size());
            entries.forEach((k, v) -> all.put(String.valueOf(k), fromJson(String.valueOf(v))));
            return all;
        }

        List<Object> raw = redisTemplate.opsForHash().multiGet(dataKey, keys.stream().map(k -> (Object) k).toList());
        Map<String, Object> map = new HashMap<>();
        int i = 0;
        for (String key : keys) {
            Object val = raw.get(i++);
            if (val != null) {
                map.put(key, fromJson(String.valueOf(val)));
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

    private String toJson(Object value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize batch value", e);
        }
    }

    private Object fromJson(String value) {
        try {
            return objectMapper.readValue(value, Object.class);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to deserialize batch value", e);
        }
    }
}

