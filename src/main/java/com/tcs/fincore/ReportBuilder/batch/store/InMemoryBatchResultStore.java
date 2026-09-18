package com.tcs.fincore.ReportBuilder.batch.store;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Component
@ConditionalOnProperty(name = "report.batch.result-store", havingValue = "memory", matchIfMissing = true)
public class InMemoryBatchResultStore implements BatchResultStore {

    private final Map<String, Map<String, Map<String, Map<String, Object>>>> jobs = new ConcurrentHashMap<>();
    private final Map<String, Set<String>> availablePatterns = new ConcurrentHashMap<>();

    @Override
    public void initializeJob(String jobId) {
        jobs.put(jobId, new ConcurrentHashMap<>());
        availablePatterns.put(jobId, ConcurrentHashMap.newKeySet());
    }

    @Override
    public void storeValue(String jobId, String patternId, String dimensionValue, String bulkKey, Object value) {
        jobs.computeIfAbsent(jobId, k -> new ConcurrentHashMap<>())
                .computeIfAbsent(patternId, k -> new ConcurrentHashMap<>())
                .computeIfAbsent(dimensionValue, k -> new ConcurrentHashMap<>())
                .put(bulkKey, value);
    }

    @Override
    public Map<String, Object> fetchValues(String jobId, String patternId, String dimensionValue, Set<String> keys) {
        Map<String, Object> data = jobs.getOrDefault(jobId, Collections.emptyMap())
                .getOrDefault(patternId, Collections.emptyMap())
                .getOrDefault(dimensionValue, Collections.emptyMap());

        if (keys == null || keys.isEmpty()) {
            return data;
        }

        Map<String, Object> filtered = new ConcurrentHashMap<>();
        for (String key : keys) {
            if (data.containsKey(key)) {
                filtered.put(key, data.get(key));
            }
        }
        return filtered;
    }

    @Override
    public boolean hasPattern(String jobId, String patternId) {
        return availablePatterns.getOrDefault(jobId, Collections.emptySet()).contains(patternId);
    }

    @Override
    public void markPatternAvailable(String jobId, String patternId) {
        availablePatterns.computeIfAbsent(jobId, k -> ConcurrentHashMap.newKeySet()).add(patternId);
    }

    @Override
    public void cleanupJob(String jobId) {
        jobs.remove(jobId);
        availablePatterns.remove(jobId);
    }
}