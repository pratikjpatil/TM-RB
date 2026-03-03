package com.tcs.fincore.ReportBuilder.batch.store;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface BatchResultStore {

    void initializeJob(String jobId, Duration ttl);

    void storeValue(String jobId, String patternId, String dimensionValue, String bulkKey, Object value);

    default void storeValues(String jobId, String patternId, List<BatchWrite> writes, Set<String> indexDataKeys) {
        for (BatchWrite write : writes) {
            storeValue(jobId, patternId, write.dimensionValue(), write.bulkKey(), write.value());
        }
    }

    Map<String, Object> fetchValues(String jobId, String patternId, String dimensionValue, Set<String> keys);

    boolean hasPattern(String jobId, String patternId);

    void markPatternAvailable(String jobId, String patternId);

    void cleanupJob(String jobId);

    record BatchWrite(String dimensionValue, String bulkKey, Object value) {
    }
}
