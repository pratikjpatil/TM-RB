package com.tcs.fincore.ReportBuilder.batch.store;

import java.time.Duration;
import java.util.Map;
import java.util.Set;

public interface BatchResultStore {

    void initializeJob(String jobId, Duration ttl);

    void storeValue(String jobId, String patternId, String dimensionValue, String bulkKey, Object value);

    Map<String, Object> fetchValues(String jobId, String patternId, String dimensionValue, Set<String> keys);

    boolean hasPattern(String jobId, String patternId);

    void markPatternAvailable(String jobId, String patternId);

    void cleanupJob(String jobId);
}

