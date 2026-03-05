package com.tcs.fincore.ReportBuilder.batch.distributor;

import com.tcs.fincore.ReportBuilder.batch.model.QueryPattern;
import com.tcs.fincore.ReportBuilder.batch.store.BatchResultStore;
import com.tcs.fincore.ReportBuilder.batch.store.InMemoryBatchResultStore;
import com.tcs.fincore.ReportBuilder.model.ReportTemplate;
import com.tcs.fincore.ReportBuilder.service.aggregation.DataSourceSpec;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class ResultDistributorTest {

    private InMemoryBatchResultStore store;
    private ResultDistributor distributor;

    @BeforeEach
    void setUp() {
        store = new InMemoryBatchResultStore();
        distributor = new ResultDistributor(store);
    }

    @Test
    void shouldResolveValuesPerJobAndDimension() {
        String jobId = "job-1";
        String patternId = "p1";
        String specId = "s1";

        store.initializeJob(jobId, Duration.ofMinutes(5));
        store.storeValue(jobId, patternId, "D1", "DEFAULT", 123.0);
        store.markPatternAvailable(jobId, patternId);

        QueryPattern pattern = QueryPattern.builder()
                .patternId(patternId)
                .sourceSpecIds(Set.of(specId))
                .build();

        distributor.initialize(jobId, List.of(pattern), null, new ReportTemplate());

        DataSourceSpec spec = new DataSourceSpec(specId, DataSourceSpec.Type.DB_SUM,
                "T1", "AMOUNT", Map.of(), "");

        Map<String, Object> result = distributor.getResultsForDimension(jobId, "D1", List.of(spec));

        assertEquals(123.0, result.get(specId));
    }

    @Test
    void shouldKeepContextsIsolatedAcrossJobs() {
        QueryPattern pattern = QueryPattern.builder()
                .patternId("pattern")
                .sourceSpecIds(Set.of("s1"))
                .build();

        store.initializeJob("job-A", Duration.ofMinutes(5));
        store.initializeJob("job-B", Duration.ofMinutes(5));
        store.storeValue("job-A", "pattern", "A", "DEFAULT", 10.0);
        store.storeValue("job-B", "pattern", "B", "DEFAULT", 20.0);
        store.markPatternAvailable("job-A", "pattern");
        store.markPatternAvailable("job-B", "pattern");

        distributor.initialize("job-A", List.of(pattern), null, new ReportTemplate());
        distributor.initialize("job-B", List.of(pattern), null, new ReportTemplate());

        DataSourceSpec spec = new DataSourceSpec("s1", DataSourceSpec.Type.DB_SUM,
                "T1", "AMOUNT", Map.of(), "");

        Map<String, Object> resultA = distributor.getResultsForDimension("job-A", "A", List.of(spec));
        Map<String, Object> resultB = distributor.getResultsForDimension("job-B", "B", List.of(spec));

        assertEquals(10.0, resultA.get("s1"));
        assertEquals(20.0, resultB.get("s1"));
    }

    @Test
    void shouldCachePatternAvailabilityAndFetchedValuesForRepeatedReads() {
        String jobId = "job-cache";
        String patternId = "p-cache";
        String specId = "s-cache";

        CountingBatchResultStore countingStore = new CountingBatchResultStore();
        ResultDistributor cachedDistributor = new ResultDistributor(countingStore);

        countingStore.initializeJob(jobId, Duration.ofMinutes(5));
        countingStore.storeValue(jobId, patternId, "D1", "DEFAULT", 88.0);
        countingStore.markPatternAvailable(jobId, patternId);

        QueryPattern pattern = QueryPattern.builder()
                .patternId(patternId)
                .sourceSpecIds(Set.of(specId))
                .build();
        cachedDistributor.initialize(jobId, List.of(pattern), null, new ReportTemplate());

        DataSourceSpec spec = new DataSourceSpec(specId, DataSourceSpec.Type.DB_SUM,
                "T1", "AMOUNT", Map.of(), "");

        cachedDistributor.getResultsForDimension(jobId, "D1", List.of(spec));
        cachedDistributor.getResultsForDimension(jobId, "D1", List.of(spec));

        assertEquals(1, countingStore.hasPatternCalls.get(), "pattern availability should be cached");
        assertEquals(1, countingStore.fetchValuesCalls.get(), "dimension values should be cached after first lookup");
    }

    @Test
    void shouldFetchRedisOncePerPatternAndDimensionForMultipleSpecs() {
        String jobId = "job-grouped";
        String patternId = "p-grouped";

        CountingBatchResultStore countingStore = new CountingBatchResultStore();
        ResultDistributor cachedDistributor = new ResultDistributor(countingStore);

        countingStore.initializeJob(jobId, Duration.ofMinutes(5));
        countingStore.storeValue(jobId, patternId, "D1", "B1", 11.0);
        countingStore.storeValue(jobId, patternId, "D1", "B2", 22.0);
        countingStore.markPatternAvailable(jobId, patternId);

        QueryPattern pattern = QueryPattern.builder()
                .patternId(patternId)
                .sourceSpecIds(Set.of("s1", "s2"))
                .bulkFilters(Map.of("BRANCH", Set.of("B1", "B2")))
                .build();
        cachedDistributor.initialize(jobId, List.of(pattern), null, new ReportTemplate());

        DataSourceSpec spec1 = new DataSourceSpec("s1", DataSourceSpec.Type.DB_SUM,
                "T1", "AMOUNT", Map.of("BRANCH", Map.of("op", "=", "value", "B1")), "");
        DataSourceSpec spec2 = new DataSourceSpec("s2", DataSourceSpec.Type.DB_SUM,
                "T1", "AMOUNT", Map.of("BRANCH", Map.of("op", "=", "value", "B2")), "");

        Map<String, Object> result = cachedDistributor.getResultsForDimension(jobId, "D1", List.of(spec1, spec2));

        assertEquals(11.0, result.get("s1"));
        assertEquals(22.0, result.get("s2"));
        assertEquals(1, countingStore.hasPatternCalls.get(), "pattern availability should be checked once per pattern");
        assertEquals(1, countingStore.fetchValuesCalls.get(), "store should be fetched once for grouped specs");

        cachedDistributor.getResultsForDimension(jobId, "D1", List.of(spec1, spec2));
        assertEquals(1, countingStore.fetchValuesCalls.get(), "subsequent calls should hit in-memory fetch cache");
        assertFalse(countingStore.requestedKeySets.isEmpty(), "store should receive key set for selective fetch");
        assertFalse(countingStore.requestedKeySets.get(0).isEmpty(), "redis fetch should not request full hash for grouped specs");
    }

    private static class CountingBatchResultStore implements BatchResultStore {
        private final InMemoryBatchResultStore delegate = new InMemoryBatchResultStore();
        private final AtomicInteger hasPatternCalls = new AtomicInteger();
        private final AtomicInteger fetchValuesCalls = new AtomicInteger();
        private final List<Set<String>> requestedKeySets = new CopyOnWriteArrayList<>();

        @Override
        public void initializeJob(String jobId, Duration ttl) {
            delegate.initializeJob(jobId, ttl);
        }

        @Override
        public void storeValue(String jobId, String patternId, String dimensionValue, String bulkKey, Object value) {
            delegate.storeValue(jobId, patternId, dimensionValue, bulkKey, value);
        }

        @Override
        public Map<String, Object> fetchValues(String jobId, String patternId, String dimensionValue, Set<String> keys) {
            fetchValuesCalls.incrementAndGet();
            requestedKeySets.add(keys == null ? Set.of() : Set.copyOf(keys));
            return delegate.fetchValues(jobId, patternId, dimensionValue, keys);
        }

        @Override
        public boolean hasPattern(String jobId, String patternId) {
            hasPatternCalls.incrementAndGet();
            return delegate.hasPattern(jobId, patternId);
        }

        @Override
        public void markPatternAvailable(String jobId, String patternId) {
            delegate.markPatternAvailable(jobId, patternId);
        }

        @Override
        public void cleanupJob(String jobId) {
            delegate.cleanupJob(jobId);
        }
    }
}
