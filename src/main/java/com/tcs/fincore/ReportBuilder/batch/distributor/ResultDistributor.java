package com.tcs.fincore.ReportBuilder.batch.distributor;

import com.tcs.fincore.ReportBuilder.batch.model.QueryPattern;
import com.tcs.fincore.ReportBuilder.batch.store.BatchResultStore;
import com.tcs.fincore.ReportBuilder.model.ReportTemplate;
import com.tcs.fincore.ReportBuilder.service.aggregation.DataSourceSpec;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Component
@RequiredArgsConstructor
public class ResultDistributor {

    private final BatchResultStore batchResultStore;

    @Value("${report.batch.distribution.cache.max-entries-per-job:${report.batch.distribution.cache-size:50000}}")
    private int maxCacheEntriesPerJob;

    private final Map<String, JobDistributionContext> jobContexts = new ConcurrentHashMap<>();

    public void initialize(String jobId, List<QueryPattern> patterns, Map<String, Map<String, Object>> preFetchedGlobals, ReportTemplate template) {
        jobContexts.put(jobId, new JobDistributionContext(
                buildSpecToPatternMap(patterns),
                preFetchedGlobals,
                buildColumnMap(template),
                new ValueFetchCache(maxCacheEntriesPerJob)));
    }

    private Map<String, String> buildColumnMap(ReportTemplate template) {
        Map<String, String> map = new HashMap<>();
        if (template.getGlobalParams() != null) {
            for (ReportTemplate.GlobalParamDef def : template.getGlobalParams()) {
                if (def.getMode() == ReportTemplate.ParamMode.QUERY && def.getSource() != null) {
                    map.put(def.getSource().getColumn(), def.getName());
                }
            }
        }
        return map;
    }

    public Map<String, Object> getResultsForDimension(String jobId, String dimensionValue, List<DataSourceSpec> specs) {
        JobDistributionContext context = jobContexts.get(jobId);
        if (context == null) throw new IllegalStateException("Job context not initialized for " + jobId);
        Map<String, Object> results = new HashMap<>();

        Map<QueryPattern, List<DataSourceSpec>> groupedSpecs = new LinkedHashMap<>();
        for (DataSourceSpec spec : specs) {
            QueryPattern pattern = context.specToPatternMap().get(spec.getSpecId());
            if (pattern == null) {
                results.put(spec.getSpecId(), "FALLBACK");
            } else {
                groupedSpecs.computeIfAbsent(pattern, ignored -> new ArrayList<>()).add(spec);
            }
        }

        for (Map.Entry<QueryPattern, List<DataSourceSpec>> entry : groupedSpecs.entrySet()) {
            populatePatternResults(jobId, dimensionValue, entry.getKey(), entry.getValue(), context, results);
        }

        return results;
    }

    private void populatePatternResults(String jobId,
                                        String dimensionValue,
                                        QueryPattern pattern,
                                        List<DataSourceSpec> specs,
                                        JobDistributionContext context,
                                        Map<String, Object> results) {
        if (!context.isPatternAvailable(jobId, pattern.getPatternId(), batchResultStore)) {
            for (DataSourceSpec spec : specs) {
                results.put(spec.getSpecId(), "FALLBACK");
            }
            return;
        }

        Map<String, List<String>> keysBySpecId = new HashMap<>();
        Set<String> unionKeys = new LinkedHashSet<>();
        for (DataSourceSpec spec : specs) {
            List<String> bulkKeys = resolveBulkKeys(spec, dimensionValue, pattern, context);
            keysBySpecId.put(spec.getSpecId(), bulkKeys);
            unionKeys.addAll(bulkKeys);
        }

        Map<String, Object> data = context.fetchValues(batchResultStore, jobId, pattern.getPatternId(), dimensionValue, unionKeys);
        if (data.isEmpty() && !"DEFAULT".equals(dimensionValue)) {
            data = context.fetchValues(batchResultStore, jobId, pattern.getPatternId(), "DEFAULT", unionKeys);
        }

        for (DataSourceSpec spec : specs) {
            try {
                List<String> bulkKeys = keysBySpecId.getOrDefault(spec.getSpecId(), Collections.emptyList());
                Object value = data.isEmpty() ? getDefaultValue(spec.getType()) : aggregateValues(data, bulkKeys, spec.getType());
                results.put(spec.getSpecId(), value != null ? value : getDefaultValue(spec.getType()));
            } catch (RuntimeException e) {
                log.error("Distributor error for spec {}: {}", spec.getSpecId(), e.getMessage());
                results.put(spec.getSpecId(), getDefaultValue(spec.getType()));
            }
        }
    }

    /**
     * FIXED: Returns a LIST of keys to look up.
     * Handles Indirection (Global Params) and Standard Bulk Filters.
     */
    private List<String> resolveBulkKeys(DataSourceSpec spec, String dimensionValue, QueryPattern pattern, JobDistributionContext context) {
        // A. check if this column is driven by a Global Param (Indirection)
        String globalParamName = context.columnToGlobalMap().get(spec.getColumn());
        if (globalParamName != null && context.preFetchedGlobals() != null) {
            Map<String, Object> globalValues = context.preFetchedGlobals().get(globalParamName);
            if (globalValues != null && globalValues.containsKey(dimensionValue)) {
                return Collections.singletonList(String.valueOf(globalValues.get(dimensionValue)));
            }
        }

        // B. Standard Bulk Filter Logic (Direct Mapping with Cartesian Product)
        return extractBulkKeysFromSpec(spec, pattern);
    }

    /**
     * FIXED: Generates keys matching the Materializer's format.
     * 1. Matches "SingleColumnValue" if only 1 bulk column.
     * 2. Matches "Val1:Val2" if multiple bulk columns.
     * 3. Generates Cartesian product if filters contain IN clauses.
     */
    private List<String> extractBulkKeysFromSpec(DataSourceSpec spec, QueryPattern pattern) {
        if (pattern.getBulkFilters() == null || pattern.getBulkFilters().isEmpty()) {
            return Collections.singletonList("DEFAULT");
        }

        // 1. Identify Bulk Columns and Sort them (Must match Materializer sorting)
        List<String> sortedCols = new ArrayList<>(pattern.getBulkFilters().keySet());
        Collections.sort(sortedCols);

        // 2. Extract values for each column from the Spec
        List<List<String>> columnsValues = new ArrayList<>();

        for (String col : sortedCols) {
            Object filterVal = spec.getFilters() != null ? spec.getFilters().get(col) : null;
            List<String> valuesForCol = new ArrayList<>();

            if (filterVal == null) {
                // If spec has no filter for a bulk column, it technically implies "ALL"
                // but in our grouping logic, specs without filters shouldn't group with specs with filters
                // unless strictly handled. Assuming NULL/Missing matches "NULL" string from DB or nothing.
                // However, usually specs in the same pattern share the filter structure.
                // We will treat it as "NULL" to match Materializer's behavior for missing data.
                valuesForCol.add("NULL");
            } else {
                List<Map<String, Object>> constraints = normalizeConstraints(filterVal);
                for (Map<String, Object> c : constraints) {
                    Object val = c.get("value");
                    String op = String.valueOf(c.get("op")).toUpperCase();

                    if ("IN".equals(op) && val instanceof Collection<?> list) {
                        for (Object v : list) valuesForCol.add(String.valueOf(v));
                    } else if (val != null) {
                        valuesForCol.add(String.valueOf(val));
                    }
                }
            }

            if (valuesForCol.isEmpty()) return Collections.emptyList(); // No valid values -> No results
            columnsValues.add(valuesForCol);
        }

        // 3. Generate Keys
        if (sortedCols.size() == 1) {
            // Single column: No joining needed
            return columnsValues.get(0);
        } else {
            // Multiple columns: Cartesian Product + Join with ":"
            return generateCompositeKeys(columnsValues);
        }
    }

    // Helper to generate Cartesian product of keys (e.g. [A,B] x [1,2] -> [A:1, A:2, B:1, B:2])
    private List<String> generateCompositeKeys(List<List<String>> lists) {
        List<String> result = new ArrayList<>();
        if (lists.isEmpty()) {
            result.add("");
            return result;
        }

        List<String> firstList = lists.get(0);
        List<String> remainingRecursive = generateCompositeKeys(lists.subList(1, lists.size()));

        for (String head : firstList) {
            for (String tail : remainingRecursive) {
                if (tail.isEmpty()) {
                    result.add(head);
                } else {
                    result.add(head + ":" + tail);
                }
            }
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> normalizeConstraints(Object filterValue) {
        List<Map<String, Object>> result = new ArrayList<>();
        if (filterValue instanceof List<?> list) {
            for (Object item : list) {
                if (item instanceof Map) result.add((Map<String, Object>) item);
            }
        } else if (filterValue instanceof Map) {
            result.add((Map<String, Object>) filterValue);
        } else {
            Map<String, Object> simple = new HashMap<>();
            simple.put("op", "=");
            simple.put("value", filterValue);
            result.add(simple);
        }
        return result;
    }

    private Object aggregateValues(Map<String, Object> data, List<String> keys, DataSourceSpec.Type type) {
        List<Object> values = new ArrayList<>();
        for (String key : keys) {
            Object v = data.get(key);
            if (v != null) values.add(v);
        }

        if (values.isEmpty()) return getDefaultValue(type);

        if (type == DataSourceSpec.Type.DB_SUM) {
            double sum = 0;
            for (Object v : values) if (v instanceof Number n) sum += n.doubleValue();
            return sum;
        } else if (type == DataSourceSpec.Type.DB_COUNT) {
            long sum = 0;
            for (Object v : values) if (v instanceof Number n) sum += n.longValue();
            return sum;
        } else if (type == DataSourceSpec.Type.DB_MAX) {
            // Logic for Max
            return values.stream().map(v -> (Comparable) v).max(Comparator.naturalOrder()).orElse(null);
        } else if (type == DataSourceSpec.Type.DB_MIN) {
            // Logic for Min
            return values.stream().map(v -> (Comparable) v).min(Comparator.naturalOrder()).orElse(null);
        }

        // For DB_VALUE or TEXT, we generally return the first match.
        // If multiple keys matched (e.g. IN clause for Text?), it's ambiguous, returning first is standard fallback.
        return values.get(0);
    }

    public void clearJobContext(String jobId) {
        jobContexts.remove(jobId);
    }

    private record JobDistributionContext(Map<String, QueryPattern> specToPatternMap,
                                          Map<String, Map<String, Object>> preFetchedGlobals,
                                          Map<String, String> columnToGlobalMap,
                                          ValueFetchCache valueFetchCache) {

        private boolean isPatternAvailable(String jobId, String patternId, BatchResultStore store) {
            return valueFetchCache.patternAvailability.computeIfAbsent(patternId, ignored -> store.hasPattern(jobId, patternId));
        }

        private Map<String, Object> fetchValues(BatchResultStore store,
                                                String jobId,
                                                String patternId,
                                                String dimensionValue,
                                                Set<String> keys) {
            return valueFetchCache.getOrLoad(
                    patternId,
                    dimensionValue,
                    keys,
                    requestedKeys -> store.fetchValues(jobId, patternId, dimensionValue, requestedKeys));
        }
    }

    private static class ValueFetchCache {
        private final int maxEntries;
        private final Map<String, Boolean> patternAvailability = new ConcurrentHashMap<>();
        private final Map<DimensionCacheKey, DimensionValueCache> values = new ConcurrentHashMap<>();
        private final Deque<DimensionCacheKey> accessOrder = new ArrayDeque<>();

        private ValueFetchCache(int maxEntries) {
            this.maxEntries = Math.max(1000, maxEntries);
        }

        private Map<String, Object> getOrLoad(String patternId,
                                              String dimensionValue,
                                              Set<String> keys,
                                              java.util.function.Function<Set<String>, Map<String, Object>> loader) {
            DimensionCacheKey cacheKey = new DimensionCacheKey(patternId, dimensionValue);
            DimensionValueCache valueCache = values.computeIfAbsent(cacheKey, ignored -> new DimensionValueCache());
            touch(cacheKey);

            if (keys == null || keys.isEmpty()) {
                return valueCache.loadAll(loader);
            }

            return valueCache.loadKeys(keys, loader);
        }

        private void touch(DimensionCacheKey cacheKey) {
            synchronized (accessOrder) {
                accessOrder.remove(cacheKey);
                accessOrder.addLast(cacheKey);
                while (accessOrder.size() > maxEntries) {
                    DimensionCacheKey evicted = accessOrder.pollFirst();
                    if (evicted != null) {
                        values.remove(evicted);
                    }
                }
            }
        }

        private static class DimensionValueCache {
            private final Map<String, Object> cachedValues = new HashMap<>();
            private boolean fullSnapshotLoaded;

            private synchronized Map<String, Object> loadAll(java.util.function.Function<Set<String>, Map<String, Object>> loader) {
                if (!fullSnapshotLoaded) {
                    Map<String, Object> loaded = loader.apply(Collections.emptySet());
                    if (loaded != null && !loaded.isEmpty()) {
                        cachedValues.putAll(loaded);
                    }
                    fullSnapshotLoaded = true;
                }
                return new HashMap<>(cachedValues);
            }

            private synchronized Map<String, Object> loadKeys(Set<String> keys,
                                                              java.util.function.Function<Set<String>, Map<String, Object>> loader) {
                if (fullSnapshotLoaded) {
                    return filterKeys(keys);
                }

                Set<String> missing = new LinkedHashSet<>();
                for (String key : keys) {
                    if (!cachedValues.containsKey(key)) {
                        missing.add(key);
                    }
                }

                if (!missing.isEmpty()) {
                    Map<String, Object> loaded = loader.apply(missing);
                    if (loaded != null && !loaded.isEmpty()) {
                        cachedValues.putAll(loaded);
                    }
                }

                return filterKeys(keys);
            }

            private Map<String, Object> filterKeys(Set<String> keys) {
                Map<String, Object> filtered = new HashMap<>(keys.size());
                for (String key : keys) {
                    Object value = cachedValues.get(key);
                    if (value != null) {
                        filtered.put(key, value);
                    }
                }
                return filtered;
            }
        }
    }

    private record DimensionCacheKey(String patternId, String dimensionValue) {}

    private Object getDefaultValue(DataSourceSpec.Type type) {
        return switch (type) {
            case DB_SUM, DB_AVG, DB_MIN, DB_MAX -> 0.0;
            case DB_COUNT -> 0L;
            default -> "";
        };
    }

    private Map<String, QueryPattern> buildSpecToPatternMap(List<QueryPattern> patterns) {
        Map<String, QueryPattern> map = new HashMap<>();
        for (QueryPattern p : patterns) {
            for (String sid : p.getSourceSpecIds()) map.put(sid, p);
        }
        return map;
    }
}
