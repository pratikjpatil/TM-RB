


package com.tcs.fincore.ReportBuilder.batch.distributor;

import com.tcs.fincore.ReportBuilder.batch.model.QueryPattern;
import com.tcs.fincore.ReportBuilder.batch.store.BatchResultStore;
import com.tcs.fincore.ReportBuilder.model.ReportTemplate;
import com.tcs.fincore.ReportBuilder.service.aggregation.DataSourceSpec;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.*;

@Slf4j
@Component
@RequiredArgsConstructor
public class ResultDistributor {

    private final BatchResultStore batchResultStore;

    private final Map<String, JobDistributionContext> jobContexts = new java.util.concurrent.ConcurrentHashMap<>();

    public void initialize(
            String jobId,
            List<QueryPattern> patterns,
            Map<String, Map<String, Object>> preFetchedGlobals,
            ReportTemplate template
    ) {
        jobContexts.put(jobId, new JobDistributionContext(
                buildSpecToPatternMap(patterns),
                preFetchedGlobals,
                buildColumnMap(template)
        ));
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

        for (DataSourceSpec spec : specs) {
            try {
                Object val = extractValueForSpec(jobId, dimensionValue, spec, context);
                if (val != null && !"FALLBACK".equals(val)) {
                    results.put(spec.getSpecId(), val);
                } else {
                    // Logic for fallback: If explicitly FALLBACK, we skip putting it here so AggregatorService handles it.
                    // However, if we want to force 0.00 instead of fallback for batch misses:
                    if ("FALLBACK".equals(val)) {
                        // Mark for fallback or return default based on strictness policy.
                        // Returning "FALLBACK" string allows orchestrator to query individually.
                        results.put(spec.getSpecId(), "FALLBACK");
                    } else {
                        results.put(spec.getSpecId(), getDefaultValue(spec.getType()));
                    }
                }
            } catch (RuntimeException e) {
                log.error("Distributor error for spec {}: {}", spec.getSpecId(), e.getMessage());
                results.put(spec.getSpecId(), getDefaultValue(spec.getType()));
            }
        }
        return results;
    }

    private Object extractValueForSpec(String jobId, String dimensionValue, DataSourceSpec spec,
                                       JobDistributionContext context) {
        QueryPattern pattern = context.specToPatternMap().get(spec.getSpecId());
        if (pattern == null) return "FALLBACK";

        if (!batchResultStore.hasPattern(jobId, pattern.getPatternId())) return "FALLBACK";

        // 1. Resolve Keys
        List<String> bulkKeys = resolveBulkKeys(spec, dimensionValue, pattern, context);
        Set<String> keySet = new LinkedHashSet<>(bulkKeys);
        Map<String, Object> data = batchResultStore.fetchValues(jobId, pattern.getPatternId(), dimensionValue, keySet);
        if (data.isEmpty()) {
            data = batchResultStore.fetchValues(jobId, pattern.getPatternId(), "DEFAULT", keySet);
        }

        if (data.isEmpty()) return getDefaultValue(spec.getType());

        // 4. Aggregate values for all matched keys
        return aggregateValues(data, bulkKeys, spec.getType());
    }

    /**
     * FIXED: Returns a LIST of keys to look up.
     * Handles Indirection (Global Params) and Standard Bulk Filters.
     */
    private List<String> resolveBulkKeys(DataSourceSpec spec, String dimensionValue, QueryPattern pattern,
                                         JobDistributionContext context) {
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
            return values.stream().map(v -> (Comparable)v).max(Comparator.naturalOrder()).orElse(null);
        } else if (type == DataSourceSpec.Type.DB_MIN) {
            // Logic for Min
            return values.stream().map(v -> (Comparable)v).min(Comparator.naturalOrder()).orElse(null);
        }

        // For DB_VALUE or TEXT, we generally return the first match.
        // If multiple keys matched (e.g. IN clause for Text?), it's ambiguous, returning first is standard fallback.
        return values.get(0);
    }

    public void clearJobContext(String jobId) {
        jobContexts.remove(jobId);
    }

    private record JobDistributionContext(
            Map<String, QueryPattern> specToPatternMap,
            Map<String, Map<String, Object>> preFetchedGlobals,
            Map<String, String> columnToGlobalMap
    ) {
    }

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
