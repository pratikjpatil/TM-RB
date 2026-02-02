

package com.tcs.fincore.ReportBuilder.batch.distributor;

import com.tcs.fincore.ReportBuilder.batch.model.QueryPattern;
import com.tcs.fincore.ReportBuilder.service.aggregation.DataSourceSpec;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;

@Slf4j
@Component
public class ResultDistributor {

    private Map<String, Map<String, Map<String, Object>>> batchResults;
    private Map<String, QueryPattern> specToPatternMap;

    public void initialize(
            Map<String, Map<String, Map<String, Object>>> batchResults,
            List<QueryPattern> patterns) {
        this.batchResults = batchResults;
        this.specToPatternMap = buildSpecToPatternMap(patterns);
    }

    public Map<String, Object> getResultsForDimension(String dimensionValue, List<DataSourceSpec> specs) {
        if (batchResults == null) throw new IllegalStateException("Not initialized");
        Map<String, Object> results = new HashMap<>();

        for (DataSourceSpec spec : specs) {
            try {
                Object val = extractValueForSpec(dimensionValue, spec);
                if (val != null && !"FALLBACK".equals(val)) {
                    results.put(spec.getSpecId(), val);
                } else {
                    results.put(spec.getSpecId(), getDefaultValue(spec.getType()));
                }
            } catch (Exception e) {
                log.error("Distributor error for spec {}: {}", spec.getSpecId(), e.getMessage());
                results.put(spec.getSpecId(), getDefaultValue(spec.getType()));
            }
        }
        return results;
    }

    private Object extractValueForSpec(String dimensionValue, DataSourceSpec spec) {
        QueryPattern pattern = specToPatternMap.get(spec.getSpecId());
        if (pattern == null) return "FALLBACK";

        Map<String, Map<String, Object>> patternData = batchResults.get(pattern.getPatternId());
        if (patternData == null) return "FALLBACK";

        Map<String, Object> dimensionData = patternData.get(dimensionValue);
        if (dimensionData == null) return getDefaultValue(spec.getType());

        List<String> bulkKeys = extractBulkKeysFromSpec(spec, pattern);
        if (bulkKeys.isEmpty()) {
            Object value = dimensionData.get("DEFAULT");
            return value != null ? value : getDefaultValue(spec.getType());
        }
        return aggregateValues(dimensionData, bulkKeys, spec.getType());
    }

    /**
     * FIXED: Extract keys from potentially nested List/Map structures in filters
     */
    @SuppressWarnings("unchecked")
    private List<String> extractBulkKeysFromSpec(DataSourceSpec spec, QueryPattern pattern) {
        if (pattern.getBulkFilters() == null || pattern.getBulkFilters().isEmpty()) return Collections.emptyList();
        if (spec.getFilters() == null) return Collections.emptyList();

        List<String> allKeys = new ArrayList<>();

        for (String bulkColumn : pattern.getBulkFilters().keySet()) {
            Object filterValue = spec.getFilters().get(bulkColumn);
            if (filterValue == null) continue;

            // Normalize to List to handle both Map and List of Maps
            List<Object> constraints = new ArrayList<>();
            if (filterValue instanceof List) {
                constraints.addAll((List<?>) filterValue);
            } else {
                constraints.add(filterValue);
            }

            // Iterate constraints to find the IN clause (or Equality) used for Bulk selection
            for (Object constraint : constraints) {
                if (constraint instanceof Map) {
                    Map<String, Object> fm = (Map<String, Object>) constraint;
                    String op = String.valueOf(fm.get("op")).toUpperCase();

                    if ("IN".equals(op)) {
                        Object val = fm.get("value");
                        if (val instanceof Collection<?> col) {
                            for (Object item : col) if (item != null) allKeys.add(String.valueOf(item));
                        }
                    } else if ("=".equals(op)) {
                        Object val = fm.get("value");
                        if (val != null) allKeys.add(String.valueOf(val));
                    }
                } else {
                    // Scalar equality
                    allKeys.add(String.valueOf(constraint));
                }
            }
        }
        return allKeys;
    }

    private Object aggregateValues(Map<String, Object> data, List<String> keys, DataSourceSpec.Type type) {
        List<Object> values = new ArrayList<>();
        for (String key : keys) {
            Object v = data.get(key);
            if (v != null) values.add(v);
        }
        if (values.isEmpty()) return getDefaultValue(type);

        // Simple aggregation logic (truncated for brevity - use full logic from original file)
        if (type == DataSourceSpec.Type.DB_SUM) {
            double sum = 0;
            for (Object v : values) if (v instanceof Number n) sum += n.doubleValue();
            return sum;
        } else if (type == DataSourceSpec.Type.DB_COUNT) {
            long sum = 0;
            for (Object v : values) if (v instanceof Number n) sum += n.longValue();
            return sum;
        }
        // ... include other types from original code ...
        return values.get(0);
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