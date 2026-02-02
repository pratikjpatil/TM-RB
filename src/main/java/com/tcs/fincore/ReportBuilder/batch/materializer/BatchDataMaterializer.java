
package com.tcs.fincore.ReportBuilder.batch.materializer;

import com.tcs.fincore.ReportBuilder.batch.model.QueryPattern;
import com.tcs.fincore.ReportBuilder.exception.ValidationException;
import com.tcs.fincore.ReportBuilder.util.FilterSqlBuilder;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.*;

/**
 * FIXED: Materializer supporting Multi-Operator filters (List structures)
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class BatchDataMaterializer {

    private final NamedParameterJdbcTemplate jdbc;
    private final ExecutorService materializationExecutor = Executors.newFixedThreadPool(3);

    public Map<String, Map<String, Map<String, Object>>> materializeAll(
            List<QueryPattern> patterns,
            long timeoutMinutes) {

        Map<String, Map<String, Map<String, Object>>> allResults = new ConcurrentHashMap<>();
        List<CompletableFuture<Void>> futures = new ArrayList<>();

        for (QueryPattern pattern : patterns) {
            CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                try {
                    Map<String, Map<String, Object>> res = materializePattern(pattern);
                    allResults.put(pattern.getPatternId(), res);
                } catch (Exception e) {
                    log.error("Failed to materialize pattern {}: {}", pattern.getPatternId(), e.getMessage(), e);
                    throw new CompletionException(e);
                }
            }, materializationExecutor);
            futures.add(future);
        }

        try {
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                    .get(timeoutMinutes, TimeUnit.MINUTES);
        } catch (Exception e) {
            throw new ValidationException("Data materialization failed or timed out", e);
        }

        return allResults;
    }

    private Map<String, Map<String, Object>> materializePattern(QueryPattern pattern) {
        long start = System.currentTimeMillis();
        List<Map<String, Object>> rows = executeQuery(pattern);
        Map<String, Map<String, Object>> nestedMap = transformResults(rows, pattern);
        log.debug("Materialized pattern {} in {}ms ({} rows)", pattern.getPatternId(), System.currentTimeMillis() - start, rows.size());
        return nestedMap;
    }

    private List<Map<String, Object>> executeQuery(QueryPattern pattern) {
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT /*+ PARALLEL(8) */ ");

        List<String> groupByCols = new ArrayList<>();

        // Varying Dimension
        if (pattern.getVaryingDimension() != null) {
            String dimCol = pattern.getVaryingDimension().getColumnName();
            FilterSqlBuilder.validateIdentifier(dimCol);
            sql.append(dimCol).append(", ");
            groupByCols.add(dimCol);
        }

        // Bulk Filters
        if (pattern.getBulkFilters() != null) {
            for (String bulkCol : pattern.getBulkFilters().keySet()) {
                FilterSqlBuilder.validateIdentifier(bulkCol);
                sql.append(bulkCol).append(", ");
                groupByCols.add(bulkCol);
            }
        }

        // Aggregation
        String aggFunc = pattern.getAggregationType();
        FilterSqlBuilder.validateIdentifier(pattern.getColumn());
        if ("VALUE".equals(aggFunc)) {
            sql.append(pattern.getColumn());
        } else {
            sql.append(aggFunc).append("(").append(pattern.getColumn()).append(")");
        }
        sql.append(" AS AGG_VALUE FROM ").append(pattern.getTable()).append(" WHERE 1=1 ");

        Map<String, Object> params = new HashMap<>();

        // Build Unified Filter Map (Handling Lists)
        Map<String, Object> allFilters = buildUnifiedFilterMap(pattern);
        FilterSqlBuilder.appendFilters(sql, params, allFilters);

        if (!"VALUE".equals(aggFunc) && !groupByCols.isEmpty()) {
            sql.append(" GROUP BY ").append(String.join(", ", groupByCols));
        }
        log.debug("pattern :: {} sql : {}, params: {}", pattern.getPatternId(), sql.toString(), params);
        return jdbc.queryForList(sql.toString(), params);
    }

    /**
     * Reconstructs the filter map for SQL Builder, merging Static, Bulk, and Conditional filters.
     * Ensures values are Lists where necessary to support multiple operators per column.
     */
    private Map<String, Object> buildUnifiedFilterMap(QueryPattern pattern) {
        Map<String, Object> filters = new HashMap<>();

        // 1. Static Filters (Now stored as List<Map> or Map in QueryPattern)
        if (pattern.getStaticFilters() != null) {
            pattern.getStaticFilters().forEach((col, val) -> {
                // If val is already a List, put it directly.
                // If it's a Map, put it.
                // If it's scalar, wrap it.
                filters.put(col, val);
            });
        }

        // 2. Varying Dimension (IN clause)
        if (pattern.getVaryingDimension() != null) {
            filters.put(pattern.getVaryingDimension().getColumnName(), Map.of(
                    "op", "IN",
                    "value", new ArrayList<>(pattern.getVaryingDimension().getAllValues()),
                    "dataType", pattern.getVaryingDimension().getDataType()
            ));
        }

        // 3. Bulk Filters (IN clauses) - Need to Merge if column already exists
        if (pattern.getBulkFilters() != null) {
            pattern.getBulkFilters().forEach((col, values) -> {
                Map<String, Object> bulkIn = Map.of(
                        "op", "IN",
                        "value", new ArrayList<>(values),
                        "dataType", "STRING" // Default
                );

                filters.merge(col, bulkIn, (existing, newIn) -> {
                    // If existing is a List, add to it.
                    if (existing instanceof List) {
                        @SuppressWarnings("unchecked")
                        List<Object> list = new ArrayList<>((List<Object>) existing);
                        list.add(newIn);
                        return list;
                    } else {
                        // Existing is single object, create list
                        List<Object> list = new ArrayList<>();
                        list.add(existing);
                        list.add(newIn);
                        return list;
                    }
                });
            });
        }

        // 4. Conditional Filters (Legacy, mainly used for non-batchable logic, but good to check)
        if (pattern.getConditionalFilters() != null) {
            for (QueryPattern.ConditionalFilter cf : pattern.getConditionalFilters()) {
                Map<String, Object> cfMap = Map.of(
                        "op", cf.getOperator(),
                        "value", cf.getValue(),
                        "dataType", cf.getDataType()
                );

                filters.merge(cf.getColumnName(), cfMap, (existing, newCf) -> {
                    if (existing instanceof List) {
                        @SuppressWarnings("unchecked")
                        List<Object> list = new ArrayList<>((List<Object>) existing);
                        list.add(newCf);
                        return list;
                    } else {
                        List<Object> list = new ArrayList<>();
                        list.add(existing);
                        list.add(newCf);
                        return list;
                    }
                });
            }
        }

        return filters;
    }

    private Map<String, Map<String, Object>> transformResults(List<Map<String, Object>> rows, QueryPattern pattern) {
        Map<String, Map<String, Object>> result = new HashMap<>();
        String dimColumn = pattern.getVaryingDimension() != null ? pattern.getVaryingDimension().getColumnName() : null;
        Set<String> bulkColumns = pattern.getBulkFilters() != null ? pattern.getBulkFilters().keySet() : Collections.emptySet();

        for (Map<String, Object> row : rows) {
            String dimValue = dimColumn != null ? String.valueOf(row.get(dimColumn)) : "DEFAULT";
            String bulkKey = buildBulkKey(row, bulkColumns);
            result.computeIfAbsent(dimValue, k -> new HashMap<>()).put(bulkKey, row.get("AGG_VALUE"));
        }
        return result;
    }

    private String buildBulkKey(Map<String, Object> row, Set<String> bulkColumns) {
        if (bulkColumns.isEmpty()) return "DEFAULT";
        if (bulkColumns.size() == 1) {
            String col = bulkColumns.iterator().next();
            Object val = row.get(col);
            return val != null ? String.valueOf(val) : "NULL";
        }
        List<String> sortedColumns = new ArrayList<>(bulkColumns);
        Collections.sort(sortedColumns);
        List<String> keyParts = new ArrayList<>();
        for (String col : sortedColumns) {
            Object val = row.get(col);
            keyParts.add(val != null ? String.valueOf(val) : "NULL");
        }
        return String.join(":", keyParts);
    }
}