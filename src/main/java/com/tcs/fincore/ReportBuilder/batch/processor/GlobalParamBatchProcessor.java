//package com.tcs.fincore.ReportBuilder.batch.processor;
//
//import com.tcs.fincore.ReportBuilder.model.ReportTemplate;
//import com.tcs.fincore.ReportBuilder.model.ReportTemplate.GlobalParamDef;
//import com.tcs.fincore.ReportBuilder.util.FilterSqlBuilder;
//import com.tcs.fincore.ReportBuilder.util.GlobalParamResolver;
//import lombok.RequiredArgsConstructor;
//import lombok.extern.slf4j.Slf4j;
//import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
//import org.springframework.stereotype.Service;
//
//import java.util.*;
//import java.util.concurrent.ConcurrentHashMap;
//import java.util.stream.Collectors;
//
//@Slf4j
//@Service
//@RequiredArgsConstructor
//public class GlobalParamBatchProcessor {
//
//    private final NamedParameterJdbcTemplate jdbc;
//
//    /**
//     * Pre-fetches varying global parameters for all batch items.
//     * * @param template The report template
//     * @param staticContext Context containing already resolved static globals (Dates, Constants)
//     * @param varyingParamName The name of the dimension varying in batch (e.g., "BRANCH_CODE")
//     * @param allDimensionValues List of all values for the varying dimension (e.g., ["001", "002"...])
//     * @return Map<ParamName, Map<DimensionValue, ResultValue>>
//     */
//    public Map<String, Map<String, Object>> prefetchDynamicGlobals(
//            ReportTemplate template,
//            Map<String, Object> staticContext,
//            String varyingParamName,
//            List<String> allDimensionValues
//    ) {
//        Map<String, Map<String, Object>> resultMap = new ConcurrentHashMap<>();
//
//        if (template.getGlobalParams() == null) return resultMap;
//
//        // 1. Identify Batchable Parameters
//        // Must be QUERY mode AND depend on the varyingParamName
//        for (GlobalParamDef def : template.getGlobalParams()) {
//            if (def.getMode() != ReportTemplate.ParamMode.QUERY) continue;
//
//            // Optimization: If it's already in static context, skip
//            if (staticContext.containsKey(def.getName())) continue;
//
//            BatchQueryPlan plan = analyzeForBatching(def, varyingParamName, staticContext);
//            if (plan != null) {
//                log.info("Batching Global Param '{}' using table '{}' linked by '{}'",
//                        def.getName(), plan.table, plan.linkColumn);
//
//                Map<String, Object> bulkResults = executeBulkFetch(plan, allDimensionValues);
//                resultMap.put(def.getName(), bulkResults);
//            } else {
//                log.debug("Global Param '{}' is not suitable for simple batching. Falling back to per-row execution.", def.getName());
//            }
//        }
//        return resultMap;
//    }
//
////    private record BatchQueryPlan(
////            String table,
////            String targetColumn,
////            String linkColumn, // The DB column that matches BRANCH_CODE
////            Map<String, Object> staticFilters // Other filters (e.g., STATUS='A')
////    ) {}
//
//    private record BatchQueryPlan(
//            String table,
//            String targetColumn,
//            String linkColumn,
//            Map<String, Object> staticFilters,
//            String aggregationType // <--- ADD THIS FIELD
//    ) {}
//
//    /**
//     * Analyzes if a Global Param can be batched.
//     * Requirement:
//     * 1. Must filter on the varyingParamName (e.g. CODE = $G{BRANCH_CODE})
//     * 2. All OTHER filters must be resolvable via staticContext.
//     */
//    private BatchQueryPlan analyzeForBatching(GlobalParamDef def, String varyingParamName, Map<String, Object> staticContext) {
//        if (def.getSource() == null || def.getSource().getFilters() == null) return null;
//
//        String varyingRef = "$G{" + varyingParamName + "}";
//        String linkCol = null;
//        Map<String, Object> finalFilters = new HashMap<>();
//
//        try {
//            // Iterate filters to find the link
//            Map<String, Object> filters = def.getSource().getFilters();
//
//            for (Map.Entry<String, Object> entry : filters.entrySet()) {
//                String dbCol = entry.getKey();
//                Object conditions = entry.getValue(); // List or Map
//
//                List<Map<String, Object>> condList = normalizeConditions(conditions);
//                List<Map<String, Object>> resolvedList = new ArrayList<>();
//
//                for (Map<String, Object> cond : condList) {
//                    Object val = cond.get("value");
//                    String op = String.valueOf(cond.getOrDefault("op", "="));
//
//                    if (val instanceof String s && s.equals(varyingRef)) {
//                        // Found the link!
//                        if (!"=".equals(op)) return null; // Only support direct equality for batching currently
//                        if (linkCol != null) return null; // Multiple links to varying param not supported in simple logic
//                        linkCol = dbCol;
//                    } else {
//                        // Must be static resolvable
//                        Object resolvedVal = GlobalParamResolver.resolveValue(val, staticContext);
//                        // If it still contains $G, it depends on another dynamic param -> Abort batching for this node
//                        if (String.valueOf(resolvedVal).contains("$G{")) return null;
//
//                        Map<String, Object> newCond = new HashMap<>(cond);
//                        newCond.put("value", resolvedVal);
//                        resolvedList.add(newCond);
//                    }
//                }
//                if (!resolvedList.isEmpty()) {
//                    finalFilters.put(dbCol, resolvedList);
//                }
//            }
//        } catch (Exception e) {
//            return null;
//        }
//
////        if (linkCol != null) {
////            return new BatchQueryPlan(def.getSource().getTable(), def.getSource().getColumn(), linkCol, finalFilters);
////        }
//
//        if (linkCol != null) {
//            // Pass the source type (DB_SUM, DB_COUNT) to the plan
//            return new BatchQueryPlan(
//                    def.getSource().getTable(),
//                    def.getSource().getColumn(),
//                    linkCol,
//                    finalFilters,
//                    def.getSource().getType() // <--- CAPTURE TYPE
//            );
//        }
//        return null;
//    }
//
//    private Map<String, Object> executeBulkFetch(BatchQueryPlan plan, List<String> allKeys) {
//        Map<String, Object> results = new HashMap<>();
//
//        // Split keys into chunks of 1000 to avoid Oracle limit
//        List<List<String>> chunks = new ArrayList<>();
//        for (int i = 0; i < allKeys.size(); i += 1000) {
//            chunks.add(allKeys.subList(i, Math.min(allKeys.size(), i + 1000)));
//        }
//
//        for (List<String> chunk : chunks) {
//            StringBuilder sql = new StringBuilder();
//            sql.append("SELECT ").append(plan.linkColumn).append(", ").append(plan.targetColumn)
//                    .append(" FROM ").append(plan.table).append(" WHERE 1=1 ");
//
//            Map<String, Object> params = new HashMap<>();
//
//            // Append Static Filters
//            FilterSqlBuilder.appendFilters(sql, params, plan.staticFilters);
//
//            // Append IN clause for Link
//            String inParam = "batch_ids_" + System.nanoTime();
//            sql.append(" AND ").append(plan.linkColumn).append(" IN (:").append(inParam).append(")");
//            params.put(inParam, chunk);
//
//            try {
//                log.info("sql.toString :: {} params:: {}", sql.toString(), params);
//                List<Map<String, Object>> rows = jdbc.queryForList(sql.toString(), params);
//                log.info("Rows of result of global param fetch:: {}", rows);
//                for (Map<String, Object> row : rows) {
//                    String key = String.valueOf(row.get(plan.linkColumn));
//                    Object val = row.get(plan.targetColumn);
//                    results.put(key, val);
//                }
//            } catch (Exception e) {
//                log.error("Failed to execute batch chunk for table {}", plan.table, e);
//            }
//        }
//        return results;
//    }
//
//    @SuppressWarnings("unchecked")
//    private List<Map<String, Object>> normalizeConditions(Object conds) {
//        if (conds instanceof List) return (List<Map<String, Object>>) conds;
//        if (conds instanceof Map) return List.of((Map<String, Object>) conds);
//        return List.of();
//    }
//}

////////////////////////////////

package com.tcs.fincore.ReportBuilder.batch.processor;

import com.tcs.fincore.ReportBuilder.model.ReportTemplate;
import com.tcs.fincore.ReportBuilder.model.ReportTemplate.GlobalParamDef;
import com.tcs.fincore.ReportBuilder.service.aggregation.DataSourceSpec;
import com.tcs.fincore.ReportBuilder.util.FilterSqlBuilder;
import com.tcs.fincore.ReportBuilder.util.GlobalParamResolver;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * FIXED: Global Parameter Batch Processor.
 * Now supports Aggregations (SUM, COUNT, etc.) and correctly Groups By the linking column.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class GlobalParamBatchProcessor {

    private final NamedParameterJdbcTemplate jdbc;

    /**
     * Pre-fetches varying global parameters for all batch items.
     */
    public Map<String, Map<String, Object>> prefetchDynamicGlobals(
            ReportTemplate template,
            Map<String, Object> staticContext,
            String varyingParamName,
            List<String> allDimensionValues
    ) {
        Map<String, Map<String, Object>> resultMap = new ConcurrentHashMap<>();
        if (template.getGlobalParams() == null) return resultMap;

        // 1. Identify Batchable Parameters
        for (GlobalParamDef def : template.getGlobalParams()) {
            if (def.getMode() != ReportTemplate.ParamMode.QUERY) continue;
            // Optimization: If it's already in static context (calculated from fixed dates), skip
            if (staticContext.containsKey(def.getName())) continue;

            BatchQueryPlan plan = analyzeForBatching(def, varyingParamName, staticContext);
            if (plan != null) {
                log.info("Batching Global Param '{}' | Type: {} | Table: {} | Link: {}",
                        def.getName(), plan.aggregationType, plan.table, plan.linkColumn);

                try {
                    Map<String, Object> bulkResults = executeBulkFetch(plan, allDimensionValues);
                    resultMap.put(def.getName(), bulkResults);
                } catch (Exception e) {
                    log.error("Failed to batch global param '{}'. Falling back to per-row fetch.", def.getName(), e);
                }
            } else {
                log.debug("Global Param '{}' not batchable. Will fetch per-report.", def.getName());
            }
        }
        return resultMap;
    }

    private record BatchQueryPlan(
            String table,
            String targetColumn,
            String linkColumn,         // The DB column that matches the varying param (e.g. BRANCH_CODE)
            Map<String, Object> staticFilters,
            DataSourceSpec.Type aggregationType // DB_SUM, DB_VALUE, etc.
    ) {}

    /**
     * Analyzes if a Global Param can be batched.
     */
    private BatchQueryPlan analyzeForBatching(GlobalParamDef def, String varyingParamName, Map<String, Object> staticContext) {
        if (def.getSource() == null || def.getSource().getFilters() == null) return null;

        String varyingRef = "$G{" + varyingParamName + "}";
        String linkCol = null;
        Map<String, Object> finalFilters = new HashMap<>();

        try {
            // Resolve Static Dependencies in Filters (e.g., $G{REPORT_DATE} -> '2023-01-01')
            Map<String, Object> filters = def.getSource().getFilters();

            for (Map.Entry<String, Object> entry : filters.entrySet()) {
                String dbCol = entry.getKey();
                List<Map<String, Object>> condList = normalizeConditions(entry.getValue());
                List<Map<String, Object>> resolvedList = new ArrayList<>();

                for (Map<String, Object> cond : condList) {
                    Object val = cond.get("value");
                    String op = String.valueOf(cond.getOrDefault("op", "="));

                    // Check if this filter links to the Varying Dimension (e.g. BRANCH_CODE)
                    if (val instanceof String s && s.equals(varyingRef)) {
                        if (!"=".equals(op)) return null; // Only direct equality supported for batch linking
                        if (linkCol != null) return null; // Complex multiple links not supported yet
                        linkCol = dbCol;
                    } else {
                        // It must be a static filter (resolvable via staticContext)
                        Object resolvedVal = GlobalParamResolver.resolveValue(val, staticContext);

                        // If it still depends on another unresolved $G, we can't batch it easily here
                        if (String.valueOf(resolvedVal).contains("$G{")) return null;

                        Map<String, Object> newCond = new HashMap<>(cond);
                        newCond.put("value", resolvedVal);
                        resolvedList.add(newCond);
                    }
                }
                if (!resolvedList.isEmpty()) {
                    finalFilters.put(dbCol, resolvedList);
                }
            }
        } catch (Exception e) {
            log.warn("Error analyzing global param batching: {}", e.getMessage());
            return null;
        }

        if (linkCol != null) {
            // Map string type (e.g., "DB_SUM") to Enum
            DataSourceSpec.Type type = DataSourceSpec.Type.DB_VALUE;
            try {
                if(def.getSource().getType() != null) {
                    type = DataSourceSpec.Type.valueOf(def.getSource().getType());
                }
            } catch (Exception e) {
                log.warn("Invalid source type in global param: {}", def.getSource().getType());
            }

            return new BatchQueryPlan(
                    def.getSource().getTable(),
                    def.getSource().getColumn(),
                    linkCol,
                    finalFilters,
                    type
            );
        }
        return null;
    }

    private Map<String, Object> executeBulkFetch(BatchQueryPlan plan, List<String> allKeys) {
        Map<String, Object> results = new HashMap<>();
        if (allKeys == null || allKeys.isEmpty()) return results;

        // Chunk keys to avoid DB limits (1000)
        List<List<String>> chunks = new ArrayList<>();
        for (int i = 0; i < allKeys.size(); i += 1000) {
            chunks.add(allKeys.subList(i, Math.min(allKeys.size(), i + 1000)));
        }

        // Determine Aggregation SQL
        String aggClause = buildAggregationClause(plan.aggregationType, plan.targetColumn);
        boolean needsGroupBy = !plan.aggregationType.name().equals("DB_VALUE")
                && !plan.aggregationType.name().equals("DB_TEXT")
                && !plan.aggregationType.name().equals("DB_DATE");

        for (List<String> chunk : chunks) {
            StringBuilder sql = new StringBuilder();
            sql.append("SELECT ").append(plan.linkColumn).append(" as LINK_KEY, ")
                    .append(aggClause).append(" as VAL ")
                    .append(" FROM ").append(plan.table)
                    .append(" WHERE 1=1 ");

            Map<String, Object> params = new HashMap<>();

            // 1. Static Filters
            FilterSqlBuilder.appendFilters(sql, params, plan.staticFilters);

            // 2. The Link Column IN clause
            String inParam = "batch_ids_" + System.nanoTime();
            sql.append(" AND ").append(plan.linkColumn).append(" IN (:").append(inParam).append(")");
            params.put(inParam, chunk);

            // 3. Group By (Critical for DB_SUM, DB_COUNT)
            if (needsGroupBy) {
                sql.append(" GROUP BY ").append(plan.linkColumn);
            }

            try {
                if (log.isDebugEnabled()) log.debug("Batch Global SQL: {} Params: {}", sql, params);

                List<Map<String, Object>> rows = jdbc.queryForList(sql.toString(), params);

                for (Map<String, Object> row : rows) {
                    String key = String.valueOf(row.get("LINK_KEY"));
                    Object val = row.get("VAL");
                    results.put(key, val);
                }
            } catch (Exception e) {
                log.error("Failed batch chunk fetch for table {}", plan.table, e);
            }
        }
        return results;
    }

    private String buildAggregationClause(DataSourceSpec.Type type, String column) {
        FilterSqlBuilder.validateIdentifier(column);
        return switch (type) {
            case DB_SUM -> "COALESCE(SUM(" + column + "), 0)";
            case DB_COUNT -> "COUNT(" + column + ")";
            case DB_AVG -> "AVG(" + column + ")";
            case DB_MIN -> "MIN(" + column + ")";
            case DB_MAX -> "MAX(" + column + ")";
            default -> column; // DB_VALUE, DB_TEXT
        };
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> normalizeConditions(Object conds) {
        if (conds instanceof List) return (List<Map<String, Object>>) conds;
        if (conds instanceof Map) return List.of((Map<String, Object>) conds);
        return List.of();
    }
}