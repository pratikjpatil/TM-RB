package com.tcs.fincore.ReportBuilder.batch.processor;

import com.tcs.fincore.ReportBuilder.model.ReportTemplate;
import com.tcs.fincore.ReportBuilder.model.ReportTemplate.GlobalParamDef;
import com.tcs.fincore.ReportBuilder.util.FilterSqlBuilder;
import com.tcs.fincore.ReportBuilder.util.GlobalParamResolver;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Service
@RequiredArgsConstructor
public class GlobalParamBatchProcessor {

    private final NamedParameterJdbcTemplate jdbc;

    /**
     * Pre-fetches varying global parameters for all batch items.
     *
     * @param template           The report template
     * @param staticContext      Context containing already resolved static globals (Dates, Constants)
     * @param varyingParamName   The name of the dimension varying in batch (e.g., "BRANCH_CODE")
     * @param allDimensionValues List of all values for the varying dimension (e.g., ["001", "002"...])
     * @return Map<ParamName, Map < DimensionValue, ResultValue>>
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
        // Must be QUERY mode AND depend on the varyingParamName
        for (GlobalParamDef def : template.getGlobalParams()) {
            if (def.getMode() != ReportTemplate.ParamMode.QUERY) continue;

            //  If it's already in static context, skip
            if (staticContext.containsKey(def.getName())) continue;

            BatchQueryPlan plan = analyzeForBatching(def, varyingParamName, staticContext);
            if (plan != null) {
                log.info("Batching Global Param '{}' using table '{}' linked by '{}' (Agg: {})",
                        def.getName(), plan.table, plan.linkColumn, plan.aggregationType);

                Map<String, Object> bulkResults = executeBulkFetch(plan, allDimensionValues);
                resultMap.put(def.getName(), bulkResults);
            } else {
                log.debug("Global Param '{}' is not suitable for simple batching. Falling back to per-row execution.", def.getName());
            }
        }
        return resultMap;
    }

    private record BatchQueryPlan(
            String table,
            String targetColumn,
            String linkColumn,
            Map<String, Object> staticFilters,
            String aggregationType
    ) {}

    /**
     * Analyzes if a Global Param can be batched.
     * Requirement:
     * 1. Must filter on the varyingParamName (e.g. CODE = $G{BRANCH_CODE})
     * 2. All OTHER filters must be resolvable via staticContext.
     */
    private BatchQueryPlan analyzeForBatching(GlobalParamDef def, String varyingParamName, Map<String, Object> staticContext) {
        if (def.getSource() == null || def.getSource().getFilters() == null) return null;

        String varyingRef = "$G{" + varyingParamName + "}";
        String linkCol = null;
        Map<String, Object> finalFilters = new HashMap<>();

        try {
            // Iterate filters to find the link
            Map<String, Object> filters = def.getSource().getFilters();
            for (Map.Entry<String, Object> entry : filters.entrySet()) {
                String dbCol = entry.getKey();
                Object conditions = entry.getValue(); // List or Map

                List<Map<String, Object>> condList = normalizeConditions(conditions);
                List<Map<String, Object>> resolvedList = new ArrayList<>();

                for (Map<String, Object> cond : condList) {
                    Object val = cond.get("value");
                    String op = String.valueOf(cond.getOrDefault("op", "="));

                    if (val instanceof String s && s.equals(varyingRef)) {
                        // Found the link!
                        if (!"=".equals(op)) return null; // Only support direct equality for batching currently
                        if (linkCol != null) return null; // Multiple links to varying param not supported in simple logic
                        linkCol = dbCol;
                    } else {
                        // Must be static resolvable
                        Object resolvedVal = GlobalParamResolver.resolveValue(val, staticContext);
                        // If it still contains $G, it depends on another dynamic param -> Abort batching for this node
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
        } catch (RuntimeException e) {
            return null;
        }

        if (linkCol != null) {
            // Pass the source type (DB_SUM, DB_COUNT) to the plan
            return new BatchQueryPlan(
                    def.getSource().getTable(),
                    def.getSource().getColumn(),
                    linkCol,
                    finalFilters,
                    def.getSource().getType()
            );
        }
        return null;
    }

    /**
     * Executes the bulk fetch using FilterSqlBuilder for consistent IN clause handling
     * and supports Aggregation types.
     */
    private Map<String, Object> executeBulkFetch(BatchQueryPlan plan, List<String> allKeys) {
        Map<String, Object> results = new HashMap<>();

        // 1. Determine Selection & Aggregation Logic
        String targetCol = plan.targetColumn;
        String selectClause;
        boolean groupByRequired = false;

        String aggType = plan.aggregationType != null ? plan.aggregationType.toUpperCase() : "DB_VALUE";

        switch (aggType) {
            case "DB_SUM" -> {
                selectClause = "SUM(" + targetCol + ")";
                groupByRequired = true;
            }
            case "DB_COUNT" -> {
                selectClause = "COUNT(" + targetCol + ")";
                groupByRequired = true;
            }
            case "DB_AVG" -> {
                selectClause = "AVG(" + targetCol + ")";
                groupByRequired = true;
            }
            case "DB_MIN" -> {
                selectClause = "MIN(" + targetCol + ")";
                groupByRequired = true;
            }
            case "DB_MAX" -> {
                selectClause = "MAX(" + targetCol + ")";
                groupByRequired = true;
            }
            default -> {
                // DB_VALUE, DB_TEXT, DB_DATE
                selectClause = targetCol;
                // If there are multiple rows per link key for DB_VALUE, database might return multiple rows.
                // We typically expect 1:1, but adding GROUP BY linkColumn is safe if we strictly want one row per key
                // and use MAX/MIN, but for pure VALUE we just fetch.
                // However, without aggregation, if multiple rows match a branch code, we get multiple map entries?
                // No, we overwrite the map key.
            }
        }

        // 2. Build Filters (Static + Dynamic IN clause)
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT ").append(plan.linkColumn).append(", ")
                .append(selectClause).append(" AS VAL FROM ")
                .append(plan.table).append(" WHERE 1=1 ");

        Map<String, Object> params = new HashMap<>();

        // A. Append Static Filters
        if (plan.staticFilters != null && !plan.staticFilters.isEmpty()) {
            FilterSqlBuilder.appendFilters(sql, params, plan.staticFilters);
        }

        // B. Append Dynamic IN clause (utilizing FilterSqlBuilder's chunking logic)
        // We construct a temporary filter map just for the builder
        Map<String, Object> dynamicFilter = new HashMap<>();
        dynamicFilter.put(plan.linkColumn, Map.of("op", "IN", "value", allKeys));
        FilterSqlBuilder.appendFilters(sql, params, dynamicFilter);

        // 3. Append Group By if needed
        if (groupByRequired) {
            sql.append(" GROUP BY ").append(plan.linkColumn);
        }

        // 4. Execute
        try {

            log.debug("Batch Global Fetch SQL: {} {}", sql, params);

            List<Map<String, Object>> rows = jdbc.queryForList(sql.toString(), params);

            for (Map<String, Object> row : rows) {
                String key = String.valueOf(row.get(plan.linkColumn));
                Object val = row.get("VAL");
                results.put(key, val);
            }

            log.info("Fetched {} global param values for table {}", results.size(), plan.table);

        } catch (RuntimeException e) {
            log.error("Failed to execute batch global fetch for table {}", plan.table, e);
        }

        return results;
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> normalizeConditions(Object conds) {
        if (conds instanceof List) return (List<Map<String, Object>>) conds;
        if (conds instanceof Map) return List.of((Map<String, Object>) conds);
        return List.of();
    }
}
