
package com.tcs.fincore.ReportBuilder.batch.analyzer;

import com.tcs.fincore.ReportBuilder.batch.model.QueryPattern;
import com.tcs.fincore.ReportBuilder.dto.BatchJobRequest;
import com.tcs.fincore.ReportBuilder.exception.ValidationException;
import com.tcs.fincore.ReportBuilder.model.ReportTemplate;
import com.tcs.fincore.ReportBuilder.model.variant.ReportVariant;
import com.tcs.fincore.ReportBuilder.model.variant.VariantParamDef;
import com.tcs.fincore.ReportBuilder.model.variant.FilterRule; // Added Import
import com.tcs.fincore.ReportBuilder.service.aggregation.DataSourceSpec;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import java.util.*;
import java.util.stream.Collectors;

/**
 * FIXED: Advanced pattern analysis supporting Multiple Operators per column (List-based filters)
 * UPDATED: Resolves column names from Variant Rules instead of inferring them.
 */
@Slf4j
@Component
public class QueryPatternAnalyzer {

    /**
     * Analyze template and extract optimizable query patterns
     */
    public List<QueryPattern> analyzeTemplate(
            ReportTemplate template,
            ReportVariant variant,
            BatchJobRequest batchRequest,
            List<DataSourceSpec> allSpecs) {

        log.info("Analyzing template {} with {} data specs",
                template.getReportMeta().getReportId(), allSpecs.size());

        // Group specs by their COMPLETE logical pattern (including all operators and constraints)
        Map<String, List<DataSourceSpec>> groupedSpecs = groupSpecsByPattern(allSpecs);

        log.debug("Grouped {} specs into {} unique patterns",
                allSpecs.size(), groupedSpecs.size());

        // Build QueryPattern objects
        List<QueryPattern> patterns = new ArrayList<>();

        for (Map.Entry<String, List<DataSourceSpec>> entry : groupedSpecs.entrySet()) {
            try {
                QueryPattern pattern = buildQueryPattern(
                        entry.getValue(),
                        batchRequest,
                        variant
                );

                if (pattern != null) {
                    patterns.add(pattern);
                    log.debug("Pattern {}: table={}, column={}, agg={}, batchable={}, specs={}, static_filters={}, bulk_filters={}, conditional_filters={}",
                            pattern.getPatternId(),
                            pattern.getTable(),
                            pattern.getColumn(),
                            pattern.getAggregationType(),
                            pattern.isBatchable(),
                            pattern.getSourceSpecIds().size(),
                            pattern.getStaticFilters().size(),
                            pattern.getBulkFilters() != null ? pattern.getBulkFilters().size() : 0,
                            pattern.getConditionalFilters() != null ? pattern.getConditionalFilters().size() : 0);
                }
            } catch (Exception e) {
//                log.warn("Failed to build pattern for group {}: {}", entry.getKey(), e.getMessage());
                throw new ValidationException("Failed to build pattern for group :"+ entry.getKey() + " error :: " +e.getMessage());
            }
        }

        return patterns;
    }

    /**
     * Group specs by their EXACT logical pattern including all operators
     */
    private Map<String, List<DataSourceSpec>> groupSpecsByPattern(List<DataSourceSpec> specs) {
        return specs.stream()
                .collect(Collectors.groupingBy(this::getSpecLogicalKey));
    }

    /**
     * Generate logical key that INCLUDES operator types and sorts them for determinism.
     * Supports both Map (legacy) and List (multi-op) filter structures.
     */
    private String getSpecLogicalKey(DataSourceSpec spec) {
        StringBuilder sb = new StringBuilder();
        sb.append(spec.getTable()).append("|")
                .append(spec.getColumn()).append("|")
                .append(spec.getType()).append("|");

        if (spec.getFilters() != null) {
            // Sort by Column Name to ensure deterministic key
            spec.getFilters().entrySet().stream()
                    .sorted(Map.Entry.comparingByKey())
                    .forEach(e -> {
                        String column = e.getKey();
                        Object filterValue = e.getValue();
                        sb.append(column).append(":");

                        // Normalize everything to a List of Constraints for key generation
                        List<Map<String, Object>> constraints = normalizeConstraints(filterValue);

                        // Sort constraints by OP and DataType to ensure [{op:>, val:A}, {op:<, val:B}] == [{op:<, val:B}, {op:>, val:A}]
                        constraints.sort(Comparator.comparing(m -> String.valueOf(m.get("op")) + m.get("dataType")));

                        for (Map<String, Object> fm : constraints) {
                            String op = String.valueOf(fm.get("op")).toUpperCase();
                            String dataType = String.valueOf(fm.getOrDefault("dataType", "STRING"));

                            sb.append(op).append(":").append(dataType);

                            // Include value in key for equality/range checks (Static)
                            // Exclude value for IN/BETWEEN if they are meant to be batched
                            // Current logic: strict grouping. If values differ, it's a different group unless we handle it later.
                            // For QueryPatternAnalyzer, we group EXACT matches first.
                            // *However*, for Batching to work, 'IN' clauses that vary should ideally map to the same pattern IF they are the bulk filter.
                            // But keeping strict keys is safer; the 'buildQueryPattern' logic merges if possible,
                            // or we rely on the fact that batch varying params are usually handled via getVaryingDimension separately.
                            // For IN clauses, we mark as IN_LIST so they group together even if values differ
                            if ("IN".equals(op)) {
                                sb.append(":IN_LIST");
                            } else {
                                sb.append(":").append(fm.get("value"));
                            }
                            sb.append("#");
                            // Separator between constraints
                        }
                        sb.append(";");
                    });
        }

        return sb.toString();
    }

    /**
     * Helper to normalize any filter object (Map, List, Scalar) into List<Map>
     */
    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> normalizeConstraints(Object filterValue) {
        List<Map<String, Object>> result = new ArrayList<>();
        if (filterValue instanceof List<?> list) {
            for (Object item : list) {
                if (item instanceof Map) {
                    result.add((Map<String, Object>) item);
                }
            }
        } else if (filterValue instanceof Map) {
            result.add((Map<String, Object>) filterValue);
        } else {
            // Simple scalar equality
            Map<String, Object> simple = new HashMap<>();
            simple.put("op", "=");
            simple.put("value", filterValue);
            simple.put("dataType", "STRING");
            result.add(simple);
        }
        return result;
    }

    /**
     * Build QueryPattern from grouped specs
     */
    private QueryPattern buildQueryPattern(
            List<DataSourceSpec> specs,
            BatchJobRequest batchRequest,
            ReportVariant variant) {

        if (specs == null || specs.isEmpty()) {
            return null;
        }

        DataSourceSpec first = specs.get(0);

        QueryPattern pattern = QueryPattern.builder()
                .patternId(UUID.randomUUID().toString().substring(0, 8))
                .table(first.getTable())
                .column(first.getColumn())
                .aggregationType(mapTypeToAggregation(first.getType()))
                .staticFilters(new LinkedHashMap<>())
                .bulkFilters(new HashMap<>())
                .conditionalFilters(new ArrayList<>())
                .sourceSpecIds(new HashSet<>())
                .build();

        // Collect all spec IDs
        specs.forEach(s -> pattern.getSourceSpecIds().add(s.getSpecId()));

        // Extract varying dimension
        if (batchRequest.getVaryingDimension() != null) {
            String paramName = batchRequest.getVaryingDimension().getParamName();

            // FIXED: Resolve DB Column from Variant Rules instead of inferring regex
            String columnName = resolveDbColumn(variant, paramName);

            // Fallback to inference only if not found in rules
            if (columnName == null) {
//                log.warn("Varying param '{}' not found in variant rules. Falling back to inference.", paramName);
//                columnName = inferColumnName(paramName);
//                throw new ValidationException("DB Column Not Found for the provided varying dimension.");
                log.warn("DB Column Not Found for the provided varying dimension., {}", paramName);
                pattern.setVaryingDimension(null);
                return pattern;
            }

            String paramType = "STRING";
            if (variant != null && variant.getParams() != null) {
                paramType = variant.getParams().stream()
                        .filter(p -> p.getParamName().equals(paramName))
                        .findFirst()
                        .map(VariantParamDef::getParamType)
                        .orElse("STRING");
            }

            QueryPattern.DynamicFilter dynFilter = QueryPattern.DynamicFilter.builder()
                    .columnName(columnName)
                    .paramName(paramName)
                    .allValues(new HashSet<>(batchRequest.getVaryingDimension().getValues()))
                    .dataType(paramType)
                    .build();
            pattern.setVaryingDimension(dynFilter);
        }

        // Analyze filters across all specs in the group
        analyzeFiltersAcrossSpecs(specs, pattern);
        return pattern;
    }

    /**
     * Logic to categorize filters into Static (Shared), Bulk (Union), or Conditional
     */
    private void analyzeFiltersAcrossSpecs(List<DataSourceSpec> specs, QueryPattern pattern) {
        String varyingColumn = pattern.getVaryingDimension() != null ?
                pattern.getVaryingDimension().getColumnName() : null;

        // We use the first spec as the "template" for structure, since grouping key ensures structure identity
        DataSourceSpec representative = specs.get(0);
        if (representative.getFilters() == null) return;

        for (String column : representative.getFilters().keySet()) {
            // Skip varying dimension (handled separately)
            if (column.equals(varyingColumn)) continue;

            // Normalize constraints for this column from the representative
            List<Map<String, Object>> repConstraints = normalizeConstraints(representative.getFilters().get(column));

            // For each constraint on this column, we must decide if it's Static or Bulk
            // We assume order/structure is preserved due to grouping key logic
            for (int i = 0; i < repConstraints.size(); i++) {
                Map<String, Object> repConst = repConstraints.get(i);
                categorizeConstraint(column, i, repConst, specs, pattern);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void categorizeConstraint(String column, int constraintIndex,
                                      Map<String, Object> repConst,
                                      List<DataSourceSpec> specs,
                                      QueryPattern pattern) {

        String op = String.valueOf(repConst.get("op")).toUpperCase();
        String dataType = String.valueOf(repConst.getOrDefault("dataType", "STRING"));
        Object repValue = repConst.get("value");

        switch (op) {
            case "IN" -> {
                // BULK FILTER CANDIDATE: Collect values from this specific constraint across all specs
                Set<Object> allValues = new HashSet<>();
                for (DataSourceSpec spec : specs) {
                    List<Map<String, Object>> specConstraints = normalizeConstraints(spec.getFilters().get(column));
                    if (constraintIndex < specConstraints.size()) {
                        Object val = specConstraints.get(constraintIndex).get("value");
                        if (val instanceof Collection) {
                            allValues.addAll((Collection<?>) val);
                        } else if (val != null) {
                            allValues.add(val);
                        }
                    }
                }

                if (!allValues.isEmpty()) {
                    // We merge into bulkFilters map.
                    // NOTE: If multiple IN clauses exist on same column, they merge into one set of keys for fetching.
                    // This assumes "Union" semantics for fetching, enabling us to get all potential rows.
                    pattern.getBulkFilters().merge(column, allValues, (a, b) -> {
                        Set<Object> merged = new HashSet<>(a);
                        merged.addAll(b);
                        return merged;
                    });
                    log.debug("Bulk filter (merged): {} size={}", column, pattern.getBulkFilters().get(column).size());
                }
            }

            case "=", "!=", "LIKE" -> {
                // STATIC FILTER CANDIDATE: Must be identical across all specs
                // Grouping key logic actually guarantees this for =, !=, LIKE if we included value in key.
                // But let's be safe and add to static filters.
                // If staticFilters already has a list for this column, append. Else create list.
                // We store the full constraint map to support multiple static filters per column.
                addToStaticFilters(pattern, column, repConst);
            }

            case ">", "<", ">=", "<=", "BETWEEN" -> {
                // CONDITIONAL or STATIC depending on variance.
                // For batch optimizations, we treat Ranges as "Static" constraints on the batch query
                // (assuming the range itself doesn't vary per report, which grouping key ensures).
                // If ranges DID vary, we couldn't batch easily without fetching Min(Start)..Max(End).
                // Current Strategy: Add as Static Filter (passed through to SQL).
                // This allows > 2023 AND < 2026 to be applied to the batch query.
                addToStaticFilters(pattern, column, repConst);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void addToStaticFilters(QueryPattern pattern, String column, Map<String, Object> constraint) {
        Object existing = pattern.getStaticFilters().get(column);
        List<Map<String, Object>> list;

        if (existing == null) {
            list = new ArrayList<>();
            pattern.getStaticFilters().put(column, list);
        } else if (existing instanceof List) {
            list = (List<Map<String, Object>>) existing;
        } else {
            // Should not happen with new logic, but handle legacy migration
            list = new ArrayList<>();
            if (existing instanceof Map) list.add((Map<String, Object>) existing);
            pattern.getStaticFilters().put(column, list);
        }

        list.add(constraint);
    }

    private String mapTypeToAggregation(DataSourceSpec.Type type) {
        return switch (type) {
            case DB_SUM -> "SUM";
            case DB_COUNT -> "COUNT";
            case DB_AVG -> "AVG";
            case DB_MIN -> "MIN";
            case DB_MAX -> "MAX";
            case DB_VALUE, DB_TEXT, DB_DATE -> "VALUE";
        };
    }

    /**
     * Looks up the DB column from the variant's filter rules.
     */
    private String resolveDbColumn(ReportVariant variant, String paramName) {
//        log.info("ParamName:: {}, variant:: {}", paramName, variant);
        if (variant == null || variant.getFilterRules() == null) {
            return null;
        }
        return variant.getFilterRules().stream()
                .filter(rule -> paramName.equals(rule.getParamName()))
                .findFirst()
                .map(FilterRule::getDbColumn)
                .orElse(null);
    }

    private String inferColumnName(String paramName) {
        return paramName.replaceAll("([a-z])([A-Z])", "$1_$2").toUpperCase();
    }

    public boolean shouldUseBatchMode(BatchJobRequest request, List<QueryPattern> patterns) {
        if (request.getVaryingDimension() == null) return false;
        long batchablePatterns = patterns.stream().filter(QueryPattern::isBatchable).count();
//        if (batchablePatterns == 0) return false;
        long totalSpecs = patterns.stream().mapToLong(p -> p.getSourceSpecIds().size()).sum();
        long totalReports = request.getVaryingDimension().getValues().size();
        long estimatedSequentialQueries = totalSpecs * totalReports;
        long estimatedBatchQueries = batchablePatterns;
        double reduction = 1.0 - ((double) estimatedBatchQueries / estimatedSequentialQueries);

        log.info("Batch analysis: {} patterns, {} batchable. Est reduction: {}%", patterns.size(), batchablePatterns, reduction * 100);
//        return reduction > 0.50;
        return true;
    }
}