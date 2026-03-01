package com.tcs.fincore.ReportBuilder.batch.analyzer;

import com.tcs.fincore.ReportBuilder.batch.model.QueryPattern;
import com.tcs.fincore.ReportBuilder.dto.BatchJobRequest;
import com.tcs.fincore.ReportBuilder.exception.ReportProcessingException;
import com.tcs.fincore.ReportBuilder.exception.ValidationException;
import com.tcs.fincore.ReportBuilder.model.ReportTemplate;
import com.tcs.fincore.ReportBuilder.model.variant.ReportVariant;
import com.tcs.fincore.ReportBuilder.model.variant.VariantParamDef;
import com.tcs.fincore.ReportBuilder.model.variant.FilterRule;
import com.tcs.fincore.ReportBuilder.service.aggregation.DataSourceSpec;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import java.util.*;
import java.util.stream.Collectors;

/**
 * FIXED: Advanced pattern analysis supporting Multiple Operators and Automatic Global Param detection.
 */
@Slf4j
@Component
public class QueryPatternAnalyzer {

    // MUST match the placeholder injected by ReportRuntimeService.applyBatchParams
    public static final String BATCH_VAR_PLACEHOLDER = "BATCH_VAR_PLACEHOLDER";

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
                    log.debug("Pattern {}: table={}, column={}, agg={}, batchable={}, varyingCol={}",
                            pattern.getPatternId(),
                            pattern.getTable(),
                            pattern.getColumn(),
                            pattern.getAggregationType(),
                            pattern.isBatchable(),
                            pattern.getVaryingDimension() != null ? pattern.getVaryingDimension().getColumnName() : "NONE");
                }
            } catch (ValidationException e) {
                // Expected — bad config in template, log and skip this pattern
                log.warn("Skipping pattern group '{}' due to validation error: {}",
                        entry.getKey(), e.getMessage());
            } catch (ReportProcessingException e) {
                // Serious but isolated — log full stack, continue with other patterns
                log.error("Processing error for pattern group '{}': {}", entry.getKey(), e.getMessage(), e);
            } catch (Exception e) {
                // Truly unexpected — log with full context for debugging
                log.error("Unexpected error building pattern for group '{}'. PatternKey={}, ErrorType={}",
                        entry.getKey(), entry.getKey(), e.getClass().getName(), e);
                // Do NOT silently swallow — rethrow as domain exception
                throw new ReportProcessingException(
                        "Unexpected failure during pattern analysis for group: " + entry.getKey(), e);
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

                        // Sort constraints by OP and DataType
                        constraints.sort(Comparator.comparing(m -> String.valueOf(m.get("op")) + m.get("dataType")));

                        for (Map<String, Object> fm : constraints) {
                            String op = String.valueOf(fm.get("op")).toUpperCase();
                            String dataType = String.valueOf(fm.getOrDefault("dataType", "STRING"));
                            Object val = fm.get("value");

                            sb.append(op).append(":").append(dataType);

                            // CRITICAL: Treat the Batch Placeholder as a generic "VAR" for grouping.
                            // This ensures queries that differ only by the placeholder value (though rare in analysis phase) group together.
                            if (BATCH_VAR_PLACEHOLDER.equals(val) || "IN".equals(op)) {
                                sb.append(":VAR");
                            } else {
                                sb.append(":").append(val);
                            }
                            sb.append("#");
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

        if (specs == null || specs.isEmpty()) return null;
//        log.info("specs testing :: {}", specs);

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

        // --- FIXED: Automatic Column Detection for Varying Dimension ---
        if (batchRequest.getVaryingDimension() != null) {
            String paramName = batchRequest.getVaryingDimension().getParamName();
            String foundDbColumn = null;

            // Strategy 1: Look for Explicit Usage in Filters (Primary Strategy)
            // We check if any filter in the spec has the value "BATCH_VAR_PLACEHOLDER"
            if (first.getFilters() != null) {
                for (Map.Entry<String, Object> entry : first.getFilters().entrySet()) {
                    if (containsPlaceholder(entry.getValue())) {
                        foundDbColumn = entry.getKey();
                        log.debug("Found explicit varying column via Global Param: {}", foundDbColumn);
                        break;
                    }
                }
            }

            // Strategy 2: Fallback to Variant Rules (Legacy/Implicit Strategy)
            if (foundDbColumn == null) {
                foundDbColumn = resolveDbColumnFromVariant(variant, paramName, first.getTable());
                if (foundDbColumn != null) {
                    log.debug("Found implicit varying column via Variant Rule: {}", foundDbColumn);
                }
            }

            if (foundDbColumn != null) {
                String paramType = getParamType(variant, paramName);
                QueryPattern.DynamicFilter dynFilter = QueryPattern.DynamicFilter.builder()
                        .columnName(foundDbColumn)
                        .paramName(paramName)
                        .allValues(new HashSet<>(batchRequest.getVaryingDimension().getValues()))
                        .dataType(paramType)
                        .build();
                pattern.setVaryingDimension(dynFilter);
            } else {
                // If we can't find the column, this pattern is NOT batchable on this dimension.
                // It might be a static global query (like fetching Bank Name).
                pattern.setVaryingDimension(null);
            }
        }

        // Analyze filters across all specs in the group
        analyzeFiltersAcrossSpecs(specs, pattern);
        return pattern;
    }

    private boolean containsPlaceholder(Object filterValue) {
        List<Map<String, Object>> constraints = normalizeConstraints(filterValue);
        for (Map<String, Object> c : constraints) {
            Object val = c.get("value");
            if (BATCH_VAR_PLACEHOLDER.equals(val)) return true;
        }
        return false;
    }



    // -------------------------------------------------------------------------
    // FIX #12: Iterate the UNION of filter keys from ALL specs, not just spec[0].
    //          If spec[0] has null filters, we no longer return early and miss data.
    // -------------------------------------------------------------------------

    private void analyzeFiltersAcrossSpecs(List<DataSourceSpec> specs, QueryPattern pattern) {
        String varyingColumn = pattern.getVaryingDimension() != null
                ? pattern.getVaryingDimension().getColumnName() : null;

        // Build the union of all filter-column names across all specs in the group.
        Set<String> allFilterColumns = new LinkedHashSet<>();
        for (DataSourceSpec spec : specs) {
            if (spec.getFilters() != null) {
                allFilterColumns.addAll(spec.getFilters().keySet());
            }
        }

        if (allFilterColumns.isEmpty()) return;

        // Use the first spec that actually has filters as the representative.
        DataSourceSpec representative = specs.stream()
                .filter(s -> s.getFilters() != null && !s.getFilters().isEmpty())
                .findFirst()
                .orElse(null);

        if (representative == null) return;

        for (String column : allFilterColumns) {
            if (column.equals(varyingColumn)) continue;

            Object repFilterValue = representative.getFilters().get(column);
            if (repFilterValue == null) continue;

            List<Map<String, Object>> repConstraints = normalizeConstraints(repFilterValue);
            for (int i = 0; i < repConstraints.size(); i++) {
                Map<String, Object> repConst = repConstraints.get(i);
                categorizeConstraint(column, i, repConst, specs, pattern);
            }
        }
    }







    // -------------------------------------------------------------------------
    // FIX #3: When op is promoted from "=" or "!=" to "IN" because value is a
    //         Collection, we create a *new* constraint map with the corrected op
    //         so downstream code (FilterSqlBuilder) never sees op="=" with a list value.
    // -------------------------------------------------------------------------

    private void categorizeConstraint(String column, int constraintIndex,
                                      Map<String, Object> repConst,
                                      List<DataSourceSpec> specs,
                                      QueryPattern pattern) {

        String op = String.valueOf(repConst.get("op")).toUpperCase();
        Object repValue = repConst.get("value");

        // Promote "=" / "!=" to "IN" when the value is already a Collection.
        // We work on a COPY so the original spec data is not mutated.
        Map<String, Object> effectiveConst = repConst;
        if (("=".equals(op) || "!=".equals(op)) && repValue instanceof Collection) {
            op = "IN";
            effectiveConst = new HashMap<>(repConst);
            effectiveConst.put("op", "IN");
        }

        switch (op) {
            case "IN" -> {
                Set<Object> allValues = new HashSet<>();
                for (DataSourceSpec spec : specs) {
                    if (spec.getFilters() == null) continue;
                    List<Map<String, Object>> specConstraints =
                            normalizeConstraints(spec.getFilters().get(column));
                    if (constraintIndex < specConstraints.size()) {
                        Object val = specConstraints.get(constraintIndex).get("value");
                        if (val instanceof Collection<?> c) {
                            allValues.addAll(c);
                        } else if (val != null) {
                            allValues.add(val);
                        }
                    }
                }
                if (!allValues.isEmpty()) {
                    pattern.getBulkFilters().merge(column, allValues, (a, b) -> {
                        Set<Object> merged = new HashSet<>(a);
                        merged.addAll(b);
                        return merged;
                    });
                }
            }
            case "=", "!=", "LIKE" -> addToStaticFilters(pattern, column, effectiveConst);
            case ">", "<", ">=", "<=", "BETWEEN" -> addToStaticFilters(pattern, column, effectiveConst);
            default -> log.warn("Unrecognised filter operator '{}' for column '{}' — skipping", op, column);
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
            default -> "VALUE";
        };
    }

    private String resolveDbColumnFromVariant(ReportVariant variant, String paramName, String currentTable) {
        if (variant == null || variant.getFilterRules() == null) return null;
        return variant.getFilterRules().stream()
                .filter(rule -> paramName.equals(rule.getParamName()))
                .filter(rule->isRuleApplicable(rule, currentTable, false))
                .findFirst()
                .map(FilterRule::getDbColumn)
                .orElse(null);
    }

    private boolean isRuleApplicable(FilterRule r, String table, boolean dynamicRow) {
        String st = r.getScopeType() == null ? "ALL_DB" : r.getScopeType().toUpperCase(Locale.ROOT);
        String sv = r.getScopeValue();
        return switch (st) {
            case "ALL_DB" -> true;
            case "TABLE" -> table != null && table.equalsIgnoreCase(sv);
            case "DYNAMIC_TABLE" -> dynamicRow && table != null && table.equalsIgnoreCase(sv);
            default -> false;
        };
    }

    private String getParamType(ReportVariant variant, String paramName) {
        if (variant != null && variant.getParams() != null) {
            return variant.getParams().stream()
                    .filter(p -> p.getParamName().equals(paramName))
                    .findFirst()
                    .map(VariantParamDef::getParamType)
                    .orElse("STRING");
        }
        return "STRING";
    }

    public boolean shouldUseBatchMode(BatchJobRequest request, List<QueryPattern> patterns) {
        if (request.getVaryingDimension() == null) return false;
        long batchablePatterns = patterns.stream().filter(QueryPattern::isBatchable).count();
        // Return true if at least one pattern is batchable
//        return batchablePatterns > 0;
        return true;
    }
}