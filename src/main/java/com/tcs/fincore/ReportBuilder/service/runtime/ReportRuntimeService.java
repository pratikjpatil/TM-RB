package com.tcs.fincore.ReportBuilder.service.runtime;

import com.tcs.fincore.ReportBuilder.exception.ValidationException;
import com.tcs.fincore.ReportBuilder.model.ReportTemplate;
import com.tcs.fincore.ReportBuilder.model.variant.FilterRule;
import com.tcs.fincore.ReportBuilder.model.variant.ReportVariant;
import com.tcs.fincore.ReportBuilder.model.variant.VariantParamDef;
import com.tcs.fincore.ReportBuilder.util.GlobalParamResolver;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.stream.Collectors;

/**
 * PRODUCTION-READY: Report Runtime Service V2.4
 * FIXED: Order of Placeholder Injection in applyBatchParams.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class ReportRuntimeService {

    private final GlobalParamEngine globalParamEngine;
    
    public void prepareRuntimeContext(
            ReportTemplate template,
            ReportVariant variant,
            Map<String, Object> inputParams) {

        log.debug("Preparing runtime context. Input Params keys: {}",
                inputParams != null ? inputParams.keySet() : "null");

        Map<String, Object> runtimeContext = globalParamEngine.resolveContext(
                template,
                inputParams,
                true
        );
        applyColumnFilters(template);
        resolveTemplateGlobals(template, runtimeContext);
        if (variant != null) {
            applyVariantAndParams(template, variant, runtimeContext);
        }
    }

    /**
     * Prepares the template for BATCH analysis.
     */
    public void applyBatchParams(
            ReportTemplate template,
            ReportVariant variant,
            Map<String, Object> commonParams,
            String varyingParamName,
            Map<String, Map<String, Object>> preFetchedGlobals) {

        log.info("Applying batch params for template: {}, varying: {}",
                template.getReportMeta().getReportId(), varyingParamName);

        Map<String, Object> batchContext = new HashMap<>(commonParams != null ? commonParams : Map.of());
        batchContext = buildBatchGlobalContext(template, batchContext, varyingParamName);

        // ---  Inject Varying Dimension Placeholder FIRST ---
        if (varyingParamName != null) {
            // Force the placeholder, overwriting any nulls/defaults
            batchContext.put(varyingParamName, "BATCH_VAR_PLACEHOLDER");
            log.debug("Injected BATCH_VAR_PLACEHOLDER for '{}'", varyingParamName);
        }

        // Inject placeholders for OTHER missing globals to prevent resolution errors
        if (template.getGlobalParams() != null) {
            for (ReportTemplate.GlobalParamDef def : template.getGlobalParams()) {
                if (!batchContext.containsKey(def.getName())) {
//                    batchContext.put(def.getName(), "0");
//                    log.debug("Injected safety placeholder '0' for skipped global '{}'", def.getName());

                    //  If we have pre-fetched values, inject the whole COLLECTION.
                    // This forces QueryPatternAnalyzer to create an IN clause (Bulk Filter).
                    if (preFetchedGlobals != null && preFetchedGlobals.containsKey(def.getName())) {
                        Collection<Object> distinctValues = new HashSet<>(preFetchedGlobals.get(def.getName()).values());
                        batchContext.put(def.getName(), new ArrayList<>(distinctValues));
                        log.debug("Injected BULK list for global '{}' ({} values)", def.getName(), distinctValues.size());
                    } else {
                        batchContext.put(def.getName(), "0"); // Fallback placeholder
                    }
                }
            }
        }

        applyColumnFilters(template);
        resolveTemplateGlobals(template, batchContext);

        if (variant != null) {
            applyVariantRulesInternal(template, variant, batchContext, varyingParamName);
        }
    }

    private Map<String, Object> buildBatchGlobalContext(
            ReportTemplate template,
            Map<String, Object> params,
            String varyingParamName) {
        return globalParamEngine.resolveContext(template, params, false);
    }

    private void applyColumnFilters(ReportTemplate template) {
        if (template.getReportData() == null ||
                template.getReportData().getColumns() == null ||
                template.getReportData().getRows() == null) {
            return;
        }

        List<ReportTemplate.ColumnDefinition> cols = template.getReportData().getColumns();
        Map<Integer, List<ReportTemplate.TemplateColumnFilter>> activeColFilters = new HashMap<>();
        for (int i = 0; i < cols.size(); i++) {
            if (cols.get(i).getTemplateColumnFilters() != null &&
                    !cols.get(i).getTemplateColumnFilters().isEmpty()) {
                activeColFilters.put(i, cols.get(i).getTemplateColumnFilters());
            }
        }

        if (activeColFilters.isEmpty()) return;

        for (ReportTemplate.Row row : template.getReportData().getRows()) {
            if (row.getCells() == null) continue;
            for (Map.Entry<Integer, List<ReportTemplate.TemplateColumnFilter>> entry : activeColFilters.entrySet()) {
                int colIdx = entry.getKey();
                List<ReportTemplate.TemplateColumnFilter> colFilters = entry.getValue();

                if (colIdx >= row.getCells().size()) continue;
                ReportTemplate.Cell cell = row.getCells().get(colIdx);
                if (cell == null) continue;

                if (cell.getType() != null && cell.getType().startsWith("DB_") && cell.getSource() != null) {
                    applyFiltersToMapIfTableMatches(cell.getSource(), colFilters);
                }
                else if ("FORMULA".equalsIgnoreCase(cell.getType()) && cell.getVariables() != null) {
                    cell.getVariables().values().stream()
                            .filter(v -> v instanceof Map)
                            .map(v -> (Map<String, Object>) v)
                            .filter(m -> String.valueOf(m.get("type")).startsWith("DB_"))
                            .forEach(m -> applyFiltersToMapIfTableMatches(m, colFilters));
                }
            }
        }
    }

    private void applyFiltersToMapIfTableMatches(
            Map<String, Object> sourceMap,
            List<ReportTemplate.TemplateColumnFilter> colFilters) {

        String cellTableName = (String) sourceMap.get("table");
        if (cellTableName == null) return;

        for (ReportTemplate.TemplateColumnFilter cf : colFilters) {
            if (cf.getTableName().equalsIgnoreCase(cellTableName)) {
                mergeFiltersIntoSource(sourceMap, cf.getFilters());
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void resolveTemplateGlobals(ReportTemplate template, Map<String, Object> context) {
        if (template.getReportMeta() != null && template.getReportMeta().getExtras() != null) {
            Object resolvedExtras = GlobalParamResolver.resolveRecursive(
                    template.getReportMeta().getExtras(),
                    context
            );
            template.getReportMeta().setExtras((List<Map<String, Object>>) resolvedExtras);
        }

        if (template.getReportData().getRows() == null) return;
        for (ReportTemplate.Row row : template.getReportData().getRows()) {
            if (row.getDynamicConfig() != null) {
                row.setDynamicConfig((Map<String, Object>)
                        GlobalParamResolver.resolveRecursive(row.getDynamicConfig(), context));
            }

            if (row.getCells() == null) continue;
            for (ReportTemplate.Cell cell : row.getCells()) {
                if (cell == null) continue;
                String type = cell.getType() != null ? cell.getType().toUpperCase() : "TEXT";

                if ("TEXT".equals(type)) {
                    cell.setValue(GlobalParamResolver.resolveValue(cell.getValue(), context));
                }

                if (cell.getSource() != null) {
                    cell.setSource((Map<String, Object>)
                            GlobalParamResolver.resolveRecursive(cell.getSource(), context));
                }

                if ("FORMULA".equals(type)) {
                    if (cell.getVariables() != null) {
                        cell.setVariables((Map<String, Object>)
                                GlobalParamResolver.resolveRecursive(cell.getVariables(), context));
                    }
//                    if (cell.getExpression() != null && cell.getExpression().contains("$G{")) {
//                        Object resolvedExpr = GlobalParamResolver.resolveValue(cell.getExpression(), context);
//                        cell.setExpression(String.valueOf(resolvedExpr));
//                    }
                    // 2. FIXED: Safe Interpolation for Formulas
                    if (cell.getExpression() != null && cell.getExpression().contains("$G{")) {
                        // Use custom interpolation that adds quotes to Strings
                        String newExpr = interpolateFormulaSafe(cell.getExpression(), context);
                        cell.setExpression(newExpr);
                    }

                }
            }
        }
    }

    private String interpolateFormulaSafe(String expression, Map<String, Object> context) {
        // Pattern to find $G{VAR}
        java.util.regex.Pattern p = java.util.regex.Pattern.compile("\\$G\\{([a-zA-Z0-9_]+)\\}");
        java.util.regex.Matcher m = p.matcher(expression);

        StringBuilder sb = new StringBuilder();
        while (m.find()) {
            String key = m.group(1);
            Object val = context.get(key);
            String replacement;

            if (val == null) {
                replacement = "null";
            } else if (val instanceof Number || val instanceof Boolean) {
                // Numbers/Booleans injected as-is (Safe)
                replacement = String.valueOf(val);
            } else {
                // Strings: Escape and Wrap in Single Quotes
                // This turns 05999 into '05999', preventing Octal parsing errors
                String safeVal = String.valueOf(val).replace("'", "\\'");
                replacement = "'" + safeVal + "'";
            }

            m.appendReplacement(sb, java.util.regex.Matcher.quoteReplacement(replacement));
        }
        m.appendTail(sb);
        return sb.toString();
    }

    public void applyVariantAndParams(ReportTemplate template, ReportVariant variant, Map<String, Object> params) {
        applyVariantRulesInternal(template, variant, params, null);
    }

    private void applyVariantRulesInternal(
            ReportTemplate template,
            ReportVariant variant,
            Map<String, Object> params,
            String varyingParamName) {

        if (variant == null) return;
        validateParameters(variant, params, varyingParamName);

        Map<String, String> paramTypes = buildParamTypesMap(variant);
        List<FilterRule> rules = variant.getFilterRules() != null ? variant.getFilterRules() : List.of();
        if (template.getReportData().getRows() == null) return;

        for (ReportTemplate.Row row : template.getReportData().getRows()) {
            if (row.getCells() != null) {
                for (ReportTemplate.Cell cell : row.getCells()) {
                    if (cell == null || cell.getType() == null) continue;
                    String type = cell.getType().toUpperCase(Locale.ROOT);

                    if (type.startsWith("DB_")) {
                        applyRulesToSource(cell.getSource(), rules, params, paramTypes, varyingParamName, false);
                    } else if ("FORMULA".equalsIgnoreCase(type) && cell.getVariables() != null) {
                        cell.getVariables().values().stream()
                                .filter(v -> v instanceof Map)
                                .map(v -> (Map<String, Object>) v)
                                .forEach(m -> applyRulesToSource(m, rules, params, paramTypes, varyingParamName, false));
                    }
                }
            }
            if ("DYNAMIC".equalsIgnoreCase(row.getRowType()) && row.getDynamicConfig() != null) {
                applyRulesToSource(row.getDynamicConfig(), rules, params, paramTypes, varyingParamName, true);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void applyRulesToSource(
            Map<String, Object> source,
            List<FilterRule> rules,
            Map<String, Object> params,
            Map<String, String> paramTypes,
            String varyingParamName,
            boolean isDynamicRow) {

        if (source == null) return;
        String table = (String) source.get("table");
        Map<String, Object> filters = new HashMap<>();
        if (source.get("filters") instanceof Map) {
            filters.putAll((Map<String, Object>) source.get("filters"));
        }

        for (FilterRule r : rules) {
//            if (varyingParamName != null && r.getParamName().equals(varyingParamName)) continue;
            if (!isRuleApplicable(r, table, isDynamicRow)) continue;

            Object val = params.get(r.getParamName());
            if (val == null) continue;

            String column = r.getDbColumn();
            String op = r.getOperator() != null ? r.getOperator() : "=";
            String type = paramTypes.getOrDefault(r.getParamName(), "STRING");
            Map<String, Object> newCondition = new HashMap<>();
            newCondition.put("op", op);
            newCondition.put("value", val);
            newCondition.put("dataType", type);

            Object existing = filters.get(column);
            List<Object> conditionList = new ArrayList<>();
            if (existing instanceof List) conditionList.addAll((List<Object>) existing);
            else if (existing instanceof Map) conditionList.add(existing);

            conditionList.add(newCondition);
            filters.put(column, conditionList);
        }

        source.put("filters", filters);
    }

    @SuppressWarnings("unchecked")
    private void mergeFiltersIntoSource(Map<String, Object> source, Map<String, Object> newFilters) {
        if (newFilters == null || newFilters.isEmpty()) return;
        Map<String, Object> existingFilters;
        if (source.get("filters") instanceof Map) {
            existingFilters = new HashMap<>((Map<String, Object>) source.get("filters"));
        } else {
            existingFilters = new HashMap<>();
        }

        for (Map.Entry<String, Object> entry : newFilters.entrySet()) {
            String dbCol = entry.getKey();
            Object newConditions = entry.getValue();
            Object existingConditions = existingFilters.get(dbCol);
            List<Object> mergedList = new ArrayList<>();

            if (existingConditions instanceof List) {
                mergedList.addAll((List<Object>) existingConditions);
            } else if (existingConditions != null) {
                mergedList.add(existingConditions);
            }

            if (newConditions instanceof List) {
                mergedList.addAll((List<Object>) newConditions);
            } else if (newConditions != null) {
                mergedList.add(newConditions);
            }

            existingFilters.put(dbCol, mergedList);
        }
        source.put("filters", existingFilters);
    }

    private void validateParameters(ReportVariant variant, Map<String, Object> params, String varyingParamName) {
        if (variant.getParams() == null) return;
        for (VariantParamDef def : variant.getParams()) {
            if (varyingParamName != null && def.getParamName().equals(varyingParamName)) continue;
            if (def.isRequired() && !params.containsKey(def.getParamName())) {
                throw new ValidationException("Missing required parameter: " + def.getParamName());
            }
        }
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

    private Map<String, String> buildParamTypesMap(ReportVariant variant) {
        if (variant == null || variant.getParams() == null) return Map.of();
        return variant.getParams().stream()
                .collect(Collectors.toMap(VariantParamDef::getParamName,
                        d -> d.getParamType() != null ? d.getParamType() : "STRING", (a, b) -> a));
    }
}
