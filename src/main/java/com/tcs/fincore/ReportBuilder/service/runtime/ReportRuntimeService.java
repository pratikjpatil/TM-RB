








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
 * PRODUCTION-READY: Report Runtime Service V2.3
 * Fixed: Deep Resolution of $G{...} in Formula Expressions.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class ReportRuntimeService {

    private final GlobalParamEngine globalParamEngine;

    /**
     * Prepares the template for a SINGLE execution (View/Download/Single Batch Iteration).
     */
    public void prepareRuntimeContext(
            ReportTemplate template,
            ReportVariant variant,
            Map<String, Object> inputParams) {

        log.debug("Preparing runtime context. Input Params keys: {}",
                inputParams != null ? inputParams.keySet() : "null");

        // 1. Resolve Global Params
        Map<String, Object> runtimeContext = globalParamEngine.resolveContext(
                template,
                inputParams,
                true
        );

        // 2. Apply Column-Level Filters
        applyColumnFilters(template);

        // 3. Resolve all Placeholders ($G{...}, #{...}) in the template structure
        resolveTemplateGlobals(template, runtimeContext);

        // 4. Apply Variant Rules
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
            String varyingParamName) {

        log.info("Applying batch params for template: {}, varying: {}",
                template.getReportMeta().getReportId(), varyingParamName);

        Map<String, Object> batchContext = new HashMap<>(commonParams != null ? commonParams : Map.of());
        batchContext = buildBatchGlobalContext(template, batchContext, varyingParamName);

        // Inject placeholders for batch analysis to prevent resolution errors
        if (template.getGlobalParams() != null) {
            for (ReportTemplate.GlobalParamDef def : template.getGlobalParams()) {
                if (!batchContext.containsKey(def.getName())) {
                    batchContext.put(def.getName(), "0"); // Use "0" to be safe for math formulas during analysis
                    log.debug("Injected placeholder for skipped global '{}'", def.getName());
                }
            }
        }

        if (varyingParamName != null && !batchContext.containsKey(varyingParamName)) {
            batchContext.put(varyingParamName, "BATCH_VAR_PLACEHOLDER");
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
                template.getReportData().getRows() == null) return;

        List<ReportTemplate.ColumnDefinition> cols = template.getReportData().getColumns();
        Map<Integer, List<Map<String, Object>>> activeColFilters = new HashMap<>();

        for (int i = 0; i < cols.size(); i++) {
            if (cols.get(i).getFilters() != null && !cols.get(i).getFilters().isEmpty()) {
                activeColFilters.put(i, cols.get(i).getFilters());
            }
        }

        if (activeColFilters.isEmpty()) return;

        for (ReportTemplate.Row row : template.getReportData().getRows()) {
            if (row.getCells() == null) continue;
            for (Map.Entry<Integer, List<Map<String, Object>>> entry : activeColFilters.entrySet()) {
                int colIdx = entry.getKey();
                if (colIdx >= row.getCells().size()) continue;

                ReportTemplate.Cell cell = row.getCells().get(colIdx);
                if (cell == null) continue;

                if (cell.getType() != null && cell.getType().startsWith("DB_") && cell.getSource() != null) {
                    mergeFiltersIntoSource(cell.getSource(), entry.getValue());
                }
                else if ("FORMULA".equalsIgnoreCase(cell.getType()) && cell.getVariables() != null) {
                    cell.getVariables().values().stream()
                            .filter(v -> v instanceof Map)
                            .map(v -> (Map<String, Object>) v)
                            .filter(m -> String.valueOf(m.get("type")).startsWith("DB_"))
                            .forEach(m -> mergeFiltersIntoSource(m, entry.getValue()));
                }
            }
        }
    }

//    /**
//     * Deep resolve all placeholders ($G and #{}) in the template.
//     * FIXED: Now interpolates Formula expressions.
//     */
//    @SuppressWarnings("unchecked")
//    private void resolveTemplateGlobals(ReportTemplate template, Map<String, Object> context) {
//
//        // Resolve Global Params in Report Extras (Header/Metadata)
//        if (template.getReportMeta() != null && template.getReportMeta().getExtras() != null) {
//            Object resolvedExtras = GlobalParamResolver.resolveRecursive(
//                    template.getReportMeta().getExtras(),
//                    context
//            );
//            template.getReportMeta().setExtras((List<Map<String, Object>>) resolvedExtras);
//        }
//
//        if (template.getReportData().getRows() == null) return;
//
//        for (ReportTemplate.Row row : template.getReportData().getRows()) {
//            // Dynamic Row Config
//            if (row.getDynamicConfig() != null) {
//                row.setDynamicConfig((Map<String, Object>)
//                        GlobalParamResolver.resolveRecursive(row.getDynamicConfig(), context));
//            }
//
//            if (row.getCells() == null) continue;
//
//            for (ReportTemplate.Cell cell : row.getCells()) {
//                if (cell == null) continue;
//                String type = cell.getType() != null ? cell.getType().toUpperCase() : "TEXT";
//
//                // Text Cells
//                if ("TEXT".equals(type)) {
//                    cell.setValue(GlobalParamResolver.resolveValue(cell.getValue(), context));
//                }
//
//                // DB Source Filters
//                if (cell.getSource() != null) {
//                    cell.setSource((Map<String, Object>)
//                            GlobalParamResolver.resolveRecursive(cell.getSource(), context));
//                }
//
//                // Formula Handling
//                if ("FORMULA".equals(type)) {
//                    // 1. Resolve Variables Map
//                    if (cell.getVariables() != null) {
//                        cell.setVariables((Map<String, Object>)
//                                GlobalParamResolver.resolveRecursive(cell.getVariables(), context));
//                    }
//
//                    // 2. FIXED: Resolve Expression String (interpolate $G{VAR})
//                    // This replaces "$G{CURRENT_BALANCE}" with "50000" (or similar value)
//                    if (cell.getExpression() != null && cell.getExpression().contains("$G{")) {
//                        Object resolvedExpr = GlobalParamResolver.resolveValue(cell.getExpression(), context);
//                        cell.setExpression(String.valueOf(resolvedExpr));
//                    }
//                }
//            }
//        }
//    }

    /**
     * Deep resolve all placeholders ($G and #{}) in the template.
     */
    @SuppressWarnings("unchecked")
    private void resolveTemplateGlobals(ReportTemplate template, Map<String, Object> context) {

        // 1. Resolve Report Extras (Header Metadata)
        if (template.getReportMeta() != null && template.getReportMeta().getExtras() != null) {
            Object resolvedExtras = GlobalParamResolver.resolveRecursive(
                    template.getReportMeta().getExtras(), context);
            template.getReportMeta().setExtras((List<Map<String, Object>>) resolvedExtras);
        }

        if (template.getReportData().getRows() == null) return;

        for (ReportTemplate.Row row : template.getReportData().getRows()) {
            // 2. Resolve Dynamic Row Config (DB_LIST filters)
            if (row.getDynamicConfig() != null) {
                row.setDynamicConfig((Map<String, Object>)
                        GlobalParamResolver.resolveRecursive(row.getDynamicConfig(), context));
            }

            if (row.getCells() == null) continue;

            for (ReportTemplate.Cell cell : row.getCells()) {
                if (cell == null) continue;
                String type = cell.getType() != null ? cell.getType().toUpperCase() : "TEXT";

                // 3. Resolve TEXT Cell Values
                if ("TEXT".equals(type)) {
                    cell.setValue(GlobalParamResolver.resolveValue(cell.getValue(), context));
                }

                // 4. Resolve DB Source Filters (The Fix for R__18)
                if (cell.getSource() != null) {
                    Map<String, Object> resolvedSource = (Map<String, Object>)
                            GlobalParamResolver.resolveRecursive(cell.getSource(), context);
                    cell.setSource(resolvedSource);

                    // Debug log to confirm injection
                    if(log.isTraceEnabled()) {
                        log.trace("Cell {} source resolved: {}", cell.getId(), resolvedSource);
                    }
                }

                // 5. Formula Handling
                if ("FORMULA".equals(type)) {
                    // Resolve Variables Map (Recursive for inline DB_* vars)
                    if (cell.getVariables() != null) {
                        cell.setVariables((Map<String, Object>)
                                GlobalParamResolver.resolveRecursive(cell.getVariables(), context));
                    }

                    // Resolve Expression String (Interpolate $G{VAR} into numbers/strings)
                    if (cell.getExpression() != null && cell.getExpression().contains("$G{")) {
                        Object resolvedExpr = GlobalParamResolver.resolveValue(cell.getExpression(), context);
                        cell.setExpression(String.valueOf(resolvedExpr));
                    }
                }
            }
        }
    }


    // ==================================================================================
    // LEGACY VARIANT LOGIC (Unchanged structure, ensuring compatibility)
    // ==================================================================================

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
            if (varyingParamName != null && r.getParamName().equals(varyingParamName)) continue;
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
    private void mergeFiltersIntoSource(Map<String, Object> source, List<Map<String, Object>> colFilters) {
        Map<String, Object> existingFilters;
        if (source.get("filters") instanceof Map) {
            existingFilters = new HashMap<>((Map<String, Object>) source.get("filters"));
        } else {
            existingFilters = new HashMap<>();
        }

        for (Map<String, Object> colFilter : colFilters) {
            String dbCol = (String) colFilter.get("dbColumn");
            if (dbCol == null) continue;

            Map<String, Object> newFilter = new HashMap<>(colFilter);
            newFilter.remove("dbColumn");

            Object existing = existingFilters.get(dbCol);
            List<Object> conditionList = new ArrayList<>();

            if (existing instanceof List) conditionList.addAll((List<Object>) existing);
            else if (existing instanceof Map) conditionList.add(existing);

            conditionList.add(newFilter);
            existingFilters.put(dbCol, conditionList);
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
            case "TABLE" -> table != null && sv != null && table.equalsIgnoreCase(sv);
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