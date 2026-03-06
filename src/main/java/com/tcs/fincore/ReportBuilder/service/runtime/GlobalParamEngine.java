package com.tcs.fincore.ReportBuilder.service.runtime;

import com.tcs.fincore.ReportBuilder.exception.ValidationException;
import com.tcs.fincore.ReportBuilder.model.ReportTemplate;
import com.tcs.fincore.ReportBuilder.model.ReportTemplate.GlobalParamDef;
import com.tcs.fincore.ReportBuilder.service.aggregation.DataSourceSpec;
import com.tcs.fincore.ReportBuilder.service.aggregation.DataSourceSpec.Type;
import com.tcs.fincore.ReportBuilder.service.db.DbResolver;
import com.tcs.fincore.ReportBuilder.util.GlobalParamResolver;
import com.tcs.fincore.ReportBuilder.util.GraphUtils;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * PRODUCTION COMPONENT: Enhanced Global Parameter Engine.
 * Supports: INPUT, EXPRESSION, and QUERY types with Dependency Injection.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class GlobalParamEngine {

    private final ParamTypeConverter typeConverter;
    private final DbResolver dbResolver;
    private final GlobalParamResolver globalParamResolver;

    // Regex to find dependencies: matches "$G{VAR}" or "G.VAR"
    private static final Pattern REF_PATTERN = Pattern.compile("\\$G\\{([a-zA-Z0-9_]+)\\}|\\bG\\.([a-zA-Z0-9_]+)\\b");

    /**
     * Resolves all global parameters for a specific execution context.
     * @param template The report template
     * @param inputParams User inputs (or pre-injected batch values)
     * @param failOnMissingDeps If true, throws exception if a dependency is missing (Single Report Mode).
     * If false, skips the parameter (Batch Analysis Mode).
     */
    public Map<String, Object> resolveContext(ReportTemplate template, Map<String, Object> inputParams, boolean failOnMissingDeps) {
        // Start with a mutable copy of inputs
        Map<String, Object> context = new HashMap<>(inputParams != null ? inputParams : Map.of());

        List<GlobalParamDef> globals = template.getGlobalParams();
        if (globals == null || globals.isEmpty()) return context;

        // 1. Build Dependency Graph
        Map<String, Set<String>> dependencyMap = buildDependencyGraph(globals);
        Map<String, GlobalParamDef> defMap = globals.stream()
                .collect(Collectors.toMap(GlobalParamDef::getName, p -> p, (a, b) -> a));

        // 2. Topological Sort
        List<String> executionOrder = GraphUtils.sort(dependencyMap);

        // 3. Execution Loop
        for (String paramName : executionOrder) {
            // If already resolved (e.g., pre-fetched by BatchProcessor), skip
            if (context.containsKey(paramName)) continue;

            GlobalParamDef def = defMap.get(paramName);
            if (def == null) continue; // Should not happen

            try {
                Object value = switch (def.getMode()) {
                    case INPUT -> resolveInput(def, context);
                    case EXPRESSION -> resolveExpression(def, context, failOnMissingDeps);
                    case QUERY -> resolveQuery(def, context, failOnMissingDeps);
                    default -> null; // Should treat as INPUT for legacy compatibility
                };

                if (value != null) {
                    context.put(paramName, value);
                } else if (def.getMode() == ReportTemplate.ParamMode.INPUT && def.isRequired() && failOnMissingDeps) {
                    throw new ValidationException("Missing required global parameter: " + paramName);
                }

            } catch (RuntimeException e) {
                if (failOnMissingDeps) {
                    throw new ValidationException("Failed to resolve global '" + paramName + "': " + e.getMessage(), e);
                } else {
                    log.debug("Skipping global '{}' due to missing dependencies (Batch Analysis)", paramName);
                }
            }
        }
        return context;
    }

    private Object resolveInput(GlobalParamDef def, Map<String, Object> context) {
        Object val = context.get(def.getName());
        if (val == null) val = def.getDefaultValue();
        return typeConverter.convert(val, def.getDataType(), def.getName());
    }

    private Object resolveExpression(GlobalParamDef def, Map<String, Object> context, boolean strict) {
        String expr = def.getExpression();
        if (expr == null) return null;

        // Ensure format #{...}
        if (!expr.trim().startsWith("#{")) expr = "#{" + expr + "}";

        // If not strict (Batch Analysis), check if all dependencies exist in context
        if (!strict && !canResolve(expr, context)) return null;

        Object res = globalParamResolver.resolveValue(expr, context);
        return typeConverter.convert(res, def.getDataType(), def.getName());
    }

    @SuppressWarnings("unchecked")
    private Object resolveQuery(GlobalParamDef def, Map<String, Object> context, boolean strict) {
        if (def.getSource() == null) return null;

        // Deep resolve filters (replace $G{...})
        // If strict=false and a dependency is missing, resolveRecursive might throw or return unresolved string.
        // We catch this to skip.
        Map<String, Object> resolvedFilters;
        try {
            Object res = globalParamResolver.resolveRecursive(def.getSource().getFilters(), context);
            resolvedFilters = (Map<String, Object>) res;
        } catch (RuntimeException e) {
            if (!strict) return null;
            throw e;
        }

        // Check if any $G{} remains unresolved
        if (!strict && containsUnresolvedRefs(resolvedFilters)) return null;

        DataSourceSpec spec = new DataSourceSpec(
                "GLOBAL::" + def.getName(),
                Type.valueOf(def.getSource().getType()),
                def.getSource().getTable(),
                def.getSource().getColumn(),
                resolvedFilters,
                "GlobalParam"
        );

        Object result = dbResolver.resolve(spec);
        return typeConverter.convert(result, def.getDataType(), def.getName());
    }

    // --- Helpers ---

    private Map<String, Set<String>> buildDependencyGraph(List<GlobalParamDef> globals) {
        Map<String, Set<String>> map = new HashMap<>();
        Map<String, GlobalParamDef> defs = globals.stream().collect(Collectors.toMap(GlobalParamDef::getName, p->p));

        for (GlobalParamDef def : globals) {
            Set<String> deps = new HashSet<>();

            // Analyze Expression
            if (def.getMode() == ReportTemplate.ParamMode.EXPRESSION && def.getExpression() != null) {
                Matcher m = REF_PATTERN.matcher(def.getExpression());
                while (m.find()) {
                    String ref = m.group(1) != null ? m.group(1) : m.group(2);
                    if (defs.containsKey(ref)) deps.add(ref);
                }
            }

            // Analyze Query Filters
            if (def.getMode() == ReportTemplate.ParamMode.QUERY && def.getSource() != null) {
                extractDepsFromObject(def.getSource().getFilters(), deps, defs);
            }

            map.put(def.getName(), deps);
        }
        return map;
    }

    @SuppressWarnings("unchecked")
    private void extractDepsFromObject(Object obj, Set<String> deps, Map<String, GlobalParamDef> defs) {
        if (obj == null) return;
        String s = obj.toString();
        Matcher m = REF_PATTERN.matcher(s);
        while(m.find()) {
            String ref = m.group(1) != null ? m.group(1) : m.group(2);
            if(defs.containsKey(ref)) deps.add(ref);
        }
        // Recursion for Maps/Lists
        if (obj instanceof Map) ((Map<?,?>)obj).values().forEach(v -> extractDepsFromObject(v, deps, defs));
        if (obj instanceof List) ((List<?>)obj).forEach(v -> extractDepsFromObject(v, deps, defs));
    }

    private boolean canResolve(String expr, Map<String, Object> context) {
        Matcher m = REF_PATTERN.matcher(expr);
        while (m.find()) {
            String ref = m.group(1) != null ? m.group(1) : m.group(2);
            if (!context.containsKey(ref)) return false;
        }
        return true;
    }

    private boolean containsUnresolvedRefs(Object obj) {
        if (obj == null) return false;
        if (obj.toString().contains("$G{")) return true;
        if (obj instanceof Map) {
            for (Object v : ((Map<?,?>)obj).values()) if (containsUnresolvedRefs(v)) return true;
        }
        if (obj instanceof List) {
            for (Object v : ((List<?>)obj) ) if (containsUnresolvedRefs(v)) return true;
        }
        return false;
    }
}