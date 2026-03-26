package com.tcs.fincore.ReportBuilder.service.calculation;

import com.tcs.fincore.ReportBuilder.exception.ValidationException;
import com.tcs.fincore.ReportBuilder.model.ReportTemplate;
import com.tcs.fincore.ReportBuilder.service.runtime.JexlEngineFactory;
import com.tcs.fincore.ReportBuilder.util.GraphUtils;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.jexl3.*;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * High-performance CalculationService.
 * Optimizations: Lazy JexlContext, Expression Caching, Compiled Regex, Branch Prediction improvements.
 */
@Slf4j
@Service
public class CalculationService {

    // Pre-compiled patterns
    private static final String CELL_REF_REGEX_STR = "cell_R__[A-Za-z0-9]+_C__[A-Za-z0-9]+";
    private static final Pattern CELL_REF_PATTERN = Pattern.compile(CELL_REF_REGEX_STR);

    // Optimization: Cache parsed JEXL expressions to avoid parsing overhead on every cell
    private final Map<String, JexlExpression> expressionCache = new ConcurrentHashMap<>();

    private final JexlEngine jexl;

    public CalculationService(JexlEngineFactory jexlEngineFactory) {
        this.jexl = jexlEngineFactory.createCalculationEngine(new SafeArithmetic(true));
    }

    public Map<String, Object> calculate(ReportTemplate template, Map<String, Object> dbResultsInput) {
        final Map<String, Object> dbResults = dbResultsInput == null ? Map.of() : dbResultsInput;

        log.debug("CalculationService starting (dbResultsCount={})", dbResults.size());

        // Use appropriate initial capacity to prevent resizing
        int estimatedSize = (template.getReportData() != null && template.getReportData().getRows() != null)
                ? template.getReportData().getRows().size() * 10
                : 100;
        Map<String, Object> computed = new HashMap<>(estimatedSize);

        // 1. Seed TEXT and DB_* cells (Single Pass)
        if (template.getReportData() != null && template.getReportData().getRows() != null) {
            for (ReportTemplate.Row row : template.getReportData().getRows()) {
                if (row.getCells() == null) continue;

                for (ReportTemplate.Cell cell : row.getCells()) {
                    if (cell == null || cell.getId() == null) continue;

                    String type = cell.getType(); // Avoid toUpperCase if possible, or assume standard casing

                    // Fast path for TEXT
                    if (type == null || "TEXT".equalsIgnoreCase(type)) {
                        computed.put(cell.getId(), cell.getValue() == null ? "" : cell.getValue());
                        continue;
                    }

                    if (type.startsWith("DB_")) {
                        Object dbVal = dbResults.get(cell.getId());
                        if (dbVal instanceof String s && s.startsWith("ERROR:")) {
                            // Log only once per run or use debug to avoid I/O flooding
                            if (log.isDebugEnabled()) log.debug("DB cell error: {}", cell.getId());
//                            computed.put(cell.getId(), 0d);
                            computed.put(cell.getId(), "");
                        } else {
//                            computed.put(cell.getId(), dbVal == null ? 0d : dbVal);
                            computed.put(cell.getId(), dbVal == null ? "" : dbVal);
                        }
                    }
                }
            }
        }

        // 2. Collect formula cells & dependencies
        Map<String, ReportTemplate.Cell> formulaCells = new LinkedHashMap<>(estimatedSize / 2);
        Map<String, Set<String>> deps = new HashMap<>(estimatedSize / 2);

        if (template.getReportData() != null && template.getReportData().getRows() != null) {
            for (ReportTemplate.Row row : template.getReportData().getRows()) {
                if (row.getCells() == null) continue;

                for (ReportTemplate.Cell cell : row.getCells()) {
                    if (cell == null || cell.getId() == null) continue;
                    if (!"FORMULA".equalsIgnoreCase(cell.getType())) continue;

                    formulaCells.put(cell.getId(), cell);
                    Set<String> depSet = new HashSet<>();

                    // Optimized Variable Parsing
                    if (cell.getVariables() != null && !cell.getVariables().isEmpty()) {
                        extractVariableDependencies(cell.getVariables(), depSet);
                    }

                    // Optimized Expression Parsing
                    if (cell.getExpression() != null) {
                        Matcher m = CELL_REF_PATTERN.matcher(cell.getExpression());
                        while (m.find()) {
                            depSet.add(m.group());
                        }
                    }
                    deps.put(cell.getId(), depSet);
                }
            }
        }

        // 3. Topological Sort (Consider caching this result if Template ID is available)
//        List<String> evalOrder = topoSort(deps);
        List<String> evalOrder = GraphUtils.sort(deps);
        // 4. Evaluate formulas using Lazy Context
        // Re-use a single context object to avoid object churn
        LazyCalculationContext context = new LazyCalculationContext(computed, dbResults);

        int successCount = 0;
        int failureCount = 0;

        for (String cid : evalOrder) {
            ReportTemplate.Cell cell = formulaCells.get(cid);
            if (cell == null) continue;

            try {
                // Update context for current cell scope
                context.setCurrentCell(cell);

                Object val = evaluateFast(cell, context);
                Object result = safeNumericOrOriginal(val);
//                if(result instanceof String && ((String) result).trim().isEmpty()){
//                    result = 0d;
//                }
                computed.put(cid, result);
                successCount++;

            } catch (RuntimeException ex) {
                failureCount++;
                log.error("Formula failed [{}]: expr='{}' err={}", cid, cell.getExpression(), ex.getMessage());
                computed.put(cid, 0d);
            }
        }

        log.debug("Calc finished: {} success, {} failed", successCount, failureCount);
        return computed;
    }

    private void extractVariableDependencies(Map<String, Object> variables, Set<String> depSet) {
        variables.forEach((k, v) -> {
            if (v instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<String, Object> vm = (Map<String, Object>) v;
                // Fast check for type without conversion overhead
                Object typeObj = vm.get("type");
                if (typeObj instanceof String && "CELL_REF".equalsIgnoreCase((String) typeObj)) {
                    Object ref = vm.get("ref");
                    if (ref instanceof String s && CELL_REF_PATTERN.matcher(s).matches()) {
                        depSet.add(s);
                    }
                }
            } else if (v instanceof String s) {
                if (CELL_REF_PATTERN.matcher(s).matches()) {
                    depSet.add(s);
                }
            }
        });
    }

    private Object evaluateFast(ReportTemplate.Cell cell, LazyCalculationContext context) {
        String expr = cell.getExpression();
        if (expr == null || expr.isEmpty()) return "";

        // Retrieve from cache or parse and cache
        JexlExpression jexlExpr = expressionCache.computeIfAbsent(expr, jexl::createExpression);

        return jexlExpr.evaluate(context);
    }


    /**
     * Optimized Helper: avoids throwing exceptions for control flow.
     */
    private Object safeNumericOrOriginal(Object v) {
        if (v == null) return 0d;

        // 1. Handle Numbers (JEXL often returns Double or BigDecimal)
        if (v instanceof Number n) {
            double d = n.doubleValue();
            // Normalize negative zero (-0.0) to positive zero (0.0)
            // In Java, -0.0 == 0.0 returns true, so this resets the sign bit.
            return (d == 0.0) ? 0.0 : d;
        }

        if (v instanceof String s) {
            if (s.isEmpty()) return 0d;
            if (s.startsWith("ERROR:") || s.equalsIgnoreCase("null")) return 0d;

            // Fast path: check if simple integer/double string to avoid heavy clean-up
            try {
                // Optimization: Try parsing directly first (fastest for clean data)
                double d = Double.parseDouble(s);
                // FIX: Normalize negative zero here as well
                return (d == 0.0) ? 0.0 : d;
            } catch (NumberFormatException e) {
                // Fallback: clean characters
                try {
                    String cleaned = s.replace(",", "").replace("$", "").replace("₹", "").trim();
                    if (cleaned.isEmpty()) return 0d;

                    double d = Double.parseDouble(cleaned);
                    return (d == 0.0) ? 0.0 : d;
                } catch (NumberFormatException finalEx) {
                    return 0d;
                }
            }
        }
        return 0d;
    }

    /**
     * Custom Arithmetic to handle Division by Zero gracefully.
     * Returns 0.0 instead of throwing ArithmeticException.
     */
    public static class SafeArithmetic extends JexlArithmetic {
        public SafeArithmetic(boolean strict) {
            super(strict);
        }

        @Override
        public Object divide(Object left, Object right) {
            // Check if denominator is effectively zero
            if (right instanceof Number && ((Number) right).doubleValue() == 0.0) {
                return 0.0d;
            }
            // Fallback to standard logic for non-zero denominators
            return super.divide(left, right);
        }
    }

    /**
     * Inner Class: Custom JexlContext.
     * PERFORMANCE CRITICAL: Reads directly from maps. Zero copying.
     */
    private class LazyCalculationContext implements JexlContext {
        private final Map<String, Object> computed;
        private final Map<String, Object> dbResults;
        @Setter
        private ReportTemplate.Cell currentCell;

        public LazyCalculationContext(Map<String, Object> computed, Map<String, Object> dbResults) {
            this.computed = computed;
            this.dbResults = dbResults;
        }

        @Override
        public Object get(String name) {
            // 1. Check Computed cells (Most common)
            if (computed.containsKey(name)) {
                return safeNumericOrOriginal(computed.get(name));
            }

            // 2. Check Local Variables defined in Cell
            if (currentCell != null && currentCell.getVariables() != null) {
                Object varVal = currentCell.getVariables().get(name);
                if (varVal != null) {
                    return resolveVariable(name, varVal);
                }
            }

            // 3. Fallback / Math namespace handled by JEXL engine
            return null;
        }

        private Object resolveVariable(String varName, Object varVal) {
            if (varVal instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<String, Object> vm = (Map<String, Object>) varVal;
                String vType = (String) vm.get("type"); // assume correct type to avoid casting overhead

                if ("CELL_REF".equals(vType) || (vType != null && "CELL_REF".equalsIgnoreCase(vType))) {
                    return safeNumericOrOriginal(computed.get(vm.get("ref")));
                }
                else if (vType != null && vType.startsWith("DB_")) {
                    // Reconstruct ID logic
                    String specId = currentCell.getId() + "::var::" + varName;
                    return safeNumericOrOriginal(dbResults.get(specId));
                }
                else if ("CONST".equals(vType)) {
                    return vm.get("value");
                }
            }
            else if (varVal instanceof String s) {
                if (CELL_REF_PATTERN.matcher(s).matches()) {
                    return safeNumericOrOriginal(computed.get(s));
                }
                return s;
            }
            return varVal;
        }

        @Override
        public void set(String name, Object value) {
            // Read-only context for formulas usually, but if needed:
            computed.put(name, value);
        }

        @Override
        public boolean has(String name) {
            return computed.containsKey(name) ||
                    (currentCell != null && currentCell.getVariables() != null && currentCell.getVariables().containsKey(name));
        }
    }

    // Standard TopoSort (remains mostly same, just slight clean up)
    private List<String> topoSort(Map<String, Set<String>> deps) {
        // Optimized to use adjacency list directly to avoid building large intermediate sets
        Map<String, Integer> indeg = new HashMap<>();
        // Ensure all keys exist
        deps.keySet().forEach(k -> indeg.put(k, 0));

        Map<String, List<String>> adj = new HashMap<>(); // List is faster than Set for iteration

        for (Map.Entry<String, Set<String>> entry : deps.entrySet()) {
            String u = entry.getKey();
            for (String v : entry.getValue()) {
                if (deps.containsKey(v)) { // Only care if dependency is a formula
                    adj.computeIfAbsent(v, k -> new ArrayList<>()).add(u);
                    indeg.merge(u, 1, Integer::sum);
                }
            }
        }

        Deque<String> queue = new ArrayDeque<>();
        indeg.forEach((k, v) -> { if (v == 0) queue.add(k); });

        List<String> result = new ArrayList<>(deps.size());
        while (!queue.isEmpty()) {
            String u = queue.poll();
            result.add(u);

            if (adj.containsKey(u)) {
                for (String v : adj.get(u)) {
                    int newDeg = indeg.get(v) - 1;
                    indeg.put(v, newDeg);
                    if (newDeg == 0) queue.add(v);
                }
            }
        }

        if (result.size() != deps.size()) {
            throw new ValidationException("Circular dependency detected");
        }
        return result;
    }
}