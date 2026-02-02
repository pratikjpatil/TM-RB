package com.tcs.fincore.ReportBuilder.util;

import com.tcs.fincore.ReportBuilder.exception.ValidationException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.jexl3.*;
import org.apache.commons.jexl3.introspection.JexlPermissions;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * PRODUCTION-GRADE: Central logic for resolving Global Params ($G{...})
 * and Inline Expressions (#{...}).
 * * Includes JexlEngine caching for performance.
 */
@Slf4j
public class GlobalParamResolver {

    private static final Pattern GLOBAL_PATTERN = Pattern.compile("\\$G\\{([a-zA-Z0-9_]+)\\}");
    private static final JexlEngine jexl = new JexlBuilder()
            .cache(512)
            .strict(true)
            .silent(false)
            .permissions(JexlPermissions.UNRESTRICTED)
            .create();

    /**
     * Resolves a value which might be a literal, a global ref ($G), or an expression (#{}).
     */
    public static Object resolveValue(Object rawValue, Map<String, Object> runtimeParams) {
        if (rawValue == null) return null;

        // Return non-strings immediately (Numbers, Dates)
        if (!(rawValue instanceof String s)) {
            return rawValue;
        }

        // 1. Check for Inline Expression: "#{ ... }"
        // Syntax: #{ DateUtils.addYears(G.REPORT_DATE, -1) }
        String trimmed = s.trim();
        if (trimmed.startsWith("#{") && trimmed.endsWith("}")) {
            String expression = trimmed.substring(2, trimmed.length() - 1);
            return evaluateExpression(expression, runtimeParams);
        }

        // 2. Check for Global Param Replacement: "$G{PARAM}"
        // Handle exact match to preserve type (e.g. return LocalDate object, not string repr)
        Matcher m = GLOBAL_PATTERN.matcher(trimmed);
        if (m.matches()) {
            String key = m.group(1);
            if (runtimeParams.containsKey(key)) {
                return runtimeParams.get(key);
            }
            // Should we return null or throw? Returning original allows lenient fallback.
            log.warn("Global param '{}' not found in runtime context", key);
            //  Fail Fast
            throw new ValidationException("Runtime Error: Parameter '$G{" + key + "}' referenced but not found in context.");
//            return rawValue;
        }

        // 3. Handle String Interpolation: "Report for $G{REGION} Region"
        if (s.contains("$G{")) {
            return interpolateString(s, runtimeParams);
        }

        // No resolution needed
        return rawValue;
    }

    /**
     * Recursively resolves values in Maps and Lists (Deep Resolve).
     */
    @SuppressWarnings("unchecked")
    public static Object resolveRecursive(Object input, Map<String, Object> params) {
        if (input instanceof String) {
            return resolveValue(input, params);
        } else if (input instanceof Map) {
            Map<String, Object> copy = new java.util.LinkedHashMap<>((Map<String, Object>) input);
            copy.replaceAll((k, v) -> resolveRecursive(v, params));
            return copy;
        } else if (input instanceof java.util.List) {
            return ((java.util.List<?>) input).stream()
                    .map(i -> resolveRecursive(i, params))
                    .collect(java.util.stream.Collectors.toList());
        }
        return input;
    }

    private static Object evaluateExpression(String expression, Map<String, Object> params) {
        try {
            // Create context
            JexlContext context = new MapContext();

            // Expose params under "G" namespace (e.g., G.REPORT_DATE)
            context.set("G", params);

            // Add Helper Libraries
            context.set("DateUtils", new DateUtilsHelper());
            // Add Math if needed
            context.set("Math", Math.class);

            // Evaluate
            JexlExpression e = jexl.createExpression(expression);
            Object result = e.evaluate(context);
            log.info("Evaluated Expression: '{}' -> Result: {}", expression, result);
            if (log.isTraceEnabled()) {
                log.trace("Evaluated Expression: '{}' -> Result: {}", expression, result);
            }
            return result;

        } catch (JexlException ex) {
            log.error("Failed to evaluate inline expression: '{}'. Error: {}", expression, ex.getMessage());
            throw new ValidationException("Invalid expression syntax: " + expression + ". " + ex.getMessage());
        } catch (Exception ex) {
            log.error("Unexpected error evaluating expression: '{}'", expression, ex);
            throw new ValidationException("Expression evaluation failed: " + ex.getMessage());
        }
    }

    private static String interpolateString(String template, Map<String, Object> params) {
        Matcher m = GLOBAL_PATTERN.matcher(template);
        StringBuilder sb = new StringBuilder();
        while (m.find()) {
            String key = m.group(1);
            Object val = params.getOrDefault(key, "");
            m.appendReplacement(sb, Matcher.quoteReplacement(String.valueOf(val)));
        }
        m.appendTail(sb);
        return sb.toString();
    }
}