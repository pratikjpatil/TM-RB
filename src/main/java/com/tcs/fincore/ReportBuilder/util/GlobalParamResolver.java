package com.tcs.fincore.ReportBuilder.util;

import com.tcs.fincore.ReportBuilder.exception.ValidationException;
import com.tcs.fincore.ReportBuilder.service.runtime.JexlEngineFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.jexl3.JexlContext;
import org.apache.commons.jexl3.JexlEngine;
import org.apache.commons.jexl3.JexlException;
import org.apache.commons.jexl3.JexlExpression;
import org.apache.commons.jexl3.MapContext;
import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Central logic for resolving global params ($G{...}) and inline expressions (#{...}).
 */
@Slf4j
@Component
public class GlobalParamResolver {

    private static final Pattern GLOBAL_PATTERN = Pattern.compile("\\$G\\{([a-zA-Z0-9_]+)\\}");

    private final JexlEngine jexl;

    public GlobalParamResolver(JexlEngineFactory jexlEngineFactory) {
        this.jexl = jexlEngineFactory.createSecureEngine();
    }

    public Object resolveValue(Object rawValue, Map<String, Object> runtimeParams) {
        if (rawValue == null) return null;
        if (!(rawValue instanceof String s)) return rawValue;

        String trimmed = s.trim();
        if (trimmed.startsWith("#{") && trimmed.endsWith("}")) {
            String expression = trimmed.substring(2, trimmed.length() - 1);
            return evaluateExpression(expression, runtimeParams);
        }

        Matcher m = GLOBAL_PATTERN.matcher(trimmed);
        if (m.matches()) {
            String key = m.group(1);
            if (runtimeParams.containsKey(key)) {
                return runtimeParams.get(key);
            }
            throw new ValidationException("Runtime Error: Parameter '$G{" + key + "}' referenced but not found in context.");
        }

        if (s.contains("$G{")) {
            return interpolateString(s, runtimeParams);
        }
        return rawValue;
    }

    @SuppressWarnings("unchecked")
    public Object resolveRecursive(Object input, Map<String, Object> params) {
        if (input instanceof String) {
            return resolveValue(input, params);
        } else if (input instanceof Map) {
            Map<String, Object> copy = new LinkedHashMap<>((Map<String, Object>) input);
            copy.replaceAll((k, v) -> resolveRecursive(v, params));
            return copy;
        } else if (input instanceof List) {
            return ((List<?>) input).stream()
                    .map(i -> resolveRecursive(i, params))
                    .collect(Collectors.toList());
        }
        return input;
    }

    private Object evaluateExpression(String expression, Map<String, Object> params) {
        try {
            JexlContext context = new MapContext();
            context.set("G", params);
            context.set("DateUtils", new DateUtilsHelper());
            context.set("Math", Math.class);

            JexlExpression e = jexl.createExpression(expression);
            return e.evaluate(context);
        } catch (JexlException ex) {
            log.error("Failed to evaluate inline expression: '{}'. Error: {}", expression, ex.getMessage());
            throw new ValidationException("Invalid expression syntax: " + expression + ". " + ex.getMessage());
        } catch (RuntimeException ex) {
            log.error("Unexpected error evaluating expression: '{}'", expression, ex);
            throw new ValidationException("Expression evaluation failed: " + ex.getMessage());
        }
    }

    private String interpolateString(String template, Map<String, Object> params) {
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
