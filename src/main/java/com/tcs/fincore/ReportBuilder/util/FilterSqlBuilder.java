package com.tcs.fincore.ReportBuilder.util;

import com.tcs.fincore.ReportBuilder.exception.ValidationException;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class FilterSqlBuilder {

    private static final String JAVA_DATE_FMT = "yyyy-MM-dd";

    private static final int MAX_IN_VALUES       = 40_000;
    private static final int MAX_FILTER_CONDITIONS = 50;

    private FilterSqlBuilder() {
        // Utility class - prevent instantiation
    }

    /**
     * Validates SQL identifiers (table/column names) to prevent injection
     *
     * @param identifier The identifier to validate
     * @throws ValidationException if identifier is invalid
     */
    public static void validateIdentifier(String identifier) {
        if (identifier == null || identifier.isBlank()) {
            throw new ValidationException("Identifier cannot be null or blank");
        }

        // Only allow alphanumeric, underscore, and dot (for schema.table)
        if (!identifier.matches("[A-Za-z0-9_\\.]+")) {
            throw new ValidationException(
                    "Invalid identifier: '" + identifier +
                            "'. Only alphanumeric, underscore, and dot allowed.");
        }

        if (identifier.length() > 128) {
            throw new ValidationException(
                    "Identifier too long: " + identifier.length() + " chars (max: 128)");
        }
    }

    /**
     * Converts raw values to proper database types for optimal performance
     * <p>
     * CRITICAL: Binding correct types prevents Oracle from suppressing indexes
     *
     * @param value The raw value (often String from JSON)
     * @param type  The target data type (DATE, NUMBER, STRING)
     * @return Properly typed value for JDBC binding
     * @throws ValidationException if type conversion fails
     */
    private static Object convertValue(Object value, String type) {
        if (value == null) {
            return null;
        }

        String typeUpper = type == null ? "STRING" : type.toUpperCase(Locale.ROOT);

        try {
            switch (typeUpper) {
                case "DATE" -> {
                    // Already correct type
                    if (value instanceof LocalDate) {
                        return value;
                    }

                    // Convert java.util.Date
                    if (value instanceof java.util.Date d) {
                        return new java.sql.Date(d.getTime()).toLocalDate();
                    }

                    // Parse string (ISO format: yyyy-MM-dd)
                    String dateStr = value.toString().trim();
                    return LocalDate.parse(dateStr,
                            DateTimeFormatter.ofPattern(JAVA_DATE_FMT));
                }

                case "NUMBER" -> {
                    // Already correct type
                    if (value instanceof Number) {
                        return value;
                    }

                    // Parse string, removing formatting
                    String numStr = value.toString().trim().replace(",", "");
                    if (numStr.isEmpty()) {
                        return null;
                    }

                    // Use BigDecimal for precision (critical for financial data)
                    return new BigDecimal(numStr);
                }

                default -> {
                    // STRING - just ensure it's a string
                    return value.toString();
                }
            }
        } catch (RuntimeException e) {
            throw new ValidationException(
                    "Type conversion failed: Cannot convert value '" + value +
                            "' to type " + type + ". Error: " + e.getMessage());
        }
    }

    @SuppressWarnings("unchecked")
    public static void appendFilters(
            StringBuilder where,
            Map<String, Object> params,
            Map<String, Object> filters) {

        if (filters == null || filters.isEmpty()) {
            return;
        }

        //  CRITICAL FIX: Global counter for unique parameter names
        // This prevents parameter name collisions when same column has multiple conditions
        AtomicInteger globalParamCounter = new AtomicInteger(0);

        int totalConditions = 0;

        for (Map.Entry<String, Object> entry : filters.entrySet()) {
            String column = entry.getKey();
            Object filterValue = entry.getValue();

            // Validate column name (SQL injection prevention)
            validateIdentifier(column);

            // Base parameter name for this column
            String paramPrefix = column.replaceAll("[^A-Za-z0-9_]", "") + "_p";

            // ========================================
            // 1. HANDLE LIST OF CONDITIONS (Multi-Operator Support)
            // ========================================
            if (filterValue instanceof List<?> list) {

                //  Safety check
                totalConditions += list.size();
                if (totalConditions > MAX_FILTER_CONDITIONS) {
                    throw new ValidationException(
                            "Too many filter conditions (" + totalConditions +
                                    ", max: " + MAX_FILTER_CONDITIONS + ")");
                }

                for (Object item : list) {
                    if (item instanceof Map<?, ?> conditionMap) {
                        //  CRITICAL: Generate unique param name for EACH condition
                        String uniqueParamName = paramPrefix +
                                globalParamCounter.getAndIncrement();

                        appendSingleCondition(
                                where,
                                params,
                                column,
                                uniqueParamName,
                                (Map<String, Object>) conditionMap
                        );
                    } else {
                        throw new ValidationException(
                                "Invalid filter structure for column " + column +
                                        ": expected Map in List");
                    }
                }
                continue;
            }

            // ========================================
            // 2. HANDLE SINGLE CONDITION
            // ========================================

            if (filterValue instanceof Map<?, ?> conditionMap) {
                totalConditions++;
                if (totalConditions > MAX_FILTER_CONDITIONS) {
                    throw new ValidationException(
                            "Too many filter conditions. Maximum allowed: " + MAX_FILTER_CONDITIONS);
                }
                String uniqueParamName = paramPrefix + globalParamCounter.getAndIncrement();
                appendSingleCondition(where, params, column, uniqueParamName,
                        (Map<String, Object>) conditionMap);
                continue;
            }

            // ========================================
            // 3. HANDLE SIMPLE SCALAR (Exact Match)
            // ========================================
            totalConditions++;
            String uniqueParamName = paramPrefix +
                    globalParamCounter.getAndIncrement();

            where.append(" AND ").append(column)
                    .append(" = :").append(uniqueParamName);
            params.put(uniqueParamName, filterValue);
        }

        log.debug("Applied {} filter conditions with {} unique parameters",
                totalConditions, globalParamCounter.get());
    }


    /// ////////////////////////////////////////////////////////////////////////////////////////////

    private static void appendSingleCondition(
            StringBuilder where, Map<String, Object> params,
            String column, String paramName, Map<String, Object> condition) {

        String op = String.valueOf(condition.getOrDefault("op", "=")).toUpperCase();
        String type = String.valueOf(condition.getOrDefault("dataType", "STRING")).toUpperCase();
        Object rawValue = condition.get("value");

        switch (op) {
            case "=", "!=", ">", "<", ">=", "<=", "LIKE" -> {
                Object typedVal = convertValue(rawValue, type);
                where.append(" AND ").append(column).append(" ").append(op)
                        .append(" :").append(paramName);
                params.put(paramName, typedVal);
            }

            case "BETWEEN" -> {
                if (!(rawValue instanceof List<?> list) || list.size() != 2) {
                    throw new ValidationException(
                            "BETWEEN requires List with exactly 2 values for column " + column);
                }
                Object val1 = convertValue(list.get(0), type);
                Object val2 = convertValue(list.get(1), type);
                String p1 = paramName + "_from";
                String p2 = paramName + "_to";
                where.append(" AND ").append(column).append(" BETWEEN :")
                        .append(p1).append(" AND :").append(p2);
                params.put(p1, val1);
                params.put(p2, val2);
            }

            case "IN" -> {
                if (!(rawValue instanceof Collection<?> col)) {
                    throw new ValidationException(
                            "IN operator requires Collection for column " + column);
                }

                List<Object> typedList = new ArrayList<>();
                for (Object item : col) {
                    typedList.add(convertValue(item, type));
                }

                if (typedList.isEmpty()) {
                    where.append(" AND 1=0");
                    return;
                }

                // SECURITY FIX: ENFORCE the IN list limit (never comment this out)
                if (typedList.size() > MAX_IN_VALUES) {
                    throw new ValidationException(
                            "IN clause too large: " + typedList.size() +
                                    " values (max: " + MAX_IN_VALUES + ") for column " + column);
                }

                padList(typedList);

                if (typedList.size() <= 1000) {
                    where.append(" AND ").append(column).append(" IN (:").append(paramName).append(")");
                    params.put(paramName, typedList);
                } else {
                    List<List<Object>> chunks = splitList(typedList, 1000);
                    where.append(" AND (");
                    for (int i = 0; i < chunks.size(); i++) {
                        String chunkParam = paramName + "_chunk_" + i;
                        where.append(column).append(" IN (:").append(chunkParam).append(")");
                        params.put(chunkParam, chunks.get(i));
                        if (i < chunks.size() - 1) where.append(" OR ");
                    }
                    where.append(")");
                    log.debug("Split IN clause into {} chunks for column {}", chunks.size(), column);
                }
            }

            default -> throw new ValidationException(
                    "Unsupported operator '" + op + "' for column " + column);
        }
    }

    /// //////////////////////////////////////////////////

    /**
     * ? OPTIMIZATION: Pad list to nearest bucket size
     * <p>
     * This helps Oracle reuse execution plans even when IN list sizes vary slightly
     * Oracle ignores NULLs in IN clauses, so padding is safe
     * <p>
     * Example: [1,2,3] with 17 items padded to 20 items
     *
     * @param list List to pad (modified in-place)
     */
    private static void padList(List<Object> list) {
        int size = list.size();

        // Don't pad very small lists
        if (size <= 10) {
            return;
        }

        // Determine target bucket size
        int target;
        if (size <= 20) target = 20;
        else if (size <= 50) target = 50;
        else if (size <= 100) target = 100;
        else if (size <= 500) target = 500;
        else if (size <= 1000) target = 1000;
        else return;  // Don't pad huge lists

        // Pad with nulls
        while (list.size() < target) {
            list.add(null);
        }

        if (log.isTraceEnabled()) {
            log.trace("Padded IN list from {} to {} items", size, target);
        }
    }

    /**
     * Split list into chunks of specified size
     *
     * @param list List to split
     * @param size Chunk size
     * @return List of chunks
     */
    private static <T> List<List<T>> splitList(List<T> list, int size) {
        List<List<T>> parts = new ArrayList<>();
        for (int i = 0; i < list.size(); i += size) {
            parts.add(list.subList(i, Math.min(list.size(), i + size)));
        }
        return parts;
    }
}