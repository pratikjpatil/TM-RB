package com.tcs.fincore.ReportBuilder.batch.validator;

import com.tcs.fincore.ReportBuilder.exception.ValidationException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.*;

@Slf4j
@Component
public class FilterValidator {

    private static final Set<String> ALLOWED_OPERATORS = Set.of(
            "=", "!=", ">", "<", ">=", "<=", "LIKE", "IN", "BETWEEN"
    );

    private static final Set<String> ALLOWED_DATA_TYPES = Set.of(
            "STRING", "NUMBER", "DATE"
    );

    @Value("${report.batch.filter.max-in-values:10000}")
    private int maxInValues;

    @Value("${report.batch.filter.max-conditions:50}")
    private int maxFilterConditions;

    /**
     * Validate all filters in a map
     *
     * @param filters Filter map to validate
     * @param context Context for error messages (e.g., "Spec[cell_R1_C1]")
     * @throws ValidationException if validation fails
     */
    @SuppressWarnings("unchecked")
    public void validateFilters(Map<String, Object> filters, String context) {
        if (filters == null || filters.isEmpty()) {
            return;
        }

        int totalConditions = 0;

        for (Map.Entry<String, Object> entry : filters.entrySet()) {
            String column = entry.getKey();
            Object filterValue = entry.getValue();

            // Validate column name
            validateColumnName(column, context);

            // Handle List of conditions
            if (filterValue instanceof List<?> list) {
                totalConditions += list.size();

                if (totalConditions > maxFilterConditions) {
                    throw new ValidationException(
                            context + ": Too many filter conditions (" +
                                    totalConditions + ", max: " + maxFilterConditions + ")");
                }

                for (int i = 0; i < list.size(); i++) {
                    Object item = list.get(i);
                    if (item instanceof Map) {
                        validateCondition(
                                (Map<String, Object>) item,
                                column,
                                context + "[condition_" + i + "]"
                        );
                    } else {
                        throw new ValidationException(
                                context + ": List element at index " + i +
                                        " for column " + column + " is not a Map");
                    }
                }
            }
            // Single condition
            else if (filterValue instanceof Map) {
                totalConditions++;
                if (totalConditions > maxFilterConditions) {
                    throw new ValidationException(
                            context + ": Too many filter conditions (" +
                                    totalConditions + ", max: " + maxFilterConditions + ")");
                }
                validateCondition((Map<String, Object>) filterValue, column, context);
            }
            // Simple value (will be treated as equality)
            else {
                totalConditions++;
            }
        }

        log.debug("Filter validation passed for {}: {} conditions",
                context, totalConditions);
    }

    /**
     * Validate a single filter condition
     */
    @SuppressWarnings("unchecked")
    private void validateCondition(
            Map<String, Object> condition,
            String column,
            String context) {

        // Validate operator
        Object opObj = condition.get("op");
        if (opObj == null) {
            throw new ValidationException(
                    context + ": Missing 'op' (operator) in filter for column " + column);
        }

        String op = String.valueOf(opObj).toUpperCase();
        if (!ALLOWED_OPERATORS.contains(op)) {
            throw new ValidationException(
                    context + ": Invalid operator '" + op +
                            "' for column " + column +
                            ". Allowed: " + ALLOWED_OPERATORS);
        }

        // Validate dataType
        Object typeObj = condition.get("dataType");
        if (typeObj != null) {
            String type = String.valueOf(typeObj).toUpperCase();
            if (!ALLOWED_DATA_TYPES.contains(type)) {
                throw new ValidationException(
                        context + ": Invalid dataType '" + type +
                                "' for column " + column +
                                ". Allowed: " + ALLOWED_DATA_TYPES);
            }
        }

        // Validate value presence
        Object value = condition.get("value");
        if (value == null) {
            throw new ValidationException(
                    context + ": Missing 'value' for operator " + op +
                            " on column " + column);
        }

        // Special validation for IN
        if ("IN".equals(op)) {
            if (!(value instanceof Collection)) {
                throw new ValidationException(
                        context + ": IN operator requires Collection value for column " +
                                column + ", got: " + value.getClass().getSimpleName());
            }

            Collection<?> col = (Collection<?>) value;
            if (col.isEmpty()) {
                log.warn("{}: IN operator has empty value list for column {}",
                        context, column);
            }

            if (col.size() > maxInValues) {
                throw new ValidationException(
                        context + ": IN clause too large (" + col.size() +
                                " values, max: " + maxInValues + ") for column " + column);
            }
        }

        // Special validation for BETWEEN
        if ("BETWEEN".equals(op)) {
            if (!(value instanceof List) || ((List<?>) value).size() != 2) {
                throw new ValidationException(
                        context + ": BETWEEN requires List with exactly 2 values for column " +
                                column);
            }
        }
    }

    /**
     * Validate column name (SQL injection prevention)
     */
    private void validateColumnName(String column, String context) {
        if (column == null || column.isBlank()) {
            throw new ValidationException(
                    context + ": Column name cannot be null or blank");
        }

        // Only allow alphanumeric, underscore, and dot
        if (!column.matches("[A-Za-z0-9_\\.]+")) {
            throw new ValidationException(
                    context + ": Invalid column name '" + column +
                            "'. Only alphanumeric, underscore, and dot allowed.");
        }

        if (column.length() > 128) {
            throw new ValidationException(
                    context + ": Column name too long: " + column.length() +
                            " chars (max: 128)");
        }
    }
}