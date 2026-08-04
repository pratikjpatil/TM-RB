package com.tcs.fincore.ReportBuilder.service.runtime;

import com.tcs.fincore.ReportBuilder.exception.ValidationException;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.Locale;

/**
 * PRODUCTION COMPONENT: Robust Type Conversion.
 * Ensures JEXL math and SQL params get correct Java types (BigDecimal, LocalDate).
 */
@Component
public class ParamTypeConverter {

    // Pre-compiled formatters for performance
    private static final List<DateTimeFormatter> SUPPORTED_DATE_FMTS = List.of(
            DateTimeFormatter.ISO_LOCAL_DATE,       // 2023-12-31
            DateTimeFormatter.ofPattern("dd-MM-yyyy"),
            DateTimeFormatter.ofPattern("dd/MM/yyyy"),
            DateTimeFormatter.ofPattern("yyyy/MM/dd")
    );

    public Object convert(Object rawValue, String targetType, String paramName) {
        if (rawValue == null) return null;

        // Default to STRING if type is unspecified
        String type = targetType == null ? "STRING" : targetType.toUpperCase(Locale.ROOT);

        if (rawValue instanceof List<?> && ("LIST".equals(type) || "STRING".equals(type))) {
            return rawValue;
        }

        try {
            return switch (type) {
                case "STRING" -> String.valueOf(rawValue);

                case "NUMBER" -> {
                    if (rawValue instanceof Number n) yield new BigDecimal(n.toString());
                    String s = rawValue.toString().trim().replace(",", "");
                    yield s.isBlank() ? null : new BigDecimal(s);
                }

                case "DATE" -> parseDate(rawValue, paramName);

                case "BOOLEAN" -> {
                    if (rawValue instanceof Boolean b) yield b;
                    yield Boolean.parseBoolean(rawValue.toString());
                }

                default -> throw new ValidationException("Unsupported Global Param Type: " + type);
            };
        } catch (RuntimeException e) {
            throw new ValidationException(String.format(
                    "Type Mismatch for '%s': Cannot convert '%s' to %s. Error: %s",
                    paramName, rawValue, type, e.getMessage()));
        }
    }

    private LocalDate parseDate(Object obj, String paramName) {
        if (obj instanceof LocalDate) return (LocalDate) obj;
        if (obj instanceof java.sql.Date d) return d.toLocalDate();
        if (obj instanceof java.util.Date d) return new java.sql.Date(d.getTime()).toLocalDate();

        String str = obj.toString().trim();
        if (str.isBlank()) return null;

        for (DateTimeFormatter fmt : SUPPORTED_DATE_FMTS) {
            try { return LocalDate.parse(str, fmt); } catch (DateTimeParseException ignored) { }
        }
        throw new ValidationException("Invalid Date format for '" + paramName + "': " + str +
                ". Expected ISO (yyyy-MM-dd) or common formats.");
    }
}
