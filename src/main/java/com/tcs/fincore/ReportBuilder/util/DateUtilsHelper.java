package com.tcs.fincore.ReportBuilder.util;

import lombok.extern.slf4j.Slf4j;

import java.sql.Date;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

/**
 * PRODUCTION-GRADE: Helper class for JEXL expressions to perform date math.
 * Exposed in JEXL context as "DateUtils".
 */
@Slf4j
public class DateUtilsHelper {

    private static final List<DateTimeFormatter> STRING_DATE_FORMATTERS = List.of(
            DateTimeFormatter.ISO_LOCAL_DATE,
            DateTimeFormatter.ofPattern("dd-MM-yyyy"),
            DateTimeFormatter.ofPattern("dd/MM/yyyy"),
            DateTimeFormatter.ofPattern("dd-MMM-yyyy", Locale.ENGLISH)
    );

    /**
     * Adds years to a date object (String or LocalDate).
     * Usage in JEXL: DateUtils.addYears(G.REPORT_DATE, -1)
     */
    public LocalDate addYears(Object dateObj, int years) {
        LocalDate date = parse(dateObj);
        return date != null ? date.plusYears(years) : null;
    }

    /**
     * Adds months to a date.
     */
    public LocalDate addMonths(Object dateObj, int months) {

        LocalDate date = parse(dateObj);
        log.debug("inside addMonths :: {} :: {} :: {}", date, months, date.plusMonths(months));

        return date != null ? date.plusMonths(months) : null;
    }

    /**
     * Adds days to a date.
     */
    public LocalDate addDays(Object dateObj, int days) {
        LocalDate date = parse(dateObj);
        return date != null ? date.plusDays(days) : null;
    }

    /**
     * Get start of month.
     */
    public LocalDate startOfMonth(Object dateObj) {
        LocalDate date = parse(dateObj);
        return date != null ? date.withDayOfMonth(1) : null;
    }

    /**
     * Get end of month.
     */
    public LocalDate endOfMonth(Object dateObj) {
        LocalDate date = parse(dateObj);
        return date != null ? date.withDayOfMonth(date.lengthOfMonth()) : null;
    }

    /**
     * NEW: Get current system date.
     * Usage: #{DateUtils.today()}
     */
    public LocalDate today() {
        return LocalDate.now();
    }

    /**
     * NEW: Get current system timestamp formatted as a String.
     * Usage: #{DateUtils.now('dd-MMM-yyyy HH:mm')}
     */
    public String now(String formatPattern) {
        try {
            return LocalDateTime.now().format(DateTimeFormatter.ofPattern(formatPattern));
        } catch (DateTimeParseException e) {
            log.error("Invalid date format pattern: {}", formatPattern);
            return LocalDateTime.now().toString();
        }
    }

    /**
     * Safe parsing that handles both String (ISO) and LocalDate inputs.
     */
    private LocalDate parse(Object obj) {
        if (obj == null) return null;
        if (obj instanceof LocalDate) return (LocalDate) obj;
        if (obj instanceof java.sql.Date) return ((java.sql.Date) obj).toLocalDate();
        if (obj instanceof java.util.Date) return new java.sql.Date(((java.util.Date) obj).getTime()).toLocalDate();

        if (obj instanceof String s) {
            try {
                // Try ISO format first
                return LocalDate.parse(s);
            } catch (DateTimeParseException e) {
                log.warn("DateUtils: Failed to parse date string '{}'", s);
                return null;
            }
        }
        return null;
    }

    public static Optional<LocalDate> toLocalDate(Object value) {
        if (value == null) {
            return Optional.empty();
        }
        if (value instanceof LocalDate localDate) {
            return Optional.of(localDate);
        }
        if (value instanceof Date sqlDate) {
            return Optional.of(sqlDate.toLocalDate());
        }
        if (value instanceof java.util.Date utilDate) {
            return Optional.of(Instant.ofEpochMilli(utilDate.getTime()).atZone(ZoneId.systemDefault()).toLocalDate());
        }
        if (value instanceof CharSequence sequence) {
            String raw = sequence.toString().trim();
            if (raw.isEmpty()) {
                return Optional.empty();
            }
            for (DateTimeFormatter formatter : STRING_DATE_FORMATTERS) {
                try {
                    return Optional.of(LocalDate.parse(raw, formatter));
                } catch (DateTimeParseException ignored) {
                    // try next formatter
                }
            }
        }
        return Optional.empty();
    }
}