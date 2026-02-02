package com.tcs.fincore.ReportBuilder.util;

import lombok.extern.slf4j.Slf4j;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;

/**
 * PRODUCTION-GRADE: Helper class for JEXL expressions to perform date math.
 * Exposed in JEXL context as "DateUtils".
 */
@Slf4j
public class DateUtilsHelper {

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
        log.info("inside addMonths :: {} :: {} :: {}", date, months, date.plusMonths(months));

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
            } catch (Exception e) {
                log.warn("DateUtils: Failed to parse date string '{}'", s);
                return null;
            }
        }
        return null;
    }
}