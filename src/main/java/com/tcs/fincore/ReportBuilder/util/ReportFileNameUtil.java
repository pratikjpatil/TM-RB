package com.tcs.fincore.ReportBuilder.util;

import com.tcs.fincore.ReportBuilder.model.ReportTemplate;
import com.tcs.fincore.ReportBuilder.model.variant.ReportVariant;
import com.tcs.fincore.ReportBuilder.model.variant.VariantParamDef;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.*;

public final class ReportFileNameUtil {

    private static final DateTimeFormatter DATE_FMT =
            DateTimeFormatter.ofPattern("ddMMyyyy");

    private ReportFileNameUtil() {
    }

    public static String buildFileName(
            ReportTemplate template,
            ReportVariant variant,
            Map<String, Object> params
    ) {

        String reportId = safe(template.getReportMeta().getReportId());
        String variantCode = variant == null ? "" : safe(variant.getVariantCode());

        List<String> paramParts = new ArrayList<>();

        if (variant != null && variant.getParams() != null && params != null) {
            for (VariantParamDef def : variant.getParams()) {

                Object value = params.get(def.getParamName());
                if (value == null) continue;

                String formatted = formatValue(value, def.getParamType());
                if (!formatted.isBlank()) {
                    paramParts.add(formatted);
                }
            }
        }

        String paramSegment = paramParts.isEmpty()
                ? ""
                : "_" + String.join("_", paramParts);

//        return reportId + "_" + variantCode + paramSegment;
        return reportId  + paramSegment;
    }

    // ---------------- HELPERS ----------------

    private static String formatValue(Object value, String type) {
        if (value == null) return "";

        String t = type == null ? "STRING" : type.toUpperCase(Locale.ROOT);

        return switch (t) {
            case "DATE" -> {
                LocalDate d = parseDate(value);
                yield d == null ? "" : d.format(DATE_FMT);
            }
            case "LIST" -> formatList(value);
            default -> sanitize(value);
        };
    }

    private static String formatList(Object value) {
        if (value instanceof Collection<?> c) {
            return c.stream()
                    .map(ReportFileNameUtil::sanitize)
                    .filter(s -> !s.isBlank())
                    .reduce((a, b) -> a + "-" + b)
                    .orElse("");
        }
        return sanitize(value);
    }

    private static LocalDate parseDate(Object v) {
        try {
            if (v instanceof LocalDate ld) return ld;
            if (v instanceof java.util.Date d)
                return new java.sql.Date(d.getTime()).toLocalDate();
            if (v instanceof String s)
                return LocalDate.parse(s); // ISO yyyy-MM-dd
        } catch (DateTimeParseException ignored) {

        }
        return null;
    }

    private static String sanitize(Object v) {
        return String.valueOf(v)
                .trim()
                .replaceAll("\\s+", "")
                .replaceAll("[^a-zA-Z0-9_-]", "");
    }

    private static String safe(String s) {
        return s == null ? "" : sanitize(s);
    }
}