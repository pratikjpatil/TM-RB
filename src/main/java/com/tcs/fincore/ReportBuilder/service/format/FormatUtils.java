package com.tcs.fincore.ReportBuilder.service.format;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.Map;

import lombok.extern.slf4j.Slf4j;

/**
 * FormatUtils - formats raw values into strings according to column/cell metadata.
 */
@Slf4j
public class FormatUtils {

    public static String format(Object raw, Map<String, Object> format) {
        if (raw == null) return "";

        String s = String.valueOf(raw);
        if (s.equalsIgnoreCase("null")) return "";


        String type = format == null ? "text" : String.valueOf(format.getOrDefault("type", "text"));


        return switch (type.toLowerCase(Locale.ROOT)) {
            case "currency" -> formatCurrency(raw, format);
            case "number" -> formatNumber(raw, format);
            case "date" -> formatDate(raw, format);
            default -> String.valueOf(raw);
        };
    }




private static String formatCurrency(Object raw, Map<String, Object> format) {
    String rawString = String.valueOf(raw);
    if (rawString == null || rawString.isEmpty() || "null".equals(rawString)) {
        return "";
    }

    double val;
    try {
        val = toDoubleSafe(raw);
    } catch (Exception e) {
        return "";
    }

    // Handle NaN and Infinity
    if (Double.isNaN(val) || Double.isInfinite(val)) {
        return "";
    }

    boolean isShowAbsoluteValue = (boolean) format.getOrDefault("showAbsoluteValue", false);

    // Convert to absolute value if user wants
    if (val < 0 && isShowAbsoluteValue) {
        val = Math.abs(val);
    }

    int decimals = ((Number) format.getOrDefault("decimals", 2)).intValue();
    // Clamp decimals to reasonable range
    decimals = Math.max(0, Math.min(decimals, 10));

//    boolean useIndianFormat = (boolean) format.getOrDefault("indianNumbering", true);
    boolean thousand = (boolean) format.getOrDefault("thousandSeparator", true);
    String symbol = String.valueOf(format.getOrDefault("currencySymbol", ""));
    String pos = String.valueOf(format.getOrDefault("currencySymbolPosition", "prefix"));

    String formattedNumber;

    if (thousand) {
        formattedNumber = formatIndianNumbering(val, decimals);
    } else {
//        boolean thousand = (boolean) format.getOrDefault("thousandSeparator", true);
        String pattern = thousand ? "#,##0" : "0";
        if (decimals > 0) {
            pattern += "." + "0".repeat(decimals);
        }
        DecimalFormat df = new DecimalFormat(pattern, DecimalFormatSymbols.getInstance(Locale.US));
        formattedNumber = df.format(val);
    }

    // Apply currency symbol
    if (symbol != null && !symbol.isEmpty() && !"null".equals(symbol)) {
        return "prefix".equalsIgnoreCase(pos) ? symbol + formattedNumber : formattedNumber + symbol;
    }

    return formattedNumber;
}

    private static String formatIndianNumbering(double value, int decimals) {
        // Split into integer and decimal parts
        boolean isNegative = value < 0;
        value = Math.abs(value);

        // Use BigDecimal for precise decimal handling
        BigDecimal bd = BigDecimal.valueOf(value);
        bd = bd.setScale(decimals, RoundingMode.HALF_UP);

        String formatted = bd.toPlainString();
        String[] parts = formatted.split("\\.");
        String integerPart = parts[0];
        String decimalPart = parts.length > 1 ? parts[1] : "";

        // Apply Indian numbering (COBOL style) to integer part
        String formattedInteger = applyIndianGrouping(integerPart);

        // Add negative sign if needed
        if (isNegative) {
            formattedInteger = "-" + formattedInteger;
        }

        // Add decimal part if exists
        if (decimals > 0 && !decimalPart.isEmpty()) {
            return formattedInteger + "." + decimalPart;
        }

        return formattedInteger;
    }

    private static String applyIndianGrouping(String integerPart) {
        int len = integerPart.length();

        // No grouping needed for numbers with 3 or fewer digits
        if (len <= 3) {
            return integerPart;
        }

        StringBuilder result = new StringBuilder();
        int position = len;

        // Last 3 digits (hundreds place)
        result.insert(0, integerPart.substring(position - 3, position));
        position -= 3;

        // Next 2 digits (thousands - if exists)
        if (position > 0) {
            int chunkSize = Math.min(2, position);
            result.insert(0, ",");
            result.insert(0, integerPart.substring(position - chunkSize, position));
            position -= chunkSize;
        }

        // Next 2 digits (lakhs - if exists)
        if (position > 0) {
            int chunkSize = Math.min(2, position);
            result.insert(0, ",");
            result.insert(0, integerPart.substring(position - chunkSize, position));
            position -= chunkSize;
        }

        // Remaining digits (crores - no internal separators)
        if (position > 0) {
            result.insert(0, ",");
            result.insert(0, integerPart.substring(0, position));
        }

        return result.toString();
    }

    private static String formatNumber(Object raw, Map<String, Object> format) {
        double val = toDoubleSafe(raw);
        int decimals = ((Number) format.getOrDefault("decimals", 2)).intValue();
        boolean thousand = (boolean) format.getOrDefault("thousandSeparator", true);

        String pattern = thousand ? "#,##0" : "0";
        if (decimals > 0) pattern += "." + "0".repeat(decimals);
        DecimalFormat df = new DecimalFormat(pattern);
        return df.format(val);
    }

    private static String formatDate(Object raw, Map<String, Object> format) {
        String outFmt = String.valueOf(format.getOrDefault("outputFormat", "dd-MMM-yyyy"));
        if (raw instanceof java.util.Date d) {
            LocalDate ld = new java.sql.Date(d.getTime()).toLocalDate();
            return ld.format(DateTimeFormatter.ofPattern(outFmt, Locale.ENGLISH));
        }
        String s = String.valueOf(raw);
        try {
            LocalDate ld = LocalDate.parse(s);
            return ld.format(DateTimeFormatter.ofPattern(outFmt, Locale.ENGLISH));
        } catch (Exception ex) {
            return s;
        }
    }

    private static double toDoubleSafe(Object o) {
        if (o instanceof Number n) return n.doubleValue();
        try {
            return Double.parseDouble(String.valueOf(o).replaceAll(",", ""));
        } catch (Exception ex) {
            return 0.0;
        }
    }
}