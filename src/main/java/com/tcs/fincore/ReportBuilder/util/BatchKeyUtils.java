package com.tcs.fincore.ReportBuilder.util;

import java.util.Objects;

/**
 * PRODUCTION UTILITY: Ensures data type safety between Database (Long/Integer)
 * and HTTP Inputs (String).
 */
public class BatchKeyUtils {

    /**
     * Generates a consistent String key for batch maps.
     * Trims whitespace and handles nulls.
     */
    public static String generateKey(Object rawValue) {
        if (rawValue == null) {
            return "NULL_KEY";
        }
        return String.valueOf(rawValue).trim();
    }
}