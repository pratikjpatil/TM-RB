package com.tcs.fincore.ReportBuilder.batch.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.*;

/**
 * Represents a unified query pattern extracted from multiple DataSourceSpecs
 * Used for batch optimization by identifying common access patterns
 */
@Slf4j
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class QueryPattern {

    // Unique identifier for this pattern
    private String patternId;

    // Source table
    private String table;

    // Column to aggregate
    private String column;

    // Aggregation type (SUM, COUNT, AVG, etc.)
    private String aggregationType;

    // Static filters (same across all reports) - e.g., {reportDate: "2025-12-14"}
    private Map<String, Object> staticFilters;

    // Dynamic filter - the dimension that varies per report
    private DynamicFilter varyingDimension;

    // Bulk filters - high cardinality IN lists (e.g., CGL lists)
    private Map<String, Set<Object>> bulkFilters;

    // Additional filter conditions (e.g., BALANCE > 0)
    private List<ConditionalFilter> conditionalFilters;

    // Specs that match this pattern
    private Set<String> sourceSpecIds;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class DynamicFilter {
        private String columnName;       // e.g., BRANCH_CODE
        private String paramName;        // e.g., branchCode
        private Set<Object> allValues;   // All values across reports
        private String dataType; //e.g., DATE,STRING,NUMBER
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ConditionalFilter {
        private String columnName;
        private String operator;  // >, <, >=, <=, !=
        private Object value;
        private String dataType;
    }

    /**
     * Generate a logical key for grouping similar patterns
     */
    public String getLogicalKey() {
        StringBuilder sb = new StringBuilder();
        sb.append(table).append("|").append(column).append("|").append(aggregationType).append("|");

        // Add static filters (sorted for consistency)
        if (staticFilters != null && !staticFilters.isEmpty()) {
            staticFilters.entrySet().stream().sorted(Map.Entry.comparingByKey()).forEach(e -> sb.append(e.getKey()).append("=").append(e.getValue()).append(";"));
        }

        // Add conditional filters signature
        if (conditionalFilters != null && !conditionalFilters.isEmpty()) {
            conditionalFilters.stream().sorted(Comparator.comparing(ConditionalFilter::getColumnName)).forEach(cf -> sb.append(cf.getColumnName()).append(cf.getOperator()).append(cf.getValue()).append(";"));
        }

        return sb.toString();
    }

    /**
     * Check if this pattern is suitable for batch optimization
     */
    public boolean isBatchable() {
        if (varyingDimension == null || varyingDimension.getAllValues() == null) {
            return false;
        }

        // Must have bulk filters OR varying dimension with high cardinality
        boolean hasBulkFilters = bulkFilters != null && !bulkFilters.isEmpty();
        boolean hasHighCardinalityDimension = varyingDimension.getAllValues().size() > 100;

        return hasBulkFilters || hasHighCardinalityDimension;
    }

    /**
     * Estimate result set size for memory planning
     */
    public long estimateResultSetSize() {
        long dimensionSize = varyingDimension != null && varyingDimension.getAllValues() != null ? varyingDimension.getAllValues().size() : 1;

        long bulkSize = 1;
        if (bulkFilters != null && !bulkFilters.isEmpty()) {
            bulkSize = bulkFilters.values().stream().mapToLong(Set::size).max().orElse(1);
        }

        // Estimate: dimension × bulk filter size
        // Actual results will be much smaller due to sparse data
        return dimensionSize * bulkSize;
    }
}