package com.tcs.fincore.ReportBuilder.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import java.util.List;
import java.util.Map;

@Data
public class ReportTemplate {

    private TemplateMeta templateMeta;
    private ReportMeta reportMeta;
    private ReportData reportData;

    // NEW: Global Parameter Definitions
    // Defines input requirements and derived calculation logic
    private List<GlobalParamDef> globalParams;

    @Data
    public static class TemplateMeta {
        private String templateId;
        private String version;
        private String description;
        private String pageSize;
        private String pageOrientation;
    }

    @Data
    public static class ReportMeta {
        private String reportName;
        private String reportId;
        private List<Map<String, Object>> extras;
    }

    @Data
    public static class ReportData {
        private List<ColumnDefinition> columns;
        private List<Row> rows;
    }

    @Data
    public static class GlobalParamDef {
        private String name;           // e.g., "BRANCH_NAME"
        private String dataType;       // STRING, NUMBER, DATE, BOOLEAN
        private String label;

        // NEW: Mode of execution
        private ParamMode mode;        // INPUT, EXPRESSION, QUERY

        // For INPUT
        private Object defaultValue;
        private boolean required;

        // For EXPRESSION
        private String expression;     // e.g., "#{DateUtils.addDays(G.REPORT_DATE, -1)}"

        // For QUERY
        private ParamDataSource source; // DB Config
    }

    public enum ParamMode {
        INPUT,          // Provided by user or defaults
        EXPRESSION,     // Calculated using JEXL
        QUERY           // Fetched from Database
    }

    @Data
    public static class ParamDataSource {
        private String type;           // DB_VALUE, DB_SUM, etc.
        private String table;
        private String alias;
        private String column;
        private List<JoinDef> joins;
        // Filters: Map<ColumnName, List<Condition>>
        private Map<String, Object> filters;
    }

    @Data
    public static class JoinDef {
        private String type;
        private String table;
        private String alias;
        private JoinOnDef on;
    }

    @Data
    public static class JoinOnDef {
        private String leftAlias;
        private String leftColumn;
        private String rightAlias;
        private String rightColumn;
    }

    @Data
    public static class ColumnDefinition {
        private String id;
        private String name;
        private Map<String, Object> format;

        // NEW: Support for the JSON structure provided
        // "templateColumnFilters": [ { "tableName": "...", "filters": {...} } ]
        @JsonProperty("templateColumnFilters")
        private List<TemplateColumnFilter> templateColumnFilters;
    }

    /**
     * NEW: Inner class to map the specific filter structure
     */
    @Data
    public static class TemplateColumnFilter {
        private String tableName;

        // Maps Column Name -> List of Conditions (e.g., "BALANCE": [{op:">", value:"0"}])
        private Map<String, Object> filters;
    }

    @Data
    public static class Row {
        private String id;
        private String rowType;
        private List<Cell> cells;
        private Map<String, Object> dynamicConfig;
    }

    @Data
    public static class Cell {
        private String id;
        private String type;
        private Object value;
        private String expression;
        private Map<String, Object> variables;
        private Map<String, Object> source;
        private Map<String, Object> render;
    }
}
