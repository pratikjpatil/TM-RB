

package com.tcs.fincore.ReportBuilder.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;

/**
 * ExportModel - rendered/presentation model for renderers.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ExportModel {

    private String title;
    private List<Map<String, Object>> headingContent;
    private List<Column> columns;
    private List<Row> rows;

    // Page configuration
    private String pageSize;        // "A4", "LETTER" etc. for pdf
    private String pageOrientation; // "portrait" or "landscape" for pdf
    private String logoPath;        // Path to logo image file for PDF/Excel header

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Column {
        private String id;
        private String name;
        private Map<String, Object> format;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Row {
        private String rowType;
        private List<Object> cells;
    }
}