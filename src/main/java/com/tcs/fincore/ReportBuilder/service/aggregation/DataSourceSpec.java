package com.tcs.fincore.ReportBuilder.service.aggregation;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

/**
 * DataSourceSpec describes a single DB requirement.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class DataSourceSpec {

    public enum Type {
        DB_VALUE,
        DB_SUM,
        DB_COUNT,
        DB_AVG,
        DB_MIN,
        DB_MAX,
        DB_TEXT,
        DB_DATE
    }

    private String specId;               // unique id: for DB cell => cellId; inline var => cellId::var::name
    private Type type;
    private String table;
    private String column;
    private Map<String, Object> filters;
    private String sourceHint;           // optional debug/human hint

    /**
     * Key used for de-duplication: same type, table, column, filters => one query.
     */
    public String logicalKey() {
        StringBuilder sb = new StringBuilder();
        sb.append(type).append('|')
                .append(table).append('|')
                .append(column).append('|');
        if (filters != null && !filters.isEmpty()) {
            filters.entrySet().stream()
                    .sorted(Map.Entry.comparingByKey())
                    .forEach(e -> {
                        sb.append(e.getKey()).append('=').append(e.getValue()).append(';');
                    });
        }
        return sb.toString();
    }
}