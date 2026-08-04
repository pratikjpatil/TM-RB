package com.tcs.fincore.ReportBuilder.service.aggregation;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * DataSourceSpec describes a single DB requirement.
 */
@Data
@NoArgsConstructor
public class DataSourceSpec {

    public enum Type {
        DB_VALUE,
        DB_SUM,
        DB_COUNT,
        DB_AVG,
        DB_MIN,
        DB_MAX,
        DB_TEXT,
        DB_DATE,
        QUERY_LIST
    }

    @Data
    @NoArgsConstructor
    public static class JoinOnSpec {
        private String leftAlias;
        private String leftColumn;
        private String rightAlias;
        private String rightColumn;

        public JoinOnSpec(String leftAlias, String leftColumn, String rightAlias, String rightColumn) {
            this.leftAlias = leftAlias;
            this.leftColumn = leftColumn;
            this.rightAlias = rightAlias;
            this.rightColumn = rightColumn;
        }
    }

    @Data
    @NoArgsConstructor
    public static class JoinSpec {
        public enum JoinType { INNER, LEFT, RIGHT }

        private JoinType type;
        private String table;
        private String alias;
        private JoinOnSpec on;

        public JoinSpec(JoinType type, String table, String alias, JoinOnSpec on) {
            this.type = type;
            this.table = table;
            this.alias = alias;
            this.on = on;
        }
    }

    private String specId;               // unique id: for DB cell => cellId; inline var => cellId::var::name
    private Type type;
    private String table;
    private String alias;
    private String column;
    private List<JoinSpec> joins;
    private Map<String, Object> filters;
    private String sourceHint;           // optional debug/human hint

    public DataSourceSpec(
            String specId,
            Type type,
            String table,
            String column,
            Map<String, Object> filters,
            String sourceHint) {
        this(specId, type, table, null, column, null, filters, sourceHint);
    }

    public DataSourceSpec(
            String specId,
            Type type,
            String table,
            String alias,
            String column,
            List<JoinSpec> joins,
            Map<String, Object> filters,
            String sourceHint) {
        this.specId = specId;
        this.type = type;
        this.table = table;
        this.alias = alias;
        this.column = column;
        this.joins = joins;
        this.filters = filters;
        this.sourceHint = sourceHint;
    }

    /**
     * Key used for de-duplication: same type, table, alias, joins, column, filters => one query.
     */
    public String logicalKey() {
        StringBuilder sb = new StringBuilder();
        sb.append(type).append('|')
                .append(table).append('|')
                .append(alias == null ? "" : alias).append('|')
                .append(column).append('|')
                .append(joinIdentity()).append('|');

        if (filters != null && !filters.isEmpty()) {
            filters.entrySet().stream()
                    .sorted(Map.Entry.comparingByKey())
                    .forEach(e -> sb.append(e.getKey()).append('=').append(e.getValue()).append(';'));
        }
        return sb.toString();
    }

    private String joinIdentity() {
        if (joins == null || joins.isEmpty()) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        for (JoinSpec join : joins) {
            if (join == null) {
                continue;
            }
            sb.append(join.getType()).append(':')
                    .append(Objects.toString(join.getTable(), "")).append(':')
                    .append(Objects.toString(join.getAlias(), "")).append(':');

            JoinOnSpec on = join.getOn();
            if (on != null) {
                sb.append(Objects.toString(on.getLeftAlias(), "")).append('.')
                        .append(Objects.toString(on.getLeftColumn(), "")).append('=')
                        .append(Objects.toString(on.getRightAlias(), "")).append('.')
                        .append(Objects.toString(on.getRightColumn(), ""));
            }
            sb.append(';');
        }
        return sb.toString();
    }
}
