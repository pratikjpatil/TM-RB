


package com.tcs.fincore.ReportBuilder.service.template;

import com.tcs.fincore.ReportBuilder.exception.ValidationException;
import com.tcs.fincore.ReportBuilder.model.ExportModel;
import com.tcs.fincore.ReportBuilder.model.ReportTemplate;
import com.tcs.fincore.ReportBuilder.service.aggregation.DataSourceSpec;
import com.tcs.fincore.ReportBuilder.service.aggregation.DataSourceSpec.Type;
import com.tcs.fincore.ReportBuilder.util.FilterSqlBuilder;
import com.tcs.fincore.ReportBuilder.util.GlobalParamResolver;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.*;

@Slf4j
@Service
public class TemplateProcessorServiceImpl implements TemplateProcessorService {

    @Value("${report.batch.max-specs-per-template:10000}")
    private int maxDynamicRowLimit;

    private final NamedParameterJdbcTemplate jdbc;
    private final GlobalParamResolver globalParamResolver;
    private static final Set<String> VALID_ORDER_DIRECTIONS = Set.of("ASC", "DESC");

    @Value("${report.logo.path:/path/to/sbi-logo.png}")
    private String defaultLogoPath;

    public TemplateProcessorServiceImpl(NamedParameterJdbcTemplate jdbc, GlobalParamResolver globalParamResolver) {
        this.jdbc = jdbc;
        this.globalParamResolver = globalParamResolver;
    }

    @Override
    public void validate(ReportTemplate t) {
        if (t == null) {
            throw new ValidationException("Template is null");
        }

        if (t.getReportMeta() == null || t.getReportMeta().getReportName() == null ||
                t.getReportMeta().getReportName().isBlank()) {
            throw new ValidationException("reportName is required");
        }

        if (t.getReportData() == null) {
            throw new ValidationException("reportData is required");
        }

        if (t.getReportData().getColumns() == null || t.getReportData().getColumns().isEmpty()) {
            throw new ValidationException("At least one column definition is required");
        }

        if (t.getReportData().getRows() == null) {
            t.getReportData().setRows(new ArrayList<>());
        }

        Set<String> colIds = new HashSet<>();
        for (ReportTemplate.ColumnDefinition c : t.getReportData().getColumns()) {
            if (c.getId() == null || c.getId().isBlank()) {
                throw new ValidationException("Column id cannot be null/blank");
            }
            if (!colIds.add(c.getId())) {
                throw new ValidationException("Duplicate column id: " + c.getId());
            }
        }

        log.debug("Template validation passed: {} with {} columns, {} rows",
                t.getReportMeta().getReportName(),
                t.getReportData().getColumns().size(),
                t.getReportData().getRows().size());
    }

    @Override
    public void assignIds(ReportTemplate t) {
        int rowNum = 0;

        for (ReportTemplate.Row row : t.getReportData().getRows()) {
            if (row.getCells() == null) {
                continue;
            }

            if (row.getId() == null || row.getId().isBlank()) {
                throw new ValidationException("Row id cannot be null/blank at row index: " + rowNum);
            }

            int colNum = 0;
            for (ReportTemplate.Cell cell : row.getCells()) {
                if (cell.getId() == null || cell.getId().isBlank()) {
                    if (colNum >= t.getReportData().getColumns().size()) {
                        throw new ValidationException(
                                "Cell at row " + rowNum + " col " + colNum + " exceeds column definitions");
                    }

                    String colId = t.getReportData().getColumns().get(colNum).getId();
                    cell.setId("cell_" + row.getId() + "_" + colId);
                }
                colNum++;
            }
            rowNum++;
        }

        log.debug("Assigned IDs to {} rows", rowNum);
    }

    @Override
    public List<DataSourceSpec> extractDbSpecs(ReportTemplate t) {
        List<DataSourceSpec> out = new ArrayList<>();

        if (t.getReportData().getRows() == null) {
            return out;
        }

        for (ReportTemplate.Row row : t.getReportData().getRows()) {
            if (row.getCells() == null) continue;

            for (ReportTemplate.Cell cell : row.getCells()) {
                String type = cell.getType() == null ? "TEXT" : cell.getType().toUpperCase(Locale.ROOT);

                if (type.startsWith("DB_")) {
                    try {
                        Map<String, Object> src = cell.getSource();
                        if (src == null) {
                            throw new ValidationException(
                                    "DB_* cell missing 'source' field: " + cell.getId());
                        }

                        String table = (String) src.get("table");
                        String column = (String) src.get("column");

                        if (table == null || table.isBlank()) {
                            throw new ValidationException(
                                    "DB cell " + cell.getId() + " missing table name");
                        }

                        if (column == null || column.isBlank()) {
                            throw new ValidationException(
                                    "DB cell " + cell.getId() + " missing column name");
                        }

                        out.add(new DataSourceSpec(
                                cell.getId(),
                                Type.valueOf(type),
                                table,
                                column,
                                safeMap(src.get("filters")),
                                "cell:" + cell.getId()
                        ));

                    } catch (IllegalArgumentException e) {
                        throw new ValidationException(
                                "Invalid DB type '" + type + "' for cell " + cell.getId());
                    }
                }

                if ("FORMULA".equalsIgnoreCase(type) && cell.getVariables() != null) {
                    for (Map.Entry<String, Object> var : cell.getVariables().entrySet()) {
                        if (var.getValue() instanceof Map<?, ?>) {
                            try {
                                Map<String, Object> map = safeMap(var.getValue());
                                String vType = String.valueOf(map.getOrDefault("type", ""))
                                        .toUpperCase(Locale.ROOT);

                                if (vType.startsWith("DB_")) {
                                    String table = (String) map.get("table");
                                    String column = (String) map.get("column");

                                    if (table == null || table.isBlank()) {
                                        throw new ValidationException(
                                                "Inline DB variable '" + var.getKey() +
                                                        "' in cell " + cell.getId() + " missing table");
                                    }

                                    if (column == null || column.isBlank()) {
                                        throw new ValidationException(
                                                "Inline DB variable '" + var.getKey() +
                                                        "' in cell " + cell.getId() + " missing column");
                                    }

                                    out.add(new DataSourceSpec(
                                            cell.getId() + "::var::" + var.getKey(),
                                            Type.valueOf(vType),
                                            table,
                                            column,
                                            safeMap(map.get("filters")),
                                            "inline:" + var.getKey()
                                    ));
                                }
                            } catch (IllegalArgumentException e) {
                                log.warn("Invalid DB type in inline variable '{}' of cell {}",
                                        var.getKey(), cell.getId());
                            }
                        }
                    }
                }
            }
        }

        log.debug("Extracted {} DB specs from template", out.size());
        return out;
    }

    @Override
    public void expandDynamicRows(ReportTemplate template) {
        if (template.getReportData().getRows() == null) {
            return;
        }

        List<ReportTemplate.Row> newRows = new ArrayList<>();
        int dynamicRowsExpanded = 0;

        for (ReportTemplate.Row row : template.getReportData().getRows()) {
            if (!"DYNAMIC".equalsIgnoreCase(row.getRowType())) {
                newRows.add(row);
                continue;
            }

            Map<String, Object> cfg = row.getDynamicConfig();
            if (cfg == null) {
                throw new ValidationException("DYNAMIC row requires dynamicConfig");
            }

            String type = String.valueOf(cfg.get("type")).toUpperCase(Locale.ROOT);

            if (!"DB_LIST".equals(type)) {
                throw new ValidationException("Unsupported dynamic type: " + type);
            }

            String table = (String) cfg.get("table");

            @SuppressWarnings("unchecked")
            List<String> selectFields = (List<String>) cfg.get("select");

            if (table == null || table.isBlank()) {
                throw new ValidationException("DYNAMIC.DB_LIST missing table name");
            }

            if (selectFields == null || selectFields.isEmpty()) {
                throw new ValidationException("DYNAMIC.DB_LIST missing select[] fields");
            }

            Map<String, Object> filters = safeMap(cfg.get("filters"));
            String orderBy = cfg.containsKey("orderby") ? (String) cfg.get("orderby") : null;
            int limit = cfg.containsKey("limit") ? ((Number) cfg.get("limit")).intValue() : 300;

            if (limit < 1 || limit > maxDynamicRowLimit) {
                throw new ValidationException(
                        "DYNAMIC row limit must be between 1 and " + maxDynamicRowLimit + ", got: " + limit);
            }

            try {
                List<Map<String, Object>> dbRows = fetchDynamic(table, selectFields, filters, orderBy, limit);

                int rowCount = 1;
                for (Map<String, Object> dbRow : dbRows) {
                    ReportTemplate.Row r = new ReportTemplate.Row();
                    r.setRowType("DATA");
                    r.setId("DynamicRow" + rowCount);
                    rowCount++;

                    List<ReportTemplate.Cell> cells = new ArrayList<>();
                    for (String field : selectFields) {
                        ReportTemplate.Cell c = new ReportTemplate.Cell();
                        c.setType("TEXT");
                        c.setValue(dbRow.get(field));
                        cells.add(c);
                    }

                    r.setCells(cells);
                    newRows.add(r);
                }

                dynamicRowsExpanded += dbRows.size();
                log.info("Expanded dynamic row -> {} new data rows from table {}", dbRows.size(), table);

            } catch (RuntimeException e) {
                log.error("Failed to expand dynamic row from table {}: {}", table, e.getMessage());
                throw new ValidationException("Dynamic row expansion failed: " + e.getMessage(), e);
            }
        }

        template.getReportData().setRows(newRows);

        if (dynamicRowsExpanded > 0) {
            log.info("Total dynamic rows expanded: {}", dynamicRowsExpanded);
        }
    }

    private List<Map<String, Object>> fetchDynamic(
            String table,
            List<String> fields,
            Map<String, Object> filters,
            String orderBy,
            int limit) {

        FilterSqlBuilder.validateIdentifier(table);
        fields.forEach(FilterSqlBuilder::validateIdentifier);

        StringBuilder sql = new StringBuilder("SELECT ");
        sql.append(String.join(",", fields));
        sql.append(" FROM ").append(table);
        sql.append(" WHERE 1=1");

        Map<String, Object> params = new HashMap<>();

        FilterSqlBuilder.appendFilters(sql, params, filters);

        if (orderBy != null && !orderBy.isBlank()) {
            validateOrderByClause(orderBy, fields);
            sql.append(" ORDER BY ").append(orderBy);
        }

        sql.append(" FETCH FIRST ").append(limit).append(" ROWS ONLY");

        log.debug("Executing dynamic row SQL: {}", sql);

        try {
            return jdbc.queryForList(sql.toString(), params);
        } catch (RuntimeException e) {
            log.error("Dynamic row query failed: {}", e.getMessage());
            throw new ValidationException("Failed to fetch dynamic rows: " + e.getMessage(), e);
        }
    }

    private void validateOrderByClause(String orderBy, List<String> allowedColumns) {
        if (orderBy == null || orderBy.isBlank()) {
            return;
        }

        String[] parts = orderBy.split(",");

        for (String part : parts) {
            String trimmed = part.trim();
            String[] tokens = trimmed.split("\\s+");

            if (tokens.length == 0 || tokens.length > 2) {
                throw new ValidationException(
                        "Invalid ORDER BY clause: '" + trimmed + "'. Expected: COLUMN [ASC|DESC]");
            }

            String column = tokens[0];
            String direction = tokens.length == 2 ? tokens[1].toUpperCase() : "ASC";

            if (!allowedColumns.contains(column)) {
                throw new ValidationException(
                        "ORDER BY column '" + column + "' not in SELECT fields: " + allowedColumns);
            }

            if (!VALID_ORDER_DIRECTIONS.contains(direction)) {
                throw new ValidationException(
                        "Invalid ORDER BY direction: '" + direction + "'. Must be ASC or DESC");
            }

            if (column.contains(";") || column.contains("--") || column.contains("/*") ||
                    column.contains("*/") || column.contains("'") || column.contains("\"")) {
                throw new ValidationException(
                        "Invalid characters in ORDER BY column: " + column);
            }
        }
    }

    /**
     * Extracts DB Specs from Global Parameters (Mode = QUERY).
     * Applies static context (e.g., resolved dates) to filters before extraction.
     */
    @Override
    public List<DataSourceSpec> extractGlobalParamSpecs(ReportTemplate t, Map<String, Object> staticContext) {
        List<DataSourceSpec> out = new ArrayList<>();
        if (t.getGlobalParams() == null) return out;

        for (ReportTemplate.GlobalParamDef def : t.getGlobalParams()) {
            if (def.getMode() != ReportTemplate.ParamMode.QUERY || def.getSource() == null) {
                continue;
            }

            try {
                // 1. Resolve Static Dependencies in Filters (e.g. $G{REPORT_DATE} -> '2025-01-01')
                // We do NOT fail on missing deps here because the Varying Dimension (e.g. $G{BRANCH_CODE})
                // will be missing (or set to placeholder) in staticContext.
                Map<String, Object> resolvedSource = (Map<String, Object>)
                        globalParamResolver.resolveRecursive(def.getSource(), staticContext);

                // 2. Extract Fields
                String table = (String) resolvedSource.get("table");
                String column = (String) resolvedSource.get("column");
                String typeStr = (String) resolvedSource.get("type");

                if (table == null || column == null || typeStr == null) {
                    log.warn("Skipping invalid Global Param Query def: {}", def.getName());
                    continue;
                }

                // 3. Build Spec
                // SpecID format: "GLOBAL::{ParamName}"
                out.add(new DataSourceSpec(
                        "GLOBAL::" + def.getName(),
                        DataSourceSpec.Type.valueOf(typeStr.toUpperCase()),
                        table,
                        column,
                        safeMap(resolvedSource.get("filters")),
                        "GlobalParam:" + def.getName()
                ));

            } catch (RuntimeException e) {
                log.error("Failed to extract spec for Global Param '{}': {}", def.getName(), e.getMessage());
                // Don't crash entire batch analysis for one bad param, but log it.
            }
        }
        return out;
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> safeMap(Object o) {
        if (o instanceof Map<?, ?> m) {
            Map<String, Object> nm = new LinkedHashMap<>();
            m.forEach((k, v) -> nm.put(String.valueOf(k), v));
            return nm;
        }
        return Map.of();
    }

    @Override
    public ExportModel toExportModel(ReportTemplate t, Map<String, Object> computed) {
        List<ExportModel.Column> exportCols = new ArrayList<>();
        for (ReportTemplate.ColumnDefinition c : t.getReportData().getColumns()) {
            exportCols.add(new ExportModel.Column(c.getId(), c.getName(), c.getFormat()));
        }

        List<ExportModel.Row> exportRows = new ArrayList<>();

        for (ReportTemplate.Row row : t.getReportData().getRows()) {
            List<Object> cellValues = new ArrayList<>();

            if (row.getCells() != null) {
                for (ReportTemplate.Cell cell : row.getCells()) {
                    if ("TEXT".equalsIgnoreCase(cell.getType())) {
                        if (cell.getRender() != null && !cell.getRender().isEmpty()) {
                            Map<String, Object> m = new LinkedHashMap<>();
                            m.put("value", cell.getValue());
                            m.putAll(cell.getRender());
                            cellValues.add(m);
                        } else {
                            cellValues.add(cell.getValue());
                        }
                        continue;
                    }

                    Object v = computed.get(cell.getId());
                    Map<String, Object> wrapped = new LinkedHashMap<>();
                    wrapped.put("value", v);

                    if (cell.getRender() != null) {
                        wrapped.putAll(cell.getRender());
                    }
                    cellValues.add(wrapped);
                }
            }

            exportRows.add(new ExportModel.Row(row.getRowType(), cellValues));
        }

        // Get logo path from template metadata or use default
        String logoPath = defaultLogoPath;
        if (t.getTemplateMeta() != null /*&& t.getTemplateMeta().getHeaderLayout() != null*/) {
            // You can extend TemplateMeta to include logoPath if needed
            // For now using default configured path
        }

        return new ExportModel(
                t.getReportMeta().getReportName(),
                t.getReportMeta().getExtras(),
                exportCols,
                exportRows,
                t.getTemplateMeta().getPageSize(),
                t.getTemplateMeta().getPageOrientation(),
                logoPath  // Pass logo path to ExportModel
        );
    }
}