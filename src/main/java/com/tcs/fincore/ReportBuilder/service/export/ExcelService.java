




package com.tcs.fincore.ReportBuilder.service.export;

import com.tcs.fincore.ReportBuilder.model.ExportModel;
import com.tcs.fincore.ReportBuilder.service.format.FormatUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.ss.util.CellRangeAddress;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;
import org.springframework.stereotype.Service;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.*;

@Slf4j
@Service
public class ExcelService implements ReportExporter {

    private static final String BANK_NAME = "STATE BANK OF INDIA";

    @Override
    public boolean supports(String formatType) {
        return "xlsx".equalsIgnoreCase(formatType);
    }

    @Override
    public String getMediaType() {
        return "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet";
    }

    @Override
    public String getFileExtension() {
        return ".xlsx";
    }

    @Override
    public byte[] generate(ExportModel model) {
        try (SXSSFWorkbook workbook = new SXSSFWorkbook(100);
             ByteArrayOutputStream baos = new ByteArrayOutputStream()) {

            Sheet sheet = workbook.createSheet(sanitizeSheetName(model.getTitle()));

            // Styles
            CellStyle bankHeaderStyle = createBankHeaderStyle(workbook);
            CellStyle reportTitleStyle = createReportTitleStyle(workbook);
            CellStyle headerStyle = createHeaderStyle(workbook);
            CellStyle dataStyle = workbook.createCellStyle();
            Map<String, CellStyle> numericStyleCache = new HashMap<>();

            int rowIndex = 0;

            // 1. Bank Name Header (Centered, Bold, Large)
            Row bankRow = sheet.createRow(rowIndex++);
            Cell bankCell = bankRow.createCell(0);
            bankCell.setCellValue(BANK_NAME);
            bankCell.setCellStyle(bankHeaderStyle);

            // Merge across all columns
            int lastColIndex = model.getColumns() != null ? model.getColumns().size() - 1 : 0;
            if (lastColIndex > 0) {
                sheet.addMergedRegion(new CellRangeAddress(rowIndex - 1, rowIndex - 1, 0, lastColIndex));
            }

            // 2. Report Title (Centered, below bank name)
            if (model.getTitle() != null && !model.getTitle().isBlank()) {
                Row titleRow = sheet.createRow(rowIndex++);
                Cell titleCell = titleRow.createCell(0);
                titleCell.setCellValue(model.getTitle());
                titleCell.setCellStyle(reportTitleStyle);

                if (lastColIndex > 0) {
                    sheet.addMergedRegion(new CellRangeAddress(rowIndex - 1, rowIndex - 1, 0, lastColIndex));
                }
            }

            // Spacer
            rowIndex++;

            // 3. Metadata / Extras
            if (model.getHeadingContent() != null) {
                for (Map<String, Object> meta : model.getHeadingContent()) {
                    Row row = sheet.createRow(rowIndex++);
                    row.createCell(0).setCellValue(meta.get("name").toString());
                    row.createCell(1).setCellValue(meta.get("value").toString());
                }
                rowIndex++; // spacer
            }

            // 4. Column Headers
            Row headerRow = sheet.createRow(rowIndex++);
            List<ExportModel.Column> columns = model.getColumns();
            if (columns != null) {
                for (int i = 0; i < columns.size(); i++) {
                    Cell cell = headerRow.createCell(i);
                    cell.setCellValue(columns.get(i).getName());
                    cell.setCellStyle(headerStyle);
                    sheet.setColumnWidth(i, 15 * 256);
                }
            }

            // 5. Data Rows
            if (model.getRows() != null) {
                RowSpanState rowSpanState = new RowSpanState(columns == null ? 0 : columns.size());

                for (ExportModel.Row exportRow : model.getRows()) {
                    Row row = sheet.createRow(rowIndex++);
                    renderRow(sheet, row, exportRow, columns, rowSpanState, workbook, numericStyleCache);
                }
            }

            workbook.write(baos);
            log.debug("Excel generation completed: {} bytes", baos.size());
            return baos.toByteArray();

        } catch (IOException e) {
            log.error("Excel generation failed for report: {}", model.getTitle(), e);
            throw new RuntimeException("Excel generation failed: " + e.getMessage(), e);
        }
    }

    private CellStyle createBankHeaderStyle(Workbook wb) {
        CellStyle style = wb.createCellStyle();
        Font font = wb.createFont();
        font.setBold(true);
        font.setFontHeightInPoints((short) 14);
        style.setFont(font);
        style.setAlignment(HorizontalAlignment.CENTER);
        style.setVerticalAlignment(VerticalAlignment.CENTER);
        return style;
    }

    private CellStyle createReportTitleStyle(Workbook wb) {
        CellStyle style = wb.createCellStyle();
        Font font = wb.createFont();
        font.setBold(false);
        font.setFontHeightInPoints((short) 11);
        style.setFont(font);
        style.setAlignment(HorizontalAlignment.CENTER);
        style.setVerticalAlignment(VerticalAlignment.CENTER);
        return style;
    }

    private void renderRow(Sheet sheet, Row row, ExportModel.Row exportRow,
                           List<ExportModel.Column> cols, RowSpanState rowSpanState,
                           Workbook wb, Map<String, CellStyle> styleCache) {

        List<Object> rawCells = exportRow.getCells();
        if (rawCells == null) rawCells = Collections.emptyList();

        int colCount = cols.size();
        int currentCol = 0;
        int rawIndex = 0;

        while (currentCol < colCount) {
            if (rowSpanState.isCovered(currentCol)) {
                rowSpanState.consume(currentCol);
                currentCol++;
                continue;
            }

            Object raw = rawIndex < rawCells.size() ? rawCells.get(rawIndex) : null;
            rawIndex++;

            CellSpec s = parseCellSpec(raw, cols.get(currentCol));

            int effectiveColspan = Math.min(s.colspan, colCount - currentCol);

            Cell cell = row.createCell(currentCol);
            setCellValue(cell, s.value, s.format, wb, styleCache);

            if (s.rowspan > 1 || effectiveColspan > 1) {
                int firstRow = row.getRowNum();
                int lastRow = firstRow + s.rowspan - 1;
                int firstCol = currentCol;
                int lastCol = currentCol + effectiveColspan - 1;

                try {
                    sheet.addMergedRegion(new CellRangeAddress(firstRow, lastRow, firstCol, lastCol));
                } catch (IllegalStateException e) {
                    log.warn("Failed to merge cells [{},{}]->[{},{}]: {}",
                            firstRow, firstCol, lastRow, lastCol, e.getMessage());
                }

                if (s.rowspan > 1) {
                    rowSpanState.addSpan(currentCol, effectiveColspan, s.rowspan - 1);
                }
            }

            currentCol += effectiveColspan;
        }
    }

    private void setCellValue(Cell cell, Object value, Map<String, Object> format,
                              Workbook wb, Map<String, CellStyle> styleCache) {
        if (value == null) {
            cell.setCellValue("");
            return;
        }

        String type = format == null ? "text" : String.valueOf(format.getOrDefault("type", "text"));

        if (value instanceof Number n) {
            cell.setCellValue(n.doubleValue());

            if ("currency".equalsIgnoreCase(type) || "number".equalsIgnoreCase(type)) {
                int decimals = format != null ?
                        ((Number) format.getOrDefault("decimals", 2)).intValue() : 2;
                String cacheKey = type + "_" + decimals;

                CellStyle style = styleCache.computeIfAbsent(cacheKey, k -> {
                    CellStyle s = wb.createCellStyle();
                    DataFormat df = wb.createDataFormat();
                    String pattern = "#,##0" + (decimals > 0 ? "." + "0".repeat(decimals) : "");
                    s.setDataFormat(df.getFormat(pattern));
                    return s;
                });

                cell.setCellStyle(style);
            }
        } else {
            try {
                String text = FormatUtils.format(value, format);
                cell.setCellValue(text);
            } catch (Exception e) {
                log.warn("Failed to format value '{}', using toString(): {}", value, e.getMessage());
                cell.setCellValue(String.valueOf(value));
            }
        }
    }

    @SuppressWarnings("unchecked")
    private CellSpec parseCellSpec(Object raw, ExportModel.Column columnMeta) {
        CellSpec s = new CellSpec();
        s.format = columnMeta == null ? Map.of() : columnMeta.getFormat();

        if (raw instanceof Map<?, ?> m) {
            s.value = m.get("value");
            Number cs = (Number) m.get("colspan");
            if (cs != null && cs.intValue() > 0) s.colspan = cs.intValue();
            Number rs = (Number) m.get("rowspan");
            if (rs != null && rs.intValue() > 0) s.rowspan = rs.intValue();
            if (m.containsKey("format")) s.format = (Map<String, Object>) m.get("format");
        } else {
            s.value = raw;
        }
        return s;
    }

    private CellStyle createHeaderStyle(Workbook wb) {
        CellStyle style = wb.createCellStyle();
        Font font = wb.createFont();
        font.setBold(true);
        style.setFont(font);
        style.setFillForegroundColor(IndexedColors.GREY_25_PERCENT.getIndex());
        style.setFillPattern(FillPatternType.SOLID_FOREGROUND);
        style.setAlignment(HorizontalAlignment.CENTER);
        style.setVerticalAlignment(VerticalAlignment.CENTER);
        return style;
    }

    private String sanitizeSheetName(String name) {
        if (name == null || name.isBlank()) return "Report";
        return name.replaceAll("[\\\\/*?:\\[\\]]", "").substring(0, Math.min(name.length(), 31));
    }

    private static class CellSpec {
        Object value;
        int colspan = 1;
        int rowspan = 1;
        Map<String, Object> format;
    }

    private static class RowSpanState {
        private final int[] remaining;

        RowSpanState(int colCount) {
            this.remaining = new int[colCount];
        }

        boolean isCovered(int colIndex) {
            return colIndex < remaining.length && remaining[colIndex] > 0;
        }

        void consume(int colIndex) {
            if (colIndex < remaining.length && remaining[colIndex] > 0) {
                remaining[colIndex]--;
            }
        }

        void addSpan(int startCol, int colspan, int rows) {
            for (int i = 0; i < colspan && (startCol + i) < remaining.length; i++) {
                remaining[startCol + i] = Math.max(remaining[startCol + i], rows);
            }
        }
    }
}