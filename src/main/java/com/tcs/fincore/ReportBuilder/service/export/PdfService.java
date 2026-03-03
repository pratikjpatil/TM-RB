package com.tcs.fincore.ReportBuilder.service.export;

import org.openpdf.text.*;
import org.openpdf.text.pdf.*;
import com.tcs.fincore.ReportBuilder.model.ExportModel;
import com.tcs.fincore.ReportBuilder.service.format.FormatUtils;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.awt.Color;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.*;
import java.util.List;

/**
 * Production-grade PDF Exporter using OpenPDF.
 * Features:
 * - Dynamic Header Height Calculation (prevents body overlap).
 * - Repeating Header on every page (Logo, Title, Metadata).
 * - Smart Metadata Grid (4-column layout with auto-wrapping for long values).
 * - Row/Colspan support for data tables.
 */
@Slf4j
@Service
public class PdfService implements ReportExporter {

    private static final String FONT_NAME = FontFactory.HELVETICA;
    private static final Color HEADER_BG_COLOR = new Color(220, 220, 220);
    private static final String BANK_NAME = "State Bank of India";

    // Threshold for switching a metadata value from shared row to full row
    private static final int META_CHAR_THRESHOLD = 35;

    // 1. Inject the path from properties (Default to classpath)
    @Value("${report.logo.path:classpath:images/sbi-logo.png}")
    private String configuredLogoPath;

    // 2. Cache the actual Image object, not just the path
    private Image cachedLogoImage;

    @PostConstruct
    public void init() {
        // Load the image ONCE when the application starts
        this.cachedLogoImage = loadLogo(configuredLogoPath);
    }

    private Image loadLogo(String path) {
        if (path == null || path.isBlank()) return null;
        try {
            Image img;
            if (path.startsWith("classpath:")) {
                String resourceName = path.substring("classpath:".length());
                java.net.URL url = getClass().getClassLoader().getResource(resourceName);
                if (url == null) {
                    log.error("Logo resource not found: {}", resourceName);
                    return null;
                }
                img = Image.getInstance(url);
            } else {
                // Fallback for external files
                img = Image.getInstance(path);
            }
            // Scale it once globally
            img.scaleAbsolute(50f, 17f);
            log.info("Logo loaded successfully from: {}", path);
            return img;
        } catch (RuntimeException e) {
            log.error("Failed to load logo from {}: {}", path, e.getMessage());
            return null;
        }
    }

    @Override
    public boolean supports(String formatType) {
        return "pdf".equalsIgnoreCase(formatType);
    }

    @Override
    public String getMediaType() {
        return "application/pdf";
    }

    @Override
    public String getFileExtension() {
        return ".pdf";
    }

    @Override
    public byte[] generate(ExportModel model) {
        Objects.requireNonNull(model, "ExportModel cannot be null");
        log.debug("PDF generate start: title='{}'", model.getTitle());

        try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {

            // 1. Resolve Page Size
            Rectangle pageSize = resolvePageSize(model.getPageSize(), model.getPageOrientation());

            // 2. Calculate Dynamic Top Margin
            float dynamicTopMargin = calculateDynamicHeaderMargin(model.getHeadingContent());

            // 3. Create Document with Dynamic Margin
            Document document = new Document(pageSize, 18f, 12f, dynamicTopMargin, 40f);

            PdfWriter writer = PdfWriter.getInstance(document, baos);

            Image logoForThisReport = (this.cachedLogoImage != null) ?
                    Image.getInstance(this.cachedLogoImage) : null;

            // 4. Register Page Event
            writer.setPageEvent(new HeaderFooterPageEvent(
                    logoForThisReport,
                    model.getTitle(),
                    model.getHeadingContent()
            ));

            document.open();

            // 5. Validate Columns
            int colCount = model.getColumns() == null ? 0 : model.getColumns().size();
            if (colCount == 0) {
                document.add(new Paragraph("No columns defined", FontFactory.getFont(FONT_NAME, 10)));
                document.close();
                return baos.toByteArray();
            }

            // 6. Initialize Main Data Table
            float[] colWidths = computeColumnWidths(model.getColumns());
            PdfPTable table = new PdfPTable(colWidths);
            table.setWidthPercentage(100);
            table.setHeaderRows(1);

            // 7. Render Table Headers
            Font headerFont = FontFactory.getFont(FONT_NAME, 10f, Font.BOLD, Color.BLACK);
            for (ExportModel.Column c : model.getColumns()) {
                PdfPCell cell = new PdfPCell(new Phrase(c.getName() == null ? "" : c.getName(), headerFont));
                cell.setBackgroundColor(HEADER_BG_COLOR);
                cell.setHorizontalAlignment(Element.ALIGN_CENTER);
                cell.setVerticalAlignment(Element.ALIGN_MIDDLE);
                cell.setPadding(7f);
                table.addCell(cell);
            }

            // 8. Render Data Rows
            if (model.getRows() != null) {
                RowSpanState rowSpanState = new RowSpanState(colCount);
                Font bodyFont = FontFactory.getFont(FONT_NAME, 9f, Font.NORMAL, Color.BLACK);
                Font bodyBoldFont = FontFactory.getFont(FONT_NAME, 9f, Font.BOLD, Color.BLACK);

                for (ExportModel.Row row : model.getRows()) {
                    renderRow(table, row, model.getColumns(), bodyFont, bodyBoldFont, rowSpanState);
                }
            }

            document.add(table);
            document.close();

            return baos.toByteArray();

        } catch (DocumentException | IOException ex) {
            log.error("PDF generation failed", ex);
            throw new RuntimeException("PDF generation failed: " + ex.getMessage(), ex);
        }
    }

    private float calculateDynamicHeaderMargin(List<Map<String, Object>> metadata) {
        float totalHeight = 80f;

        if (metadata != null && !metadata.isEmpty()) {
            totalHeight += 11f;

            int rowCount = 0;
            int itemsInCurrentRow = 0;

            for (Map<String, Object> m : metadata) {
                String value = String.valueOf(m.get("value"));
                boolean isLongValue = value.length() > META_CHAR_THRESHOLD;

                if (isLongValue) {
                    if (itemsInCurrentRow > 0) {
                        rowCount++;
                        itemsInCurrentRow = 0;
                    }
                    rowCount++;
                } else {
                    itemsInCurrentRow++;
                    if (itemsInCurrentRow == 2) {
                        rowCount++;
                        itemsInCurrentRow = 0;
                    }
                }
            }
            if (itemsInCurrentRow > 0) {
                rowCount++;
            }

            float heightPerRow = 18f;
            totalHeight += (rowCount * heightPerRow);
        }

        return totalHeight;
    }

    private static class HeaderFooterPageEvent extends PdfPageEventHelper {
        private final Image logoImage;
        private final String reportTitle;
        private final List<Map<String, Object>> headingContent;

        public HeaderFooterPageEvent(Image logoImage, String reportTitle, List<Map<String, Object>> headingContent) {
            this.logoImage = logoImage;
            this.reportTitle = reportTitle;
            this.headingContent = headingContent;
        }

        @Override
        public void onEndPage(PdfWriter writer, Document document) {
            Rectangle pageSize = document.getPageSize();

            PdfPTable headerTable = new PdfPTable(1);
            headerTable.setTotalWidth(pageSize.getWidth() - document.leftMargin() - document.rightMargin());

            try {
                // --- 1. Logo Section ---
                if (logoImage != null) {
                    PdfPCell logoCell = new PdfPCell();
                    Chunk logoChunk = new Chunk(logoImage, 0, 0, true);
                    Paragraph logoPara = new Paragraph();
                    logoPara.setAlignment(Element.ALIGN_CENTER);
                    logoPara.add(logoChunk);

                    logoCell.addElement(logoPara);
                    logoCell.setBorder(Rectangle.NO_BORDER);
                    logoCell.setPaddingBottom(1f);
                    headerTable.addCell(logoCell);
                }

                // --- 2. Bank Name ---
                Font bankFont = FontFactory.getFont(FONT_NAME, 14f, Font.BOLD, Color.BLACK);
                PdfPCell bankCell = new PdfPCell(new Phrase(BANK_NAME, bankFont));
                bankCell.setHorizontalAlignment(Element.ALIGN_CENTER);
                bankCell.setBorder(Rectangle.NO_BORDER);
                bankCell.setPaddingBottom(5f);
                headerTable.addCell(bankCell);

                // --- 3. Report Title ---
                if (reportTitle != null && !reportTitle.isBlank()) {
                    Font titleFont = FontFactory.getFont(FONT_NAME, 11f, Font.NORMAL, Color.BLACK);
                    PdfPCell titleCell = new PdfPCell(new Phrase(reportTitle, titleFont));
                    titleCell.setHorizontalAlignment(Element.ALIGN_CENTER);
                    titleCell.setBorder(Rectangle.NO_BORDER);
                    titleCell.setPaddingBottom(6f);
                    headerTable.addCell(titleCell);
                }

                // --- 4. Metadata Grid ---
                if (headingContent != null && !headingContent.isEmpty()) {

                    float[] colWidths = new float[]{1.5f, 3.5f, 1.5f, 3.5f};
                    PdfPTable metaTable = new PdfPTable(colWidths);
                    metaTable.setWidthPercentage(100);

                    Font metaLabelFont = FontFactory.getFont(FONT_NAME, 9, Font.BOLD, Color.DARK_GRAY);
                    Font metaValueFont = FontFactory.getFont(FONT_NAME, 9, Font.NORMAL, Color.BLACK);

                    int itemsInCurrentRow = 0;

                    for (Map<String, Object> m : headingContent) {
                        String name = String.valueOf(m.get("name"));
                        String value = String.valueOf(m.get("value"));

                        boolean isLongValue = value.length() > META_CHAR_THRESHOLD;

                        if (isLongValue) {
                            if (itemsInCurrentRow % 2 != 0) {
                                addSimpleCell(metaTable, "", metaLabelFont, false);
                                addSimpleCell(metaTable, "", metaValueFont, false);
                                itemsInCurrentRow = 0;
                            }

                            addSimpleCell(metaTable, name, metaLabelFont, false);

                            PdfPCell longCell = new PdfPCell(new Phrase(value, metaValueFont));
                            longCell.setBorder(Rectangle.NO_BORDER);
                            longCell.setPadding(2f);
                            longCell.setColspan(3);
                            metaTable.addCell(longCell);

                            itemsInCurrentRow = 0;
                        } else {
                            addSimpleCell(metaTable, name, metaLabelFont, false);
                            addSimpleCell(metaTable, value, metaValueFont, false);

                            itemsInCurrentRow++;

                            if (itemsInCurrentRow == 2) {
                                itemsInCurrentRow = 0;
                            }
                        }
                    }

                    if (itemsInCurrentRow % 2 != 0) {
                        addSimpleCell(metaTable, "", metaLabelFont, false);
                        addSimpleCell(metaTable, "", metaValueFont, false);
                    }

                    PdfPCell metaWrapper = new PdfPCell(metaTable);
                    metaWrapper.setBorder(Rectangle.NO_BORDER);
                    metaWrapper.setPaddingTop(5f);
                    metaWrapper.setPaddingBottom(6f);

                    headerTable.addCell(metaWrapper);
                }

                headerTable.writeSelectedRows(0, -1,
                        document.leftMargin(),
                        pageSize.getHeight() - 14f,
                        writer.getDirectContent());

            } catch (RuntimeException e) {
                log.error("Error creating header", e);
            }

            // --- Footer ---
            try {
                Font footerFont = FontFactory.getFont(FONT_NAME, 9f, Font.NORMAL, Color.DARK_GRAY);
                Phrase pagePhrase = new Phrase("Page " + writer.getPageNumber(), footerFont);

                ColumnText.showTextAligned(writer.getDirectContent(),
                        Element.ALIGN_RIGHT,
                        pagePhrase,
                        pageSize.getWidth() - document.rightMargin(),
                        document.bottomMargin() - 20f,
                        0);
            } catch (RuntimeException e) {
                log.error("Error creating footer", e);
            }
        }

        private void addSimpleCell(PdfPTable table, String text, Font font, boolean border) {
            PdfPCell cell = new PdfPCell(new Phrase(text, font));
            if (!border) cell.setBorder(Rectangle.NO_BORDER);
            cell.setPadding(2f);
            table.addCell(cell);
        }
    }

    private void renderRow(PdfPTable table, ExportModel.Row row, List<ExportModel.Column> cols,
                           Font fontRegular, Font fontBold, RowSpanState rowSpanState) {

        List<Object> rawCells = row.getCells() != null ? row.getCells() : Collections.emptyList();
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
            int effectiveColspan = Math.min(Math.max(1, s.colspan), colCount - currentCol);

            for (int c = 1; c < effectiveColspan; c++) {
                if (rowSpanState.isCovered(currentCol + c)) {
                    effectiveColspan = c;
                    break;
                }
            }

            s.colspan = effectiveColspan;
            addCellToTable(table, s, fontRegular, fontBold);

            if (s.rowspan > 1) {
                rowSpanState.addSpan(currentCol, effectiveColspan, s.rowspan - 1);
            }
            currentCol += effectiveColspan;
        }
    }

    private void addCellToTable(PdfPTable table, CellSpec s, Font fontRegular, Font fontBold) {
        String raw = (s.value == null || "null".equalsIgnoreCase(String.valueOf(s.value))) ? "" : String.valueOf(s.value);
        String formattedValue = raw;
        try {
            formattedValue = FormatUtils.format(raw, s.format);
        } catch (RuntimeException e) {
            log.warn("Format error for value '{}'", raw);
        }

        PdfPCell cell = new PdfPCell(new Phrase(formattedValue, s.bold ? fontBold : fontRegular));
        if (s.colspan > 1) cell.setColspan(s.colspan);
        if (s.rowspan > 1) cell.setRowspan(s.rowspan);
        cell.setHorizontalAlignment(s.align);
        cell.setVerticalAlignment(Element.ALIGN_MIDDLE);
        cell.setPadding(7f);
        table.addCell(cell);
    }

    @SuppressWarnings("unchecked")
    private CellSpec parseCellSpec(Object raw, ExportModel.Column columnMeta) {
        CellSpec s = new CellSpec();
        s.colspan = 1; s.rowspan = 1; s.bold = false;

        String colAlign = (columnMeta != null && columnMeta.getFormat() != null)
                ? String.valueOf(columnMeta.getFormat().getOrDefault("align", "LEFT")).toUpperCase()
                : "LEFT";
        s.align = "RIGHT".equals(colAlign) ? Element.ALIGN_RIGHT :
                "CENTER".equals(colAlign) ? Element.ALIGN_CENTER : Element.ALIGN_LEFT;
        s.format = (columnMeta != null) ? columnMeta.getFormat() : Map.of();

        if (raw == null) { s.value = ""; return s; }
        if (!(raw instanceof Map)) { s.value = raw.toString(); return s; }

        Map<String, Object> m = (Map<String, Object>) raw;
        s.value = m.getOrDefault("value", "");
        if (m.containsKey("colspan") && m.get("colspan") instanceof Number n) s.colspan = n.intValue();
        if (m.containsKey("rowspan") && m.get("rowspan") instanceof Number n) s.rowspan = n.intValue();
        if (Boolean.TRUE.equals(m.get("bold"))) s.bold = true;
        if (m.containsKey("align")) {
            String a = String.valueOf(m.get("align")).toUpperCase();
            if ("RIGHT".equals(a)) s.align = Element.ALIGN_RIGHT;
            else if ("CENTER".equals(a)) s.align = Element.ALIGN_CENTER;
            else if ("LEFT".equals(a)) s.align = Element.ALIGN_LEFT;
        }
        if (m.containsKey("format") && m.get("format") instanceof Map) s.format = (Map<String, Object>) m.get("format");

        return s;
    }

    private float[] computeColumnWidths(List<ExportModel.Column> cols) {
        int n = cols.size();
        float[] widths = new float[n];
        float totalSpecified = 0f;
        int unspecifiedCount = 0;

        for (int i = 0; i < n; i++) {
            Map<String, Object> fmt = cols.get(i).getFormat();
            if (fmt != null && fmt.containsKey("width") && fmt.get("width") instanceof Number val) {
                widths[i] = val.floatValue();
                totalSpecified += widths[i];
            } else {
                widths[i] = -1f;
                unspecifiedCount++;
            }
        }
        if (unspecifiedCount > 0) {
            float remaining = Math.max(0f, 100f - totalSpecified);
            float perColumn = remaining / unspecifiedCount;
            if (perColumn <= 0) perColumn = 10f;
            for (int i = 0; i < n; i++) if (widths[i] < 0) widths[i] = perColumn;
        } else if (totalSpecified <= 0) {
            Arrays.fill(widths, 1f);
        }
        return widths;
    }

    private Rectangle resolvePageSize(String size, String orientation) {
        Rectangle rect;
        String s = size == null ? "A4" : size.toUpperCase(Locale.ROOT);
        switch (s) {
            case "LETTER" -> rect = PageSize.LETTER;
            case "LEGAL" -> rect = PageSize.LEGAL;
            case "A3" -> rect = PageSize.A3;
            case "A5" -> rect = PageSize.A5;
            default -> rect = PageSize.A4;
        }
        return "landscape".equalsIgnoreCase(orientation) ? rect.rotate() : rect;
    }

    private static class CellSpec {
        Object value; int colspan; int rowspan; boolean bold; int align; Map<String, Object> format;
    }

    private static class RowSpanState {
        private final int[] remainingRows;
        RowSpanState(int colCount) { this.remainingRows = new int[colCount]; }
        boolean isCovered(int col) { return col < remainingRows.length && remainingRows[col] > 0; }
        void consume(int col) { if (isCovered(col)) remainingRows[col]--; }
        void addSpan(int startCol, int colspan, int rows) {
            for (int i = 0; i < colspan; i++) {
                int idx = startCol + i;
                if (idx < remainingRows.length) remainingRows[idx] = Math.max(remainingRows[idx], rows);
            }
        }
    }
}