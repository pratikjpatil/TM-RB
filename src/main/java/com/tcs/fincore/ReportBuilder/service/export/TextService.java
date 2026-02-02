

package com.tcs.fincore.ReportBuilder.service.export;

import com.tcs.fincore.ReportBuilder.model.ExportModel;
import com.tcs.fincore.ReportBuilder.service.format.FormatUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.util.*;

@Slf4j
@Service
public class TextService implements ReportExporter {

    private static final String DELIMITER = "|";
    private static final String LINE_SEP = System.lineSeparator();
    private static final String BANK_NAME = "STATE BANK OF INDIA";
    private static final String SEPARATOR_LINE = "=".repeat(80);

    @Override
    public boolean supports(String formatType) {
        return "txt".equalsIgnoreCase(formatType);
    }

    @Override
    public String getMediaType() {
        return "text/plain";
    }

    @Override
    public String getFileExtension() {
        return ".psv";
    }

    @Override
    public byte[] generate(ExportModel model) {
        StringBuilder sb = new StringBuilder();

        // 1. Header - Bank Name (Centered)
        sb.append(SEPARATOR_LINE).append(LINE_SEP);
        sb.append(centerText(BANK_NAME, 80)).append(LINE_SEP);
        sb.append(SEPARATOR_LINE).append(LINE_SEP);
        sb.append(LINE_SEP);

        // 2. Report Title (Centered if present)
        if (model.getTitle() != null && !model.getTitle().isBlank()) {
            sb.append(centerText(model.getTitle(), 80)).append(LINE_SEP);
            sb.append(LINE_SEP);
        }

        // 3. Metadata
        if (model.getHeadingContent() != null) {
            for (Map<String, Object> meta : model.getHeadingContent()) {
                sb.append(meta.get("name").toString()).append(": ").append(meta.get("value").toString()).append(LINE_SEP);
            }
            sb.append(LINE_SEP);
        }

        List<ExportModel.Column> cols = model.getColumns();
        if (cols == null) cols = Collections.emptyList();

        // 4. Column Headers
        List<String> headers = new ArrayList<>();
        for (ExportModel.Column c : cols) {
            headers.add(c.getName() == null ? "" : c.getName());
        }
        sb.append(String.join(DELIMITER, headers)).append(LINE_SEP);

        // 5. Data Rows
        if (model.getRows() != null) {
            for (ExportModel.Row row : model.getRows()) {
                List<String> lineValues = new ArrayList<>();
                List<Object> rawCells = row.getCells() == null ? Collections.emptyList() : row.getCells();

                int currentRawIndex = 0;

                for (int i = 0; i < cols.size(); i++) {
                    if (currentRawIndex < rawCells.size()) {
                        Object cellObj = rawCells.get(currentRawIndex);
                        Map<String, Object> format = cols.get(i).getFormat();

                        Object val = cellObj;
                        if (cellObj instanceof Map<?, ?> m) {
                            val = m.get("value");
                        }

                        try {
                            lineValues.add(FormatUtils.format(val, format));
                        } catch (Exception e) {
                            log.warn("Failed to format cell value: {}", e.getMessage());
                            lineValues.add(String.valueOf(val));
                        }
                        currentRawIndex++;
                    } else {
                        lineValues.add("");
                    }
                }
                sb.append(String.join(DELIMITER, lineValues)).append(LINE_SEP);
            }
        }

        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Center text within a given width
     */
    private String centerText(String text, int width) {
        if (text == null || text.isBlank()) return "";

        int textLen = text.length();
        if (textLen >= width) return text;

        int padding = (width - textLen) / 2;
        return " ".repeat(padding) + text;
    }
}