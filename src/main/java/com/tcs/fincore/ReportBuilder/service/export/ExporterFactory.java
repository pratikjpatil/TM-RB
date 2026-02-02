package com.tcs.fincore.ReportBuilder.service.export;

import com.tcs.fincore.ReportBuilder.exception.ValidationException;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@RequiredArgsConstructor
public class ExporterFactory {

    private final List<ReportExporter> exporters;

    public ReportExporter getExporter(String formatType) {
        return exporters.stream()
                .filter(e -> e.supports(formatType))
                .findFirst()
                .orElseThrow(() -> new ValidationException("Unsupported report format: " + formatType));
    }
}