package com.tcs.fincore.ReportBuilder.batch.application.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tcs.fincore.ReportBuilder.model.ReportTemplate;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Slf4j
@Component
@RequiredArgsConstructor
public class TemplateCloner {

    private final ObjectMapper objectMapper;

    public ReportTemplate deepClone(ReportTemplate template) {
        try {
            return objectMapper.readValue(objectMapper.writeValueAsBytes(template), ReportTemplate.class);
        } catch (IOException e) {
            log.error("Deep copy failed for template {}: {}",
                    template.getReportMeta().getReportId(), e.getMessage());
            throw new IllegalStateException("Critical: Template cloning failed", e);
        }
    }
}