package com.tcs.fincore.ReportBuilder.batch.application.util;

import com.tcs.fincore.ReportBuilder.model.ReportTemplate;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.json.JsonMapper;

@Slf4j
@Component
@RequiredArgsConstructor
public class TemplateCloner {

    private final JsonMapper jsonMapper;

    public ReportTemplate deepClone(ReportTemplate template) {
        try {
                return jsonMapper.readValue(jsonMapper.writeValueAsBytes(template), ReportTemplate.class);
        } catch (JacksonException e) {
            log.error("Deep copy failed for template {}: {}",
                    template.getReportMeta().getReportId(), e.getMessage());
            throw new IllegalStateException("Critical: Template cloning failed", e);
        }
    }
}