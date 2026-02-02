package com.tcs.fincore.ReportBuilder.repository;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tcs.fincore.ReportBuilder.exception.ValidationException;
import com.tcs.fincore.ReportBuilder.model.ReportTemplate;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.Map;

@Slf4j
@Repository
@RequiredArgsConstructor
public class ReportTemplateRepository {

    private final NamedParameterJdbcTemplate jdbc;
    private final ObjectMapper objectMapper;

    public ReportTemplate loadActiveTemplate(String templateId) {
        String sql = """
                SELECT TEMPLATE_JSON
                  FROM RB_REPORT_TEMPLATE
                 WHERE TEMPLATE_ID = :tid
                   AND STATUS = 'ACTIVE'
                """;

        try {
            String json = jdbc.queryForObject(sql, Map.of("tid", templateId), String.class);
            if (json == null || json.isBlank()) {
                throw new ValidationException("Template JSON is empty for templateId=" + templateId);
            }
            return objectMapper.readValue(json, ReportTemplate.class);
        } catch (EmptyResultDataAccessException ex) {
            throw new ValidationException("Active template not found for templateId=" + templateId);
        } catch (Exception ex) {
            log.error("Error loading template {}: {}", templateId, ex.getMessage(), ex);
            throw new ValidationException("Failed to load template: " + ex.getMessage(), ex);
        }
    }

}