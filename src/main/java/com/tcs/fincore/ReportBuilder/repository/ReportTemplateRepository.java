package com.tcs.fincore.ReportBuilder.repository;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tcs.fincore.ReportBuilder.exception.InfrastructureException;
import com.tcs.fincore.ReportBuilder.exception.ValidationException;
import com.tcs.fincore.ReportBuilder.model.ReportTemplate;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.dao.DataAccessException;
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
        } catch (DataAccessException ex) {
            log.error("Template query failed: jobId={} templateId={} correlationId={} errorType={}",
                    "N/A", templateId, "N/A", ex.getClass().getSimpleName(), ex);
            throw new InfrastructureException("Failed to load active template metadata", ex);
        } catch (java.io.IOException ex) {
            log.error("Template parse failed: jobId={} templateId={} correlationId={} errorType={}",
                    "N/A", templateId, "N/A", ex.getClass().getSimpleName(), ex);
            throw new InfrastructureException("Failed to parse template definition", ex);
        }
    }

    public ReportTemplate loadActiveTemplateByReportId(String reportId) {
        String sql = """
                SELECT TEMPLATE_JSON
                  FROM RB_REPORT_TEMPLATE
                 WHERE REPORT_ID = :rid
                   AND STATUS = 'ACTIVE'
                 ORDER BY VERSION_NO DESC
                 FETCH FIRST 1 ROWS ONLY
                """;

        try {
            String json = jdbc.queryForObject(sql, Map.of("rid", reportId), String.class);
            if (json == null || json.isBlank()) {
                throw new ValidationException("Template JSON is empty for reportId=" + reportId);
            }
            return objectMapper.readValue(json, ReportTemplate.class);
        } catch (EmptyResultDataAccessException ex) {
            throw new ValidationException("Active template not found for reportId=" + reportId);
        } catch (DataAccessException ex) {
            log.error("Template query failed: jobId={} reportId={} correlationId={} errorType={}",
                    "N/A", reportId, "N/A", ex.getClass().getSimpleName(), ex);
            throw new InfrastructureException("Failed to load active template metadata by reportId", ex);
        } catch (java.io.IOException ex) {
            log.error("Template parse failed: jobId={} reportId={} correlationId={} errorType={}",
                    "N/A", reportId, "N/A", ex.getClass().getSimpleName(), ex);
            throw new InfrastructureException("Failed to parse template definition", ex);
        }
    }

    public String findActiveTemplateIdByReportId(String reportId) {
        String sql = """
                SELECT TEMPLATE_ID
                  FROM RB_REPORT_TEMPLATE
                 WHERE REPORT_ID = :rid
                   AND STATUS = 'ACTIVE'
                 ORDER BY VERSION_NO DESC
                 FETCH FIRST 1 ROWS ONLY
                """;
        try {
            return jdbc.queryForObject(sql, Map.of("rid", reportId), String.class);
        } catch (EmptyResultDataAccessException ex) {
            throw new ValidationException("Active templateId not found for reportId=" + reportId);
        } catch (DataAccessException ex) {
            log.error("TemplateId query failed: jobId={} reportId={} correlationId={} errorType={}",
                    "N/A", reportId, "N/A", ex.getClass().getSimpleName(), ex);
            throw new InfrastructureException("Failed to resolve active templateId by reportId", ex);
        }
    }

}
