package com.tcs.fincore.ReportBuilder.repository;

import com.tcs.fincore.ReportBuilder.exception.ValidationException;
import com.tcs.fincore.ReportBuilder.model.variant.FilterRule;
import com.tcs.fincore.ReportBuilder.model.variant.ReportVariant;
import com.tcs.fincore.ReportBuilder.model.variant.VariantParamDef;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

@Slf4j
@Repository
@RequiredArgsConstructor
public class ReportVariantRepository {

    private final NamedParameterJdbcTemplate jdbc;

    public ReportVariant loadActiveVariant(String templateId, String variantCode) {
        String sql = """
                SELECT VARIANT_ID, TEMPLATE_ID, VARIANT_CODE, VARIANT_NAME,
                       DESCRIPTION, STATUS
                  FROM RB_REPORT_VARIANT
                 WHERE TEMPLATE_ID = :tid
                   AND VARIANT_CODE = :vcode
                   AND STATUS = 'ACTIVE'
                """;

        try {
            ReportVariant v = jdbc.queryForObject(
                    sql,
                    Map.of("tid", templateId, "vcode", variantCode),
                    new VariantRowMapper()
            );
            v.setParams(loadParams(v.getVariantId()));
            v.setFilterRules(loadRules(v.getVariantId()));
            return v;
        } catch (EmptyResultDataAccessException ex) {
            throw new ValidationException("Active variant not found for templateId=" +
                    templateId + ", variantCode=" + variantCode);
        }
    }

    private List<VariantParamDef> loadParams(String variantId) {
        String sql = """
                SELECT PARAM_ID, VARIANT_ID, PARAM_NAME, LABEL, PARAM_TYPE,
                       REQUIRED_YN, MULTI_VALUED_YN, VALIDATION_REGEX, UI_HINT
                  FROM RB_VARIANT_PARAM_DEF
                 WHERE VARIANT_ID = :vid
                 ORDER BY PARAM_NAME
                """;

        return jdbc.query(sql, Map.of("vid", variantId), new ParamRowMapper());
    }

    private List<FilterRule> loadRules(String variantId) {
        String sql = """
                SELECT RULE_ID, VARIANT_ID, SCOPE_TYPE, SCOPE_VALUE,
                       PARAM_NAME, DB_COLUMN, OPERATOR
                  FROM RB_FILTER_RULE
                 WHERE VARIANT_ID = :vid
                """;

        return jdbc.query(sql, Map.of("vid", variantId), new RuleRowMapper());
    }

    // --- RowMappers ---

    private static class VariantRowMapper implements RowMapper<ReportVariant> {
        @Override
        public ReportVariant mapRow(ResultSet rs, int rowNum) throws SQLException {
            ReportVariant v = new ReportVariant();
            v.setVariantId(rs.getString("VARIANT_ID"));
            v.setTemplateId(rs.getString("TEMPLATE_ID"));
            v.setVariantCode(rs.getString("VARIANT_CODE"));
            v.setVariantName(rs.getString("VARIANT_NAME"));
            v.setDescription(rs.getString("DESCRIPTION"));
            v.setStatus(rs.getString("STATUS"));
            return v;
        }
    }

    private static class ParamRowMapper implements RowMapper<VariantParamDef> {
        @Override
        public VariantParamDef mapRow(ResultSet rs, int rowNum) throws SQLException {
            VariantParamDef p = new VariantParamDef();
            p.setParamId(rs.getLong("PARAM_ID"));
            p.setVariantId(rs.getString("VARIANT_ID"));
            p.setParamName(rs.getString("PARAM_NAME"));
            p.setLabel(rs.getString("LABEL"));
            p.setParamType(rs.getString("PARAM_TYPE"));
            p.setRequired("Y".equalsIgnoreCase(rs.getString("REQUIRED_YN")));
            p.setMultiValued("Y".equalsIgnoreCase(rs.getString("MULTI_VALUED_YN")));
            p.setValidationRegex(rs.getString("VALIDATION_REGEX"));
            p.setUiHint(rs.getString("UI_HINT"));
            return p;
        }
    }

    private static class RuleRowMapper implements RowMapper<FilterRule> {
        @Override
        public FilterRule mapRow(ResultSet rs, int rowNum) throws SQLException {
            FilterRule r = new FilterRule();
            r.setRuleId(rs.getLong("RULE_ID"));
            r.setVariantId(rs.getString("VARIANT_ID"));
            r.setScopeType(rs.getString("SCOPE_TYPE"));
            r.setScopeValue(rs.getString("SCOPE_VALUE"));
            r.setParamName(rs.getString("PARAM_NAME"));
            r.setDbColumn(rs.getString("DB_COLUMN"));
            r.setOperator(rs.getString("OPERATOR"));
            return r;
        }
    }
}