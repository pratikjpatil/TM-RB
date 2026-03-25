package com.tcs.fincore.ReportBuilder.service.storage;

import com.tcs.fincore.ReportBuilder.exception.ValidationException;
import com.tcs.fincore.ReportBuilder.model.variant.ReportVariant;
import com.tcs.fincore.ReportBuilder.model.variant.VariantParamDef;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.test.util.ReflectionTestUtils;

import java.time.LocalDate;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ReportExecutionDateResolverTest {

    private JdbcTemplate jdbcTemplate;
    private ReportExecutionDateResolver resolver;

    @BeforeEach
    void setUp() {
        jdbcTemplate = mock(JdbcTemplate.class);
        NamedParameterJdbcTemplate namedJdbc = mock(NamedParameterJdbcTemplate.class);
        when(namedJdbc.getJdbcTemplate()).thenReturn(jdbcTemplate);

        resolver = new ReportExecutionDateResolver(namedJdbc);
        ReflectionTestUtils.setField(
                resolver,
                "fallbackEtlDateQuery",
                "SELECT MAX(ETL_DATE) FROM FINCORE_ETL_DATE");
    }

    @Test
    void shouldPreferVariantDateFromPayload() {
        ReportVariant variant = new ReportVariant();
        VariantParamDef dateParam = new VariantParamDef();
        dateParam.setParamName("reportDate");
        dateParam.setParamType("DATE");
        variant.setParams(List.of(dateParam));

        LocalDate resolved = resolver.resolveStorageDate(Map.of("reportDate", "2026-01-15"), variant);

        assertEquals(LocalDate.of(2026, 1, 15), resolved);
    }

    @Test
    void shouldResolveFromKnownPayloadKeyWhenVariantMissing() {
        LocalDate resolved = resolver.resolveStorageDate(Map.of("asOnDate", "15-02-2026"), null);

        assertEquals(LocalDate.of(2026, 2, 15), resolved);
    }

    @Test
    void shouldFallbackToEtlDateQueryWhenPayloadMissingDate() {
        when(jdbcTemplate.queryForObject("SELECT MAX(ETL_DATE) FROM FINCORE_ETL_DATE", Object.class))
                .thenReturn(java.sql.Date.valueOf("2026-03-01"));

        LocalDate resolved = resolver.resolveStorageDate(Map.of("branchCode", "001"), null);

        assertEquals(LocalDate.of(2026, 3, 1), resolved);
    }

    @Test
    void shouldFailWhenPayloadAndEtlDateAreUnavailable() {
        when(jdbcTemplate.queryForObject("SELECT MAX(ETL_DATE) FROM FINCORE_ETL_DATE", Object.class))
                .thenReturn(null);

        assertThrows(ValidationException.class,
                () -> resolver.resolveStorageDate(Map.of("branchCode", "001"), null));
    }
}
