package com.tcs.fincore.ReportBuilder.repository;

import com.tcs.fincore.ReportBuilder.exception.ValidationException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;


@Slf4j
@Repository
@RequiredArgsConstructor
public class ETLDateRepository {
    private final NamedParameterJdbcTemplate jdbc;

    public String resolveFromEtlDateTable() {
        try {
            String fallbackEtlDateQuery = "SELECT MAX(ETL_DATE) FROM FINCORE_DATE";
            Object raw = jdbc.getJdbcTemplate().queryForObject(fallbackEtlDateQuery, Object.class);
//            return toLocalDate(raw).orElseThrow(() ->
//                    new ValidationException("ETL date query returned a value that could not be parsed as date."));
            return raw != null ? raw.toString() : null;
        } catch (RuntimeException e) {
            throw new ValidationException(
                    "Unable to resolve report storage date from payload or FINCORE_ETL_DATE fallback query.", e);
        }
    }
}