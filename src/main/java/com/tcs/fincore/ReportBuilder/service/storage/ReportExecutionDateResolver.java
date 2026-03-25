package com.tcs.fincore.ReportBuilder.service.storage;

import com.tcs.fincore.ReportBuilder.exception.ValidationException;
import com.tcs.fincore.ReportBuilder.model.variant.ReportVariant;
import com.tcs.fincore.ReportBuilder.model.variant.VariantParamDef;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Service;

import java.sql.Date;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

/**
 * Resolves report execution date used for HDFS output path.
 *
 * Resolution order:
 * 1) User payload date (variant DATE param preferred)
 * 2) Known date-like keys from payload
 * 3) FINCORE_ETL_DATE fallback query
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class ReportExecutionDateResolver {

    private static final List<String> FALLBACK_DATE_KEYS = List.of(
            "reportDate", "report_date", "asOnDate", "as_of_date", "etlDate", "businessDate", "processingDate"
    );

    private static final List<DateTimeFormatter> STRING_DATE_FORMATTERS = List.of(
            DateTimeFormatter.ISO_LOCAL_DATE,
            DateTimeFormatter.ofPattern("dd-MM-yyyy"),
            DateTimeFormatter.ofPattern("dd/MM/yyyy"),
            DateTimeFormatter.ofPattern("dd-MMM-yyyy", Locale.ENGLISH)
    );

    @Value("${report.storage.fallback-etl-date-query:SELECT MAX(ETL_DATE) FROM FINCORE_ETL_DATE}")
    private String fallbackEtlDateQuery;

    private final NamedParameterJdbcTemplate jdbcTemplate;

    public LocalDate resolveStorageDate(Map<String, Object> inputParams, ReportVariant variant) {
        Map<String, Object> safeParams = inputParams != null ? inputParams : Map.of();

        Optional<LocalDate> payloadDate = resolveFromVariantDateParams(safeParams, variant)
                .or(() -> resolveFromKnownPayloadKeys(safeParams));

        if (payloadDate.isPresent()) {
            return payloadDate.get();
        }

        return resolveFromEtlDateTable();
    }

    private Optional<LocalDate> resolveFromVariantDateParams(Map<String, Object> inputParams, ReportVariant variant) {
        if (variant == null || variant.getParams() == null || variant.getParams().isEmpty()) {
            return Optional.empty();
        }

        for (VariantParamDef param : variant.getParams()) {
            if (param == null || param.getParamName() == null) {
                continue;
            }

            if (!"DATE".equalsIgnoreCase(param.getParamType())) {
                continue;
            }

            Object rawValue = inputParams.get(param.getParamName());
            Optional<LocalDate> parsed = toLocalDate(rawValue);
            if (parsed.isPresent()) {
                log.debug("Resolved storage date from variant DATE parameter '{}': {}",
                        param.getParamName(), parsed.get());
                return parsed;
            }
        }

        return Optional.empty();
    }

    private Optional<LocalDate> resolveFromKnownPayloadKeys(Map<String, Object> inputParams) {
        for (String key : FALLBACK_DATE_KEYS) {
            Optional<LocalDate> parsed = toLocalDate(inputParams.get(key));
            if (parsed.isPresent()) {
                log.debug("Resolved storage date from payload key '{}': {}", key, parsed.get());
                return parsed;
            }
        }
        return Optional.empty();
    }

    private LocalDate resolveFromEtlDateTable() {
        try {
            Object raw = jdbcTemplate.getJdbcTemplate().queryForObject(fallbackEtlDateQuery, Object.class);
            return toLocalDate(raw).orElseThrow(() ->
                    new ValidationException("ETL date query returned a value that could not be parsed as date."));
        } catch (RuntimeException e) {
            throw new ValidationException(
                    "Unable to resolve report storage date from payload or FINCORE_ETL_DATE fallback query.", e);
        }
    }

    private Optional<LocalDate> toLocalDate(Object value) {
        if (value == null) {
            return Optional.empty();
        }
        if (value instanceof LocalDate localDate) {
            return Optional.of(localDate);
        }
        if (value instanceof Date sqlDate) {
            return Optional.of(sqlDate.toLocalDate());
        }
        if (value instanceof java.util.Date utilDate) {
            return Optional.of(Instant.ofEpochMilli(utilDate.getTime()).atZone(ZoneId.systemDefault()).toLocalDate());
        }
        if (value instanceof CharSequence sequence) {
            String raw = sequence.toString().trim();
            if (raw.isEmpty()) {
                return Optional.empty();
            }
            for (DateTimeFormatter formatter : STRING_DATE_FORMATTERS) {
                try {
                    return Optional.of(LocalDate.parse(raw, formatter));
                } catch (DateTimeParseException ignored) {
                    // try next formatter
                }
            }
        }
        return Optional.empty();
    }
}
