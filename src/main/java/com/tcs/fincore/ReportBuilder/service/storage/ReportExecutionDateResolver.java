package com.tcs.fincore.ReportBuilder.service.storage;

import com.tcs.fincore.ReportBuilder.exception.ValidationException;
import com.tcs.fincore.ReportBuilder.model.variant.ReportVariant;
import com.tcs.fincore.ReportBuilder.model.variant.VariantParamDef;
import com.tcs.fincore.ReportBuilder.repository.ETLDateRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.tcs.fincore.ReportBuilder.util.DateUtilsHelper.toLocalDate;


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
    private final ETLDateRepository etlDateRepository;


    public LocalDate resolveStorageDate(Map<String, Object> inputParams, ReportVariant variant) {
        Map<String, Object> safeParams = inputParams != null ? inputParams : Map.of();

        Optional<LocalDate> payloadDate = resolveFromVariantDateParams(safeParams, variant)
                .or(() -> resolveFromKnownPayloadKeys(safeParams));

        if (payloadDate.isPresent()) {
            return payloadDate.get();
        }

        return toLocalDate(etlDateRepository.resolveFromEtlDateTable()).orElseThrow(() ->
                    new ValidationException("ETL date query returned a value that could not be parsed as date."));
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

}