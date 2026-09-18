package com.tcs.fincore.ReportBuilder.batch.materializer;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

/** Emits copyable named-parameter SQL without logging an unbounded amount of customer data. */
@Slf4j
@Component
public class MaterializationQueryLogger {

    public void log(String patternId, int queryNumber, String sql, Map<String, Object> parameters,
                    BatchMaterializationProperties properties) {
        if (!properties.isQueryLoggingEnabled()) {
            return;
        }

        log.info("Materialization query [pattern={}, part={}] SQL:\n{}\nBinds: {}",
                patternId, queryNumber, sql,
                new TreeMap<>(parameters).entrySet().stream()
                        .map(entry -> entry.getKey() + "=" + summarize(entry.getValue(), properties))
                        .collect(Collectors.joining(", ")));
    }

    private String summarize(Object value, BatchMaterializationProperties properties) {
        if (!(value instanceof Collection<?> values)) {
            return properties.isQueryLoggingIncludeParameterValues() ? String.valueOf(value) : "<masked>";
        }
        if (!properties.isQueryLoggingIncludeParameterValues()) {
            return "<collection size=" + values.size() + ">";
        }
        int sampleSize = Math.max(0, properties.getQueryLoggingParameterSampleSize());
        String sample = values.stream().limit(sampleSize).map(String::valueOf).collect(Collectors.joining(", "));
        return "<collection size=" + values.size() + ", sample=[" + sample +
                (values.size() > sampleSize ? ", ...]" : "]") + ">";
    }
}
