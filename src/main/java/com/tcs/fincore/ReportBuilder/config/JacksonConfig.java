package com.tcs.fincore.ReportBuilder.config;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.http.converter.json.Jackson2ObjectMapperBuilder;

@Configuration
public class JacksonConfig {

    /**
     * Primary application ObjectMapper — injected everywhere by default.
     * Starts from Spring Boot's Jackson builder so framework-discovered modules
     * and message-converter defaults remain intact after dependency upgrades.
     * - FAIL_ON_UNKNOWN_PROPERTIES disabled: tolerates DB/API schema evolution
     *   without breaking deserialization (unknown fields are silently ignored).
     * - JavaTimeModule: handles LocalDate, LocalDateTime, Instant natively.
     * - WRITE_DATES_AS_TIMESTAMPS disabled: ISO-8601 strings in JSON output.
     */
    @Bean
    @Primary
    public ObjectMapper objectMapper(Jackson2ObjectMapperBuilder builder) {
        return builder
                .modulesToInstall(new JavaTimeModule())
                .featuresToDisable(
                        SerializationFeature.WRITE_DATES_AS_TIMESTAMPS,
                        DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
                .build();
    }
}
