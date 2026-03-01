package com.tcs.fincore.ReportBuilder.config;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

@Configuration
public class JacksonConfig {

    /**
     * Primary application ObjectMapper — injected everywhere by default.
     * - FAIL_ON_UNKNOWN_PROPERTIES disabled: tolerates DB/API schema evolution
     *   without breaking deserialization (unknown fields are silently ignored).
     * - JavaTimeModule: handles LocalDate, LocalDateTime, Instant natively.
     * - WRITE_DATES_AS_TIMESTAMPS disabled: ISO-8601 strings in JSON output.
     */
    @Bean
    @Primary
    public ObjectMapper objectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        mapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        return mapper;
    }
}