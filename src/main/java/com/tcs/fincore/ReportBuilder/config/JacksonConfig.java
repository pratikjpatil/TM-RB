package com.tcs.fincore.ReportBuilder.config;

import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.SerializationFeature;
import tools.jackson.databind.cfg.DateTimeFeature;
import tools.jackson.databind.json.JsonMapper;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

@Configuration
public class JacksonConfig {

    /**
     * Primary application JsonMapper for Spring Boot 4.1+
     * - Uses immutable builder pattern (Jackson 3 requirement)
     * - JavaTimeModule auto-registered by default
     * - FAIL_ON_UNKNOWN_PROPERTIES disabled for schema evolution tolerance
     * - ISO-8601 date strings (not timestamps)
     */
    @Bean
    @Primary
    public JsonMapper jsonMapper() {
        return JsonMapper.builder()
                .disable(DateTimeFeature.WRITE_DATES_AS_TIMESTAMPS)
                .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
                .build();
    }
}