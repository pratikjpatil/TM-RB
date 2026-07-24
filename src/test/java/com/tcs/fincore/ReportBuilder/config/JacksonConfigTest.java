package com.tcs.fincore.ReportBuilder.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tcs.fincore.ReportBuilder.batch.dto.BatchJobStatus;
import org.junit.jupiter.api.Test;
import org.springframework.http.converter.json.Jackson2ObjectMapperBuilder;

import java.time.LocalDateTime;

import static org.assertj.core.api.Assertions.assertThat;

class JacksonConfigTest {

    @Test
    void objectMapperSerializesJavaTimeAsIsoStringAndIgnoresUnknownFields() throws Exception {
        ObjectMapper mapper = new JacksonConfig().objectMapper(new Jackson2ObjectMapperBuilder());

        BatchJobStatus status = BatchJobStatus.builder()
                .jobId("job-1")
                .startTime(LocalDateTime.of(2026, 7, 24, 13, 45))
                .build();

        String json = mapper.writeValueAsString(status);

        assertThat(json).contains("\"startTime\":\"2026-07-24T13:45:00\"");
        assertThat(mapper.readValue("{\"jobId\":\"job-1\",\"unknown\":true}", BatchJobStatus.class).getJobId())
                .isEqualTo("job-1");
    }
}
