package com.tcs.fincore.ReportBuilder.service.output;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tcs.fincore.ReportBuilder.model.ExportModel;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@Component
@RequiredArgsConstructor
public class DruidReportOutputSink implements ReportOutputSink {

    private final ObjectMapper objectMapper;
    private final ObjectProvider<KafkaTemplate<String, String>> kafkaTemplateProvider;

    private final Object lock = new Object();
    private final List<Map<String, Object>> rowBuffer = new ArrayList<>();
    private final RestTemplate restTemplate = new RestTemplate();

    @Value("${report.output.druid.endpoint:}")
    private String druidEndpoint;

    @Value("${report.output.druid.kafka-topic:}")
    private String druidKafkaTopic;

    @Value("${report.output.druid.batch-size:2000}")
    private int batchSize;

    @Override
    public OutputTargetType targetType() {
        return OutputTargetType.DRUID;
    }

    @Override
    public void writeReport(ReportOutputRequest request, ExportModel exportModel) {
        List<ExportModel.Row> rows = exportModel.getRows() == null ? List.of() : exportModel.getRows();
        synchronized (lock) {
            int rowOrder = 0;
            for (ExportModel.Row row : rows) {
                rowBuffer.add(toDruidRow(request, row, rowOrder++));
            }
            if (rowBuffer.size() >= batchSize) {
                flushInternal();
            }
        }
    }

    @Override
    public void flushPending(ReportOutputRequest request) {
        synchronized (lock) {
            flushInternal();
        }
    }

    private Map<String, Object> toDruidRow(ReportOutputRequest request, ExportModel.Row row, int rowOrder) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("reportId", request.getTemplate().getReportMeta().getReportId());
        payload.put("variantCode", request.getVariant().getVariantCode());
        payload.put("dimensionParam", request.getVaryingParamName());
        payload.put("dimensionValue", request.getDimensionValue());
        payload.put("storageDate", request.getStorageDate().toString());
        payload.put("rowType", row.getRowType());
        payload.put("rowOrder", rowOrder);
        payload.put("cells", row.getCells() == null ? List.of() : row.getCells());
        return payload;
    }

    private void flushInternal() {
        if (rowBuffer.isEmpty()) {
            return;
        }

        List<Map<String, Object>> batch = new ArrayList<>(rowBuffer);
        rowBuffer.clear();

        if (druidKafkaTopic != null && !druidKafkaTopic.isBlank()) {
            publishToKafka(batch);
            return;
        }
        if (druidEndpoint != null && !druidEndpoint.isBlank()) {
            publishToEndpoint(batch);
            return;
        }

        log.warn("Druid output selected but neither endpoint nor kafka topic is configured. Dropping {} row(s).", batch.size());
    }

    private void publishToEndpoint(List<Map<String, Object>> batch) {
        Map<String, Object> payload = Map.of(
                "timestamp", LocalDate.now().toString(),
                "rows", batch
        );

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<Map<String, Object>> entity = new HttpEntity<>(payload, headers);
        restTemplate.postForEntity(druidEndpoint, entity, String.class);
        log.debug("Sent {} row(s) to Druid endpoint {}", batch.size(), druidEndpoint);
    }

    private void publishToKafka(List<Map<String, Object>> batch) {
        KafkaTemplate<String, String> kafkaTemplate = kafkaTemplateProvider.getIfAvailable();
        if (kafkaTemplate == null) {
            throw new IllegalStateException("No KafkaTemplate<String, String> bean available for Druid Kafka ingestion");
        }

        for (Map<String, Object> event : batch) {
            kafkaTemplate.send(druidKafkaTopic, safeJson(event));
        }
        log.debug("Published {} row(s) to Druid Kafka topic {}", batch.size(), druidKafkaTopic);
    }

    private String safeJson(Map<String, Object> payload) {
        try {
            return objectMapper.writeValueAsString(payload);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Unable to serialize Druid payload", e);
        }
    }
}
