




package com.tcs.fincore.ReportBuilder.config;

import com.tcs.fincore.ReportBuilder.kafka.dto.KafkaReportTriggerMessage;
import com.tcs.fincore.ReportBuilder.kafka.dto.KafkaSingleReportTriggerMessage;
import com.tcs.fincore.ReportBuilder.kafka.dto.ReportLifecycleEvent;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.util.backoff.ExponentialBackOff;

import java.util.HashMap;
import java.util.Map;

@EnableKafka
@Configuration
public class KafkaConfig {

    // -- Connection -----------------------------------------------------------

    @Value("${spring.kafka.bootstrap-servers:localhost:9092}")
    private String bootstrapServers;

    // -- Consumer commons -----------------------------------------------------

    @Value("${kafka.consumer.group-id:report-builder-group}")
    private String groupId;

    @Value("${kafka.consumer.concurrency:3}")
    private int concurrency;

    @Value("${kafka.consumer.max-poll-records:5}")
    private int maxPollRecords;

    @Value("${kafka.consumer.poll-timeout-ms:3000}")
    private long pollTimeoutMs;

    // -- Retry / back-off -----------------------------------------------------

    @Value("${kafka.retry.initial-interval-ms:1000}")
    private long retryInitialIntervalMs;

    @Value("${kafka.retry.max-interval-ms:30000}")
    private long retryMaxIntervalMs;

    @Value("${kafka.retry.multiplier:2.0}")
    private double retryMultiplier;

    @Value("${kafka.retry.max-attempts:3}")
    private int retryMaxAttempts;

    // =========================================================================
    // Shared error handler (same policy for both modes)
    // =========================================================================

    /**
     * Shared {@link DefaultErrorHandler} with exponential back-off.
     * After {@code retryMaxAttempts} failures the message is routed to the DLT.
     */
    @Bean
    public DefaultErrorHandler kafkaErrorHandler() {
        ExponentialBackOff backOff = new ExponentialBackOff(retryInitialIntervalMs, retryMultiplier);
        backOff.setMaxInterval(retryMaxIntervalMs);
        backOff.setMaxAttempts(retryMaxAttempts);

        DefaultErrorHandler handler = new DefaultErrorHandler(backOff);
        // Validation errors are not retryable — send straight to DLT
        handler.addNotRetryableExceptions(
                org.springframework.kafka.support.serializer.DeserializationException.class);
        return handler;
    }

    // =========================================================================
    // BATCH MODE — KafkaReportTriggerMessage consumer
    // =========================================================================

    @Bean
    public ConsumerFactory<String, KafkaReportTriggerMessage> batchReportTriggerConsumerFactory() {
        Map<String, Object> props = buildBaseConsumerProps();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        JsonDeserializer<KafkaReportTriggerMessage> deserializer =
                new JsonDeserializer<>(KafkaReportTriggerMessage.class);
        deserializer.setRemoveTypeHeaders(false);
        deserializer.addTrustedPackages("*");
        deserializer.setUseTypeMapperForKey(false);

        ErrorHandlingDeserializer<KafkaReportTriggerMessage> errorHandlingDeserializer =
                new ErrorHandlingDeserializer<>(deserializer);

        return new DefaultKafkaConsumerFactory<>(
                props,
                new StringDeserializer(),
                errorHandlingDeserializer);
    }

    /**
     * Listener container factory for batch-mode jobs.
     * Bean name referenced by {@code @KafkaListener(containerFactory = "reportGenerationListenerContainerFactory")}
     * in {@link com.tcs.fincore.ReportBuilder.kafka.consumer.ReportGenerationConsumer}.
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, KafkaReportTriggerMessage>
    reportGenerationListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, KafkaReportTriggerMessage> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(batchReportTriggerConsumerFactory());
        factory.setConcurrency(concurrency);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        factory.getContainerProperties().setPollTimeout(pollTimeoutMs);
        factory.setCommonErrorHandler(kafkaErrorHandler());
        return factory;
    }

    // =========================================================================
    // SINGLE MODE — KafkaSingleReportTriggerMessage consumer
    // =========================================================================

    @Bean
    public ConsumerFactory<String, KafkaSingleReportTriggerMessage>
    singleModeReportTriggerConsumerFactory() {
        Map<String, Object> props = buildBaseConsumerProps();
        // Single-mode jobs share the same consumer group
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        JsonDeserializer<KafkaSingleReportTriggerMessage> deserializer =
                new JsonDeserializer<>(KafkaSingleReportTriggerMessage.class);
        deserializer.setRemoveTypeHeaders(false);
        deserializer.addTrustedPackages("*");
        deserializer.setUseTypeMapperForKey(false);

        ErrorHandlingDeserializer<KafkaSingleReportTriggerMessage> errorHandlingDeserializer =
                new ErrorHandlingDeserializer<>(deserializer);

        return new DefaultKafkaConsumerFactory<>(
                props,
                new StringDeserializer(),
                errorHandlingDeserializer);
    }

    /**
     * Listener container factory for single-mode jobs.
     * Bean name referenced by {@code @KafkaListener(containerFactory = "singleModeReportListenerContainerFactory")}
     * in {@link com.tcs.fincore.ReportBuilder.kafka.consumer.SingleModeReportGenerationConsumer}.
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, KafkaSingleReportTriggerMessage>
    singleModeReportListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, KafkaSingleReportTriggerMessage> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(singleModeReportTriggerConsumerFactory());
        factory.setConcurrency(concurrency);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        factory.getContainerProperties().setPollTimeout(pollTimeoutMs);
        factory.setCommonErrorHandler(kafkaErrorHandler());
        return factory;
    }

    // =========================================================================
    // Shared producer — ReportLifecycleEvent (used by both modes)
    // =========================================================================

    @Bean
    public ProducerFactory<String, ReportLifecycleEvent> reportEventProducerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        // Idempotent producer ensures exactly-once delivery of lifecycle events
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        return new DefaultKafkaProducerFactory<>(props);
    }

    /**
     * Shared {@link KafkaTemplate} for publishing {@link ReportLifecycleEvent} messages
     * to both batch and single mode lifecycle topics.
     */
    @Bean
    public KafkaTemplate<String, ReportLifecycleEvent> reportEventKafkaTemplate() {
        KafkaTemplate<String, ReportLifecycleEvent> template =
                new KafkaTemplate<>(reportEventProducerFactory());
        template.setObservationEnabled(true);
        return template;
    }

    // =========================================================================
    // Internal helpers
    // =========================================================================

    /**
     * Builds base consumer properties shared across all consumer factories.
     */
    private Map<String, Object> buildBaseConsumerProps() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);  // manual ack
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords);
        return props;
    }
}