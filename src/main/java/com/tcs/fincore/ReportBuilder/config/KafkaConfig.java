package com.tcs.fincore.ReportBuilder.config;

import com.tcs.fincore.ReportBuilder.kafka.dto.KafkaReportTriggerMessage;
import com.tcs.fincore.ReportBuilder.kafka.dto.KafkaSingleReportTriggerMessage;
import com.tcs.fincore.ReportBuilder.kafka.dto.ReportLifecycleEvent;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;

import java.util.HashMap;
import java.util.Map;

@EnableKafka
@Configuration(proxyBeanMethods = false)
public class KafkaConfig {

    private static final String TRUSTED_DTO_PACKAGE =
            "*";

    @Value("${spring.kafka.bootstrap-servers:localhost:9092}")
    private String bootstrapServers;

    @Value("${kafka.consumer.group-id:report-builder-group}")
    private String groupId;

    @Value("${kafka.consumer.concurrency:3}")
    private int concurrency;

    @Value("${kafka.consumer.max-poll-records:5}")
    private int maxPollRecords;

    @Value("${kafka.consumer.poll-timeout-ms:3000}")
    private long pollTimeoutMs;

    @Value("${kafka.retry.initial-interval-ms:1000}")
    private long retryInitialIntervalMs;

    @Value("${kafka.retry.max-interval-ms:30000}")
    private long retryMaxIntervalMs;

    @Value("${kafka.retry.multiplier:2.0}")
    private double retryMultiplier;

    @Value("${kafka.retry.max-attempts:3}")
    private int retryMaxAttempts;

    @Bean
    public KafkaTemplate<String, Object> deadLetterKafkaTemplate() {
        KafkaTemplate<String, Object> template =
                new KafkaTemplate<>(deadLetterProducerFactory());
        template.setObservationEnabled(true);
        return template;
    }

    @Bean
    public KafkaTemplate<String, ReportLifecycleEvent> reportEventKafkaTemplate() {
        KafkaTemplate<String, ReportLifecycleEvent> template =
                new KafkaTemplate<>(reportEventProducerFactory());
        template.setObservationEnabled(true);
        return template;
    }

    @Bean
    public DefaultErrorHandler kafkaErrorHandler(KafkaTemplate<String, Object> deadLetterKafkaTemplate) {
        int maxRetries = Math.max(0, retryMaxAttempts - 1);

        ExponentialBackOffWithMaxRetries backOff = new ExponentialBackOffWithMaxRetries(maxRetries);
        backOff.setInitialInterval(retryInitialIntervalMs);
        backOff.setMaxInterval(retryMaxIntervalMs);
        backOff.setMultiplier(retryMultiplier);

        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(deadLetterKafkaTemplate);

        DefaultErrorHandler handler = new DefaultErrorHandler(recoverer, backOff);
        handler.addNotRetryableExceptions(DeserializationException.class);
        return handler;
    }

    @Bean
    public ConsumerFactory<String, KafkaReportTriggerMessage> batchReportTriggerConsumerFactory() {
        return buildConsumerFactory(KafkaReportTriggerMessage.class);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, KafkaReportTriggerMessage>
    reportGenerationListenerContainerFactory(DefaultErrorHandler kafkaErrorHandler) {
        ConcurrentKafkaListenerContainerFactory<String, KafkaReportTriggerMessage> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(batchReportTriggerConsumerFactory());
        factory.setConcurrency(concurrency);
        factory.setBatchListener(false);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        factory.getContainerProperties().setPollTimeout(pollTimeoutMs);
        factory.setCommonErrorHandler(kafkaErrorHandler);
        return factory;
    }

    @Bean
    public ConsumerFactory<String, KafkaSingleReportTriggerMessage> singleModeReportTriggerConsumerFactory() {
        return buildConsumerFactory(KafkaSingleReportTriggerMessage.class);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, KafkaSingleReportTriggerMessage>
    singleModeReportListenerContainerFactory(DefaultErrorHandler kafkaErrorHandler) {
        ConcurrentKafkaListenerContainerFactory<String, KafkaSingleReportTriggerMessage> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(singleModeReportTriggerConsumerFactory());
        factory.setConcurrency(concurrency);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        factory.getContainerProperties().setPollTimeout(pollTimeoutMs);
        factory.setCommonErrorHandler(kafkaErrorHandler);
        return factory;
    }

    private <T> DefaultKafkaConsumerFactory<String, T> buildConsumerFactory(Class<T> targetType) {
        Map<String, Object> props = buildBaseConsumerProps();

        JacksonJsonDeserializer<T> valueDeserializer = new JacksonJsonDeserializer<>(targetType);
        valueDeserializer.ignoreTypeHeaders();
        valueDeserializer.addTrustedPackages(TRUSTED_DTO_PACKAGE);

        ErrorHandlingDeserializer<T> errorHandlingDeserializer =
                new ErrorHandlingDeserializer<>(valueDeserializer);

        DefaultKafkaConsumerFactory<String, T> factory =
                new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(), errorHandlingDeserializer);

        factory.setConfigureDeserializers(false);
        return factory;
    }

    private ProducerFactory<String, Object> deadLetterProducerFactory() {
        return buildProducerFactory(new JacksonJsonSerializer<>());
    }

    private ProducerFactory<String, ReportLifecycleEvent> reportEventProducerFactory() {
        return buildProducerFactory(new JacksonJsonSerializer<>());
    }

    private <V> DefaultKafkaProducerFactory<String, V> buildProducerFactory(Serializer<V> valueSerializer) {
        Map<String, Object> props = buildBaseProducerProps();

        DefaultKafkaProducerFactory<String, V> factory =
                new DefaultKafkaProducerFactory<>(props, new StringSerializer(), valueSerializer);

        factory.setConfigureSerializers(false);
        return factory;
    }

    private Map<String, Object> buildBaseConsumerProps() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords);
        return props;
    }

    private Map<String, Object> buildBaseProducerProps() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 120000);
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);
        return props;
    }
}