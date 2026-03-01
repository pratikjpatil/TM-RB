




package com.tcs.fincore.ReportBuilder.config;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.BasicPolymorphicTypeValidator;
import com.fasterxml.jackson.databind.jsontype.PolymorphicTypeValidator;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.cache.interceptor.CacheErrorHandler;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.cache.RedisCacheConfiguration;
import org.springframework.data.redis.cache.RedisCacheManager;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.serializer.GenericJackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.RedisSerializationContext;
import org.springframework.data.redis.serializer.StringRedisSerializer;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

@Slf4j
@Configuration
@EnableCaching
public class CacheConfig {

    @Bean
    public CacheManager cacheManager(RedisConnectionFactory connectionFactory) {

        // SECURITY FIX: Explicit allowlist restricts deserialization to ONLY our own model classes.
        PolymorphicTypeValidator typeValidator = BasicPolymorphicTypeValidator.builder()
                .allowIfSubType("com.tcs.fincore.ReportBuilder.model.")
                .allowIfSubType("com.tcs.fincore.ReportBuilder.dto.")
                .allowIfSubType("com.tcs.fincore.ReportBuilder.batch.model.")
                .allowIfSubType("java.util.")
                .allowIfSubType("java.time.")
                .allowIfSubType("java.lang.")
                .build();

        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());

        // REQUIRED FIX: Must use NON_FINAL so GenericJackson2JsonRedisSerializer
        // writes the @class property for concrete classes (like ReportTemplate).
        // Security is safely handled by the PolymorphicTypeValidator above.
        objectMapper.activateDefaultTyping(
                typeValidator,
                ObjectMapper.DefaultTyping.NON_FINAL,
                JsonTypeInfo.As.PROPERTY
        );

        GenericJackson2JsonRedisSerializer serializer = new GenericJackson2JsonRedisSerializer(objectMapper);

        RedisCacheConfiguration defaultConfig = RedisCacheConfiguration.defaultCacheConfig()
                .entryTtl(Duration.ofMinutes(5))
                .disableCachingNullValues()
                .serializeKeysWith(
                        RedisSerializationContext.SerializationPair.fromSerializer(
                                new StringRedisSerializer()))
                .serializeValuesWith(
                        RedisSerializationContext.SerializationPair.fromSerializer(serializer));

        Map<String, RedisCacheConfiguration> cacheConfigurations = new HashMap<>();
        cacheConfigurations.put("reportTemplates", defaultConfig.entryTtl(Duration.ofMinutes(5)));
        cacheConfigurations.put("reportVariants", defaultConfig.entryTtl(Duration.ofMinutes(5)));

        RedisCacheManager cacheManager = RedisCacheManager.builder(connectionFactory)
                .cacheDefaults(defaultConfig)
                .withInitialCacheConfigurations(cacheConfigurations)
                .transactionAware()
                .build();

        log.info("Redis cache manager initialized with secure type validation. Caches: {}", cacheConfigurations.keySet());
        return cacheManager;
    }

    @Bean
    public CacheErrorHandler errorHandler() {
        return new CacheErrorHandler() {
            @Override
            public void handleCacheGetError(RuntimeException ex, org.springframework.cache.Cache cache, Object key) {
                log.error("Cache GET error for cache='{}': {}", cache.getName(), ex.getMessage());
            }

            @Override
            public void handleCachePutError(RuntimeException ex, org.springframework.cache.Cache cache, Object key, Object value) {
                log.error("Cache PUT error for cache='{}': {}", cache.getName(), ex.getMessage());
            }

            @Override
            public void handleCacheEvictError(RuntimeException ex, org.springframework.cache.Cache cache, Object key) {
                log.error("Cache EVICT error for cache='{}': {}", cache.getName(), ex.getMessage());
            }

            @Override
            public void handleCacheClearError(RuntimeException ex, org.springframework.cache.Cache cache) {
                log.error("Cache CLEAR error for cache='{}': {}", cache.getName(), ex.getMessage());
            }
        };
    }
}