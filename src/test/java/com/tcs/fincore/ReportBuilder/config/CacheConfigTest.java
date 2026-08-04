package com.tcs.fincore.ReportBuilder.config;

import org.junit.jupiter.api.Test;
import org.springframework.cache.CacheManager;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.RedisClusterConnection;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisSentinelConnection;

import static org.assertj.core.api.Assertions.assertThat;

class CacheConfigTest {

    @Test
    void cacheManagerDefinesExpectedCachesWithoutConnectingToRedis() {
        CacheManager cacheManager = new CacheConfig().cacheManager(new NonConnectingRedisConnectionFactory());

        assertThat(cacheManager.getCache("reportTemplates")).isNotNull();
        assertThat(cacheManager.getCache("reportVariants")).isNotNull();
    }

    private static final class NonConnectingRedisConnectionFactory implements RedisConnectionFactory {
        @Override
        public RedisConnection getConnection() {
            throw new UnsupportedOperationException("Test connection factory must not be used");
        }

        @Override
        public RedisClusterConnection getClusterConnection() {
            throw new UnsupportedOperationException("Test connection factory must not be used");
        }

        @Override
        public RedisSentinelConnection getSentinelConnection() {
            throw new UnsupportedOperationException("Test connection factory must not be used");
        }

        @Override
        public boolean getConvertPipelineAndTxResults() {
            return true;
        }

        @Override
        public DataAccessException translateExceptionIfPossible(RuntimeException ex) {
            return null;
        }
    }
}
