package com.tcs.fincore.ReportBuilder.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.Executor;
import java.util.concurrent.ThreadPoolExecutor;

/**
 *  NEW: Async configuration for non-blocking HDFS operations
 */
@Configuration
@EnableAsync
@Slf4j
public class AsyncConfig {

    /**
     * Executor for async report generation and HDFS writes.
     *
     * <p>Uses {@link ThreadPoolExecutor.AbortPolicy} so HTTP request threads are never used to
     * execute report work when the pool is saturated (which would make async endpoints appear
     * synchronous).
     */
    @Bean(name = "reportExecutor")
    public Executor reportExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        
        // Core threads always alive
        executor.setCorePoolSize(4);
        
        // Max threads for peak load
        executor.setMaxPoolSize(60);
        
        // Queue capacity for backpressure
        executor.setQueueCapacity(100);
        
        // Thread naming for debugging
        executor.setThreadNamePrefix("report-async-");
        
        // Fail fast when saturated instead of running on the caller thread.
        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.AbortPolicy());
        
        // Graceful shutdown
        executor.setWaitForTasksToCompleteOnShutdown(true);
        executor.setAwaitTerminationSeconds(60);
        
        executor.initialize();
        
        log.info("Initialized async report executor: core={}, max={}, queue={}", 
            executor.getCorePoolSize(), 
            executor.getMaxPoolSize(),
            executor.getQueueCapacity());
        
        return executor;
    }
}
