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
     * Executor for async report generation and HDFS writes
     * Bounded with backpressure via CallerRunsPolicy
     */
    @Bean(name = "reportExecutor")
    public Executor reportExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        
        // Core threads always alive
        executor.setCorePoolSize(4);
        
        // Max threads for peak load
        executor.setMaxPoolSize(20);
        
        // Queue capacity for backpressure
        executor.setQueueCapacity(100);
        
        // Thread naming for debugging
        executor.setThreadNamePrefix("report-async-");
        
        //  CRITICAL: CallerRunsPolicy provides backpressure
        // If queue is full, the calling thread executes the task (slows down submission)
        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        
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