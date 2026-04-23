package com.tcs.fincore.ReportBuilder.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.ThreadPoolExecutor;

/**
 *  Async configuration for non-blocking HDFS operations
 */
@Configuration
@EnableAsync
@Slf4j
public class AsyncConfig {

    @Value("${spring.task.execution.pool.core-size:4}")
    private int corePoolSize;

    @Value("${spring.task.execution.pool.max-size:50}")
    private int maxPoolSize;

    @Value("${spring.task.execution.pool.queue-capacity:100}")
    private int queueCapacity;

    @Value("${spring.task.execution.thread-name-prefix:report-async-}")
    private String threadNamePrefix;

    /**
     * Executor for async report generation and HDFS writes.
     *
     * <p>Uses {@link ThreadPoolExecutor.AbortPolicy} so HTTP request threads are never used to
     * execute report work when the pool is saturated (which would make async endpoints appear
     * synchronous).
     */
    @Bean(name = "reportExecutor")
    public ThreadPoolTaskExecutor reportExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();

        // Core threads always alive
        executor.setCorePoolSize(corePoolSize);

        // Max threads for peak load
        executor.setMaxPoolSize(maxPoolSize);

        // Queue capacity for backpressure
        executor.setQueueCapacity(queueCapacity);

        // Thread naming for debugging
        executor.setThreadNamePrefix(threadNamePrefix);

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

    /**
     * Dedicated executor for batch report generation fan-out.
     * Isolates single-mode async workloads from batch workloads.
     */
    @Bean(name = "batchReportGenerationExecutor")
    public ThreadPoolTaskExecutor batchReportGenerationExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(10);
        executor.setMaxPoolSize(60);
        executor.setQueueCapacity(1000);
        executor.setThreadNamePrefix("batch-report-gen-");
        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        executor.setWaitForTasksToCompleteOnShutdown(true);
        executor.setAwaitTerminationSeconds(60);
        executor.initialize();

        log.info("Initialized batch report generation executor: core={}, max={}, queue={}",
                executor.getCorePoolSize(), executor.getMaxPoolSize(), executor.getQueueCapacity());
        return executor;
    }
}