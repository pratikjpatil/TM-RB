package com.tcs.fincore.ReportBuilder.service.aggregation;

import com.tcs.fincore.ReportBuilder.exception.ValidationException;
import com.tcs.fincore.ReportBuilder.service.db.DbResolver;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

/**
 *Proper error handling, bounded executor, and guaranteed semaphore cleanup
 */
@Slf4j
@Service
public class AggregatorServiceImpl implements AggregatorService {

    private final DbResolver dbResolver;
    private final ExecutorService exec;
    private final Semaphore dbLimiter;
    private final long globalTimeoutMs;
    private final int maxConcurrentQueries;


    // ============================================================
// FILE: AggregatorServiceImpl.java  — FIX #24
// Semaphore permit count aligned with the actual thread pool
// max size so the semaphore provides genuine back-pressure.
// ============================================================

    public AggregatorServiceImpl(
            DbResolver dbResolver,
            @Value("${report.aggregation.global-timeout-ms:1800000}") long globalTimeoutMs,
            @Value("${report.aggregation.max-concurrent-queries:50}")  int maxConcurrentQueries,
            @Value("${spring.datasource.hikari.maximum-pool-size:60}") int maxDbConnections) {

        this.dbResolver = dbResolver;
        this.globalTimeoutMs = globalTimeoutMs;

        // FIX #24: derive ONE limit used by BOTH the thread pool AND the semaphore,
        // so they are always in sync. Leave 10 connections free for other operations.
        int safeLimit = Math.max(1, Math.min(maxConcurrentQueries, maxDbConnections - 10));
        this.maxConcurrentQueries = safeLimit;

        this.exec = new ThreadPoolExecutor(
                Math.min(10, safeLimit),  // core threads
                safeLimit,                 // max threads = semaphore permits
                60L, TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(1000),
                new ThreadPoolExecutor.CallerRunsPolicy()
        );

        // FIX #24: semaphore size now matches the thread pool max exactly.
        this.dbLimiter = new Semaphore(safeLimit, true);

        log.info("AggregatorService: safeLimit={}, timeout={}ms", safeLimit, globalTimeoutMs);
    }

    @Override
    public Map<String, Object> fetchAggregates(List<DataSourceSpec> specs) {
        if (specs == null || specs.isEmpty()) {
            return Collections.emptyMap();
        }

        // Added limit check to prevent memory explosion
        if (specs.size() > 5000) {
            throw new ValidationException(
                    "Too many data specs (" + specs.size() + "). Maximum allowed: 5000. " +
                            "Consider redesigning template or splitting into multiple reports.");
        }

        Instant start = Instant.now();
        Map<String, String> contextMap = MDC.getCopyOfContextMap();

        // 1. Deduplication
        Map<String, List<DataSourceSpec>> grouped = specs.stream()
                .collect(Collectors.groupingBy(DataSourceSpec::logicalKey));

        log.info("Starting Aggregation: {} total specs condensed into {} unique queries",
                specs.size(), grouped.size());

        // 2. Async Execution with Throttling and Proper Error Handling
        List<CompletableFuture<Map.Entry<String, Object>>> futures = new ArrayList<>();

        for (Map.Entry<String, List<DataSourceSpec>> entry : grouped.entrySet()) {
            String key = entry.getKey();
            DataSourceSpec spec = entry.getValue().get(0);

            CompletableFuture<Map.Entry<String, Object>> future = CompletableFuture.supplyAsync(() -> {
                if (contextMap != null) {
                    MDC.setContextMap(contextMap);
                }

                long queueStart = System.currentTimeMillis();
                boolean permitAcquired = false;

                try {
                    // --- GATEKEEPER START ---
                    dbLimiter.acquire();
                    permitAcquired = true;
                    long queueTime = System.currentTimeMillis() - queueStart;

                    long execStart = System.currentTimeMillis();

                    // Execute Query
                    Object result = dbResolver.resolve(spec);

                    long execTime = System.currentTimeMillis() - execStart;

                    // Detailed Logging for Optimization Analysis
                    if (log.isDebugEnabled()) {
                        log.debug("Query OK: Key={} | QueueTime={}ms | ExecTime={}ms | Result={}",
                                spec.getSpecId(), queueTime, execTime, summarizeResult(result));
                    }

                    return Map.entry(key, result == null ? "NULL_RESULT" : result);

                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    log.error("Query interrupted: specId={}", spec.getSpecId());
                    throw new CompletionException("Query interrupted: " + spec.getSpecId(), ie);

                } catch (RuntimeException e) {
                    //Proper error logging instead of silent failure
                    log.error("Query FAILED: specId={} table={} column={} error={}",
                            spec.getSpecId(), spec.getTable(), spec.getColumn(), e.getMessage());

                    // Return error marker instead of crashing entire batch
                    return Map.entry(key, "ERROR: " + e.getMessage());
                } finally {
                    //Guaranteed semaphore release
                    if (permitAcquired) {
                        dbLimiter.release();
                    }
                    MDC.clear();
                }
            }, exec);

            futures.add(future);
        }

        // 3. Robust Timeout Handling with Proper Cleanup
        CompletableFuture<Void> allFutures = CompletableFuture.allOf(
                futures.toArray(new CompletableFuture[0])
        );

        Map<String, Object> finalMap = new HashMap<>();
        int successCount = 0;
        int failedCount = 0;
        int timeoutCount = 0;

        try {
            allFutures.get(globalTimeoutMs, TimeUnit.MILLISECONDS);

        } catch (TimeoutException e) {
            log.error("Global Timeout hit after {}ms! Cancelling pending tasks.", globalTimeoutMs);
            timeoutCount = (int) futures.stream().filter(f -> !f.isDone()).count();

            // Cancel all pending futures
            futures.forEach(f -> f.cancel(true));

            //Force cleanup of any stuck semaphore permits
            int availablePermits = dbLimiter.availablePermits();
            int expectedPermits = maxConcurrentQueries;
            if (availablePermits < expectedPermits) {
                log.warn("Semaphore leak detected. Available: {}, Expected: {}. Forcing cleanup.",
                        availablePermits, expectedPermits);
                dbLimiter.drainPermits();
                dbLimiter.release(expectedPermits);
            }

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Batch execution interrupted", e);
            futures.forEach(f -> f.cancel(true));
        } catch (ExecutionException e) {
            log.error("Batch execution failed", e);
        } catch (RuntimeException e) {
            log.error("Batch execution failed", e);
        }

        // 4. Collect Results with Error Tracking
        for (var future : futures) {
            try {
                if (!future.isCompletedExceptionally() && !future.isCancelled() && future.isDone()) {
                    Map.Entry<String, Object> entry = future.getNow(null);
                    if (entry != null) {
                        Object val = entry.getValue();

                        // Check for error markers
                        if (val instanceof String s && s.startsWith("ERROR:")) {
                            failedCount++;
                            log.warn("Query produced error: {}", s);
                            val = null; // Treat as null to prevent downstream crashes
                        } else if ("NULL_RESULT".equals(val)) {
                            val = null;
                        }

                        // Propagate result to all duplicate specs
                        for (DataSourceSpec s : grouped.get(entry.getKey())) {
                            finalMap.put(s.getSpecId(), val);
                        }
                        successCount++;
                    }
                }
            } catch (RuntimeException ex) {
                //Log exception instead of silent catch
                log.error("Error collecting future result: {}", ex.getMessage());
                failedCount++;
            }
        }

        long elapsedMs = Duration.between(start, Instant.now()).toMillis();

        //Comprehensive result logging
        log.info("fetch Complete: {}ms | Success: {} | Failed: {} | Timeout: {} | Total: {}",
                elapsedMs, successCount, failedCount, timeoutCount, grouped.size());

        //Fail fast if too many queries failed
        if (failedCount > grouped.size() * 0.1) { // More than 10% failed
            throw new ValidationException(String.format(
                    "Report generation failed: %d/%d queries failed (%.1f%%). Check logs for details.",
                    failedCount, grouped.size(), (failedCount * 100.0 / grouped.size())));
        }

        if (timeoutCount > 0) {
            log.warn("{} queries timed out. Consider: 1) Adding DB indexes, 2) Increasing timeout, 3) Reducing data scope",
                    timeoutCount);
        }

        return finalMap;
    }

    @PreDestroy
    public void close() {
        log.info("Shutting down AggregatorService executor...");
        exec.shutdown();
        try {
            if (!exec.awaitTermination(30, TimeUnit.SECONDS)) {
                log.warn("Executor did not terminate gracefully, forcing shutdown");
                exec.shutdownNow();
            }
        } catch (InterruptedException e) {
            log.error("Executor shutdown interrupted", e);
            exec.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    private String summarizeResult(Object o) {
        if (o == null) return "null";
        if (o instanceof Collection<?> c) return "Collection(size=" + c.size() + ")";
        if (o instanceof String s) return s.length() > 50 ? s.substring(0, 47) + "..." : s;
        return String.valueOf(o);
    }
}
