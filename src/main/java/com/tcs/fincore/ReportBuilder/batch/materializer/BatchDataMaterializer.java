package com.tcs.fincore.ReportBuilder.batch.materializer;

import com.tcs.fincore.ReportBuilder.batch.model.QueryPattern;
import com.tcs.fincore.ReportBuilder.batch.store.BatchResultStore;
import com.tcs.fincore.ReportBuilder.exception.ValidationException;
import com.tcs.fincore.ReportBuilder.util.FilterSqlBuilder;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.function.LongConsumer;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
@Component
public class BatchDataMaterializer {

    private final NamedParameterJdbcTemplate jdbc;
    private final BatchResultStore batchResultStore;

    @Value("${report.batch.materialization.parallel-threads:3}")
    private int parallelThreads;

    @Value("${report.batch.materialization.redis.flush-size:5000}")
    private int redisFlushSize;

    @Value("${report.batch.materialization.redis.flush-interval-ms:250}")
    private long redisFlushIntervalMs;

    //not final — created in @PostConstruct after @Value injection
    private ExecutorService materializationExecutor;

    public BatchDataMaterializer(NamedParameterJdbcTemplate jdbc, BatchResultStore batchResultStore) {
        this.jdbc = jdbc;
        this.batchResultStore = batchResultStore;
    }

    @PostConstruct
    public void init() {
        this.materializationExecutor = Executors.newFixedThreadPool(parallelThreads, r -> {
            Thread t = new Thread(r, "materializer-" + r.hashCode());
            t.setDaemon(true);
            return t;
        });
        log.info("BatchDataMaterializer initialized with {} parallel threads", parallelThreads);
    }

    // Executor is shut down gracefully — no thread leaks on redeploy.
    @PreDestroy
    public void destroy() {
        log.info("Shutting down materializationExecutor...");
        materializationExecutor.shutdown();
        try {
            if (!materializationExecutor.awaitTermination(30, TimeUnit.SECONDS)) {
                log.warn("Materializer executor did not finish in 30s — forcing shutdown");
                materializationExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            materializationExecutor.shutdownNow();
        }
    }


    public void materializeAll(String jobId, List<QueryPattern> patterns, long timeoutMinutes, LongConsumer rowProgressConsumer) {
        List<CompletableFuture<Void>> futures = new ArrayList<>();

        for (QueryPattern pattern : patterns) {
            CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                try {
                    materializePattern(jobId, pattern, rowProgressConsumer);
                    batchResultStore.markPatternAvailable(jobId, pattern.getPatternId());
                } catch (RuntimeException e) {
                    log.error("Failed to materialize pattern {}: {}", pattern.getPatternId(), e.getMessage(), e);
                    // Re-throw so allOf tracks failure explicitly
                    throw new CompletionException(e);
                }
            }, materializationExecutor);
            futures.add(future);
        }

        // Wait with timeout; failed patterns are already stored as empty maps above.
        try {
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get(timeoutMinutes, TimeUnit.MINUTES);
        } catch (TimeoutException te) {
            log.error("Materialization timed out after {}m — {} patterns may be incomplete", timeoutMinutes, patterns.size());
            futures.forEach(f -> f.cancel(true));
            throw new ValidationException("Data materialization timed out after " + timeoutMinutes + " minutes", te);
        } catch (ExecutionException ee) {
            // Individual pattern failures are already logged above and stored as empty maps.
            // Log aggregate summary but do NOT abort the whole batch.
            long failedCount = futures.stream().filter(CompletableFuture::isCompletedExceptionally).count();
            log.warn("{} of {} patterns failed materialization — affected specs will use fallback queries", failedCount, patterns.size());
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new ValidationException("Data materialization interrupted", ie);
        }

    }


    private void materializePattern(String jobId, QueryPattern pattern, LongConsumer rowProgressConsumer) {
        long start = System.currentTimeMillis();
        int rows = executeAndStore(pattern, jobId, rowProgressConsumer);
        log.debug("Materialized pattern {} in {}ms ({} rows)", pattern.getPatternId(), System.currentTimeMillis() - start, rows);
    }

    private int executeAndStore(QueryPattern pattern, String jobId, LongConsumer rowProgressConsumer) {
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT /*+ PARALLEL(8) */ ");

        List<String> groupByCols = new ArrayList<>();

        if (pattern.getVaryingDimension() != null) {
            String dimCol = pattern.getVaryingDimension().getColumnName();
            FilterSqlBuilder.validateIdentifier(dimCol);
            sql.append(dimCol).append(", ");
            groupByCols.add(dimCol);
        }

        if (pattern.getBulkFilters() != null) {
            for (String bulkCol : new TreeSet<>(pattern.getBulkFilters().keySet())) {
                FilterSqlBuilder.validateIdentifier(bulkCol);
                sql.append(bulkCol).append(", ");
                groupByCols.add(bulkCol);
            }
        }

        String aggFunc = pattern.getAggregationType();
        FilterSqlBuilder.validateIdentifier(pattern.getColumn());
        if ("VALUE".equals(aggFunc)) {
            sql.append(pattern.getColumn());
        } else {
            sql.append(aggFunc).append("(").append(pattern.getColumn()).append(")");
        }
        sql.append(" AS AGG_VALUE FROM ").append(pattern.getTable()).append(" WHERE 1=1 ");

        Map<String, Object> params = new HashMap<>();
        Map<String, Object> allFilters = buildUnifiedFilterMap(pattern);
        FilterSqlBuilder.appendFilters(sql, params, allFilters);

        if (!"VALUE".equals(aggFunc) && !groupByCols.isEmpty()) {
            sql.append(" GROUP BY ").append(String.join(", ", groupByCols));
        }

        log.debug("Pattern {} SQL: {} | params: {}", pattern.getPatternId(), sql, params);
        String dimColumn = pattern.getVaryingDimension() != null ? pattern.getVaryingDimension().getColumnName() : null;
        Set<String> bulkColumns = pattern.getBulkFilters() != null ? pattern.getBulkFilters().keySet() : Collections.emptySet();

        AtomicInteger rowCount = new AtomicInteger();
        BufferedResultWriter writer = writer(jobId, pattern.getPatternId());
        try {
            jdbc.query(sql.toString(), params, (RowCallbackHandler) rs -> {
                String dimValue = "DEFAULT";
                if (dimColumn != null) {
                    Object dimRaw = rs.getObject(dimColumn);
                    if (dimRaw == null) {
                        dimRaw = rs.getObject(dimColumn.toUpperCase());
                    }
                    if (dimRaw != null) {
                        dimValue = String.valueOf(dimRaw);
                    }
                }
                String bulkKey = buildBulkKey(rs, bulkColumns);
                Object value = rs.getObject("AGG_VALUE");
                if (value == null) {
                    value = rs.getObject("agg_value");
                }

                writer.buffer(dimValue, bulkKey, value);
                rowCount.incrementAndGet();
                rowProgressConsumer.accept(1L);
            });
        } finally {
            writer.flush(true);
        }
        return rowCount.get();
    }

    private BufferedResultWriter writer(String jobId, String patternId) {
        return new BufferedResultWriter(jobId, patternId, redisFlushSize, redisFlushIntervalMs);
    }

    private Map<String, Object> buildUnifiedFilterMap(QueryPattern pattern) {
        Map<String, Object> filters = new HashMap<>();

        if (pattern.getStaticFilters() != null) {
            filters.putAll(pattern.getStaticFilters());
        }

        if (pattern.getVaryingDimension() != null) {
            filters.put(pattern.getVaryingDimension().getColumnName(), Map.of("op", "IN", "value", new ArrayList<>(pattern.getVaryingDimension().getAllValues()), "dataType", pattern.getVaryingDimension().getDataType()));
        }

        if (pattern.getBulkFilters() != null) {
            pattern.getBulkFilters().forEach((col, values) -> {
                Map<String, Object> bulkIn = new HashMap<>();
                bulkIn.put("op", "IN");
                bulkIn.put("value", new ArrayList<>(values));
                bulkIn.put("dataType", "STRING");

                filters.merge(col, bulkIn, (existing, newIn) -> {
                    if (existing instanceof List<?> existingList) {
                        List<Object> list = new ArrayList<>(existingList);
                        list.add(newIn);
                        return list;
                    }
                    return List.of(existing, newIn);
                });
            });
        }

        return filters;
    }

    private String buildBulkKey(java.sql.ResultSet row, Set<String> bulkColumns) throws java.sql.SQLException {
        if (bulkColumns.isEmpty()) return "DEFAULT";

        List<String> sortedColumns = new ArrayList<>(bulkColumns);
        Collections.sort(sortedColumns);

        List<String> keyParts = new ArrayList<>(sortedColumns.size());
        for (String col : sortedColumns) {
            Object val = Optional.ofNullable(row.getObject(col)).orElse(row.getObject(col.toUpperCase()));
            keyParts.add(val != null ? String.valueOf(val) : "NULL");
        }
        return String.join(":", keyParts);
    }

    private class BufferedResultWriter {
        private final String jobId;
        private final String patternId;
        private final int flushSize;
        private final long flushIntervalMs;

        private final List<BatchResultStore.BatchWrite> pendingWrites;
        private final Set<String> seenIndexDataKeys = new HashSet<>();
        private final Set<String> pendingIndexDataKeys = new HashSet<>();
        private final AtomicLong lastFlushTs = new AtomicLong(System.currentTimeMillis());

        private BufferedResultWriter(String jobId, String patternId, int flushSize, long flushIntervalMs) {
            this.jobId = jobId;
            this.patternId = patternId;
            this.flushSize = Math.max(1000, flushSize);
            this.flushIntervalMs = Math.max(50, flushIntervalMs);
            this.pendingWrites = new ArrayList<>(this.flushSize);
        }

        private void buffer(String dimensionValue, String bulkKey, Object value) {
            pendingWrites.add(new BatchResultStore.BatchWrite(dimensionValue, bulkKey, value));

            String dataKey = String.join(":", jobId, patternId, dimensionValue);
            if (seenIndexDataKeys.add(dataKey)) {
                pendingIndexDataKeys.add(String.format("rb:batch:%s:%s:%s", jobId, patternId, dimensionValue));
            }

            if (pendingWrites.size() >= flushSize || System.currentTimeMillis() - lastFlushTs.get() >= flushIntervalMs) {
                flush(false);
            }
        }

        private void flush(boolean force) {
            if (pendingWrites.isEmpty() && !force) {
                return;
            }
            if (pendingWrites.isEmpty() && pendingIndexDataKeys.isEmpty()) {
                return;
            }

            batchResultStore.storeValues(jobId, patternId, new ArrayList<>(pendingWrites), new HashSet<>(pendingIndexDataKeys));
            pendingWrites.clear();
            pendingIndexDataKeys.clear();
            lastFlushTs.set(System.currentTimeMillis());
        }
    }
}