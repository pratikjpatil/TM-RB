




package com.tcs.fincore.ReportBuilder.batch.materializer;

import com.tcs.fincore.ReportBuilder.batch.model.QueryPattern;
import com.tcs.fincore.ReportBuilder.exception.ValidationException;
import com.tcs.fincore.ReportBuilder.util.FilterSqlBuilder;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.*;

@Slf4j
@Component
public class BatchDataMaterializer {

    private final NamedParameterJdbcTemplate jdbc;

    // FIX #7: read from application.properties instead of hardcoded 3
    @Value("${report.batch.materialization.parallel-threads:3}")
    private int parallelThreads;

    // FIX #7: not final — created in @PostConstruct after @Value injection
    private ExecutorService materializationExecutor;

    public BatchDataMaterializer(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    @PostConstruct
    public void init() {
        this.materializationExecutor = Executors.newFixedThreadPool(
                parallelThreads,
                r -> {
                    Thread t = new Thread(r, "materializer-" + r.hashCode());
                    t.setDaemon(true);
                    return t;
                }
        );
        log.info("BatchDataMaterializer initialized with {} parallel threads", parallelThreads);
    }

    // FIX #7: Executor is shut down gracefully — no thread leaks on redeploy.
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

    // -----------------------------------------------------------------------

    public Map<String, Map<String, Map<String, Object>>> materializeAll(
            List<QueryPattern> patterns, long timeoutMinutes) {

        Map<String, Map<String, Map<String, Object>>> allResults = new ConcurrentHashMap<>();
        List<CompletableFuture<Void>> futures = new ArrayList<>();

        for (QueryPattern pattern : patterns) {
            CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                try {
                    Map<String, Map<String, Object>> res = materializePattern(pattern);
                    allResults.put(pattern.getPatternId(), res);
                } catch (Exception e) {
                    log.error("Failed to materialize pattern {}: {}",
                            pattern.getPatternId(), e.getMessage(), e);
                    // Store empty result so distributor falls back gracefully for this pattern.
                    allResults.put(pattern.getPatternId(), Collections.emptyMap());
                    // Re-throw so allOf tracks failure explicitly
                    throw new CompletionException(e);
                }
            }, materializationExecutor);
            futures.add(future);
        }

        // Wait with timeout; failed patterns are already stored as empty maps above.
        try {
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                    .get(timeoutMinutes, TimeUnit.MINUTES);
        } catch (TimeoutException te) {
            log.error("Materialization timed out after {}m — {} patterns may be incomplete",
                    timeoutMinutes, patterns.size() - allResults.size());
            futures.forEach(f -> f.cancel(true));
            throw new ValidationException("Data materialization timed out after "
                    + timeoutMinutes + " minutes", te);
        } catch (ExecutionException ee) {
            // Individual pattern failures are already logged above and stored as empty maps.
            // Log aggregate summary but do NOT abort the whole batch.
            long failedCount = futures.stream().filter(CompletableFuture::isCompletedExceptionally).count();
            log.warn("{} of {} patterns failed materialization — affected specs will use fallback queries",
                    failedCount, patterns.size());
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new ValidationException("Data materialization interrupted", ie);
        }

        return allResults;
    }

    // -----------------------------------------------------------------------

    private Map<String, Map<String, Object>> materializePattern(QueryPattern pattern) {
        long start = System.currentTimeMillis();
        List<Map<String, Object>> rows = executeQuery(pattern);
        Map<String, Map<String, Object>> nestedMap = transformResults(rows, pattern);
        log.debug("Materialized pattern {} in {}ms ({} rows)",
                pattern.getPatternId(), System.currentTimeMillis() - start, rows.size());
        return nestedMap;
    }

    private List<Map<String, Object>> executeQuery(QueryPattern pattern) {
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
        return jdbc.queryForList(sql.toString(), params);
    }

    private Map<String, Object> buildUnifiedFilterMap(QueryPattern pattern) {
        Map<String, Object> filters = new HashMap<>();

        if (pattern.getStaticFilters() != null) {
            filters.putAll(pattern.getStaticFilters());
        }

        if (pattern.getVaryingDimension() != null) {
            filters.put(pattern.getVaryingDimension().getColumnName(), Map.of(
                    "op", "IN",
                    "value", new ArrayList<>(pattern.getVaryingDimension().getAllValues()),
                    "dataType", pattern.getVaryingDimension().getDataType()
            ));
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

    private Map<String, Map<String, Object>> transformResults(
            List<Map<String, Object>> rows, QueryPattern pattern) {
        Map<String, Map<String, Object>> result = new HashMap<>();
        String dimColumn = pattern.getVaryingDimension() != null
                ? pattern.getVaryingDimension().getColumnName() : null;
        Set<String> bulkColumns = pattern.getBulkFilters() != null
                ? pattern.getBulkFilters().keySet() : Collections.emptySet();

        for (Map<String, Object> row : rows) {

            // FIX #5 defence: normalise column name to upper-case for Oracle compatibility.
            String dimValue = dimColumn != null
                    ? String.valueOf(row.getOrDefault(dimColumn,
                    row.getOrDefault(dimColumn.toUpperCase(), "DEFAULT")))
                    : "DEFAULT";
            String bulkKey = buildBulkKey(row, bulkColumns);
            result.computeIfAbsent(dimValue, k -> new HashMap<>())
                    .put(bulkKey, row.get("AGG_VALUE"));
        }
        return result;
    }

    private String buildBulkKey(Map<String, Object> row, Set<String> bulkColumns) {
        if (bulkColumns.isEmpty()) return "DEFAULT";

        List<String> sortedColumns = new ArrayList<>(bulkColumns);
        Collections.sort(sortedColumns);

        List<String> keyParts = new ArrayList<>(sortedColumns.size());
        for (String col : sortedColumns) {
            Object val = row.getOrDefault(col, row.get(col.toUpperCase()));
            keyParts.add(val != null ? String.valueOf(val) : "NULL");
        }
        return String.join(":", keyParts);
    }
}