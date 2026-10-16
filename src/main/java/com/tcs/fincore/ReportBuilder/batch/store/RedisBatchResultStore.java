package com.tcs.fincore.ReportBuilder.batch.store;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.json.JsonMapper;

import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Redis-backed {@link BatchResultStore}.
 *
 * <h3>TTL strategy — "the entire batch expires together"</h3>
 * <ol>
 *   <li>{@link #initializeJob} computes one absolute expiry epoch
 *       ({@code now + ttl}) and stores it in-process.</li>
 *   <li>Every Redis key written for that job — data hashes, job-index,
 *       pattern-index — receives the <em>same</em> {@code EXPIREAT} timestamp.
 *       All keys disappear at the same wall-clock moment regardless of when
 *       each hash was first written.</li>
 *   <li>All {@code EXPIREAT} commands are emitted <em>inside</em> the same
 *       pipeline as the corresponding write commands — <strong>zero extra
 *       network round-trips</strong>.</li>
 *   <li>For bulk writes, {@code EXPIREAT} is applied only to keys appearing in
 *       {@code indexDataKeys} (newly registered data keys), not to every
 *       {@link BatchWrite} row — O(unique dimension values), not O(rows).</li>
 *   <li>{@link #storeValue} previously issued two separate round-trips (HSET +
 *       SADD); it is now pipelined into one (HSET + SADD + 2× EXPIREAT).</li>
 * </ol>
 */
@Component
@ConditionalOnProperty(name = "report.batch.result-store", havingValue = "redis")
public class RedisBatchResultStore implements BatchResultStore {

    // ── Key templates ─────────────────────────────────────────────────────────
    private static final String JOB_INDEX_KEY     = "rb:batch:%s:keys";
    private static final String PATTERN_INDEX_KEY = "rb:batch:%s:patterns";
    private static final String DATA_KEY          = "rb:batch:%s:%s:%s";

    // ── Configuration ─────────────────────────────────────────────────────────
    private static final int  DEFAULT_MAX_RETRIES = 2;
    /** Safety-net TTL used when initializeJob was not called on this instance. */
    private static final long DEFAULT_TTL_SECONDS = Duration.ofHours(6).getSeconds();

    // ── Value-serialization prefixes ──────────────────────────────────────────
    private static final String NULL_PREFIX    = "NUL:";
    private static final String NUMBER_PREFIX  = "NUM:";
    private static final String BOOLEAN_PREFIX = "BOOL:";
    private static final String STRING_PREFIX  = "STR:";
    private static final String JSON_PREFIX    = "JSON:";

    // ── Dependencies ──────────────────────────────────────────────────────────
    private final StringRedisTemplate redisTemplate;
    private final JsonMapper          jsonMapper;

    /**
     * Maps jobId → absolute Unix expiry epoch (seconds).
     * Populated on {@link #initializeJob}; removed on {@link #cleanupJob}.
     * Memory cost: ~100 bytes per active job; never a practical concern.
     */
    private final ConcurrentHashMap<String, Long> jobExpiryEpochMap = new ConcurrentHashMap<>();

    // ── Metrics ───────────────────────────────────────────────────────────────
    private final Counter rowsBufferedCounter;
    private final Counter pipelineFlushCounter;
    private final Counter redisWriteFailureCounter;
    private final Counter redisWriteRetryCounter;
    private final Timer   pipelineFlushLatency;

    public RedisBatchResultStore(StringRedisTemplate redisTemplate,
                                 JsonMapper jsonMapper,
                                 MeterRegistry meterRegistry) {
        this.redisTemplate            = redisTemplate;
        this.jsonMapper               = jsonMapper;
        this.rowsBufferedCounter      = meterRegistry.counter("report.batch.redis.rows.buffered");
        this.pipelineFlushCounter     = meterRegistry.counter("report.batch.redis.pipeline.flush.count");
        this.redisWriteFailureCounter = meterRegistry.counter("report.batch.redis.write.failures");
        this.redisWriteRetryCounter   = meterRegistry.counter("report.batch.redis.write.retries");
        this.pipelineFlushLatency     = Timer.builder("report.batch.redis.pipeline.flush.latency")
                .description("Latency for batched Redis pipeline writes")
                .register(meterRegistry);
    }

    // =========================================================================
    // Lifecycle
    // =========================================================================

    /**
     * Must be called once before any writes.
     *
     * <p>Computes and caches the job's absolute expiry epoch ({@code now + ttl}).
     * The two {@code EXPIREAT} calls here are best-effort — the index keys may
     * not yet exist; authoritative TTL is applied at each key's first write.
     */
    @Override
    public void initializeJob(String jobId) {
//        long   expiryEpoch = Instant.now().plusSeconds(ttl.getSeconds()).getEpochSecond();
        long expiryEpoch = Instant.now().plusSeconds(DEFAULT_TTL_SECONDS).getEpochSecond();
        Date   expiryDate  = Date.from(Instant.ofEpochSecond(expiryEpoch));

        jobExpiryEpochMap.put(jobId, expiryEpoch);

        // Best-effort: pre-set TTL if index keys already exist (e.g. partial restart).
        // Definitive EXPIREAT is set at each key's first write.
        redisTemplate.expireAt(String.format(JOB_INDEX_KEY,     jobId), expiryDate);
        redisTemplate.expireAt(String.format(PATTERN_INDEX_KEY, jobId), expiryDate);
    }

    @Override
    public void cleanupJob(String jobId) {
        jobExpiryEpochMap.remove(jobId);

        String       jobIndex = String.format(JOB_INDEX_KEY, jobId);
        Set<String>  dataKeys = redisTemplate.opsForSet().members(jobIndex);

        if (!CollectionUtils.isEmpty(dataKeys)) {
            redisTemplate.delete(dataKeys);
        }
        redisTemplate.delete(jobIndex);
        redisTemplate.delete(String.format(PATTERN_INDEX_KEY, jobId));
    }

    // =========================================================================
    // Writes
    // =========================================================================

    /**
     * Single-value write — now a <strong>single</strong> pipelined round-trip.
     *
     * <p>Previously: HSET (RTT 1) + SADD (RTT 2) = 2 round-trips, no TTL.
     * <br>Now: HSET + SADD + EXPIREAT(jobIndex) + EXPIREAT(dataKey) = 1 round-trip, TTL set.
     */
    @Override
    public void storeValue(String jobId, String patternId, String dimensionValue,
                           String bulkKey, Object value) {
        String dataKey    = String.format(DATA_KEY,      jobId, patternId, dimensionValue);
        String jobIndex   = String.format(JOB_INDEX_KEY, jobId);
        long   epoch      = resolveExpiryEpoch(jobId);
        String serialized = serializeValue(value);

        redisTemplate.executePipelined((RedisConnection conn) -> {
            byte[] dataKeyBytes  = serialize(dataKey);
            byte[] jobIndexBytes = serialize(jobIndex);

            conn.hashCommands().hSet(dataKeyBytes, serialize(bulkKey), serialize(serialized));
            conn.setCommands().sAdd(jobIndexBytes, dataKeyBytes);
            // EXPIREAT is idempotent with the same epoch — safe to call on every storeValue.
            conn.keyCommands().expireAt(jobIndexBytes, epoch);
            conn.keyCommands().expireAt(dataKeyBytes,  epoch);
            return null;
        });
    }

    /**
     * Bulk pipelined write.
     *
     * @param writes        rows to persist; each maps to an HSET on its data key.
     * @param indexDataKeys data key strings being registered in the job index for
     *                      the <em>first time</em> in this flush (caller deduplicates).
     *                      {@code EXPIREAT} is applied only to these — O(unique
     *                      dimension values), not O(rows).
     */
    @Override
    public void storeValues(String jobId, String patternId,
                            List<BatchWrite> writes, Set<String> indexDataKeys) {
        if (CollectionUtils.isEmpty(writes)) {
            return;
        }

        rowsBufferedCounter.increment(writes.size());
        long startNanos = System.nanoTime();

        executePipelinedWithRetry(
                jobId, patternId, writes, indexDataKeys,
                String.format(JOB_INDEX_KEY, jobId),
                resolveExpiryEpoch(jobId));

        pipelineFlushCounter.increment();
        pipelineFlushLatency.record(System.nanoTime() - startNanos, TimeUnit.NANOSECONDS);
    }

    // =========================================================================
    // Reads
    // =========================================================================

    @Override
    public Map<String, Object> fetchValues(String jobId, String patternId,
                                           String dimensionValue, Set<String> keys) {
        String dataKey = String.format(DATA_KEY, jobId, patternId, dimensionValue);

        if (keys == null || keys.isEmpty()) {
            Map<Object, Object> entries = redisTemplate.opsForHash().entries(dataKey);
            Map<String, Object> all     = new HashMap<>(entries.size());
            entries.forEach((k, v) ->
                    all.put(String.valueOf(k), deserializeValue(String.valueOf(v))));
            return all;
        }

        List<Object>      raw    = redisTemplate.opsForHash()
                .multiGet(dataKey, keys.stream().map(k -> (Object) k).toList());
        Map<String, Object> result = new HashMap<>(keys.size());
        int i = 0;
        for (String key : keys) {
            Object val = raw.get(i++);
            if (val != null) {
                result.put(key, deserializeValue(String.valueOf(val)));
            }
        }
        return result;
    }

    @Override
    public boolean hasPattern(String jobId, String patternId) {
        return Boolean.TRUE.equals(
                redisTemplate.opsForSet().isMember(
                        String.format(PATTERN_INDEX_KEY, jobId), patternId));
    }

    /**
     * Pipelines SADD + EXPIREAT in one round-trip (previously two separate calls).
     */
    @Override
    public void markPatternAvailable(String jobId, String patternId) {
        String patternIndex = String.format(PATTERN_INDEX_KEY, jobId);
        long   epoch        = resolveExpiryEpoch(jobId);

        redisTemplate.executePipelined((RedisConnection conn) -> {
            byte[] patternIndexBytes = serialize(patternIndex);
            conn.setCommands().sAdd(patternIndexBytes, serialize(patternId));
            conn.keyCommands().expireAt(patternIndexBytes, epoch);
            return null;
        });
    }

    // =========================================================================
    // Core pipeline
    // =========================================================================

    private void executePipelinedWithRetry(String jobId,
                                           String patternId,
                                           List<BatchWrite> writes,
                                           Set<String> indexDataKeys,
                                           String jobIndex,
                                           long expiryEpoch) {
        RuntimeException lastError = null;

        for (int attempt = 0; attempt <= DEFAULT_MAX_RETRIES; attempt++) {
            try {
                redisTemplate.executePipelined((RedisConnection conn) -> {

                    // ── Bulk HSET: O(n rows) — no EXPIREAT per row ─────────
                    for (BatchWrite write : writes) {
                        String dataKey = String.format(DATA_KEY,
                                jobId, patternId, write.dimensionValue());
                        conn.hashCommands().hSet(
                                serialize(dataKey),
                                serialize(write.bulkKey()),
                                serialize(serializeValue(write.value())));
                    }

                    // ── Index registration + EXPIREAT for new data keys ────
                    // O(unique dimension values per flush) — typically << O(rows).
                    // EXPIREAT rides in the same pipeline: zero extra network RTT.
                    if (!CollectionUtils.isEmpty(indexDataKeys)) {
                        byte[]   indexKeyBytes = serialize(jobIndex);
                        byte[][] members       = indexDataKeys.stream()
                                .map(this::serialize)
                                .toArray(byte[][]::new);

                        conn.setCommands().sAdd(indexKeyBytes, members);
                        // Refresh job-index TTL to the shared epoch on every flush.
                        conn.keyCommands().expireAt(indexKeyBytes, expiryEpoch);

                        // Set TTL once per newly tracked data key — same absolute epoch,
                        // so all hashes under this job expire at the same wall-clock time.
                        for (String dk : indexDataKeys) {
                            conn.keyCommands().expireAt(serialize(dk), expiryEpoch);
                        }
                    }

                    return null;
                });
                return; // success

            } catch (RuntimeException ex) {
                redisWriteFailureCounter.increment();
                lastError = ex;
                if (attempt < DEFAULT_MAX_RETRIES) {
                    redisWriteRetryCounter.increment();
                }
            }
        }

        throw new IllegalStateException(
                "Failed to write batch values to Redis after "
                        + (DEFAULT_MAX_RETRIES + 1) + " attempts",
                lastError);
    }

    // =========================================================================
    // Internal helpers
    // =========================================================================

    /**
     * Returns the absolute expiry epoch (Unix seconds) for a given job.
     * {@code computeIfAbsent} creates a safe fallback when the job was never
     * initialized through this instance — e.g., during failover replay.
     */
    private long resolveExpiryEpoch(String jobId) {
        return jobExpiryEpochMap.computeIfAbsent(
                jobId,
                id -> Instant.now().plusSeconds(DEFAULT_TTL_SECONDS).getEpochSecond());
    }

    private byte[] serialize(String value) {
        return redisTemplate.getStringSerializer().serialize(value);
    }

    private String serializeValue(Object value) {
        if (value == null)              return NULL_PREFIX;
        if (value instanceof Number)    return NUMBER_PREFIX  + value;
        if (value instanceof Boolean)   return BOOLEAN_PREFIX + value;
        if (value instanceof String s)  return STRING_PREFIX  + s;
        try {
            return JSON_PREFIX + jsonMapper.writeValueAsString(value);
        } catch (JacksonException e) {
            throw new IllegalStateException("Failed to serialize batch value", e);
        }
    }

    private Object deserializeValue(String value) {
        if (value == null || value.startsWith(NULL_PREFIX))    return null;
        if (value.startsWith(NUMBER_PREFIX))  return parseNumber(value.substring(NUMBER_PREFIX.length()));
        if (value.startsWith(BOOLEAN_PREFIX)) return Boolean.parseBoolean(value.substring(BOOLEAN_PREFIX.length()));
        if (value.startsWith(STRING_PREFIX))  return value.substring(STRING_PREFIX.length());
        if (value.startsWith(JSON_PREFIX))    return deserializeJsonValue(value.substring(JSON_PREFIX.length()));

        // Backward compatibility: raw values stored before type-prefix encoding
        if ("null".equalsIgnoreCase(value)) return null;
        Object legacyNumber = parseNumber(value);
        if (legacyNumber != null) return legacyNumber;
        if ("true".equalsIgnoreCase(value) || "false".equalsIgnoreCase(value)) {
            return Boolean.parseBoolean(value);
        }
        return deserializeJsonValue(value);
    }

    private Object deserializeJsonValue(String value) {
        try {
            return jsonMapper.readValue(value, Object.class);
        } catch (JacksonException e) {
            return value; // treat un-parseable JSON as a plain string
        }
    }

    private Number parseNumber(String value) {
        if (value == null || value.isBlank()) return null;
        try {
            return (value.indexOf('.') >= 0 || value.indexOf('e') >= 0 || value.indexOf('E') >= 0)
                    ? Double.parseDouble(value)
                    : Long.parseLong(value);
        } catch (NumberFormatException ignored) {
            return null;
        }
    }
}