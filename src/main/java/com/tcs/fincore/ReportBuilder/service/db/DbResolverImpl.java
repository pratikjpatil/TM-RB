package com.tcs.fincore.ReportBuilder.service.db;

import com.tcs.fincore.ReportBuilder.exception.ValidationException;
import com.tcs.fincore.ReportBuilder.service.aggregation.DataSourceSpec;
import com.tcs.fincore.ReportBuilder.util.FilterSqlBuilder;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@Repository
@RequiredArgsConstructor
public class DbResolverImpl implements DbResolver {

    private final NamedParameterJdbcTemplate jdbc;

    @Override
    public Object resolve(DataSourceSpec spec) {
        // 1. Validation
        FilterSqlBuilder.validateIdentifier(spec.getTable());
        FilterSqlBuilder.validateIdentifier(spec.getColumn());

        Map<String, Object> params = new HashMap<>();
        StringBuilder where = new StringBuilder(" WHERE 1=1 ");

        // 2. Build filters
        FilterSqlBuilder.appendFilters(where, params, spec.getFilters());

        try {
            // Log the attempt (Debug level to avoid flooding unless enabled)
            if (log.isDebugEnabled()) {
                log.debug("Resolve Spec: [{}] Table: [{}] Column: [{}] Filters: {}",
                        spec.getSpecId(), spec.getTable(), spec.getColumn(), params);
            }

            return switch (spec.getType()) {
                case DB_SUM -> {
                    String sql = "SELECT COALESCE(SUM(" + spec.getColumn() + "),0) FROM " +
                            spec.getTable() + where;

                    if (log.isDebugEnabled()) log.debug("SQL: {} | Params: {}", sql, params);

                    Number res = jdbc.queryForObject(sql, params, Number.class);
                    yield res == null ? 0.0 : res.doubleValue();
                }
                case DB_COUNT -> {
                    String sql = "SELECT COUNT(" + spec.getColumn() + ") FROM " +
                            spec.getTable() + where;
                    Number res = jdbc.queryForObject(sql, params, Number.class);
                    yield res == null ? 0L : res.longValue();
                }
                case DB_AVG -> {
                    String sql = "SELECT COALESCE(AVG(" + spec.getColumn() + "),0) FROM " +
                            spec.getTable() + where;
                    Number res = jdbc.queryForObject(sql, params, Number.class);
                    yield res == null ? 0.0 : res.doubleValue();
                }
                case DB_MIN -> {
                    String sql = "SELECT MIN(" + spec.getColumn() + ") FROM " +
                            spec.getTable() + where;
                    yield jdbc.queryForObject(sql, params, Object.class);
                }
                case DB_MAX -> {
                    String sql = "SELECT MAX(" + spec.getColumn() + ") FROM " +
                            spec.getTable() + where;
                    yield jdbc.queryForObject(sql, params, Object.class);
                }
                case DB_TEXT, DB_DATE, DB_VALUE -> {
                    String sql = "SELECT " + spec.getColumn() +
                            " FROM " + spec.getTable() + where;

                    if (log.isDebugEnabled()) log.debug("SQL: {} | Params: {}", sql, params);

                    // FIX: Reverted to standard queryForList.
                    // The 'protected access' error happened because we tried to access internal Spring methods.
                    // Since we validate rows.size() > 1 below, fetching all rows (likely just 1) is safe.
                    List<Map<String, Object>> rows = jdbc.queryForList(sql, params);

                    if (rows.isEmpty()) {
                        yield null;
                    }
                    if (rows.size() > 1) {
                        throw new ValidationException("DB query returned multiple rows (" + rows.size() + ") for spec " +
                                spec.getSpecId());
                    }
                    // Extract value from the first row map
                    Object value = rows.get(0).get(spec.getColumn());
                    yield value == null ? "" : value;
                }
            };
        } catch (ValidationException ve) {
            log.error("DB validation error for spec {}: {}", spec.getSpecId(), ve.getMessage());
            throw ve;
        } catch (EmptyResultDataAccessException er) {
            return null;
        } catch (RuntimeException ex) {
            log.error("DB query failed for spec {}: {} SQL: {}", spec.getSpecId(), ex.getMessage(), where);
            throw new ValidationException("DB error: " + ex.getMessage(), ex);
        }
    }
}