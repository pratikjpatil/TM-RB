package com.tcs.fincore.ReportBuilder.service.db;

import com.tcs.fincore.ReportBuilder.exception.ValidationException;
import com.tcs.fincore.ReportBuilder.service.aggregation.DataSourceSpec;
import com.tcs.fincore.ReportBuilder.util.FilterSqlBuilder;
import com.tcs.fincore.ReportBuilder.util.JoinSqlBuilder;
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
        FilterSqlBuilder.validateColumnReference(spec.getColumn());

        Map<String, Object> params = new HashMap<>();
        StringBuilder where = new StringBuilder(" WHERE 1=1 ");
        FilterSqlBuilder.appendFilters(where, params, spec.getFilters());

        String fromClause = JoinSqlBuilder.buildFromClause(spec.getTable(), spec.getAlias(), spec.getJoins());

        try {
            if (log.isDebugEnabled()) {
                log.debug("Resolve Spec: [{}] Table: [{}] Alias: [{}] Column: [{}] Filters: {}",
                        spec.getSpecId(), spec.getTable(), spec.getAlias(), spec.getColumn(), params);
            }

            return switch (spec.getType()) {
                case DB_SUM -> {
                    String sql = "SELECT COALESCE(SUM(" + spec.getColumn() + "),0)" + fromClause + where;
                    Number res = jdbc.queryForObject(sql, params, Number.class);
                    yield res == null ? 0.0 : res.doubleValue();
                }
                case DB_COUNT -> {
                    String sql = "SELECT COUNT(" + spec.getColumn() + ")" + fromClause + where;
                    Number res = jdbc.queryForObject(sql, params, Number.class);
                    yield res == null ? 0L : res.longValue();
                }
                case DB_AVG -> {
                    String sql = "SELECT COALESCE(AVG(" + spec.getColumn() + "),0)" + fromClause + where;
                    Number res = jdbc.queryForObject(sql, params, Number.class);
                    yield res == null ? 0.0 : res.doubleValue();
                }
                case DB_MIN -> {
                    String sql = "SELECT MIN(" + spec.getColumn() + ")" + fromClause + where;
                    yield jdbc.queryForObject(sql, params, Object.class);
                }
                case DB_MAX -> {
                    String sql = "SELECT MAX(" + spec.getColumn() + ")" + fromClause + where;
                    yield jdbc.queryForObject(sql, params, Object.class);
                }
                case DB_TEXT, DB_DATE, DB_VALUE -> {
                    String sql = "SELECT " + spec.getColumn() + fromClause + where;
                    List<Map<String, Object>> rows = jdbc.queryForList(sql, params);

                    if (rows.isEmpty()) {
                        yield null;
                    }
                    if (rows.size() > 1) {
                        throw new ValidationException("DB query returned multiple rows (" + rows.size() + ") for spec " +
                                spec.getSpecId());
                    }

                    Object value = rows.get(0).values().stream().findFirst().orElse(null);
                    yield value == null ? "" : value;
                }
                case QUERY_LIST -> {
                    String sql = "SELECT " + spec.getColumn() + fromClause + where;
                    List<Object> values = jdbc.query(sql, params, (rs, rowNum) -> rs.getObject(1));
                    yield values;
                }
            };
        } catch (ValidationException ve) {
            log.error("DB validation error for spec {}: {}", spec.getSpecId(), ve.getMessage());
            throw ve;
        } catch (EmptyResultDataAccessException er) {
            return null;
        } catch (RuntimeException ex) {
            log.error("DB query failed for spec {}: {} SQL WHERE: {}", spec.getSpecId(), ex.getMessage(), where);
            throw new ValidationException("DB error: " + ex.getMessage(), ex);
        }
    }
}
