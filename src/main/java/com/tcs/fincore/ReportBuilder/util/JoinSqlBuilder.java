package com.tcs.fincore.ReportBuilder.util;

import com.tcs.fincore.ReportBuilder.exception.ValidationException;
import com.tcs.fincore.ReportBuilder.service.aggregation.DataSourceSpec;

import java.util.List;
import java.util.Locale;

public final class JoinSqlBuilder {

    private JoinSqlBuilder() {
    }

    public static String buildFromClause(String table, String alias, List<DataSourceSpec.JoinSpec> joins) {
        FilterSqlBuilder.validateIdentifier(table);

        boolean hasJoins = joins != null && !joins.isEmpty();
        if (hasJoins && (alias == null || alias.isBlank())) {
            throw new ValidationException("Primary table alias is required when joins are present");
        }

        StringBuilder from = new StringBuilder(" FROM ").append(table);
        if (alias != null && !alias.isBlank()) {
            FilterSqlBuilder.validateIdentifier(alias);
            from.append(' ').append(alias);
        }

        if (!hasJoins) {
            return from.toString();
        }

        for (DataSourceSpec.JoinSpec join : joins) {
            if (join == null) continue;

            DataSourceSpec.JoinSpec.JoinType type = join.getType();
            if (type == null) {
                throw new ValidationException("Join type is required");
            }
            if (!(type == DataSourceSpec.JoinSpec.JoinType.INNER
                    || type == DataSourceSpec.JoinSpec.JoinType.LEFT
                    || type == DataSourceSpec.JoinSpec.JoinType.RIGHT)) {
                throw new ValidationException("Unsupported join type: " + type);
            }

            FilterSqlBuilder.validateIdentifier(join.getTable());
            if (join.getAlias() == null || join.getAlias().isBlank()) {
                throw new ValidationException("Join alias is required for table: " + join.getTable());
            }
            FilterSqlBuilder.validateIdentifier(join.getAlias());

            DataSourceSpec.JoinOnSpec on = join.getOn();
            if (on == null) {
                throw new ValidationException("Join ON clause is required for table: " + join.getTable());
            }

            FilterSqlBuilder.validateIdentifier(on.getLeftAlias());
            FilterSqlBuilder.validateIdentifier(on.getLeftColumn());
            FilterSqlBuilder.validateIdentifier(on.getRightAlias());
            FilterSqlBuilder.validateIdentifier(on.getRightColumn());

            from.append(' ')
                    .append(type.name().toUpperCase(Locale.ROOT))
                    .append(" JOIN ")
                    .append(join.getTable())
                    .append(' ')
                    .append(join.getAlias())
                    .append(" ON ")
                    .append(on.getLeftAlias()).append('.').append(on.getLeftColumn())
                    .append(" = ")
                    .append(on.getRightAlias()).append('.').append(on.getRightColumn());
        }

        return from.toString();
    }
}
