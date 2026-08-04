package com.tcs.fincore.ReportBuilder.service.output;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tcs.fincore.ReportBuilder.model.ExportModel;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.sql.Types;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class OracleReportOutputSink implements ReportOutputSink {

    private final NamedParameterJdbcTemplate namedParameterJdbcTemplate;
    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;
    private final ObjectProvider<PlatformTransactionManager> transactionManagerProvider;

    private final Object lock = new Object();
    private final List<RowInsert> buffer = new ArrayList<>();
    private int reportsSinceLastCommit = 0;

    @Value("${report.output.oracle.table-name:RB_REPORT_OUTPUT}")
    private String tableName;

    @Value("${report.output.oracle.batch-size:2000}")
    private int batchSize;

    @Value("${report.output.oracle.commit-interval:20}")
    private int commitInterval;

    @Override
    public OutputTargetType targetType() {
        return OutputTargetType.ORACLE;
    }

    @Override
    public void writeReport(ReportOutputRequest request, ExportModel exportModel) {
        LocalDate storageDate = request.getStorageDate();
        List<ExportModel.Row> rows = exportModel.getRows() == null ? List.of() : exportModel.getRows();

        synchronized (lock) {
            int rowOrder = 0;
            for (ExportModel.Row row : rows) {
                buffer.add(new RowInsert(
                        request.getTemplate().getReportMeta().getReportId(),
                        request.getVariant().getVariantCode(),
                        request.getVaryingParamName(),
                        request.getDimensionValue(),
                        storageDate,
                        row.getRowType(),
                        rowOrder++,
                        serializeCells(row.getCells())));
            }

            reportsSinceLastCommit++;
            if (buffer.size() >= batchSize || reportsSinceLastCommit >= commitInterval) {
                flushInternal();
            }
        }
    }

    @Override
    public void flushPending(ReportOutputRequest request) {
        synchronized (lock) {
            flushInternal();
        }
    }

    private void flushInternal() {
        if (buffer.isEmpty()) {
            return;
        }

        List<RowInsert> rowsToInsert = new ArrayList<>(buffer);
        buffer.clear();
        reportsSinceLastCommit = 0;

        Runnable operation = () -> {
            String sql = "INSERT INTO " + tableName + " ("
                    + "report_id, variant_code, dimension_param, dimension_value, storage_date, row_type, row_order, row_payload"
                    + ") VALUES (:reportId, :variantCode, :dimensionParam, :dimensionValue, :storageDate, :rowType, :rowOrder, :rowPayload)";

            int fromIndex = 0;
            while (fromIndex < rowsToInsert.size()) {
                int toIndex = Math.min(rowsToInsert.size(), fromIndex + batchSize);
                List<RowInsert> chunk = rowsToInsert.subList(fromIndex, toIndex);

                MapSqlParameterSource[] batchParams = chunk.stream()
                        .map(row -> new MapSqlParameterSource()
                                .addValue("reportId", row.reportId())
                                .addValue("variantCode", row.variantCode())
                                .addValue("dimensionParam", row.dimensionParam())
                                .addValue("dimensionValue", row.dimensionValue())
                                .addValue("storageDate", java.sql.Date.valueOf(row.storageDate()), Types.DATE)
                                .addValue("rowType", row.rowType())
                                .addValue("rowOrder", row.rowOrder())
                                .addValue("rowPayload", row.rowPayload()))
                        .toArray(MapSqlParameterSource[]::new);

                namedParameterJdbcTemplate.batchUpdate(sql, batchParams);
                fromIndex = toIndex;
            }
        };

        PlatformTransactionManager txManager = transactionManagerProvider.getIfAvailable();
        if (txManager != null) {
            new TransactionTemplate(txManager).executeWithoutResult(status -> operation.run());
        } else {
            jdbcTemplate.execute("SELECT 1 FROM DUAL");
            operation.run();
        }

        log.debug("Flushed {} row(s) to Oracle output table {}", rowsToInsert.size(), tableName);
    }

    private String serializeCells(List<Object> cells) {
        try {
            return objectMapper.writeValueAsString(cells == null ? List.of() : cells);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Unable to serialize report row cells", e);
        }
    }

    private record RowInsert(String reportId,
                             String variantCode,
                             String dimensionParam,
                             String dimensionValue,
                             LocalDate storageDate,
                             String rowType,
                             int rowOrder,
                             String rowPayload) {
    }
}
