package com.tcs.fincore.ReportBuilder.service.pipeline;

import com.tcs.fincore.ReportBuilder.exception.ReportProcessingException;
import com.tcs.fincore.ReportBuilder.exception.ValidationException;
import com.tcs.fincore.ReportBuilder.model.ExportModel;
import com.tcs.fincore.ReportBuilder.model.ReportTemplate;
import com.tcs.fincore.ReportBuilder.service.aggregation.AggregatorService;
import com.tcs.fincore.ReportBuilder.service.aggregation.DataSourceSpec;
import com.tcs.fincore.ReportBuilder.service.calculation.CalculationService;
import com.tcs.fincore.ReportBuilder.service.template.TemplateProcessorService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Slf4j
@Service
@RequiredArgsConstructor
public class ReportExecutionService {

    private final TemplateProcessorService templateProcessor;
    private final AggregatorService aggregatorService;
    private final CalculationService calculationService;

    /**
     * Executes the report pipeline and returns the format-agnostic ExportModel.
     */
    public ExportModel generateModelOnly(ReportTemplate template) {
        try {
            log.info("Starting pipeline execution for report: {}", template.getReportMeta().getReportId());

            // 1. Validate
            templateProcessor.validate(template);

            // 2. Expand Dynamic Rows if any (DB_LIST)
            templateProcessor.expandDynamicRows(template);

            // 3. Assign Cell IDs
            templateProcessor.assignIds(template);

            // 4. Extract Data Specs
            List<DataSourceSpec> specs = templateProcessor.extractDbSpecs(template);

            // 5. Aggregate Data (DB Fetch)
            Map<String, Object> dbResults = aggregatorService.fetchAggregates(specs);

            // 6. Calculate Formulas
            Map<String, Object> computed = calculationService.calculate(template, dbResults);

            // 7. Convert to Export Model
            return templateProcessor.toExportModel(template, computed);

        } catch (ValidationException ex) {
            throw ex;
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new ReportProcessingException("Report generation pipeline interrupted", ex);
        } catch (RuntimeException ex) {
            String templateId = template != null && template.getReportMeta() != null ? template.getReportMeta().getReportId() : "N/A";
            log.error("Pipeline execution failed: jobId={} templateId={} correlationId={} errorType={}",
                    "N/A", templateId, "N/A", ex.getClass().getSimpleName(), ex);
            throw new ReportProcessingException("Report generation pipeline failed", ex);
        }
    }
}
