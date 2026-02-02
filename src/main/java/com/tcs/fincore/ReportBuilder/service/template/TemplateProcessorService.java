package com.tcs.fincore.ReportBuilder.service.template;

import com.tcs.fincore.ReportBuilder.model.ExportModel;
import com.tcs.fincore.ReportBuilder.model.ReportTemplate;
import com.tcs.fincore.ReportBuilder.service.aggregation.DataSourceSpec;

import java.util.List;
import java.util.Map;

public interface TemplateProcessorService {

    void validate(ReportTemplate template);

    /**
     * Assigns deterministic cell ids: cell_R__{rowId}_C__{colId}.
     */
    void assignIds(ReportTemplate template);

    /**
     * Extract all DB specs (direct cell DB_* + inline DB_* in formula variables).
     */
    List<DataSourceSpec> extractDbSpecs(ReportTemplate template);

    /**
     * expand dynamic rows if any (no-op unless dynamicConfig present).
     */
    void expandDynamicRows(ReportTemplate template);

    /**
     * Convert template + computed cell values to ExportModel.
     */
    ExportModel toExportModel(ReportTemplate template, Map<String, Object> computedValues);
}