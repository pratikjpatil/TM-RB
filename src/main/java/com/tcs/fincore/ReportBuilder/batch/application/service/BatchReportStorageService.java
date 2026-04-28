package com.tcs.fincore.ReportBuilder.batch.application.service;

import com.tcs.fincore.ReportBuilder.model.ExportModel;
import com.tcs.fincore.ReportBuilder.model.ReportTemplate;
import com.tcs.fincore.ReportBuilder.model.variant.ReportVariant;
import com.tcs.fincore.ReportBuilder.service.output.HdfsReportOutputSink;
import com.tcs.fincore.ReportBuilder.service.output.ReportOutputRequest;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.util.List;
import java.util.Map;

@Service
@RequiredArgsConstructor
public class BatchReportStorageService {

    private final HdfsReportOutputSink hdfsReportOutputSink;

    public void saveReports(String basePath,
                            LocalDate storageDate,
                            Map<String, Object> commonParams,
                            String varyingParamName,
                            String dimensionValue,
                            List<String> requestedFormats,
                            ReportTemplate template,
                            ReportVariant variant,
                            ExportModel exportModel) throws Exception {
        ReportOutputRequest outputRequest = ReportOutputRequest.builder()
                .hdfsBasePath(basePath)
                .storageDate(storageDate)
                .commonParams(commonParams)
                .varyingParamName(varyingParamName)
                .dimensionValue(dimensionValue)
                .requestedFormats(requestedFormats)
                .template(template)
                .variant(variant)
                .build();

        hdfsReportOutputSink.writeReport(outputRequest, exportModel);
    }
}
