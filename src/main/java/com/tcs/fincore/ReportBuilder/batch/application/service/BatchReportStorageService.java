package com.tcs.fincore.ReportBuilder.batch.application.service;

import com.tcs.fincore.ReportBuilder.exception.ValidationException;
import com.tcs.fincore.ReportBuilder.model.ExportModel;
import com.tcs.fincore.ReportBuilder.model.ReportTemplate;
import com.tcs.fincore.ReportBuilder.model.variant.ReportVariant;
import com.tcs.fincore.ReportBuilder.service.export.ExporterFactory;
import com.tcs.fincore.ReportBuilder.service.export.ReportExporter;
import com.tcs.fincore.ReportBuilder.service.storage.ReportStoragePathService;
import com.tcs.fincore.ReportBuilder.util.FileStorageUtil;
import com.tcs.fincore.ReportBuilder.util.ReportFileNameUtil;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Slf4j
@Service
@RequiredArgsConstructor
public class BatchReportStorageService {

    private static final Set<String> ALLOWED_FORMATS = Set.of("pdf", "xlsx", "txt");

    private final ExporterFactory exporterFactory;
    private final FileSystem hdfsFileSystem;
    private final ReportStoragePathService storagePathService;

    @Value("${report.batch.hdfs-base-path:/reports}")
    private String defaultHdfsBasePath;

    public void saveReports(String basePath,
                            LocalDate storageDate,
                            Map<String, Object> commonParams,
                            String varyingParamName,
                            String dimensionValue,
                            List<String> requestedFormats,
                            ReportTemplate template,
                            ReportVariant variant,
                            ExportModel exportModel) throws Exception {
        String safeDimensionValue = sanitizeDimensionValueForPath(dimensionValue);
        String safeReportId = sanitizeDimensionValueForPath(template.getReportMeta().getReportId());

        Map<String, Object> params = new HashMap<>(commonParams);
        params.put(varyingParamName, dimensionValue);

        String fileName = ReportFileNameUtil.buildFileName(template, variant, params);
        Path directory = storagePathService.ensureDateBasedReportDirectory(
                hdfsFileSystem,
                resolveBasePath(basePath),
                storageDate,
                safeReportId,
                safeDimensionValue);

        List<String> formats = requestedFormats != null ? requestedFormats : List.of("pdf", "xlsx");
        for (String format : formats) {
            String lowered = format.toLowerCase();
            if (!ALLOWED_FORMATS.contains(lowered)) {
                log.warn("Unsupported output format requested: {}. Skipping.", format);
                continue;
            }
            try {
                ReportExporter exporter = exporterFactory.getExporter(lowered);
                byte[] content = exporter.generate(exportModel);
                Path filePath = new Path(directory, fileName + exporter.getFileExtension());
                FileStorageUtil.writeToFile(content, hdfsFileSystem, filePath);
                log.debug("Saved {} report for {}: {}", lowered, safeDimensionValue, filePath);
            } catch (RuntimeException e) {
                log.error("Failed to save {} report for {}: {}", lowered, safeDimensionValue, e.getClass().getSimpleName());
            }
        }
    }

    private String resolveBasePath(String requestedBasePath) {
        return requestedBasePath != null ? requestedBasePath : defaultHdfsBasePath;
    }

    private String sanitizeDimensionValueForPath(String dimensionValue) {
        try {
            return storagePathService.sanitizePathSegment(dimensionValue);
        } catch (ValidationException e) {
            throw new ValidationException("Invalid dimension value for path: " + dimensionValue);
        }
    }
}
