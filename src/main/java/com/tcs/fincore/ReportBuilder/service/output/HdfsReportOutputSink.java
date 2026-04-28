package com.tcs.fincore.ReportBuilder.service.output;

import com.tcs.fincore.ReportBuilder.exception.ValidationException;
import com.tcs.fincore.ReportBuilder.model.ExportModel;
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
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Slf4j
@Component
@RequiredArgsConstructor
public class HdfsReportOutputSink implements ReportOutputSink {

    private static final Set<String> ALLOWED_FORMATS = Set.of("pdf", "xlsx", "txt");

    private final ExporterFactory exporterFactory;
    private final FileSystem hdfsFileSystem;
    private final ReportStoragePathService storagePathService;

    @Value("${report.batch.hdfs-base-path:/reports}")
    private String defaultHdfsBasePath;

    @Override
    public OutputTargetType targetType() {
        return OutputTargetType.HDFS;
    }

    @Override
    public void writeReport(ReportOutputRequest request, ExportModel exportModel) throws Exception {
        String safeDimensionValue = sanitizePathSegment(request.getDimensionValue());
        String safeReportId = sanitizePathSegment(request.getTemplate().getReportMeta().getReportId());

        Map<String, Object> params = new HashMap<>(request.getCommonParams());
        params.put(request.getVaryingParamName(), request.getDimensionValue());

        String fileName = ReportFileNameUtil.buildFileName(request.getTemplate(), request.getVariant(), params);
        Path directory = storagePathService.ensureDateBasedReportDirectory(
                hdfsFileSystem,
                resolveBasePath(request.getHdfsBasePath()),
                request.getStorageDate(),
                safeReportId,
                safeDimensionValue);

        List<String> formats = request.getRequestedFormats() != null ? request.getRequestedFormats() : List.of("pdf", "xlsx");
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

    private String sanitizePathSegment(String value) {
        try {
            return storagePathService.sanitizePathSegment(value);
        } catch (ValidationException e) {
            throw new ValidationException("Invalid dimension value for path: " + value);
        }
    }
}
