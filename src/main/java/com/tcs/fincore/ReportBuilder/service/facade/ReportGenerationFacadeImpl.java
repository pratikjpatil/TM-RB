package com.tcs.fincore.ReportBuilder.service.facade;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tcs.fincore.ReportBuilder.exception.ValidationException;
import com.tcs.fincore.ReportBuilder.model.ExportModel;
import com.tcs.fincore.ReportBuilder.model.ReportTemplate;
import com.tcs.fincore.ReportBuilder.model.ReportViewRequest;
import com.tcs.fincore.ReportBuilder.model.variant.ReportVariant;
import com.tcs.fincore.ReportBuilder.service.export.ExporterFactory;
import com.tcs.fincore.ReportBuilder.service.export.ReportExporter;
import com.tcs.fincore.ReportBuilder.service.metadata.ReportMetadataService;
import com.tcs.fincore.ReportBuilder.service.pipeline.ReportExecutionService;
import com.tcs.fincore.ReportBuilder.service.runtime.ReportRuntimeService;
import com.tcs.fincore.ReportBuilder.service.facade.dto.ReportFileResponse;
import com.tcs.fincore.ReportBuilder.util.FileStorageUtil;
import com.tcs.fincore.ReportBuilder.util.ReportFileNameUtil;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.List;


@Service
@RequiredArgsConstructor
@Slf4j
public class ReportGenerationFacadeImpl implements ReportGenerationFacade {

    private final ReportMetadataService metadataService;
    private final ReportRuntimeService runtimeService;
    private final ReportExecutionService executionService;
    private final ExporterFactory exporterFactory;
    private final FileSystem hdfsFileSystem;

    @Override
    public ReportFileResponse generateReportFile(ReportViewRequest request, String formatType) {
        log.info("Starting report generation: template={}, variant={}, format={}",
                request.getTemplateId(), request.getVariantCode(), formatType);

        try {
            // 1. Prepare Data (caching handled in metadata service)
            ReportContext context = prepareReportContext(request);

//            log.info("Report context :: {}", context);

            // 2. Execute Pipeline (Fetch Data -> Calculate -> Create Model)
            ExportModel model = executionService.generateModelOnly(context.template());

            // 3. Export to specific format using Strategy Pattern
            ReportExporter exporter = exporterFactory.getExporter(formatType);
            byte[] content = exporter.generate(model);

            // 4. Build Response
            String fileName = ReportFileNameUtil.buildFileName(
                    context.template(), context.variant(), request.getParams());

            log.info("Report generation completed: {} ({} bytes)",
                    fileName + exporter.getFileExtension(), content.length);

            return new ReportFileResponse(
                    fileName,
                    exporter.getMediaType(),
                    content);

        } catch (Exception e) {
            log.error("Report generation failed for template {}: {}",
                    request.getTemplateId(), e.getMessage(), e);
            throw new ValidationException("Report generation failed: " + e.getMessage(), e);
        }
    }

    /**
     * Made async to prevent blocking HTTP threads
     * Returns immediately with job ID, actual generation happens in background
     */
    @Override
    @Async("reportExecutor")
    public void generateAndStoreToHdfs(ReportViewRequest request) {
        log.info("Starting async HDFS report generation: template={}, variant={}",
                request.getTemplateId(), request.getVariantCode());

        try {
            // 1. Prepare Data
            ReportContext context = prepareReportContext(request);

            // 2. Execute Pipeline
            ExportModel model = executionService.generateModelOnly(context.template());

            // 3. Prepare HDFS Path
            String fileName = ReportFileNameUtil.buildFileName(
                    context.template(), context.variant(), request.getParams());
            Path directory = createHdfsDirectory("/reports", LocalDate.now());

            // 4. Generate and Store ALL formats with error tracking
            List<String> formats = List.of("pdf", "xlsx", "txt");
            int successCount = 0;
            int failureCount = 0;

            for (String format : formats) {
                try {
                    ReportExporter exporter = exporterFactory.getExporter(format);
                    byte[] content = exporter.generate(model);
                    Path filePath = new Path(directory, fileName);

                    // Thread-safe write with retry
                    FileStorageUtil.writeToFile(content, hdfsFileSystem, filePath);
                    successCount++;

                    log.info("Stored {} format to HDFS: {}", format, filePath);

                } catch (Exception e) {
                    failureCount++;
                    log.error("Failed to store {} format: {}", format, e.getMessage());
                }
            }

            if (failureCount > 0) {
                log.warn("HDFS storage completed with errors: {}/{} formats succeeded",
                        successCount, formats.size());
            } else {
                log.info("Successfully stored all {} report formats to HDFS for template: {}",
                        formats.size(), request.getTemplateId());
            }

        } catch (Exception e) {
            log.error("HDFS report generation failed for template {}: {}",
                    request.getTemplateId(), e.getMessage(), e);
            throw new ValidationException("HDFS report generation failed: " + e.getMessage(), e);
        }
    }

    // --- Helper Methods ---

    private record ReportContext(ReportTemplate template, ReportVariant variant) {}


    private ReportContext prepareReportContext(ReportViewRequest request) {
        // Load Template
        ReportTemplate rawTemplate = metadataService.getTemplate(request.getTemplateId());

        // CRITICAL: Deep Copy Template
        // Since runtime service modifies the template (injects filters, replaces text),
        // we must not modify the cached instance.
        ReportTemplate runtimeTemplate = deepCopyTemplate(rawTemplate);

        // Load Variant
        ReportVariant variant = null;
        if (request.getVariantCode() != null && !request.getVariantCode().isBlank()) {
            variant = metadataService.getVariant(request.getTemplateId(), request.getVariantCode());
        }

        // NEW: Centralized Context Preparation
        runtimeService.prepareRuntimeContext(runtimeTemplate, variant, request.getParams());

        return new ReportContext(runtimeTemplate, variant);
    }

    private ReportTemplate deepCopyTemplate(ReportTemplate original) {
        try {
            // Using Jackson ObjectMapper (assumes ObjectMapper is injected or available)
            ObjectMapper mapper = new ObjectMapper();
            return mapper.readValue(mapper.writeValueAsString(original), ReportTemplate.class);
        } catch (Exception e) {
            throw new RuntimeException("Deep copy failed", e);
        }
    }

    private Path createHdfsDirectory(String basePath, LocalDate date) throws IOException {
        String formattedDate = date.format(DateTimeFormatter.ISO_LOCAL_DATE);
        Path path = new Path(basePath, formattedDate);

        if (!hdfsFileSystem.exists(path)) {
            boolean created = hdfsFileSystem.mkdirs(path);
            if (!created) {
                throw new IOException("Failed to create HDFS directory: " + path);
            }
            log.debug("Created HDFS directory: {}", path);
        }

        return path;
    }
}