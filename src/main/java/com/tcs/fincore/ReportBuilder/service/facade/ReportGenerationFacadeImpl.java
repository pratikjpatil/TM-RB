package com.tcs.fincore.ReportBuilder.service.facade;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.tcs.fincore.ReportBuilder.exception.ReportProcessingException;
import com.tcs.fincore.ReportBuilder.exception.ValidationException;
import com.tcs.fincore.ReportBuilder.kafka.context.ReportExecutionContext;
import com.tcs.fincore.ReportBuilder.kafka.publisher.ReportEventPublisher;
import com.tcs.fincore.ReportBuilder.model.ExportModel;
import com.tcs.fincore.ReportBuilder.model.ReportTemplate;
import com.tcs.fincore.ReportBuilder.model.ReportViewRequest;
import com.tcs.fincore.ReportBuilder.model.variant.ReportVariant;
import com.tcs.fincore.ReportBuilder.service.export.ExporterFactory;
import com.tcs.fincore.ReportBuilder.service.export.ReportExporter;
import com.tcs.fincore.ReportBuilder.service.metadata.ReportMetadataService;
import com.tcs.fincore.ReportBuilder.service.pipeline.ReportExecutionService;
import com.tcs.fincore.ReportBuilder.service.runtime.ReportRuntimeService;
import com.tcs.fincore.ReportBuilder.service.storage.ReportExecutionDateResolver;
import com.tcs.fincore.ReportBuilder.service.storage.ReportStoragePathService;
import com.tcs.fincore.ReportBuilder.service.facade.dto.ReportFileResponse;
import com.tcs.fincore.ReportBuilder.util.FileStorageUtil;
import com.tcs.fincore.ReportBuilder.util.ReportFileNameUtil;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class ReportGenerationFacadeImpl implements ReportGenerationFacade {

    private final ObjectMapper objectMapper;
    private final ReportMetadataService metadataService;
    private final ReportRuntimeService runtimeService;
    private final ReportExecutionService executionService;
    private final ExporterFactory exporterFactory;
    private final FileSystem hdfsFileSystem;
    private final ReportEventPublisher eventPublisher;
    private final ReportStoragePathService storagePathService;
    private final ReportExecutionDateResolver reportExecutionDateResolver;

    @Value("${report.batch.hdfs-base-path:/reports}")
    private String defaultHdfsBasePath;

    private static final List<String> ALLOWED_FORMATS = List.of("pdf", "xlsx", "txt");
    private static final List<String> DEFAULT_FORMATS  = List.of("pdf", "xlsx", "txt");

    // =========================================================================
    // Synchronous generation — REST only, no lifecycle events
    // =========================================================================

    @Override
    public ReportFileResponse generateReportFile(ReportViewRequest request, String formatType) {
        log.info("Starting synchronous report generation: template={}, variant={}, format={}",
                request.getTemplateId(), request.getVariantCode(), formatType);

        try {
            ReportContext context = prepareReportContext(request);
            ExportModel model = executionService.generateModelOnly(context.template());

            ReportExporter exporter = exporterFactory.getExporter(formatType);
            byte[] content = exporter.generate(model);

            String fileName = ReportFileNameUtil.buildFileName(
                    context.template(), context.variant(), request.getParams());

            log.info("Synchronous report generation completed: {} ({} bytes)",
                    fileName + exporter.getFileExtension(), content.length);

            return new ReportFileResponse(fileName, exporter.getMediaType(), content);

        } catch (ValidationException e) {
            throw e;
        } catch (RuntimeException e) {
            log.error("Synchronous generation failed: jobId={} templateId={} correlationId={} errorType={}",
                    "N/A", request.getTemplateId(), "N/A", e.getClass().getSimpleName(), e);
            throw new ReportProcessingException("Synchronous report generation failed", e);
        }
    }

    // =========================================================================
    // Asynchronous HDFS generation — REST or Kafka triggered, with lifecycle events
    // =========================================================================

    /**
     * Generates a single report and stores all configured formats to HDFS.
     *
     * <p>Runs on the {@code reportExecutor} thread pool so the calling thread (HTTP or Kafka
     * consumer) returns immediately.
     *
     * <p>HDFS path: {@code {basePath}/{date}/{reportId}/{fileName}.{ext}}
     */
    @Override
    @Async("reportExecutor")
    public void generateAndStoreToHdfs(ReportViewRequest request, ReportExecutionContext executionContext) {
        Instant jobStartTime = Instant.now();
        String jobId   = resolveJobId(request, executionContext);
        String templateId   = request.getTemplateId();
        String variantCode  = request.getVariantCode();

        log.info("Starting async single-mode HDFS generation: jobId={} templateId={} variant={}",
                jobId, templateId, variantCode);

        // -- Publish STARTED event (Kafka-triggered jobs only) -----------------
        eventPublisher.publishSingleStarted(executionContext, jobId, templateId, variantCode);

        try {
            // 1. Prepare data
            ReportContext reportCtx = prepareReportContext(request);

            // 2. Execute report pipeline
            ExportModel model = executionService.generateModelOnly(reportCtx.template());

            // 3. Resolve HDFS directory: basePath/date/reportId/
            LocalDate storageDate = reportExecutionDateResolver.resolveStorageDate(request.getParams(), reportCtx.variant());
            String reportId  = reportCtx.template().getReportMeta().getReportId();
            Path   directory = resolveHdfsDirectory(defaultHdfsBasePath, storageDate, reportId);

            // 4. Build file name (without extension — added per-format)
            String fileName = ReportFileNameUtil.buildFileName(
                    reportCtx.template(), reportCtx.variant(), request.getParams());

            // 5. Export and persist all requested formats
            List<String> formats = resolveFormats();
            int successCount = 0;
            int failureCount = 0;

            for (String format : formats) {
                if (!ALLOWED_FORMATS.contains(format.toLowerCase())) {
                    log.warn("Unsupported output format '{}' requested for jobId={}. Skipping.", format, jobId);
                    continue;
                }
                try {
                    ReportExporter exporter = exporterFactory.getExporter(format);
                    byte[] content  = exporter.generate(model);
                    Path   filePath = new Path(directory, fileName + exporter.getFileExtension());

                    FileStorageUtil.writeToFile(content, hdfsFileSystem, filePath);
                    successCount++;
                    log.info("Stored single-mode {} report to HDFS: jobId={} path={}", format, jobId, filePath);

                } catch (Exception e) {
                    failureCount++;
                    log.error("Failed to store export artifact: jobId={} templateId={} correlationId={} format={} errorType={}",
                            jobId, templateId, "N/A", format, e.getClass().getSimpleName(), e);
                }
            }

            // 6. Log summary
            if (failureCount > 0) {
                log.warn("Single-mode HDFS storage completed with errors: {}/{} formats succeeded. jobId={}",
                        successCount, formats.size(), jobId);
            } else {
                log.info("Single-mode HDFS generation completed successfully: {} formats written. jobId={}",
                        successCount, jobId);
            }

            // -- Publish COMPLETED event (Kafka-triggered jobs only) -----------
            eventPublisher.publishSingleCompleted(executionContext, jobId, templateId, variantCode, jobStartTime);

        } catch (Exception e) {
            log.error("Single-mode HDFS generation failed: jobId={} templateId={} correlationId={} errorType={}",
                    jobId, templateId, "N/A", e.getClass().getSimpleName(), e);

            // -- Publish FAILED event (Kafka-triggered jobs only) --------------
            eventPublisher.publishSingleFailed(
                    executionContext, jobId, templateId, variantCode,
                    "Single-mode generation failed", e.getClass().getSimpleName(), jobStartTime);

            // Re-throw so @Async captures it and Kafka consumer can detect failure
            throw new ReportProcessingException("Single-mode HDFS generation failed", e);
        }
    }

    // =========================================================================
    // Internal helpers
    // =========================================================================

    private record ReportContext(ReportTemplate template, ReportVariant variant) {}

    private ReportContext prepareReportContext(ReportViewRequest request) {
        ReportTemplate rawTemplate = metadataService.getTemplate(request.getTemplateId());

        // Deep copy is critical: the runtime service mutates the template (injects filters,
        // replaces placeholders) and the original is cached — never modify the cached instance.
        ReportTemplate runtimeTemplate = deepCopyTemplate(rawTemplate);

        ReportVariant variant = null;
        if (request.getVariantCode() != null && !request.getVariantCode().isBlank()) {
            variant = metadataService.getVariant(request.getTemplateId(), request.getVariantCode());
        }

        runtimeService.prepareRuntimeContext(runtimeTemplate, variant, request.getParams());
        return new ReportContext(runtimeTemplate, variant);
    }

    private ReportTemplate deepCopyTemplate(ReportTemplate original) {
        try {
            byte[] bytes = objectMapper.writeValueAsBytes(original);
            return objectMapper.readValue(bytes, ReportTemplate.class);
        } catch (RuntimeException | JsonProcessingException e) {
            log.error("Deep copy failed for template {}: {}",
                    original.getReportMeta().getReportId(), e.getClass().getSimpleName());
            throw new ReportProcessingException("Failed to prepare report template for execution.", e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Resolves the HDFS output directory for a single-mode job.
     *
     * <p><b>Structure:</b> {@code {basePath}/{date}/{reportId}/}
     *
     * <p>This deliberately omits the dimension-value subfolder used in batch mode.
     * Single-mode generates one report per invocation — the report-ID folder is the
     * natural grouping boundary.
     */
    private Path resolveHdfsDirectory(String basePath, LocalDate date, String reportId)
            throws IOException {
        return storagePathService.ensureDateBasedReportDirectory(
                hdfsFileSystem,
                basePath,
                date,
                reportId,
                null);
    }

    /**
     * Sanitizes a path segment to prevent path traversal and filesystem issues.
     * Mirrors the sanitization logic in {@code BatchJobOrchestrator}.
     */
    private String sanitizePathSegment(String value) {
        return storagePathService.sanitizePathSegment(value);
    }

    /**
     * Returns the output formats for a single-mode job.
     *
     * ReportViewRequest is a single-mode DTO and does not carry output format preferences
     * (that is a BatchJobRequest concern). Single-mode jobs always use the default format set.
     */
    private List<String> resolveFormats() {
        return DEFAULT_FORMATS;
    }

    private String resolveJobId(ReportViewRequest request, ReportExecutionContext context) {
        if (context.isKafkaTriggered()
                && context.getProcessRunId() != null
                && context.getStageId() != null
                && context.getRunId() != null) {
            return String.join("_",
                    context.getProcessRunId(), context.getStageId(), context.getRunId());
        }
        return "single_" + request.getTemplateId() + "_" + System.currentTimeMillis();
    }
}