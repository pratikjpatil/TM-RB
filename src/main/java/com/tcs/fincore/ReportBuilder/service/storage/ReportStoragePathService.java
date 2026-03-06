package com.tcs.fincore.ReportBuilder.service.storage;

import com.tcs.fincore.ReportBuilder.exception.ValidationException;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

@Slf4j
@Service
public class ReportStoragePathService {

    public Path ensureDateBasedReportDirectory(FileSystem fileSystem,
                                               String basePath,
                                               LocalDate date,
                                               String reportId,
                                               String subDirectory) throws IOException {
        String dateStr = date.format(DateTimeFormatter.ISO_LOCAL_DATE);
        String safeReportId = sanitizePathSegment(reportId);

        String resolvedSubDir = (subDirectory == null || subDirectory.isBlank())
                ? ""
                : "/" + sanitizePathSegment(subDirectory);

        Path path = new Path(basePath, dateStr + "/" + safeReportId + resolvedSubDir);
        if (!fileSystem.exists(path)) {
            boolean created = fileSystem.mkdirs(path);
            if (!created) {
                throw new IOException("Failed to create HDFS directory: " + path);
            }
            log.debug("Created HDFS directory: {}", path);
        }
        return path;
    }

    public String sanitizePathSegment(String value) {
        if (value == null || value.isBlank()) {
            throw new ValidationException("Path segment cannot be null or blank");
        }
        String sanitized = value.replaceAll("[^A-Za-z0-9_\\-]", "_");
        if (sanitized.contains("..") || sanitized.startsWith("/") || sanitized.startsWith("\\")) {
            throw new ValidationException("Invalid path segment: " + value);
        }
        return sanitized.length() > 64 ? sanitized.substring(0, 64) : sanitized;
    }
}
