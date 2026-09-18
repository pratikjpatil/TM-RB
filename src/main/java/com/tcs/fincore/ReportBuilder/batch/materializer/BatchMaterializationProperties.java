package com.tcs.fincore.ReportBuilder.batch.materializer;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/** Externalized controls for protecting Oracle from oversized bind sets. */
@Data
@Component
@ConfigurationProperties(prefix = "report.batch.materialization.varying-filter")
public class BatchMaterializationProperties {

    private VaryingDimensionFilterMode mode = VaryingDimensionFilterMode.CHUNK;
    private int threshold = 10_000;
    private int chunkSize = 5_000;
    private boolean queryLoggingEnabled;
    private boolean queryLoggingIncludeParameterValues;
    private int queryLoggingParameterSampleSize = 10;
}
