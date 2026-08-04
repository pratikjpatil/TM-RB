package com.tcs.fincore.ReportBuilder.service.output;

import com.tcs.fincore.ReportBuilder.model.ExportModel;

public interface ReportOutputSink {

    OutputTargetType targetType();

    void writeReport(ReportOutputRequest request, ExportModel exportModel) throws Exception;

    default void flushPending(ReportOutputRequest request) {
        // default no-op
    }
}
