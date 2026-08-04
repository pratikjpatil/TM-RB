package com.tcs.fincore.ReportBuilder.service.output;

import com.tcs.fincore.ReportBuilder.model.ReportTemplate;
import com.tcs.fincore.ReportBuilder.model.variant.ReportVariant;
import lombok.Builder;
import lombok.Value;

import java.time.LocalDate;
import java.util.List;
import java.util.Map;

@Value
@Builder
public class ReportOutputRequest {
    String hdfsBasePath;
    LocalDate storageDate;
    Map<String, Object> commonParams;
    String varyingParamName;
    String dimensionValue;
    List<String> requestedFormats;
    ReportTemplate template;
    ReportVariant variant;
    String outputTargetConfigKey;
}
