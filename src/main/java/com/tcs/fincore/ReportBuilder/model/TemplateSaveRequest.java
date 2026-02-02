package com.tcs.fincore.ReportBuilder.model;

import com.tcs.fincore.ReportBuilder.model.variant.ReportVariant;
import lombok.Data;

import java.util.List;

/**
 * Request used by "maker" to save a template + its variants.
 */
@Data
public class TemplateSaveRequest {
    private ReportTemplate template;         // existing full template
    private List<ReportVariant> variants;    // variant + params + rules
}