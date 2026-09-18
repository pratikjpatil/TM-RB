package com.tcs.fincore.ReportBuilder.service.metadata;

import com.tcs.fincore.ReportBuilder.model.ReportTemplate;
import com.tcs.fincore.ReportBuilder.model.variant.ReportVariant;
import com.tcs.fincore.ReportBuilder.repository.ReportTemplateRepository;
import com.tcs.fincore.ReportBuilder.repository.ReportVariantRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cache.annotation.CachePut;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@RequiredArgsConstructor
@Slf4j
public class ReportMetadataServiceImpl implements ReportMetadataService {

    private final ReportTemplateRepository templateRepository;
    private final ReportVariantRepository variantRepository;

    @Override
    @Transactional(readOnly = true)
    @Cacheable(value = "reportTemplates", key = "#templateId", unless = "#result == null")
    public ReportTemplate getTemplate(String templateId) {
        log.debug("CACHE MISS - Loading template from DB: {}", templateId);
        ReportTemplate template = templateRepository.loadActiveTemplate(templateId);
        if (template != null && template.getReportMeta() != null) {
            log.debug("Loaded template: {} with {} columns",
                    template.getReportMeta().getReportName(),
                    template.getReportData().getColumns().size());
        }
        return template;
    }

    @Override
    @Transactional(readOnly = true)
    @CachePut(value = "reportTemplates", key = "#result.templateMeta.templateId", unless = "#result == null")
    public ReportTemplate getTemplateByReportId(String reportId) {
        log.debug("Loading template from DB using reportId: {} and caching by templateId", reportId);
        // A direct DB fetch by reportId will now hydrate the unified templateId cache
        return templateRepository.loadActiveTemplateByReportId(reportId);
    }

    @Override
    @Transactional(readOnly = true)
    @Cacheable(value = "reportVariants", key = "#templateId + ':' + #variantCode", unless = "#result == null")
    public ReportVariant getVariant(String templateId, String variantCode) {
        return variantRepository.loadActiveVariant(templateId, variantCode);
    }
}