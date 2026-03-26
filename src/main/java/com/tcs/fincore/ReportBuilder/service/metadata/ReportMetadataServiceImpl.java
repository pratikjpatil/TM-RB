package com.tcs.fincore.ReportBuilder.service.metadata;

import com.tcs.fincore.ReportBuilder.model.ReportTemplate;
import com.tcs.fincore.ReportBuilder.model.variant.ReportVariant;
import com.tcs.fincore.ReportBuilder.model.variant.VariantParamDef;
import com.tcs.fincore.ReportBuilder.repository.ReportTemplateRepository;
import com.tcs.fincore.ReportBuilder.repository.ReportVariantRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.List;

/**
 * Added caching annotations for template and variant retrieval
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class ReportMetadataServiceImpl implements ReportMetadataService {

    private final ReportTemplateRepository templateRepository;
    private final ReportVariantRepository variantRepository;

    /**
     * Added Redis caching with 5-minute TTL
     * Cache key: templateId
     * Cache name: reportTemplates (configured in CacheConfig)
     */
    @Override
    @Transactional(readOnly = true)
    @Cacheable(value = "reportTemplates", key = "#templateId", unless = "#result == null")
    public ReportTemplate getTemplate(String templateId) {
        log.info("CACHE MISS - Loading template from DB: {}", templateId);
        ReportTemplate template = templateRepository.loadActiveTemplate(templateId);
        log.debug("Loaded template: {} with {} columns",
                template.getReportMeta().getReportName(),
                template.getReportData().getColumns().size());
        return template;
    }

    @Override
    @Transactional(readOnly = true)
    @Cacheable(value = "reportTemplates", key = "'reportId:' + #reportId", unless = "#result == null")
    public ReportTemplate getTemplateByReportId(String reportId) {
        log.info("CACHE MISS - Loading template from DB using reportId: {}", reportId);
        return templateRepository.loadActiveTemplateByReportId(reportId);
    }

    @Override
    @Transactional(readOnly = true)
    @Cacheable(value = "reportTemplates", key = "'templateIdByReportId:' + #reportId", unless = "#result == null")
    public String findTemplateIdByReportId(String reportId) {
        return templateRepository.findActiveTemplateIdByReportId(reportId);
    }

    /**
     * Added Redis caching with 5-minute TTL
     * Cache key: templateId:variantCode
     * Cache name: reportVariants (configured in CacheConfig)
     */
  /*  @Override
    @Transactional(readOnly = true)
    @Cacheable(value = "reportVariants",
            key = "#templateId + ':' + #variantCode",
            unless = "#result == null")
    public ReportVariant getVariant(String templateId, String variantCode) {
        log.info(" CACHE MISS - Loading variant from DB: {}:{}", templateId, variantCode);
        ReportVariant variant = variantRepository.loadActiveVariant(templateId, variantCode);
        log.debug("Loaded variant: {} with {} params, {} rules",
                variant.getVariantName(),
                variant.getParams() != null ? variant.getParams().size() : 0,
                variant.getFilterRules() != null ? variant.getFilterRules().size() : 0);
        return variant;
    }*/

    @Override
    @Transactional(readOnly = true)
    @Cacheable(value = "reportVariants", key = "#templateId + ':' + #variantCode", unless = "#result == null")
    public ReportVariant getVariant(String templateId, String variantCode) {
//        ReportTemplate template = getTemplate(templateId); // Cached lookup

        return variantRepository.loadActiveVariant(templateId, variantCode);
    }
}
