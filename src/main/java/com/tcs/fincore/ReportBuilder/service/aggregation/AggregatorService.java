package com.tcs.fincore.ReportBuilder.service.aggregation;

import java.util.List;
import java.util.Map;

public interface AggregatorService {
    /**
     * Fetch aggregates for given specs. Returns map specId -> value.
     * de-duplicate DB queries for identical logical specs.
     */
    Map<String, Object> fetchAggregates(List<DataSourceSpec> specs) throws InterruptedException;
}