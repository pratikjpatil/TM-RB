package com.tcs.fincore.ReportBuilder.service.db;

import com.tcs.fincore.ReportBuilder.service.aggregation.DataSourceSpec;

import java.util.List;
import java.util.Map;

public interface DbResolver {
    /**
     * Resolve a spec and return the query result.
     *
     * @param spec the data source spec (type, table, column, filters etc.)
     * @return result object (Number/String/List/whatever the spec type implies)
     */
    Object resolve(DataSourceSpec spec);
}