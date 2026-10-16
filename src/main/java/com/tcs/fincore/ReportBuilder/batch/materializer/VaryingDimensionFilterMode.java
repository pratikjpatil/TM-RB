package com.tcs.fincore.ReportBuilder.batch.materializer;

/** Strategy used when a batch varying-dimension IN list is too large for one query. */
public enum VaryingDimensionFilterMode {
    /** Execute one query for each bounded subset of the varying-dimension values. */
    CHUNK,
    /** Omit only the varying-dimension predicate and materialize all matching dimensions. */
    OMIT
}