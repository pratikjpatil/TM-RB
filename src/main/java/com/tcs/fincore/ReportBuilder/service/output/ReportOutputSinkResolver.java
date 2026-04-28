package com.tcs.fincore.ReportBuilder.service.output;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.EnumMap;
import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

@Component
@RequiredArgsConstructor
public class ReportOutputSinkResolver {

    private final List<ReportOutputSink> sinks;

    public ReportOutputSink resolve(OutputTargetType targetType) {
        OutputTargetType effectiveTarget = targetType == null ? OutputTargetType.HDFS : targetType;
        ReportOutputSink sink = sinkByType().get(effectiveTarget);
        return requireNonNull(sink, "No ReportOutputSink registered for target type " + effectiveTarget);
    }

    private Map<OutputTargetType, ReportOutputSink> sinkByType() {
        Map<OutputTargetType, ReportOutputSink> map = new EnumMap<>(OutputTargetType.class);
        for (ReportOutputSink sink : sinks) {
            map.put(sink.targetType(), sink);
        }
        return map;
    }
}
