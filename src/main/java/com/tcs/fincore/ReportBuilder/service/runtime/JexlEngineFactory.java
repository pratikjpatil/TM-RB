package com.tcs.fincore.ReportBuilder.service.runtime;

import org.apache.commons.jexl3.JexlArithmetic;
import org.apache.commons.jexl3.JexlBuilder;
import org.apache.commons.jexl3.JexlEngine;
import org.apache.commons.jexl3.introspection.JexlPermissions;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * Centralized JEXL engine factory to keep expression configuration DRY and secure.
 */
@Component
public class JexlEngineFactory {

    private static final JexlPermissions SAFE_PERMISSIONS = JexlPermissions.parse(
            "com.tcs.fincore.ReportBuilder.util.DateUtilsHelper",
            "java.lang.Math",
            "java.lang.String",
            "java.lang.Number",
            "java.time.LocalDate",
            "java.time.LocalDateTime"
    );

    public JexlEngine createSecureEngine() {
        return new JexlBuilder()
                .cache(512)
                .strict(true)
                .silent(false)
                .permissions(SAFE_PERMISSIONS)
                .create();
    }

    public JexlEngine createCalculationEngine(JexlArithmetic arithmetic) {
        return new JexlBuilder()
                .cache(512)
                .silent(false)
                .strict(true)
                .arithmetic(arithmetic)
                .namespaces(Map.of("math", Math.class))
                .create();
    }
}
