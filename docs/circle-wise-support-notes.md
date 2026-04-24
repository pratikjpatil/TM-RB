# Circle-Wise Report Support Notes

## Static filter decision
This implementation uses the existing `filters` block and supports a metadata-only `"static": true` flag in condition entries (example: `{"op":"=","value":"M","dataType":"STRING","static":true}`).

`FilterSqlBuilder` continues to read `op`, `value`, and `dataType`; the `static` flag is ignored by SQL generation, but preserved for frontend/backend semantics so constants can be distinguished from runtime-injected conditions.

## Backward compatibility confirmation
No runtime behavior is changed for templates that do not declare `alias` and `joins`:

- `JoinSqlBuilder` returns plain `FROM <table>` when `joins` is absent/empty.
- `TemplateProcessorServiceImpl.fetchDynamic` and `DbResolverImpl` both use that same fallback path.
- Existing `DB_*` and `DYNAMIC.DB_LIST` templates without join metadata continue to resolve exactly as before.

## Explicitly not changed
The following existing engine components were intentionally not modified:

- `CalculationService`
- Kafka consumers (`ReportGenerationConsumer`, `SingleModeReportGenerationConsumer`)
- `BatchJobOrchestrator`
- Export pipeline services (HDFS/PDF/XLSX)
- Existing `FilterSqlBuilder` method behavior (`appendFilters`, `validateIdentifier`)

These were left unchanged to preserve execution flow and output contracts.
