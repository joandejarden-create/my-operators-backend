# Legacy vs HPC population comparison v1

Generated: 2026-11-19T23:57:48.327Z
Sample limit: 300

HPC sample count: **300**
Legacy sample count: **300**

## Variance explanations (not forced parity)

- **DENOMINATOR_DEFINITION**: Legacy Census status uses Open/Pipeline on `status`; HPC uses Radar Status Open|Pipeline|Hidden — counts are not forced to parity.
- **COVERAGE_DIFFERENCE**: HPC may exclude unverified / incomplete properties that remain in legacy Census; conversely HPC may include new CREATE hotels absent from legacy.
- **DUPLICATE_REMOVAL**: Multiple legacy rows can map to one HPC id (see DUPLICATE_LEGACY_RECORD disposition) — hotel count can fall without disappearance of a real property.
- **PILOT_OVERLAY**: Secrets Papagayo: legacy recyX1wLsHBJYRPw7 → HPC recFQ0WvGLbfK5Gpa (HE 5.0 CREATE). Not a disappearance.

## HPC status buckets (sample)

```json
{
  "Open": 258,
  "null": 42
}
```