# Setup vs Operator Intelligence Crosswalk

| Setup Field | Setup Coverage (Prod %) | Normalized Source | Normalized Coverage (Prod %) | Derivable? | Recommended Treatment |
| ----------- | ----------------------: | ----------------- | ---------------------------: | ---------- | --------------------- |
| Platform.Active Countries | 48.1 | Market Presence (Current Operating/Managed) + Assignments.Country | 97.2 | Yes | DERIVE — never manually SoT |
| Profile.Brand Families Operated / brands | 83.4 | Operator Intelligence - Brand Relationships + Assignments.Brand | 100 | Yes | DERIVE |
| Platform.conversionExperience / Commercial.Conversion / Reflag | 1.9 | Assignments.Development Context | 100 | Yes | DERIVE |
| Profile.locationTypeResort / Platform.locationTypeUrban | 7.9 | Assignments.Urban / Resort | 97.2 | Yes | DERIVE |
| Master.Operating Model / Management Availability | 0 | Master DIRECT (+ Assignments structure as evidence) | 77.8 | No | DIRECT backfill |
| Setup Brand Relationships (section rows) | n/a | Operator Intelligence - Brand Relationships (authoritative structured) | 86.1 | No | Keep Setup BR as Explorer narrative only; intel BR is SoT |
| Case Studies | n/a | Assignments (+ optional story Claims) | 100 | No | LEGACY — Assignments preferenced for evidence |

**Rule:** Do not manually maintain Setup geography/brand/experience lists when Assignments / Presence / Brand Relationships already evidence them.
